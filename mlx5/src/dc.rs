//! DC (Dynamically Connected) transport.
//!
//! DC provides scalable connectionless RDMA operations using:
//! - DCI (DC Initiator): Sends RDMA operations to DCTs
//! - DCT (DC Target): Receives RDMA operations via SRQ

use std::{io, mem::MaybeUninit, ptr::NonNull};

use crate::cq::CompletionQueue;
use crate::device::Context;
use crate::pd::ProtectionDomain;
use crate::qp::QpInfo;
use crate::srq::SharedReceiveQueue;
use crate::wqe::{
    AddressVector, CtrlSeg, DataSeg, InlineHeader, OrderedSendQueue, RdmaSeg, WQEBB_SIZE, WqeFlags,
    WqeHandle, WqeOpcode, WqeTable, calc_wqebb_cnt,
};

/// DCI configuration.
#[derive(Debug, Clone)]
pub struct DciConfig {
    /// Maximum number of outstanding send WRs.
    pub max_send_wr: u32,
    /// Maximum number of SGEs per send WR.
    pub max_send_sge: u32,
    /// Maximum inline data size.
    pub max_inline_data: u32,
}

impl Default for DciConfig {
    fn default() -> Self {
        Self {
            max_send_wr: 256,
            max_send_sge: 4,
            max_inline_data: 64,
        }
    }
}

/// DCT configuration.
#[derive(Debug, Clone)]
pub struct DctConfig {
    /// DC key for this DCT.
    pub dc_key: u64,
}

impl Default for DctConfig {
    fn default() -> Self {
        Self { dc_key: 0 }
    }
}

/// DC QP state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DcQpState {
    Reset,
    Init,
    Rtr,
    Rts,
    Error,
}

/// Remote DCT information for sending.
#[derive(Debug, Clone, Copy)]
pub struct RemoteDctInfo {
    /// DCT number.
    pub dctn: u32,
    /// DC key.
    pub dc_key: u64,
    /// Remote LID.
    pub lid: u16,
}

// =============================================================================
// DCI Send Queue State
// =============================================================================

/// Send Queue state for DCI.
struct DciSendQueueState<T> {
    buf: *mut u8,
    wqe_cnt: u16,
    sqn: u32,
    pi: u16,
    ci: u16,
    last_wqe: Option<(*mut u8, usize)>,
    dbrec: *mut u32,
    bf_reg: *mut u8,
    bf_size: u32,
    bf_offset: u32,
    table: WqeTable<T>,
}

impl<T> DciSendQueueState<T> {
    fn available(&self) -> u16 {
        self.wqe_cnt - self.pi.wrapping_sub(self.ci)
    }

    fn get_wqe_ptr(&self, idx: u16) -> *mut u8 {
        let offset = ((idx & (self.wqe_cnt - 1)) as usize) * WQEBB_SIZE;
        unsafe { self.buf.add(offset) }
    }

    fn advance_pi(&mut self, count: u16) {
        self.pi = self.pi.wrapping_add(count);
    }

    fn set_last_wqe(&mut self, ptr: *mut u8, size: usize) {
        self.last_wqe = Some((ptr, size));
    }

    fn ring_doorbell(&mut self) {
        let Some((last_wqe_ptr, last_wqe_size)) = self.last_wqe.take() else {
            return;
        };

        mmio_flush_writes!();

        unsafe {
            std::ptr::write_volatile(self.dbrec.add(1), (self.pi as u32).to_be());
        }

        udma_to_device_barrier!();

        if self.bf_size > 0 && last_wqe_size <= WQEBB_SIZE {
            self.ring_bf(last_wqe_ptr);
        } else {
            self.ring_db(last_wqe_ptr);
        }
    }

    fn ring_bf(&mut self, wqe_ptr: *mut u8) {
        let bf = unsafe { self.bf_reg.add(self.bf_offset as usize) };
        mlx5_bf_copy!(bf, wqe_ptr);
        mmio_flush_writes!();
        self.bf_offset ^= self.bf_size;
    }

    fn ring_db(&mut self, wqe_ptr: *mut u8) {
        let bf = unsafe { self.bf_reg.add(self.bf_offset as usize) as *mut u64 };
        let ctrl = wqe_ptr as *const u64;
        unsafe {
            std::ptr::write_volatile(bf, *ctrl);
        }
        mmio_flush_writes!();
        self.bf_offset ^= self.bf_size;
    }

    fn process_completions<F>(&mut self, new_ci: u16, mut callback: F)
    where
        F: FnMut(u16, T),
    {
        for (idx, entry) in self.table.drain_range(self.ci, new_ci) {
            callback(idx, entry);
        }
        self.ci = new_ci;
    }
}

// =============================================================================
// DCI WQE Builder
// =============================================================================

/// Zero-copy WQE builder for DCI.
///
/// Similar to WqeBuilder but has Address Vector segment for DC operations.
pub struct DciWqeBuilder<'a, T> {
    sq: &'a mut DciSendQueueState<T>,
    wqe_ptr: *mut u8,
    wqe_idx: u16,
    offset: usize,
    ds_count: u8,
    entry: Option<T>,
}

impl<'a, T> DciWqeBuilder<'a, T> {
    /// Write the control segment.
    ///
    /// This must be the first segment in every WQE.
    /// The SIGNALED flag is automatically set if an entry was provided.
    pub fn ctrl(mut self, opcode: WqeOpcode, flags: u8, imm: u32) -> Self {
        let flags = if self.entry.is_some() {
            flags | WqeFlags::COMPLETION
        } else {
            flags
        };
        unsafe {
            CtrlSeg::write(
                self.wqe_ptr,
                opcode as u8,
                self.wqe_idx,
                self.sq.sqn,
                0,
                flags,
                imm,
            );
        }
        self.offset = CtrlSeg::SIZE;
        self.ds_count = 1;
        self
    }

    /// Add an address vector (required for DC operations).
    ///
    /// This segment specifies the destination DCT.
    pub fn av(mut self, dc_key: u64, dctn: u32, dlid: u16) -> Self {
        unsafe {
            AddressVector::write(self.wqe_ptr.add(self.offset), dc_key, dctn, dlid);
        }
        self.offset += AddressVector::SIZE;
        self.ds_count += 3; // AV = 48 bytes = 3 DS
        self
    }

    /// Add an RDMA segment (for WRITE/READ).
    pub fn rdma(mut self, remote_addr: u64, rkey: u32) -> Self {
        unsafe {
            RdmaSeg::write(self.wqe_ptr.add(self.offset), remote_addr, rkey);
        }
        self.offset += RdmaSeg::SIZE;
        self.ds_count += 1;
        self
    }

    /// Add a data segment (SGE).
    pub fn sge(mut self, addr: u64, len: u32, lkey: u32) -> Self {
        unsafe {
            DataSeg::write(self.wqe_ptr.add(self.offset), len, lkey, addr);
        }
        self.offset += DataSeg::SIZE;
        self.ds_count += 1;
        self
    }

    /// Add inline data.
    pub fn inline_data(mut self, data: &[u8]) -> Self {
        let padded_size = unsafe {
            let ptr = self.wqe_ptr.add(self.offset);
            let size = InlineHeader::write(ptr, data.len() as u32);
            std::ptr::copy_nonoverlapping(data.as_ptr(), ptr.add(4), data.len());
            size
        };
        self.offset += padded_size;
        self.ds_count += (padded_size / 16) as u8;
        self
    }

    /// Get a mutable slice for inline data (zero-copy).
    ///
    /// Returns the builder and a slice that can be written to directly.
    pub fn inline_slice(mut self, len: usize) -> (Self, &'a mut [u8]) {
        let padded_size = unsafe {
            let ptr = self.wqe_ptr.add(self.offset);
            InlineHeader::write(ptr, len as u32)
        };
        let data_ptr = unsafe { self.wqe_ptr.add(self.offset + 4) };
        let slice = unsafe { std::slice::from_raw_parts_mut(data_ptr, len) };
        self.offset += padded_size;
        self.ds_count += (padded_size / 16) as u8;
        (self, slice)
    }

    /// Finish the WQE construction.
    ///
    /// Updates the DS count and advances the SQ producer index.
    /// If an entry was provided, stores it in the WQE table.
    pub fn finish(self) -> WqeHandle {
        unsafe {
            CtrlSeg::update_ds_cnt(self.wqe_ptr, self.ds_count);
        }

        let wqebb_cnt = calc_wqebb_cnt(self.offset);
        let wqe_idx = self.wqe_idx;

        // Store entry in table if provided
        if let Some(entry) = self.entry {
            self.sq.table.store(wqe_idx, entry);
        }

        self.sq.advance_pi(wqebb_cnt);
        self.sq.set_last_wqe(self.wqe_ptr, self.offset);

        WqeHandle {
            wqe_idx,
            size: self.offset,
        }
    }
}

// =============================================================================
// DCI
// =============================================================================

/// DC Initiator (DCI).
///
/// Used for sending RDMA operations to DCTs.
/// Created using mlx5dv_create_qp with DC type.
///
/// Type parameter `T` is the entry type stored in the WQE table for tracking
/// in-flight operations. When a completion arrives, the associated entry is
/// returned via the callback.
pub struct Dci<T> {
    qp: NonNull<mlx5_sys::ibv_qp>,
    state: DcQpState,
    sq: Option<DciSendQueueState<T>>,
}

impl Context {
    /// Create a DCI (DC Initiator) using mlx5dv_create_qp.
    ///
    /// # Arguments
    /// * `pd` - Protection Domain
    /// * `send_cq` - Completion Queue for send completions
    /// * `config` - DCI configuration
    ///
    /// # Errors
    /// Returns an error if the DCI cannot be created.
    pub fn create_dci<T>(
        &self,
        pd: &ProtectionDomain,
        send_cq: &CompletionQueue,
        config: &DciConfig,
    ) -> io::Result<Dci<T>> {
        unsafe {
            let mut qp_attr: mlx5_sys::ibv_qp_init_attr_ex = MaybeUninit::zeroed().assume_init();
            qp_attr.qp_type = mlx5_sys::ibv_qp_type_IBV_QPT_DRIVER;
            qp_attr.send_cq = send_cq.as_ptr();
            qp_attr.recv_cq = send_cq.as_ptr();
            qp_attr.cap.max_send_wr = config.max_send_wr;
            qp_attr.cap.max_recv_wr = 0;
            qp_attr.cap.max_send_sge = config.max_send_sge;
            qp_attr.cap.max_recv_sge = 0;
            qp_attr.cap.max_inline_data = config.max_inline_data;
            qp_attr.comp_mask = mlx5_sys::ibv_qp_init_attr_mask_IBV_QP_INIT_ATTR_PD;
            qp_attr.pd = pd.as_ptr();

            let mut mlx5_attr: mlx5_sys::mlx5dv_qp_init_attr = MaybeUninit::zeroed().assume_init();
            mlx5_attr.comp_mask =
                mlx5_sys::mlx5dv_qp_init_attr_mask_MLX5DV_QP_INIT_ATTR_MASK_DC as u64;
            mlx5_attr.dc_init_attr.dc_type = mlx5_sys::mlx5dv_dc_type_MLX5DV_DCTYPE_DCI;
            mlx5_attr
                .dc_init_attr
                .__bindgen_anon_1
                .dci_streams
                .log_num_concurent = 0;
            mlx5_attr
                .dc_init_attr
                .__bindgen_anon_1
                .dci_streams
                .log_num_errored = 0;

            let qp = mlx5_sys::mlx5dv_create_qp(self.ctx.as_ptr(), &mut qp_attr, &mut mlx5_attr);
            NonNull::new(qp).map_or(Err(io::Error::last_os_error()), |qp| {
                Ok(Dci {
                    qp,
                    state: DcQpState::Reset,
                    sq: None,
                })
            })
        }
    }
}

impl<T> Drop for Dci<T> {
    fn drop(&mut self) {
        unsafe {
            mlx5_sys::ibv_destroy_qp(self.qp.as_ptr());
        }
    }
}

impl<T> Dci<T> {
    /// Get the QP number.
    pub fn qpn(&self) -> u32 {
        unsafe { (*self.qp.as_ptr()).qp_num }
    }

    /// Get the current QP state.
    pub fn state(&self) -> DcQpState {
        self.state
    }

    /// Get mlx5-specific QP information for direct WQE access.
    fn query_info(&self) -> io::Result<QpInfo> {
        unsafe {
            let mut dv_qp: MaybeUninit<mlx5_sys::mlx5dv_qp> = MaybeUninit::zeroed();
            let mut obj: MaybeUninit<mlx5_sys::mlx5dv_obj> = MaybeUninit::zeroed();

            let dv_qp_ptr = dv_qp.as_mut_ptr();
            (*dv_qp_ptr).comp_mask =
                mlx5_sys::mlx5dv_qp_comp_mask_MLX5DV_QP_MASK_RAW_QP_HANDLES as u64;

            let obj_ptr = obj.as_mut_ptr();
            (*obj_ptr).qp.in_ = self.qp.as_ptr();
            (*obj_ptr).qp.out = dv_qp_ptr;

            let ret =
                mlx5_sys::mlx5dv_init_obj(obj_ptr, mlx5_sys::mlx5dv_obj_type_MLX5DV_OBJ_QP as u64);
            if ret != 0 {
                return Err(io::Error::from_raw_os_error(-ret));
            }

            let dv_qp = dv_qp.assume_init();
            let qp_num = (*self.qp.as_ptr()).qp_num;
            let sqn = if dv_qp.sqn != 0 { dv_qp.sqn } else { qp_num };

            Ok(QpInfo {
                dbrec: dv_qp.dbrec as *mut u32,
                sq_buf: dv_qp.sq.buf as *mut u8,
                sq_wqe_cnt: dv_qp.sq.wqe_cnt,
                sq_stride: dv_qp.sq.stride,
                rq_buf: dv_qp.rq.buf as *mut u8,
                rq_wqe_cnt: dv_qp.rq.wqe_cnt,
                rq_stride: dv_qp.rq.stride,
                bf_reg: dv_qp.bf.reg as *mut u8,
                bf_size: dv_qp.bf.size,
                sqn,
            })
        }
    }

    /// Initialize direct queue access.
    ///
    /// Call this after the DCI is ready (RTS state).
    pub fn init_direct_access(&mut self) -> io::Result<()> {
        let info = self.query_info()?;
        let wqe_cnt = info.sq_wqe_cnt as u16;

        self.sq = Some(DciSendQueueState {
            buf: info.sq_buf,
            wqe_cnt,
            sqn: info.sqn,
            pi: 0,
            ci: 0,
            last_wqe: None,
            dbrec: info.dbrec,
            bf_reg: info.bf_reg,
            bf_size: info.bf_size,
            bf_offset: 0,
            table: WqeTable::new(wqe_cnt),
        });

        Ok(())
    }

    /// Transition DCI from RESET to INIT.
    pub fn modify_to_init(&mut self, port: u8) -> io::Result<()> {
        if self.state != DcQpState::Reset {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "DCI must be in RESET state",
            ));
        }

        unsafe {
            let mut attr: mlx5_sys::ibv_qp_attr = MaybeUninit::zeroed().assume_init();
            attr.qp_state = mlx5_sys::ibv_qp_state_IBV_QPS_INIT;
            attr.pkey_index = 0;
            attr.port_num = port;

            let mask = mlx5_sys::ibv_qp_attr_mask_IBV_QP_STATE
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_PKEY_INDEX
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_PORT;

            let ret = mlx5_sys::ibv_modify_qp(self.qp.as_ptr(), &mut attr, mask as i32);
            if ret != 0 {
                return Err(io::Error::from_raw_os_error(ret));
            }
        }

        self.state = DcQpState::Init;
        Ok(())
    }

    /// Transition DCI from INIT to RTR.
    pub fn modify_to_rtr(&mut self, port: u8) -> io::Result<()> {
        if self.state != DcQpState::Init {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "DCI must be in INIT state",
            ));
        }

        unsafe {
            let mut attr: mlx5_sys::ibv_qp_attr = MaybeUninit::zeroed().assume_init();
            attr.qp_state = mlx5_sys::ibv_qp_state_IBV_QPS_RTR;
            attr.path_mtu = mlx5_sys::ibv_mtu_IBV_MTU_4096;
            attr.ah_attr.port_num = port;

            let mask = mlx5_sys::ibv_qp_attr_mask_IBV_QP_STATE
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_PATH_MTU
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_AV;

            let ret = mlx5_sys::ibv_modify_qp(self.qp.as_ptr(), &mut attr, mask as i32);
            if ret != 0 {
                return Err(io::Error::from_raw_os_error(ret));
            }
        }

        self.state = DcQpState::Rtr;
        Ok(())
    }

    /// Transition DCI from RTR to RTS.
    pub fn modify_to_rts(&mut self, local_psn: u32, max_rd_atomic: u8) -> io::Result<()> {
        if self.state != DcQpState::Rtr {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "DCI must be in RTR state",
            ));
        }

        unsafe {
            let mut attr: mlx5_sys::ibv_qp_attr = MaybeUninit::zeroed().assume_init();
            attr.qp_state = mlx5_sys::ibv_qp_state_IBV_QPS_RTS;
            attr.timeout = 14;
            attr.retry_cnt = 7;
            attr.rnr_retry = 7;
            attr.sq_psn = local_psn;
            attr.max_rd_atomic = max_rd_atomic;

            let mask = mlx5_sys::ibv_qp_attr_mask_IBV_QP_STATE
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_TIMEOUT
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_RETRY_CNT
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_RNR_RETRY
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_SQ_PSN
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_MAX_QP_RD_ATOMIC;

            let ret = mlx5_sys::ibv_modify_qp(self.qp.as_ptr(), &mut attr, mask as i32);
            if ret != 0 {
                return Err(io::Error::from_raw_os_error(ret));
            }
        }

        self.state = DcQpState::Rts;
        Ok(())
    }

    /// Activate DCI (transition to RTS) and initialize direct access.
    pub fn activate(&mut self, port: u8, local_psn: u32, max_rd_atomic: u8) -> io::Result<()> {
        self.modify_to_init(port)?;
        self.modify_to_rtr(port)?;
        self.modify_to_rts(local_psn, max_rd_atomic)?;
        self.init_direct_access()?;
        Ok(())
    }

    fn sq_mut(&mut self) -> io::Result<&mut DciSendQueueState<T>> {
        self.sq
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "direct access not initialized"))
    }
}

impl<T> OrderedSendQueue for Dci<T> {
    type Builder<'a>
        = DciWqeBuilder<'a, T>
    where
        T: 'a;
    type Entry = T;

    fn sq_available(&self) -> u16 {
        self.sq.as_ref().map(|sq| sq.available()).unwrap_or(0)
    }

    fn wqe_builder(&mut self, entry: Option<Self::Entry>) -> io::Result<DciWqeBuilder<'_, T>> {
        let sq = self.sq_mut()?;
        if sq.available() == 0 {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }

        let wqe_idx = sq.pi;
        let wqe_ptr = sq.get_wqe_ptr(wqe_idx);

        Ok(DciWqeBuilder {
            sq,
            wqe_ptr,
            wqe_idx,
            offset: 0,
            ds_count: 0,
            entry,
        })
    }

    fn ring_sq_doorbell(&mut self) {
        if let Some(sq) = self.sq.as_mut() {
            sq.ring_doorbell();
        }
    }

    fn process_completions<F>(&mut self, new_ci: u16, callback: F)
    where
        F: FnMut(u16, Self::Entry),
    {
        if let Some(sq) = self.sq.as_mut() {
            sq.process_completions(new_ci, callback);
        }
    }
}

// =============================================================================
// DCT
// =============================================================================

/// DC Target (DCT).
///
/// Receives RDMA operations from DCIs via an SRQ.
/// Created using mlx5dv_create_qp with DC type.
pub struct Dct {
    qp: NonNull<mlx5_sys::ibv_qp>,
    dc_key: u64,
    state: DcQpState,
}

impl Context {
    /// Create a DCT (DC Target) using mlx5dv_create_qp.
    ///
    /// # Arguments
    /// * `pd` - Protection Domain
    /// * `srq` - Shared Receive Queue for incoming messages
    /// * `cq` - Completion Queue
    /// * `config` - DCT configuration
    ///
    /// # Errors
    /// Returns an error if the DCT cannot be created.
    pub fn create_dct(
        &self,
        pd: &ProtectionDomain,
        srq: &SharedReceiveQueue,
        cq: &CompletionQueue,
        config: &DctConfig,
    ) -> io::Result<Dct> {
        unsafe {
            let mut qp_attr: mlx5_sys::ibv_qp_init_attr_ex = MaybeUninit::zeroed().assume_init();
            qp_attr.qp_type = mlx5_sys::ibv_qp_type_IBV_QPT_DRIVER;
            qp_attr.send_cq = cq.as_ptr();
            qp_attr.recv_cq = cq.as_ptr();
            qp_attr.srq = srq.as_ptr();
            qp_attr.cap.max_recv_wr = 0;
            qp_attr.cap.max_recv_sge = 0;
            qp_attr.comp_mask = mlx5_sys::ibv_qp_init_attr_mask_IBV_QP_INIT_ATTR_PD;
            qp_attr.pd = pd.as_ptr();

            let mut mlx5_attr: mlx5_sys::mlx5dv_qp_init_attr = MaybeUninit::zeroed().assume_init();
            mlx5_attr.comp_mask =
                mlx5_sys::mlx5dv_qp_init_attr_mask_MLX5DV_QP_INIT_ATTR_MASK_DC as u64;
            mlx5_attr.dc_init_attr.dc_type = mlx5_sys::mlx5dv_dc_type_MLX5DV_DCTYPE_DCT;
            mlx5_attr.dc_init_attr.__bindgen_anon_1.dct_access_key = config.dc_key;

            let qp = mlx5_sys::mlx5dv_create_qp(self.ctx.as_ptr(), &mut qp_attr, &mut mlx5_attr);
            NonNull::new(qp).map_or(Err(io::Error::last_os_error()), |qp| {
                Ok(Dct {
                    qp,
                    dc_key: config.dc_key,
                    state: DcQpState::Reset,
                })
            })
        }
    }
}

impl Drop for Dct {
    fn drop(&mut self) {
        unsafe {
            mlx5_sys::ibv_destroy_qp(self.qp.as_ptr());
        }
    }
}

impl Dct {
    /// Get the DCT number.
    pub fn dctn(&self) -> u32 {
        unsafe { (*self.qp.as_ptr()).qp_num }
    }

    /// Get the DC key.
    pub fn dc_key(&self) -> u64 {
        self.dc_key
    }

    /// Get the current state.
    pub fn state(&self) -> DcQpState {
        self.state
    }

    /// Transition DCT from RESET to INIT.
    pub fn modify_to_init(&mut self, port: u8, access_flags: u32) -> io::Result<()> {
        if self.state != DcQpState::Reset {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "DCT must be in RESET state",
            ));
        }

        unsafe {
            let mut attr: mlx5_sys::ibv_qp_attr = MaybeUninit::zeroed().assume_init();
            attr.qp_state = mlx5_sys::ibv_qp_state_IBV_QPS_INIT;
            attr.pkey_index = 0;
            attr.port_num = port;
            attr.qp_access_flags = access_flags;

            let mask = mlx5_sys::ibv_qp_attr_mask_IBV_QP_STATE
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_PKEY_INDEX
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_PORT
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_ACCESS_FLAGS;

            let ret = mlx5_sys::ibv_modify_qp(self.qp.as_ptr(), &mut attr, mask as i32);
            if ret != 0 {
                return Err(io::Error::from_raw_os_error(ret));
            }
        }

        self.state = DcQpState::Init;
        Ok(())
    }

    /// Transition DCT from INIT to RTR.
    pub fn modify_to_rtr(&mut self, port: u8, max_dest_rd_atomic: u8) -> io::Result<()> {
        if self.state != DcQpState::Init {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "DCT must be in INIT state",
            ));
        }

        unsafe {
            let mut attr: mlx5_sys::ibv_qp_attr = MaybeUninit::zeroed().assume_init();
            attr.qp_state = mlx5_sys::ibv_qp_state_IBV_QPS_RTR;
            attr.path_mtu = mlx5_sys::ibv_mtu_IBV_MTU_4096;
            attr.max_dest_rd_atomic = max_dest_rd_atomic;
            attr.min_rnr_timer = 12;
            attr.ah_attr.port_num = port;

            let mask = mlx5_sys::ibv_qp_attr_mask_IBV_QP_STATE
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_PATH_MTU
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_MAX_DEST_RD_ATOMIC
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_MIN_RNR_TIMER
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_AV;

            let ret = mlx5_sys::ibv_modify_qp(self.qp.as_ptr(), &mut attr, mask as i32);
            if ret != 0 {
                return Err(io::Error::from_raw_os_error(ret));
            }
        }

        self.state = DcQpState::Rtr;
        Ok(())
    }

    /// Activate DCT (transition to RTR).
    ///
    /// DCT does not need RTS state, RTR is sufficient for receiving.
    pub fn activate(
        &mut self,
        port: u8,
        access_flags: u32,
        max_dest_rd_atomic: u8,
    ) -> io::Result<()> {
        self.modify_to_init(port, access_flags)?;
        self.modify_to_rtr(port, max_dest_rd_atomic)?;
        Ok(())
    }

    /// Get remote DCT info for senders.
    pub fn remote_info(&self, lid: u16) -> RemoteDctInfo {
        RemoteDctInfo {
            dctn: self.dctn(),
            dc_key: self.dc_key,
            lid,
        }
    }
}
