//! Queue Pair (QP) management.
//!
//! Queue Pairs are the fundamental communication endpoints in RDMA.
//! This module provides RC (Reliable Connection) QP creation using mlx5dv_create_qp.

use std::{io, mem::MaybeUninit, ptr::NonNull};

use crate::cq::CompletionQueue;
use crate::device::Context;
use crate::pd::Pd;
use crate::wqe::{
    CtrlSeg, DataSeg, DenseSendQueue, DenseWqeTable, InlineHeader, RdmaSeg, ReceiveQueue,
    SparseSendQueue, SparseWqeTable, WQEBB_SIZE, WqeFlags, WqeHandle, WqeOpcode, calc_wqebb_cnt,
};

/// RC QP configuration.
#[derive(Debug, Clone)]
pub struct RcQpConfig {
    /// Maximum number of outstanding send WRs.
    pub max_send_wr: u32,
    /// Maximum number of outstanding receive WRs.
    pub max_recv_wr: u32,
    /// Maximum number of SGEs per send WR.
    pub max_send_sge: u32,
    /// Maximum number of SGEs per receive WR.
    pub max_recv_sge: u32,
    /// Maximum inline data size.
    pub max_inline_data: u32,
}

impl Default for RcQpConfig {
    fn default() -> Self {
        Self {
            max_send_wr: 256,
            max_recv_wr: 256,
            max_send_sge: 4,
            max_recv_sge: 4,
            max_inline_data: 64,
        }
    }
}

/// QP state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QpState {
    Reset,
    Init,
    Rtr,
    Rts,
    Error,
}

/// Remote QP information for connection.
#[derive(Debug, Clone)]
pub struct RemoteQpInfo {
    /// Remote QP number.
    pub qpn: u32,
    /// Remote packet sequence number.
    pub psn: u32,
    /// Remote LID (Local Identifier).
    pub lid: u16,
}

/// QP internal info obtained from mlx5dv_init_obj.
#[derive(Debug)]
pub struct QpInfo {
    /// Doorbell record pointer.
    pub dbrec: *mut u32,
    /// Send Queue buffer pointer.
    pub sq_buf: *mut u8,
    /// Send Queue WQE count.
    pub sq_wqe_cnt: u32,
    /// Send Queue stride (bytes per WQE slot).
    pub sq_stride: u32,
    /// Receive Queue buffer pointer.
    pub rq_buf: *mut u8,
    /// Receive Queue WQE count.
    pub rq_wqe_cnt: u32,
    /// Receive Queue stride.
    pub rq_stride: u32,
    /// BlueFlame register pointer.
    pub bf_reg: *mut u8,
    /// BlueFlame size.
    pub bf_size: u32,
    /// Send Queue Number.
    pub sqn: u32,
}

// =============================================================================
// Send Queue State
// =============================================================================

/// Send Queue state for direct WQE posting.
///
/// Generic over the table type `Tab` which determines dense vs sparse behavior.
pub(crate) struct SendQueueState<T, Tab> {
    /// SQ buffer base address
    buf: *mut u8,
    /// Number of WQEBBs (64-byte blocks)
    wqe_cnt: u16,
    /// SQ number
    sqn: u32,
    /// Producer index (next WQE slot)
    pi: u16,
    /// Consumer index (last completed WQE)
    ci: u16,
    /// Last posted WQE pointer and size (for BlueFlame)
    last_wqe: Option<(*mut u8, usize)>,
    /// Doorbell record pointer
    dbrec: *mut u32,
    /// BlueFlame register pointer
    bf_reg: *mut u8,
    /// BlueFlame size (64 or 0 if not available)
    bf_size: u32,
    /// Current BlueFlame offset (alternates between 0 and bf_size)
    bf_offset: u32,
    /// WQE table for tracking in-flight operations
    table: Tab,
    /// Phantom for entry type
    _marker: std::marker::PhantomData<T>,
}

impl<T, Tab> SendQueueState<T, Tab> {
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

    /// Ring the doorbell using regular doorbell write.
    fn ring_doorbell(&mut self) {
        let Some((last_wqe_ptr, _)) = self.last_wqe.take() else {
            return;
        };

        mmio_flush_writes!();

        unsafe {
            std::ptr::write_volatile(self.dbrec.add(1), (self.pi as u32).to_be());
        }

        udma_to_device_barrier!();

        self.ring_db(last_wqe_ptr);
    }

    /// Ring the doorbell using BlueFlame (low latency, single WQE).
    fn ring_blueflame(&mut self, wqe_ptr: *mut u8) {
        mmio_flush_writes!();

        unsafe {
            std::ptr::write_volatile(self.dbrec.add(1), (self.pi as u32).to_be());
        }

        udma_to_device_barrier!();

        if self.bf_size > 0 {
            let bf = unsafe { self.bf_reg.add(self.bf_offset as usize) };
            mlx5_bf_copy!(bf, wqe_ptr);
            mmio_flush_writes!();
            self.bf_offset ^= self.bf_size;
        } else {
            // Fallback to regular doorbell if BlueFlame not available
            self.ring_db(wqe_ptr);
        }
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
}

impl<T> SendQueueState<T, SparseWqeTable<T>> {
    fn process_completion_sparse(&mut self, wqe_idx: u16) -> Option<T> {
        self.ci = wqe_idx;
        self.table.take(wqe_idx)
    }
}

impl<T> SendQueueState<T, DenseWqeTable<T>> {
    fn process_completions_dense<F>(&mut self, new_ci: u16, mut callback: F)
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
// Receive Queue State
// =============================================================================

/// Receive Queue state for direct WQE posting.
pub(crate) struct ReceiveQueueState {
    /// RQ buffer base address
    buf: *mut u8,
    /// Number of WQE slots
    wqe_cnt: u32,
    /// Stride (bytes per WQE slot)
    stride: u32,
    /// Producer index (next WQE slot)
    pi: u16,
    /// Doorbell record pointer (dbrec[0] for RQ)
    dbrec: *mut u32,
}

impl ReceiveQueueState {
    fn get_wqe_ptr(&self, idx: u16) -> *mut u8 {
        let offset = ((idx as u32) & (self.wqe_cnt - 1)) * self.stride;
        unsafe { self.buf.add(offset as usize) }
    }

    unsafe fn post(&mut self, addr: u64, len: u32, lkey: u32) {
        let wqe_ptr = self.get_wqe_ptr(self.pi);
        DataSeg::write(wqe_ptr, len, lkey, addr);
        self.pi = self.pi.wrapping_add(1);
    }

    fn ring_doorbell(&mut self) {
        mmio_flush_writes!();
        unsafe {
            std::ptr::write_volatile(self.dbrec, (self.pi as u32).to_be());
        }
    }
}

// =============================================================================
// WQE Builder
// =============================================================================

/// Zero-copy WQE builder for RC QP.
///
/// Writes segments directly to the SQ buffer without intermediate copies.
pub struct WqeBuilder<'a, T, Tab> {
    sq: &'a mut SendQueueState<T, Tab>,
    wqe_ptr: *mut u8,
    wqe_idx: u16,
    offset: usize,
    ds_count: u8,
    /// Whether SIGNALED flag is set
    signaled: bool,
}

impl<'a, T, Tab> WqeBuilder<'a, T, Tab> {
    /// Write the control segment.
    ///
    /// This must be the first segment in every WQE.
    pub fn ctrl(mut self, opcode: WqeOpcode, flags: WqeFlags, imm: u32) -> Self {
        let flags = if self.signaled {
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
                flags.bits(),
                imm,
            );
        }
        self.offset = CtrlSeg::SIZE;
        self.ds_count = 1;
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

    /// Finish the WQE construction (internal).
    fn finish_internal(self) -> WqeHandle {
        unsafe {
            CtrlSeg::update_ds_cnt(self.wqe_ptr, self.ds_count);
        }

        let wqebb_cnt = calc_wqebb_cnt(self.offset);
        let wqe_idx = self.wqe_idx;

        self.sq.advance_pi(wqebb_cnt);
        self.sq.set_last_wqe(self.wqe_ptr, self.offset);

        WqeHandle {
            wqe_idx,
            size: self.offset,
        }
    }

    /// Finish the WQE construction and immediately ring BlueFlame doorbell.
    ///
    /// Use this for low-latency single WQE submission. The doorbell is issued
    /// immediately, so no need to call `ring_sq_doorbell()` afterwards.
    fn finish_internal_with_blueflame(self) -> WqeHandle {
        unsafe {
            CtrlSeg::update_ds_cnt(self.wqe_ptr, self.ds_count);
        }

        let wqebb_cnt = calc_wqebb_cnt(self.offset);
        let wqe_idx = self.wqe_idx;
        let wqe_ptr = self.wqe_ptr;

        self.sq.advance_pi(wqebb_cnt);
        self.sq.ring_blueflame(wqe_ptr);

        WqeHandle {
            wqe_idx,
            size: self.offset,
        }
    }
}

// =============================================================================
// RC QP
// =============================================================================

/// RC (Reliable Connection) Queue Pair with sparse WQE table.
///
/// Only signaled WQEs have entries stored. Use this when you only need
/// to track completions for signaled WQEs.
///
/// For tracking all WQEs, use `DenseRcQp` instead.
pub type RcQp<T> = RcQpInner<T, SparseWqeTable<T>>;

/// RC (Reliable Connection) Queue Pair with dense WQE table.
///
/// Every WQE must have an entry stored. Use this when you need to track
/// all completions, including unsignaled WQEs.
///
/// For tracking only signaled WQEs, use `RcQp` instead.
pub type DenseRcQp<T> = RcQpInner<T, DenseWqeTable<T>>;

/// RC (Reliable Connection) Queue Pair (internal implementation).
///
/// Created using mlx5dv_create_qp for direct hardware access.
///
/// Type parameter `T` is the entry type stored in the WQE table.
/// Type parameter `Tab` determines sparse vs dense table behavior.
pub struct RcQpInner<T, Tab> {
    qp: NonNull<mlx5_sys::ibv_qp>,
    state: QpState,
    sq: Option<SendQueueState<T, Tab>>,
    rq: Option<ReceiveQueueState>,
}

impl Context {
    /// Create an RC Queue Pair with sparse WQE table using mlx5dv_create_qp.
    ///
    /// Only signaled WQEs have entries stored.
    ///
    /// # Arguments
    /// * `pd` - Protection Domain
    /// * `send_cq` - Completion Queue for send completions
    /// * `recv_cq` - Completion Queue for receive completions
    /// * `config` - QP configuration
    ///
    /// # Errors
    /// Returns an error if the QP cannot be created.
    pub fn create_rc_qp<T>(
        &self,
        pd: &Pd,
        send_cq: &CompletionQueue,
        recv_cq: &CompletionQueue,
        config: &RcQpConfig,
    ) -> io::Result<RcQp<T>> {
        self.create_rc_qp_inner(pd, send_cq, recv_cq, config)
    }

    /// Create an RC Queue Pair with dense WQE table using mlx5dv_create_qp.
    ///
    /// Every WQE must have an entry stored.
    ///
    /// # Arguments
    /// * `pd` - Protection Domain
    /// * `send_cq` - Completion Queue for send completions
    /// * `recv_cq` - Completion Queue for receive completions
    /// * `config` - QP configuration
    ///
    /// # Errors
    /// Returns an error if the QP cannot be created.
    pub fn create_dense_rc_qp<T>(
        &self,
        pd: &Pd,
        send_cq: &CompletionQueue,
        recv_cq: &CompletionQueue,
        config: &RcQpConfig,
    ) -> io::Result<DenseRcQp<T>> {
        self.create_rc_qp_inner(pd, send_cq, recv_cq, config)
    }

    fn create_rc_qp_inner<T, Tab>(
        &self,
        pd: &Pd,
        send_cq: &CompletionQueue,
        recv_cq: &CompletionQueue,
        config: &RcQpConfig,
    ) -> io::Result<RcQpInner<T, Tab>> {
        unsafe {
            let mut qp_attr: mlx5_sys::ibv_qp_init_attr_ex = MaybeUninit::zeroed().assume_init();
            qp_attr.qp_type = mlx5_sys::ibv_qp_type_IBV_QPT_RC;
            qp_attr.send_cq = send_cq.as_ptr();
            qp_attr.recv_cq = recv_cq.as_ptr();
            qp_attr.cap.max_send_wr = config.max_send_wr;
            qp_attr.cap.max_recv_wr = config.max_recv_wr;
            qp_attr.cap.max_send_sge = config.max_send_sge;
            qp_attr.cap.max_recv_sge = config.max_recv_sge;
            qp_attr.cap.max_inline_data = config.max_inline_data;
            qp_attr.comp_mask = mlx5_sys::ibv_qp_init_attr_mask_IBV_QP_INIT_ATTR_PD;
            qp_attr.pd = pd.as_ptr();

            let mut mlx5_attr: mlx5_sys::mlx5dv_qp_init_attr = MaybeUninit::zeroed().assume_init();

            let qp = mlx5_sys::mlx5dv_create_qp(self.ctx.as_ptr(), &mut qp_attr, &mut mlx5_attr);
            NonNull::new(qp).map_or(Err(io::Error::last_os_error()), |qp| {
                Ok(RcQpInner {
                    qp,
                    state: QpState::Reset,
                    sq: None,
                    rq: None,
                })
            })
        }
    }
}

impl<T, Tab> Drop for RcQpInner<T, Tab> {
    fn drop(&mut self) {
        unsafe {
            mlx5_sys::ibv_destroy_qp(self.qp.as_ptr());
        }
    }
}

impl<T, Tab> RcQpInner<T, Tab> {
    /// Get the QP number.
    pub fn qpn(&self) -> u32 {
        unsafe { (*self.qp.as_ptr()).qp_num }
    }

    /// Get the current QP state.
    pub fn state(&self) -> QpState {
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

    /// Transition QP from RESET to INIT.
    pub fn modify_to_init(&mut self, port: u8, access_flags: u32) -> io::Result<()> {
        if self.state != QpState::Reset {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "QP must be in RESET state",
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

        self.state = QpState::Init;
        Ok(())
    }

    /// Transition QP from INIT to RTR (Ready to Receive).
    pub fn modify_to_rtr(
        &mut self,
        remote: &RemoteQpInfo,
        port: u8,
        max_dest_rd_atomic: u8,
    ) -> io::Result<()> {
        if self.state != QpState::Init {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "QP must be in INIT state",
            ));
        }

        unsafe {
            let mut attr: mlx5_sys::ibv_qp_attr = MaybeUninit::zeroed().assume_init();
            attr.qp_state = mlx5_sys::ibv_qp_state_IBV_QPS_RTR;
            attr.path_mtu = mlx5_sys::ibv_mtu_IBV_MTU_4096;
            attr.dest_qp_num = remote.qpn;
            attr.rq_psn = remote.psn;
            attr.max_dest_rd_atomic = max_dest_rd_atomic;
            attr.min_rnr_timer = 12;
            attr.ah_attr.dlid = remote.lid;
            attr.ah_attr.sl = 0;
            attr.ah_attr.src_path_bits = 0;
            attr.ah_attr.is_global = 0;
            attr.ah_attr.port_num = port;

            let mask = mlx5_sys::ibv_qp_attr_mask_IBV_QP_STATE
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_AV
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_PATH_MTU
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_DEST_QPN
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_RQ_PSN
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_MAX_DEST_RD_ATOMIC
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_MIN_RNR_TIMER;

            let ret = mlx5_sys::ibv_modify_qp(self.qp.as_ptr(), &mut attr, mask as i32);
            if ret != 0 {
                return Err(io::Error::from_raw_os_error(ret));
            }
        }

        self.state = QpState::Rtr;
        Ok(())
    }

    /// Transition QP from RTR to RTS (Ready to Send).
    pub fn modify_to_rts(&mut self, local_psn: u32, max_rd_atomic: u8) -> io::Result<()> {
        if self.state != QpState::Rtr {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "QP must be in RTR state",
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

        self.state = QpState::Rts;
        Ok(())
    }

    fn sq_mut(&mut self) -> io::Result<&mut SendQueueState<T, Tab>> {
        self.sq
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "direct access not initialized"))
    }

    fn sq_available(&self) -> u16 {
        self.sq.as_ref().map(|sq| sq.available()).unwrap_or(0)
    }

    fn ring_sq_doorbell(&mut self) {
        if let Some(sq) = self.sq.as_mut() {
            sq.ring_doorbell();
        }
    }
}

impl<T> RcQp<T> {
    /// Initialize direct queue access.
    ///
    /// Call this after the QP is ready (RTS state).
    pub fn init_direct_access(&mut self) -> io::Result<()> {
        let info = self.query_info()?;
        let wqe_cnt = info.sq_wqe_cnt as u16;

        self.sq = Some(SendQueueState {
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
            table: SparseWqeTable::new(wqe_cnt),
            _marker: std::marker::PhantomData,
        });

        self.rq = Some(ReceiveQueueState {
            buf: info.rq_buf,
            wqe_cnt: info.rq_wqe_cnt,
            stride: info.rq_stride,
            pi: 0,
            dbrec: info.dbrec,
        });

        Ok(())
    }

    /// Connect to a remote QP.
    ///
    /// Transitions the QP through RESET -> INIT -> RTR -> RTS and initializes
    /// direct queue access.
    pub fn connect(
        &mut self,
        remote: &RemoteQpInfo,
        port: u8,
        local_psn: u32,
        max_rd_atomic: u8,
        max_dest_rd_atomic: u8,
        access_flags: u32,
    ) -> io::Result<()> {
        self.modify_to_init(port, access_flags)?;
        self.modify_to_rtr(remote, port, max_dest_rd_atomic)?;
        self.modify_to_rts(local_psn, max_rd_atomic)?;
        self.init_direct_access()?;
        Ok(())
    }
}

impl<T> DenseRcQp<T> {
    /// Initialize direct queue access.
    ///
    /// Call this after the QP is ready (RTS state).
    pub fn init_direct_access(&mut self) -> io::Result<()> {
        let info = self.query_info()?;
        let wqe_cnt = info.sq_wqe_cnt as u16;

        self.sq = Some(SendQueueState {
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
            table: DenseWqeTable::new(wqe_cnt),
            _marker: std::marker::PhantomData,
        });

        self.rq = Some(ReceiveQueueState {
            buf: info.rq_buf,
            wqe_cnt: info.rq_wqe_cnt,
            stride: info.rq_stride,
            pi: 0,
            dbrec: info.dbrec,
        });

        Ok(())
    }

    /// Connect to a remote QP.
    ///
    /// Transitions the QP through RESET -> INIT -> RTR -> RTS and initializes
    /// direct queue access.
    pub fn connect(
        &mut self,
        remote: &RemoteQpInfo,
        port: u8,
        local_psn: u32,
        max_rd_atomic: u8,
        max_dest_rd_atomic: u8,
        access_flags: u32,
    ) -> io::Result<()> {
        self.modify_to_init(port, access_flags)?;
        self.modify_to_rtr(remote, port, max_dest_rd_atomic)?;
        self.modify_to_rts(local_psn, max_rd_atomic)?;
        self.init_direct_access()?;
        Ok(())
    }
}

// =============================================================================
// Sparse WQE Builder finish
// =============================================================================

/// Sparse WQE builder that stores entry on finish.
pub struct SparseWqeBuilder<'a, T> {
    inner: WqeBuilder<'a, T, SparseWqeTable<T>>,
    entry: Option<T>,
}

impl<'a, T> SparseWqeBuilder<'a, T> {
    /// Write the control segment.
    pub fn ctrl(mut self, opcode: WqeOpcode, flags: WqeFlags, imm: u32) -> Self {
        self.inner = self.inner.ctrl(opcode, flags, imm);
        self
    }

    /// Add an RDMA segment.
    pub fn rdma(mut self, remote_addr: u64, rkey: u32) -> Self {
        self.inner = self.inner.rdma(remote_addr, rkey);
        self
    }

    /// Add a data segment.
    pub fn sge(mut self, addr: u64, len: u32, lkey: u32) -> Self {
        self.inner = self.inner.sge(addr, len, lkey);
        self
    }

    /// Add inline data.
    pub fn inline_data(mut self, data: &[u8]) -> Self {
        self.inner = self.inner.inline_data(data);
        self
    }

    /// Finish the WQE construction.
    pub fn finish(self) -> WqeHandle {
        let wqe_idx = self.inner.wqe_idx;
        if let Some(entry) = self.entry {
            self.inner.sq.table.store(wqe_idx, entry);
        }
        self.inner.finish_internal()
    }

    /// Finish the WQE construction and immediately ring BlueFlame doorbell.
    ///
    /// Use this for low-latency single WQE submission. No need to call
    /// `ring_sq_doorbell()` afterwards.
    pub fn finish_with_blueflame(self) -> WqeHandle {
        let wqe_idx = self.inner.wqe_idx;
        if let Some(entry) = self.entry {
            self.inner.sq.table.store(wqe_idx, entry);
        }
        self.inner.finish_internal_with_blueflame()
    }
}

/// Dense WQE builder that stores entry on finish.
pub struct DenseWqeBuilder<'a, T> {
    inner: WqeBuilder<'a, T, DenseWqeTable<T>>,
    entry: T,
}

impl<'a, T> DenseWqeBuilder<'a, T> {
    /// Write the control segment.
    pub fn ctrl(mut self, opcode: WqeOpcode, flags: WqeFlags, imm: u32) -> Self {
        self.inner = self.inner.ctrl(opcode, flags, imm);
        self
    }

    /// Add an RDMA segment.
    pub fn rdma(mut self, remote_addr: u64, rkey: u32) -> Self {
        self.inner = self.inner.rdma(remote_addr, rkey);
        self
    }

    /// Add a data segment.
    pub fn sge(mut self, addr: u64, len: u32, lkey: u32) -> Self {
        self.inner = self.inner.sge(addr, len, lkey);
        self
    }

    /// Add inline data.
    pub fn inline_data(mut self, data: &[u8]) -> Self {
        self.inner = self.inner.inline_data(data);
        self
    }

    /// Finish the WQE construction.
    pub fn finish(self) -> WqeHandle {
        let wqe_idx = self.inner.wqe_idx;
        self.inner.sq.table.store(wqe_idx, self.entry);
        self.inner.finish_internal()
    }

    /// Finish the WQE construction and immediately ring BlueFlame doorbell.
    ///
    /// Use this for low-latency single WQE submission. No need to call
    /// `ring_sq_doorbell()` afterwards.
    pub fn finish_with_blueflame(self) -> WqeHandle {
        let wqe_idx = self.inner.wqe_idx;
        self.inner.sq.table.store(wqe_idx, self.entry);
        self.inner.finish_internal_with_blueflame()
    }
}

// =============================================================================
// SparseSendQueue impl for RcQp
// =============================================================================

impl<T> SparseSendQueue for RcQp<T> {
    type Builder<'a>
        = SparseWqeBuilder<'a, T>
    where
        T: 'a;
    type Entry = T;

    fn sq_available(&self) -> u16 {
        RcQpInner::sq_available(self)
    }

    fn wqe_builder(&mut self, entry: Option<Self::Entry>) -> io::Result<SparseWqeBuilder<'_, T>> {
        let sq = self.sq_mut()?;
        if sq.available() == 0 {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }

        let wqe_idx = sq.pi;
        let wqe_ptr = sq.get_wqe_ptr(wqe_idx);
        let signaled = entry.is_some();

        Ok(SparseWqeBuilder {
            inner: WqeBuilder {
                sq,
                wqe_ptr,
                wqe_idx,
                offset: 0,
                ds_count: 0,
                signaled,
            },
            entry,
        })
    }

    fn ring_sq_doorbell(&mut self) {
        RcQpInner::ring_sq_doorbell(self);
    }

    fn process_completion(&mut self, wqe_idx: u16) -> Option<Self::Entry> {
        self.sq
            .as_mut()
            .and_then(|sq| sq.process_completion_sparse(wqe_idx))
    }
}

// =============================================================================
// DenseSendQueue impl for DenseRcQp
// =============================================================================

impl<T> DenseSendQueue for DenseRcQp<T> {
    type Builder<'a>
        = DenseWqeBuilder<'a, T>
    where
        T: 'a;
    type Entry = T;

    fn sq_available(&self) -> u16 {
        RcQpInner::sq_available(self)
    }

    fn wqe_builder(
        &mut self,
        entry: Self::Entry,
        signaled: bool,
    ) -> io::Result<DenseWqeBuilder<'_, T>> {
        let sq = self.sq_mut()?;
        if sq.available() == 0 {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }

        let wqe_idx = sq.pi;
        let wqe_ptr = sq.get_wqe_ptr(wqe_idx);

        Ok(DenseWqeBuilder {
            inner: WqeBuilder {
                sq,
                wqe_ptr,
                wqe_idx,
                offset: 0,
                ds_count: 0,
                signaled,
            },
            entry,
        })
    }

    fn ring_sq_doorbell(&mut self) {
        RcQpInner::ring_sq_doorbell(self);
    }

    fn process_completions<F>(&mut self, new_ci: u16, callback: F)
    where
        F: FnMut(u16, Self::Entry),
    {
        if let Some(sq) = self.sq.as_mut() {
            sq.process_completions_dense(new_ci, callback);
        }
    }
}

// =============================================================================
// ReceiveQueue impl
// =============================================================================

impl<T, Tab> ReceiveQueue for RcQpInner<T, Tab> {
    unsafe fn post_recv(&mut self, addr: u64, len: u32, lkey: u32) {
        if let Some(rq) = self.rq.as_mut() {
            rq.post(addr, len, lkey);
        }
    }

    fn ring_rq_doorbell(&mut self) {
        if let Some(rq) = self.rq.as_mut() {
            rq.ring_doorbell();
        }
    }
}
