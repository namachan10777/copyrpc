//! DC (Dynamically Connected) transport.
//!
//! DC provides scalable connectionless RDMA operations using:
//! - DCI (DC Initiator): Sends RDMA operations to DCTs
//! - DCT (DC Target): Receives RDMA operations via SRQ

use std::cell::{Cell, RefCell};
use std::rc::{Rc, Weak};
use std::{io, marker::PhantomData, mem::MaybeUninit, ptr::NonNull};

use crate::CompletionTarget;
use crate::cq::{CompletionQueue, Cqe};
use crate::device::Context;
use crate::pd::Pd;
use crate::qp::QpInfo;
use crate::srq::Srq;
use crate::transport::IbRemoteDctInfo;
use crate::types::GrhAttr;
use crate::wqe::{
    AddressVector, CtrlSeg, DataSeg, HasData, InfiniBand, InlineHeader, NoData,
    OrderedWqeTable, RdmaSeg, RoCE, SubmissionError, WQEBB_SIZE, WqeFlags,
    WqeHandle, WqeOpcode, calc_wqebb_cnt,
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
#[derive(Debug, Clone, Default)]
pub struct DctConfig {
    /// DC key for this DCT.
    pub dc_key: u64,
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

/// Remote DCT information for sending (InfiniBand).
///
/// This is an alias for [`IbRemoteDctInfo`] for backward compatibility.
/// New code should use [`IbRemoteDctInfo`] directly.
///
/// For RoCE, use [`crate::transport::RoCERemoteDctInfo`] instead.
pub type RemoteDctInfo = IbRemoteDctInfo;

// =============================================================================
// DCI Send Queue State
// =============================================================================

/// Send Queue state for DCI.
///
/// Generic over the table type `TableType` which determines dense vs sparse behavior.
/// Both table types use interior mutability (Cell) so no RefCell wrapper is needed.
struct DciSendQueueState<Entry, TableType> {
    buf: *mut u8,
    wqe_cnt: u16,
    sqn: u32,
    /// Maximum inline data size (from DCI config)
    max_inline_data: u32,
    pi: Cell<u16>,
    ci: Cell<u16>,
    last_wqe: Cell<Option<(*mut u8, usize)>>,
    dbrec: *mut u32,
    bf_reg: *mut u8,
    bf_size: u32,
    bf_offset: Cell<u32>,
    /// WQE table for tracking in-flight operations.
    /// Uses interior mutability (Cell<Option<Entry>>) so no RefCell needed.
    table: TableType,
    _marker: PhantomData<Entry>,
}

impl<Entry, TableType> DciSendQueueState<Entry, TableType> {
    fn available(&self) -> u16 {
        self.wqe_cnt - self.pi.get().wrapping_sub(self.ci.get())
    }

    fn max_inline_data(&self) -> u32 {
        self.max_inline_data
    }

    fn get_wqe_ptr(&self, idx: u16) -> *mut u8 {
        let offset = ((idx & (self.wqe_cnt - 1)) as usize) * WQEBB_SIZE;
        unsafe { self.buf.add(offset) }
    }

    /// Returns the number of WQEBBs from current PI to the end of the ring buffer.
    fn slots_to_end(&self) -> u16 {
        self.wqe_cnt - (self.pi.get() & (self.wqe_cnt - 1))
    }

    fn advance_pi(&self, count: u16) {
        self.pi.set(self.pi.get().wrapping_add(count));
    }

    fn set_last_wqe(&self, ptr: *mut u8, size: usize) {
        self.last_wqe.set(Some((ptr, size)));
    }

    /// Post a NOP WQE to fill the remaining slots until the ring end.
    ///
    /// # Safety
    /// Caller must ensure there are enough available slots for the NOP WQE.
    unsafe fn post_nop(&self, nop_wqebb_cnt: u16) {
        debug_assert!(nop_wqebb_cnt >= 1, "NOP must be at least 1 WQEBB");
        debug_assert!(
            self.available() >= nop_wqebb_cnt,
            "Not enough slots for NOP"
        );

        let wqe_idx = self.pi.get();
        let wqe_ptr = self.get_wqe_ptr(wqe_idx);

        // Write NOP control segment
        let ds_count = (nop_wqebb_cnt as u8) * 4;
        CtrlSeg::write(
            wqe_ptr,
            0, // opmod = 0 for NOP
            WqeOpcode::Nop as u8,
            wqe_idx,
            self.sqn,
            ds_count,
            0,
            0,
        );

        self.advance_pi(nop_wqebb_cnt);
        self.set_last_wqe(wqe_ptr, (nop_wqebb_cnt as usize) * WQEBB_SIZE);
    }

    /// Ring the doorbell using regular doorbell write.
    fn ring_doorbell(&self) {
        let Some((last_wqe_ptr, _)) = self.last_wqe.get() else {
            return;
        };
        self.last_wqe.set(None);

        mmio_flush_writes!();

        unsafe {
            std::ptr::write_volatile(self.dbrec.add(1), (self.pi.get() as u32).to_be());
        }

        udma_to_device_barrier!();

        self.ring_db(last_wqe_ptr);
    }

    fn ring_db(&self, wqe_ptr: *mut u8) {
        let bf = unsafe { self.bf_reg.add(self.bf_offset.get() as usize) as *mut u64 };
        let ctrl = wqe_ptr as *const u64;
        unsafe {
            std::ptr::write_volatile(bf, *ctrl);
        }
        mmio_flush_writes!();
        self.bf_offset.set(self.bf_offset.get() ^ self.bf_size);
    }
}

impl<Entry> DciSendQueueState<Entry, OrderedWqeTable<Entry>> {
    fn process_completion(&self, wqe_idx: u16) -> Option<Entry> {
        let entry = self.table.take(wqe_idx)?;
        // ci_delta is the accumulated PI value at completion
        self.ci.set(entry.ci_delta);
        Some(entry.data)
    }
}

// =============================================================================
// DCI
// =============================================================================

/// Type alias for DCI with ordered WQE table (InfiniBand transport).
pub type DciWithTable<Entry, OnComplete> = Dci<Entry, InfiniBand, OrderedWqeTable<Entry>, OnComplete>;

/// Type alias for DCI with ordered WQE table (InfiniBand transport).
pub type DciIb<Entry, OnComplete> = Dci<Entry, InfiniBand, OrderedWqeTable<Entry>, OnComplete>;

/// Type alias for DCI with ordered WQE table (RoCE transport).
pub type DciRoCE<Entry, OnComplete> = Dci<Entry, RoCE, OrderedWqeTable<Entry>, OnComplete>;

/// DC Initiator (DCI).
///
/// Used for sending RDMA operations to DCTs.
/// Created using mlx5dv_create_qp with DC type.
///
/// Type parameter `Entry` is the entry type stored in the WQE table for tracking
/// in-flight operations. When a completion arrives, the associated entry is
/// returned via the callback.
/// Type parameter `Transport` determines InfiniBand vs RoCE transport.
/// Type parameter `TableType` determines sparse vs dense table behavior.
/// Type parameter `OnComplete` is the completion callback type.
pub struct Dci<Entry, Transport, TableType, OnComplete> {
    qp: NonNull<mlx5_sys::ibv_qp>,
    state: Cell<DcQpState>,
    /// Maximum inline data size (from DCI config)
    max_inline_data: u32,
    sq: Option<DciSendQueueState<Entry, TableType>>,
    callback: OnComplete,
    /// Weak reference to the CQ for unregistration on drop
    send_cq: Weak<CompletionQueue>,
    /// Keep the PD alive while this DCI exists.
    _pd: Pd,
    /// Phantom data for transport type parameter
    _transport: PhantomData<Transport>,
}

impl Context {
    /// Create a DCI (DC Initiator) using mlx5dv_create_qp.
    ///
    /// Only signaled WQEs have entries stored in the WQE table.
    /// The callback is invoked for each completion with the CQE and the entry.
    ///
    /// # Arguments
    /// * `pd` - Protection Domain
    /// * `send_cq` - Completion Queue for send completions
    /// * `config` - DCI configuration
    /// * `callback` - Completion callback `Fn(Cqe, Entry)` called for each signaled completion
    ///
    /// # Errors
    /// Returns an error if the DCI cannot be created.
    ///
    /// # Note
    /// The send_cq must have `init_direct_access()` called before this function.
    pub fn create_dci<Entry, OnComplete>(
        &self,
        pd: &Pd,
        send_cq: &Rc<CompletionQueue>,
        config: &DciConfig,
        callback: OnComplete,
    ) -> io::Result<Rc<RefCell<DciWithTable<Entry, OnComplete>>>>
    where
        Entry: 'static,
        OnComplete: Fn(Cqe, Entry) + 'static,
    {
        let dci = self.create_dci_raw(pd, send_cq, config, callback)?;
        let dci_rc = Rc::new(RefCell::new(dci));
        let qpn = dci_rc.borrow().qpn();

        // Register this DCI with the CQ for completion dispatch
        send_cq.register_queue(qpn, Rc::downgrade(&dci_rc) as _);

        Ok(dci_rc)
    }

    fn create_dci_raw<Entry, OnComplete>(
        &self,
        pd: &Pd,
        send_cq: &Rc<CompletionQueue>,
        config: &DciConfig,
        callback: OnComplete,
    ) -> io::Result<DciWithTable<Entry, OnComplete>>
    where
        OnComplete: Fn(Cqe, Entry),
    {
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

            let qp = mlx5_sys::mlx5dv_create_qp(self.as_ptr(), &mut qp_attr, &mut mlx5_attr);
            let qp = NonNull::new(qp).ok_or_else(io::Error::last_os_error)?;

            let mut result = Dci {
                qp,
                state: Cell::new(DcQpState::Reset),
                max_inline_data: config.max_inline_data,
                sq: None,
                callback,
                send_cq: Rc::downgrade(send_cq),
                _pd: pd.clone(),
                _transport: PhantomData,
            };

            // Auto-initialize direct access
            DciWithTable::<Entry, OnComplete>::init_direct_access_internal(&mut result)?;

            Ok(result)
        }
    }
}

impl<Entry, Transport, TableType, OnComplete> Drop for Dci<Entry, Transport, TableType, OnComplete> {
    fn drop(&mut self) {
        // Unregister from CQ before destroying QP
        if let Some(cq) = self.send_cq.upgrade() {
            cq.unregister_queue(self.qpn());
        }
        unsafe {
            mlx5_sys::ibv_destroy_qp(self.qp.as_ptr());
        }
    }
}

impl<Entry, Transport, TableType, OnComplete> Dci<Entry, Transport, TableType, OnComplete> {
    /// Get the QP number.
    pub fn qpn(&self) -> u32 {
        unsafe { (*self.qp.as_ptr()).qp_num }
    }

    /// Get the current QP state.
    pub fn state(&self) -> DcQpState {
        self.state.get()
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
                dbrec: dv_qp.dbrec,
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

    /// Transition DCI from RESET to INIT.
    pub fn modify_to_init(&mut self, port: u8) -> io::Result<()> {
        if self.state.get() != DcQpState::Reset {
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

        self.state.set(DcQpState::Init);
        Ok(())
    }

    /// Transition DCI from INIT to RTR.
    pub fn modify_to_rtr(&mut self, port: u8) -> io::Result<()> {
        if self.state.get() != DcQpState::Init {
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

        self.state.set(DcQpState::Rtr);
        Ok(())
    }

    /// Transition DCI from RTR to RTS.
    pub fn modify_to_rts(&mut self, local_psn: u32, max_rd_atomic: u8) -> io::Result<()> {
        if self.state.get() != DcQpState::Rtr {
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

        self.state.set(DcQpState::Rts);
        Ok(())
    }

    fn sq(&self) -> io::Result<&DciSendQueueState<Entry, TableType>> {
        self.sq
            .as_ref()
            .ok_or_else(|| io::Error::other("direct access not initialized"))
    }

    /// Get the number of available WQEBBs in the Send Queue.
    pub fn sq_available(&self) -> u16 {
        self.sq.as_ref().map(|sq| sq.available()).unwrap_or(0)
    }

    /// Ring the Send Queue doorbell to signal new WQEs.
    pub fn ring_sq_doorbell(&self) {
        if let Some(sq) = self.sq.as_ref() {
            sq.ring_doorbell();
        }
    }

}

impl<Entry, OnComplete> DciWithTable<Entry, OnComplete> {
    /// Initialize direct queue access (internal implementation).
    fn init_direct_access_internal(&mut self) -> io::Result<()> {
        if self.sq.is_some() {
            return Ok(()); // Already initialized
        }

        let info = self.query_info()?;
        let wqe_cnt = info.sq_wqe_cnt as u16;

        self.sq = Some(DciSendQueueState {
            buf: info.sq_buf,
            wqe_cnt,
            sqn: info.sqn,
            max_inline_data: self.max_inline_data,
            pi: Cell::new(0),
            ci: Cell::new(0),
            last_wqe: Cell::new(None),
            dbrec: info.dbrec,
            bf_reg: info.bf_reg,
            bf_size: info.bf_size,
            bf_offset: Cell::new(0),
            table: OrderedWqeTable::new(wqe_cnt),
            _marker: PhantomData,
        });

        Ok(())
    }

    /// Activate DCI (transition to RTS).
    /// Direct queue access is auto-initialized at creation time.
    pub fn activate(&mut self, port: u8, local_psn: u32, max_rd_atomic: u8) -> io::Result<()> {
        self.modify_to_init(port)?;
        self.modify_to_rtr(port)?;
        self.modify_to_rts(local_psn, max_rd_atomic)?;
        Ok(())
    }
}

// =============================================================================
// CompletionTarget impl for DciWithTable
// =============================================================================

impl<Entry, OnComplete> CompletionTarget for DciWithTable<Entry, OnComplete>
where
    OnComplete: Fn(Cqe, Entry),
{
    fn qpn(&self) -> u32 {
        Dci::qpn(self)
    }

    fn dispatch_cqe(&self, cqe: Cqe) {
        if let Some(sq) = self.sq.as_ref()
            && let Some(entry) = sq.process_completion(cqe.wqe_counter)
        {
            (self.callback)(cqe, entry);
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
///
/// Type parameter `T` is the entry type stored in the SRQ for tracking
/// in-flight receives.
pub struct Dct<T> {
    qp: NonNull<mlx5_sys::ibv_qp>,
    dc_key: u64,
    state: DcQpState,
    /// Keep the PD alive while this DCT exists.
    _pd: Pd,
    /// SRQ for receive processing.
    srq: Srq<T>,
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
    pub fn create_dct<T>(
        &self,
        pd: &Pd,
        srq: &Srq<T>,
        cq: &CompletionQueue,
        config: &DctConfig,
    ) -> io::Result<Dct<T>> {
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

            let qp = mlx5_sys::mlx5dv_create_qp(self.as_ptr(), &mut qp_attr, &mut mlx5_attr);
            NonNull::new(qp).map_or(Err(io::Error::last_os_error()), |qp| {
                Ok(Dct {
                    qp,
                    dc_key: config.dc_key,
                    state: DcQpState::Reset,
                    _pd: pd.clone(),
                    srq: srq.clone(),
                })
            })
        }
    }
}

impl<T> Drop for Dct<T> {
    fn drop(&mut self) {
        unsafe {
            mlx5_sys::ibv_destroy_qp(self.qp.as_ptr());
        }
    }
}

impl<T> Dct<T> {
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
    ///
    /// DCT requires STATE, PATH_MTU, MIN_RNR_TIMER, and AV attributes for RTR transition.
    pub fn modify_to_rtr(&mut self, port: u8, min_rnr_timer: u8) -> io::Result<()> {
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
            attr.min_rnr_timer = min_rnr_timer;
            attr.ah_attr.port_num = port;

            let mask = mlx5_sys::ibv_qp_attr_mask_IBV_QP_STATE
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_PATH_MTU
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
    pub fn activate(&mut self, port: u8, access_flags: u32, min_rnr_timer: u8) -> io::Result<()> {
        self.modify_to_init(port, access_flags)?;
        self.modify_to_rtr(port, min_rnr_timer)?;
        Ok(())
    }

    /// Get remote DCT info for senders.
    pub fn remote_info(&self, local_identifier: u16) -> RemoteDctInfo {
        RemoteDctInfo {
            dct_number: self.dctn(),
            dc_key: self.dc_key,
            local_identifier,
        }
    }

    /// Get access to the SRQ.
    pub fn srq(&self) -> &Srq<T> {
        &self.srq
    }

    /// Process a receive completion and return the associated entry.
    ///
    /// Call this when a receive CQE for this DCT is received.
    ///
    /// # Arguments
    /// * `wqe_idx` - WQE index from the CQE (wqe_counter field)
    pub fn process_recv_completion(&self, wqe_idx: u16) -> Option<T> {
        self.srq.process_recv_completion(wqe_idx)
    }
}

// =============================================================================
// Simplified WQE Builder API
// =============================================================================

use crate::wqe::{
    AtomicSeg,
    // Flags
    TxFlags,
};

// =============================================================================
// Maximum WQEBB Calculation Functions for DCI
// =============================================================================

/// Calculate the padded inline data size (16-byte aligned).
///
/// inline_padded = ((4 + max_inline_data + 15) & !15)
#[inline]
fn calc_inline_padded(max_inline_data: u32) -> usize {
    ((4 + max_inline_data as usize) + 15) & !15
}

/// Calculate maximum WQEBB count for DCI SEND operation.
///
/// Layout: ctrl(16) + AV(48) + inline_padded
#[inline]
fn calc_max_wqebb_send(max_inline_data: u32) -> u16 {
    let size = CtrlSeg::SIZE + AddressVector::SIZE + calc_inline_padded(max_inline_data);
    calc_wqebb_cnt(size)
}

/// Calculate maximum WQEBB count for DCI WRITE operation.
///
/// Layout: ctrl(16) + AV(48) + rdma(16) + inline_padded
#[inline]
fn calc_max_wqebb_write(max_inline_data: u32) -> u16 {
    let size = CtrlSeg::SIZE + AddressVector::SIZE + RdmaSeg::SIZE + calc_inline_padded(max_inline_data);
    calc_wqebb_cnt(size)
}

/// Calculate maximum WQEBB count for DCI READ operation.
///
/// Layout: ctrl(16) + AV(48) + rdma(16) + sge(16)
#[inline]
fn calc_max_wqebb_read() -> u16 {
    let size = CtrlSeg::SIZE + AddressVector::SIZE + RdmaSeg::SIZE + DataSeg::SIZE;
    calc_wqebb_cnt(size)
}

/// Calculate maximum WQEBB count for DCI Atomic operation.
///
/// Layout: ctrl(16) + AV(48) + rdma(16) + atomic(16) + sge(16)
#[inline]
fn calc_max_wqebb_atomic() -> u16 {
    let size = CtrlSeg::SIZE + AddressVector::SIZE + RdmaSeg::SIZE + AtomicSeg::SIZE + DataSeg::SIZE;
    calc_wqebb_cnt(size)
}

/// Calculate maximum WQEBB count for DCI NOP operation.
///
/// Layout: ctrl(16) + AV(48)
#[inline]
fn calc_max_wqebb_nop() -> u16 {
    let size = CtrlSeg::SIZE + AddressVector::SIZE;
    calc_wqebb_cnt(size)
}

/// Pending AV info to be written after ctrl segment.
enum PendingAv<'a> {
    None,
    Ib { dc_key: u64, dctn: u32, dlid: u16 },
    RoCE { dc_key: u64, dctn: u32, grh: &'a GrhAttr },
}

/// Internal WQE builder core for DCI that handles direct buffer writes.
struct DciWqeCore<'a, Entry> {
    sq: &'a DciSendQueueState<Entry, OrderedWqeTable<Entry>>,
    wqe_ptr: *mut u8,
    wqe_idx: u16,
    offset: usize,
    ds_count: u8,
    signaled: bool,
    entry: Option<Entry>,
    pending_av: PendingAv<'a>,
}

impl<'a, Entry> DciWqeCore<'a, Entry> {
    #[inline]
    fn new(
        sq: &'a DciSendQueueState<Entry, OrderedWqeTable<Entry>>,
        entry: Option<Entry>,
    ) -> Self {
        let wqe_idx = sq.pi.get();
        let wqe_ptr = sq.get_wqe_ptr(wqe_idx);
        Self {
            sq,
            wqe_ptr,
            wqe_idx,
            offset: 0,
            ds_count: 0,
            signaled: entry.is_some(),
            entry,
            pending_av: PendingAv::None,
        }
    }

    #[inline]
    fn available(&self) -> u16 {
        self.sq.available()
    }

    #[inline]
    fn max_inline_data(&self) -> u32 {
        self.sq.max_inline_data()
    }

    /// Store AV parameters for IB transport to be written later.
    #[inline]
    fn set_av_ib(&mut self, dc_key: u64, dctn: u32, dlid: u16) {
        self.pending_av = PendingAv::Ib { dc_key, dctn, dlid };
    }

    /// Store AV parameters for RoCE transport to be written later.
    #[inline]
    fn set_av_roce(&mut self, dc_key: u64, dctn: u32, grh: &'a GrhAttr) {
        self.pending_av = PendingAv::RoCE { dc_key, dctn, grh };
    }

    /// Write the pending AV segment (must be called after write_ctrl).
    #[inline]
    fn write_pending_av(&mut self) {
        match self.pending_av {
            PendingAv::None => {}
            PendingAv::Ib { dc_key, dctn, dlid } => {
                unsafe {
                    AddressVector::write_ib(self.wqe_ptr.add(self.offset), dc_key, dctn, dlid);
                }
                self.offset += AddressVector::SIZE;
                self.ds_count += 3;
            }
            PendingAv::RoCE { dc_key, dctn, grh } => {
                unsafe {
                    AddressVector::write_roce(self.wqe_ptr.add(self.offset), dc_key, dctn, grh);
                }
                self.offset += AddressVector::SIZE;
                self.ds_count += 3;
            }
        }
    }

    #[inline]
    fn write_ctrl(&mut self, opcode: WqeOpcode, flags: WqeFlags, imm: u32) {
        let flags = if self.signaled {
            flags | WqeFlags::COMPLETION
        } else {
            flags
        };
        unsafe {
            CtrlSeg::write(
                self.wqe_ptr,
                0,
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
    }

    #[inline]
    fn write_rdma(&mut self, addr: u64, rkey: u32) {
        unsafe {
            RdmaSeg::write(self.wqe_ptr.add(self.offset), addr, rkey);
        }
        self.offset += RdmaSeg::SIZE;
        self.ds_count += 1;
    }

    #[inline]
    fn write_sge(&mut self, addr: u64, len: u32, lkey: u32) {
        unsafe {
            DataSeg::write(self.wqe_ptr.add(self.offset), len, lkey, addr);
        }
        self.offset += DataSeg::SIZE;
        self.ds_count += 1;
    }

    #[inline]
    fn write_inline(&mut self, data: &[u8]) {
        let padded_size = unsafe {
            let ptr = self.wqe_ptr.add(self.offset);
            let size = InlineHeader::write(ptr, data.len() as u32);
            std::ptr::copy_nonoverlapping(data.as_ptr(), ptr.add(4), data.len());
            size
        };
        self.offset += padded_size;
        self.ds_count += (padded_size / 16) as u8;
    }

    #[inline]
    fn write_atomic_cas(&mut self, swap: u64, compare: u64) {
        unsafe {
            AtomicSeg::write_cas(self.wqe_ptr.add(self.offset), swap, compare);
        }
        self.offset += AtomicSeg::SIZE;
        self.ds_count += 1;
    }

    #[inline]
    fn write_atomic_fa(&mut self, add_value: u64) {
        unsafe {
            AtomicSeg::write_fa(self.wqe_ptr.add(self.offset), add_value);
        }
        self.offset += AtomicSeg::SIZE;
        self.ds_count += 1;
    }

    #[inline]
    fn finish_internal(self) -> io::Result<WqeHandle> {
        let wqebb_cnt = calc_wqebb_cnt(self.offset);
        let slots_to_end = self.sq.slots_to_end();

        if wqebb_cnt > slots_to_end && slots_to_end < self.sq.wqe_cnt {
            return self.finish_with_wrap_around(wqebb_cnt, slots_to_end);
        }

        unsafe {
            CtrlSeg::update_ds_cnt(self.wqe_ptr, self.ds_count);
            // Set completion flag if signaled (may not have been set at write_ctrl time)
            if self.signaled {
                CtrlSeg::set_completion_flag(self.wqe_ptr);
            }
        }

        let wqe_idx = self.wqe_idx;
        self.sq.advance_pi(wqebb_cnt);
        self.sq.set_last_wqe(self.wqe_ptr, self.offset);

        if let Some(entry) = self.entry {
            self.sq.table.store(wqe_idx, entry, self.sq.pi.get());
        }

        Ok(WqeHandle {
            wqe_idx,
            size: self.offset,
        })
    }

    #[cold]
    fn finish_with_wrap_around(self, wqebb_cnt: u16, slots_to_end: u16) -> io::Result<WqeHandle> {
        let total_needed = slots_to_end + wqebb_cnt;
        if self.sq.available() < total_needed {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }

        let mut temp_buf = [0u8; 256];
        unsafe {
            std::ptr::copy_nonoverlapping(self.wqe_ptr, temp_buf.as_mut_ptr(), self.offset);
            self.sq.post_nop(slots_to_end);
        }

        let new_wqe_idx = self.sq.pi.get();
        let new_wqe_ptr = self.sq.get_wqe_ptr(new_wqe_idx);

        unsafe {
            std::ptr::copy_nonoverlapping(temp_buf.as_ptr(), new_wqe_ptr, self.offset);
            CtrlSeg::update_wqe_idx(new_wqe_ptr, new_wqe_idx);
            CtrlSeg::update_ds_cnt(new_wqe_ptr, self.ds_count);
            // Set completion flag if signaled (may not have been set at write_ctrl time)
            if self.signaled {
                CtrlSeg::set_completion_flag(new_wqe_ptr);
            }
        }

        self.sq.advance_pi(wqebb_cnt);
        self.sq.set_last_wqe(new_wqe_ptr, self.offset);

        if let Some(entry) = self.entry {
            self.sq.table.store(new_wqe_idx, entry, self.sq.pi.get());
        }

        Ok(WqeHandle {
            wqe_idx: new_wqe_idx,
            size: self.offset,
        })
    }
}

// =============================================================================
// Simplified DCI SQ WQE Entry Point (IB)
// =============================================================================

/// DCI SQ WQE entry point for IB transport.
///
/// DC info (dc_key, dctn, dlid) is passed to entry point method.
#[must_use = "WQE builder must be finished"]
pub struct DciSqWqeEntryPoint<'a, Entry> {
    core: DciWqeCore<'a, Entry>,
}

impl<'a, Entry> DciSqWqeEntryPoint<'a, Entry> {
    /// Create a new DCI entry point with IB AV.
    #[inline]
    pub(crate) fn new(
        sq: &'a DciSendQueueState<Entry, OrderedWqeTable<Entry>>,
        dc_key: u64,
        dctn: u32,
        dlid: u16,
    ) -> Self {
        let mut core = DciWqeCore::new(sq, None);
        // Store AV parameters for later - AV is written AFTER ctrl segment
        core.set_av_ib(dc_key, dctn, dlid);
        Self { core }
    }

    // -------------------------------------------------------------------------
    // SEND operations
    // -------------------------------------------------------------------------

    /// Start building a SEND WQE.
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the maximum
    /// possible WQE size (based on max_inline_data).
    #[inline]
    pub fn send(mut self, flags: TxFlags) -> io::Result<DciSendWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_send(self.core.max_inline_data());
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        self.core.write_ctrl(WqeOpcode::Send, flags, 0);
        self.core.write_pending_av();
        Ok(DciSendWqeBuilder { core: self.core, _data: PhantomData })
    }

    /// Start building a SEND with immediate data WQE.
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the maximum
    /// possible WQE size (based on max_inline_data).
    #[inline]
    pub fn send_imm(mut self, flags: TxFlags, imm: u32) -> io::Result<DciSendWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_send(self.core.max_inline_data());
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        self.core.write_ctrl(WqeOpcode::SendImm, flags, imm);
        self.core.write_pending_av();
        Ok(DciSendWqeBuilder { core: self.core, _data: PhantomData })
    }

    // -------------------------------------------------------------------------
    // WRITE operations
    // -------------------------------------------------------------------------

    /// Start building an RDMA WRITE WQE.
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the maximum
    /// possible WQE size (based on max_inline_data).
    #[inline]
    pub fn write(mut self, flags: TxFlags, remote_addr: u64, rkey: u32) -> io::Result<DciWriteWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_write(self.core.max_inline_data());
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        self.core.write_ctrl(WqeOpcode::RdmaWrite, flags, 0);
        self.core.write_pending_av();
        self.core.write_rdma(remote_addr, rkey);
        Ok(DciWriteWqeBuilder { core: self.core, _data: PhantomData })
    }

    /// Start building an RDMA WRITE with immediate data WQE.
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the maximum
    /// possible WQE size (based on max_inline_data).
    #[inline]
    pub fn write_imm(mut self, flags: TxFlags, remote_addr: u64, rkey: u32, imm: u32) -> io::Result<DciWriteWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_write(self.core.max_inline_data());
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        self.core.write_ctrl(WqeOpcode::RdmaWriteImm, flags, imm);
        self.core.write_pending_av();
        self.core.write_rdma(remote_addr, rkey);
        Ok(DciWriteWqeBuilder { core: self.core, _data: PhantomData })
    }

    // -------------------------------------------------------------------------
    // READ operations
    // -------------------------------------------------------------------------

    /// Start building an RDMA READ WQE.
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the WQE.
    #[inline]
    pub fn read(mut self, flags: TxFlags, remote_addr: u64, rkey: u32) -> io::Result<DciReadWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_read();
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        self.core.write_ctrl(WqeOpcode::RdmaRead, flags, 0);
        self.core.write_pending_av();
        self.core.write_rdma(remote_addr, rkey);
        Ok(DciReadWqeBuilder { core: self.core, _data: PhantomData })
    }

    // -------------------------------------------------------------------------
    // Atomic operations
    // -------------------------------------------------------------------------

    /// Start building an Atomic Compare-and-Swap WQE.
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the WQE.
    #[inline]
    pub fn cas(mut self, flags: TxFlags, remote_addr: u64, rkey: u32, swap: u64, compare: u64) -> io::Result<DciAtomicWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_atomic();
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        self.core.write_ctrl(WqeOpcode::AtomicCs, flags, 0);
        self.core.write_pending_av();
        self.core.write_rdma(remote_addr, rkey);
        self.core.write_atomic_cas(swap, compare);
        Ok(DciAtomicWqeBuilder { core: self.core, _data: PhantomData })
    }

    /// Start building an Atomic Fetch-and-Add WQE.
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the WQE.
    #[inline]
    pub fn fetch_add(mut self, flags: TxFlags, remote_addr: u64, rkey: u32, add_value: u64) -> io::Result<DciAtomicWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_atomic();
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        self.core.write_ctrl(WqeOpcode::AtomicFa, flags, 0);
        self.core.write_pending_av();
        self.core.write_rdma(remote_addr, rkey);
        self.core.write_atomic_fa(add_value);
        Ok(DciAtomicWqeBuilder { core: self.core, _data: PhantomData })
    }

    // -------------------------------------------------------------------------
    // NOP operation
    // -------------------------------------------------------------------------

    /// Start building a NOP WQE.
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the WQE.
    #[inline]
    pub fn nop(mut self, flags: TxFlags) -> io::Result<DciNopWqeBuilder<'a, Entry>> {
        let max_wqebb = calc_max_wqebb_nop();
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        self.core.write_ctrl(WqeOpcode::Nop, flags, 0);
        self.core.write_pending_av();
        Ok(DciNopWqeBuilder { core: self.core })
    }
}

// =============================================================================
// Simplified DCI SQ WQE Entry Point (RoCE)
// =============================================================================

/// DCI SQ WQE entry point for RoCE transport.
#[must_use = "WQE builder must be finished"]
pub struct DciRoceSqWqeEntryPoint<'a, Entry> {
    core: DciWqeCore<'a, Entry>,
}

impl<'a, Entry> DciRoceSqWqeEntryPoint<'a, Entry> {
    /// Create a new DCI entry point with RoCE AV.
    #[inline]
    pub(crate) fn new(
        sq: &'a DciSendQueueState<Entry, OrderedWqeTable<Entry>>,
        dc_key: u64,
        dctn: u32,
        grh: &'a GrhAttr,
    ) -> Self {
        let mut core = DciWqeCore::new(sq, None);
        // Store AV parameters for later - AV is written AFTER ctrl segment
        core.set_av_roce(dc_key, dctn, grh);
        Self { core }
    }

    // -------------------------------------------------------------------------
    // SEND operations
    // -------------------------------------------------------------------------

    /// Start building a SEND WQE.
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the maximum
    /// possible WQE size (based on max_inline_data).
    #[inline]
    pub fn send(mut self, flags: TxFlags) -> io::Result<DciSendWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_send(self.core.max_inline_data());
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        self.core.write_ctrl(WqeOpcode::Send, flags, 0);
        self.core.write_pending_av();
        Ok(DciSendWqeBuilder { core: self.core, _data: PhantomData })
    }

    /// Start building a SEND with immediate data WQE.
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the maximum
    /// possible WQE size (based on max_inline_data).
    #[inline]
    pub fn send_imm(mut self, flags: TxFlags, imm: u32) -> io::Result<DciSendWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_send(self.core.max_inline_data());
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        self.core.write_ctrl(WqeOpcode::SendImm, flags, imm);
        self.core.write_pending_av();
        Ok(DciSendWqeBuilder { core: self.core, _data: PhantomData })
    }

    // -------------------------------------------------------------------------
    // WRITE operations
    // -------------------------------------------------------------------------

    /// Start building an RDMA WRITE WQE.
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the maximum
    /// possible WQE size (based on max_inline_data).
    #[inline]
    pub fn write(mut self, flags: TxFlags, remote_addr: u64, rkey: u32) -> io::Result<DciWriteWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_write(self.core.max_inline_data());
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        self.core.write_ctrl(WqeOpcode::RdmaWrite, flags, 0);
        self.core.write_pending_av();
        self.core.write_rdma(remote_addr, rkey);
        Ok(DciWriteWqeBuilder { core: self.core, _data: PhantomData })
    }

    /// Start building an RDMA WRITE with immediate data WQE.
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the maximum
    /// possible WQE size (based on max_inline_data).
    #[inline]
    pub fn write_imm(mut self, flags: TxFlags, remote_addr: u64, rkey: u32, imm: u32) -> io::Result<DciWriteWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_write(self.core.max_inline_data());
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        self.core.write_ctrl(WqeOpcode::RdmaWriteImm, flags, imm);
        self.core.write_pending_av();
        self.core.write_rdma(remote_addr, rkey);
        Ok(DciWriteWqeBuilder { core: self.core, _data: PhantomData })
    }

    // -------------------------------------------------------------------------
    // READ operations
    // -------------------------------------------------------------------------

    /// Start building an RDMA READ WQE.
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the WQE.
    #[inline]
    pub fn read(mut self, flags: TxFlags, remote_addr: u64, rkey: u32) -> io::Result<DciReadWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_read();
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        self.core.write_ctrl(WqeOpcode::RdmaRead, flags, 0);
        self.core.write_pending_av();
        self.core.write_rdma(remote_addr, rkey);
        Ok(DciReadWqeBuilder { core: self.core, _data: PhantomData })
    }

    // -------------------------------------------------------------------------
    // Atomic operations
    // -------------------------------------------------------------------------

    /// Start building an Atomic Compare-and-Swap WQE.
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the WQE.
    #[inline]
    pub fn cas(mut self, flags: TxFlags, remote_addr: u64, rkey: u32, swap: u64, compare: u64) -> io::Result<DciAtomicWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_atomic();
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        self.core.write_ctrl(WqeOpcode::AtomicCs, flags, 0);
        self.core.write_pending_av();
        self.core.write_rdma(remote_addr, rkey);
        self.core.write_atomic_cas(swap, compare);
        Ok(DciAtomicWqeBuilder { core: self.core, _data: PhantomData })
    }

    /// Start building an Atomic Fetch-and-Add WQE.
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the WQE.
    #[inline]
    pub fn fetch_add(mut self, flags: TxFlags, remote_addr: u64, rkey: u32, add_value: u64) -> io::Result<DciAtomicWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_atomic();
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        self.core.write_ctrl(WqeOpcode::AtomicFa, flags, 0);
        self.core.write_pending_av();
        self.core.write_rdma(remote_addr, rkey);
        self.core.write_atomic_fa(add_value);
        Ok(DciAtomicWqeBuilder { core: self.core, _data: PhantomData })
    }

    // -------------------------------------------------------------------------
    // NOP operation
    // -------------------------------------------------------------------------

    /// Start building a NOP WQE.
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the WQE.
    #[inline]
    pub fn nop(mut self, flags: TxFlags) -> io::Result<DciNopWqeBuilder<'a, Entry>> {
        let max_wqebb = calc_max_wqebb_nop();
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        self.core.write_ctrl(WqeOpcode::Nop, flags, 0);
        self.core.write_pending_av();
        Ok(DciNopWqeBuilder { core: self.core })
    }
}

// =============================================================================
// DCI Send WQE Builder
// =============================================================================

/// DCI SEND WQE builder.
///
/// Uses type-state pattern: `DataState` is `NoData` initially,
/// transitions to `HasData` after calling `sge()` or `inline()`.
#[must_use = "WQE builder must be finished"]
pub struct DciSendWqeBuilder<'a, Entry, DataState> {
    core: DciWqeCore<'a, Entry>,
    _data: PhantomData<DataState>,
}

/// sge/inline: NoData state, transitions to HasData.
impl<'a, Entry> DciSendWqeBuilder<'a, Entry, NoData> {
    /// Add a scatter/gather entry.
    #[inline]
    pub fn sge(mut self, addr: u64, len: u32, lkey: u32) -> DciSendWqeBuilder<'a, Entry, HasData> {
        self.core.write_sge(addr, len, lkey);
        DciSendWqeBuilder { core: self.core, _data: PhantomData }
    }

    /// Add inline data.
    #[inline]
    pub fn inline(mut self, data: &[u8]) -> DciSendWqeBuilder<'a, Entry, HasData> {
        self.core.write_inline(data);
        DciSendWqeBuilder { core: self.core, _data: PhantomData }
    }
}

/// finish methods: only available in HasData state.
impl<'a, Entry> DciSendWqeBuilder<'a, Entry, HasData> {
    /// Add another scatter/gather entry.
    #[inline]
    pub fn sge(mut self, addr: u64, len: u32, lkey: u32) -> Self {
        self.core.write_sge(addr, len, lkey);
        self
    }

    /// Add more inline data.
    #[inline]
    pub fn inline(mut self, data: &[u8]) -> Self {
        self.core.write_inline(data);
        self
    }

    /// Finish the WQE construction (unsignaled).
    #[inline]
    pub fn finish_unsignaled(self) -> io::Result<WqeHandle> {
        self.core.finish_internal()
    }

    /// Finish the WQE construction with completion signaling.
    #[inline]
    pub fn finish_signaled(mut self, entry: Entry) -> io::Result<WqeHandle> {
        self.core.entry = Some(entry);
        self.core.signaled = true;
        self.core.finish_internal()
    }
}

// =============================================================================
// DCI Write WQE Builder
// =============================================================================

/// DCI RDMA WRITE WQE builder.
///
/// Uses type-state pattern: `DataState` is `NoData` initially,
/// transitions to `HasData` after calling `sge()` or `inline()`.
#[must_use = "WQE builder must be finished"]
pub struct DciWriteWqeBuilder<'a, Entry, DataState> {
    core: DciWqeCore<'a, Entry>,
    _data: PhantomData<DataState>,
}

/// sge/inline: NoData state, transitions to HasData.
impl<'a, Entry> DciWriteWqeBuilder<'a, Entry, NoData> {
    /// Add a scatter/gather entry.
    #[inline]
    pub fn sge(mut self, addr: u64, len: u32, lkey: u32) -> DciWriteWqeBuilder<'a, Entry, HasData> {
        self.core.write_sge(addr, len, lkey);
        DciWriteWqeBuilder { core: self.core, _data: PhantomData }
    }

    /// Add inline data.
    #[inline]
    pub fn inline(mut self, data: &[u8]) -> DciWriteWqeBuilder<'a, Entry, HasData> {
        self.core.write_inline(data);
        DciWriteWqeBuilder { core: self.core, _data: PhantomData }
    }
}

/// finish methods: only available in HasData state.
impl<'a, Entry> DciWriteWqeBuilder<'a, Entry, HasData> {
    /// Add another scatter/gather entry.
    #[inline]
    pub fn sge(mut self, addr: u64, len: u32, lkey: u32) -> Self {
        self.core.write_sge(addr, len, lkey);
        self
    }

    /// Add more inline data.
    #[inline]
    pub fn inline(mut self, data: &[u8]) -> Self {
        self.core.write_inline(data);
        self
    }

    /// Finish the WQE construction (unsignaled).
    #[inline]
    pub fn finish_unsignaled(self) -> io::Result<WqeHandle> {
        self.core.finish_internal()
    }

    /// Finish the WQE construction with completion signaling.
    #[inline]
    pub fn finish_signaled(mut self, entry: Entry) -> io::Result<WqeHandle> {
        self.core.entry = Some(entry);
        self.core.signaled = true;
        self.core.finish_internal()
    }
}

// =============================================================================
// DCI Read WQE Builder
// =============================================================================

/// DCI RDMA READ WQE builder.
///
/// Uses type-state pattern: `DataState` is `NoData` initially,
/// transitions to `HasData` after calling `sge()`.
/// Note: READ operations do not support inline data.
#[must_use = "WQE builder must be finished"]
pub struct DciReadWqeBuilder<'a, Entry, DataState> {
    core: DciWqeCore<'a, Entry>,
    _data: PhantomData<DataState>,
}

/// sge: NoData state, transitions to HasData.
impl<'a, Entry> DciReadWqeBuilder<'a, Entry, NoData> {
    /// Add a scatter/gather entry.
    #[inline]
    pub fn sge(mut self, addr: u64, len: u32, lkey: u32) -> DciReadWqeBuilder<'a, Entry, HasData> {
        self.core.write_sge(addr, len, lkey);
        DciReadWqeBuilder { core: self.core, _data: PhantomData }
    }
}

/// finish methods: only available in HasData state.
impl<'a, Entry> DciReadWqeBuilder<'a, Entry, HasData> {
    /// Add another scatter/gather entry.
    #[inline]
    pub fn sge(mut self, addr: u64, len: u32, lkey: u32) -> Self {
        self.core.write_sge(addr, len, lkey);
        self
    }

    /// Finish the WQE construction (unsignaled).
    #[inline]
    pub fn finish_unsignaled(self) -> io::Result<WqeHandle> {
        self.core.finish_internal()
    }

    /// Finish the WQE construction with completion signaling.
    #[inline]
    pub fn finish_signaled(mut self, entry: Entry) -> io::Result<WqeHandle> {
        self.core.entry = Some(entry);
        self.core.signaled = true;
        self.core.finish_internal()
    }
}

// =============================================================================
// DCI Atomic WQE Builder
// =============================================================================

/// DCI Atomic operation WQE builder.
///
/// Uses type-state pattern: `DataState` is `NoData` initially,
/// transitions to `HasData` after calling `sge()`.
#[must_use = "WQE builder must be finished"]
pub struct DciAtomicWqeBuilder<'a, Entry, DataState> {
    core: DciWqeCore<'a, Entry>,
    _data: PhantomData<DataState>,
}

/// sge: NoData state, transitions to HasData.
impl<'a, Entry> DciAtomicWqeBuilder<'a, Entry, NoData> {
    /// Add a scatter/gather entry for the result buffer.
    #[inline]
    pub fn sge(mut self, addr: u64, len: u32, lkey: u32) -> DciAtomicWqeBuilder<'a, Entry, HasData> {
        self.core.write_sge(addr, len, lkey);
        DciAtomicWqeBuilder { core: self.core, _data: PhantomData }
    }
}

/// finish methods: only available in HasData state.
impl<'a, Entry> DciAtomicWqeBuilder<'a, Entry, HasData> {
    /// Add another scatter/gather entry.
    #[inline]
    pub fn sge(mut self, addr: u64, len: u32, lkey: u32) -> Self {
        self.core.write_sge(addr, len, lkey);
        self
    }

    /// Finish the WQE construction (unsignaled).
    #[inline]
    pub fn finish_unsignaled(self) -> io::Result<WqeHandle> {
        self.core.finish_internal()
    }

    /// Finish the WQE construction with completion signaling.
    #[inline]
    pub fn finish_signaled(mut self, entry: Entry) -> io::Result<WqeHandle> {
        self.core.entry = Some(entry);
        self.core.signaled = true;
        self.core.finish_internal()
    }
}

// =============================================================================
// DCI NOP WQE Builder
// =============================================================================

/// DCI NOP WQE builder.
///
/// NOP operations have no data segment, so `finish_*()` methods are
/// available immediately without needing to call `sge()` or `inline()`.
#[must_use = "WQE builder must be finished"]
pub struct DciNopWqeBuilder<'a, Entry> {
    core: DciWqeCore<'a, Entry>,
}

impl<'a, Entry> DciNopWqeBuilder<'a, Entry> {
    /// Finish the WQE construction (unsignaled).
    #[inline]
    pub fn finish_unsignaled(self) -> io::Result<WqeHandle> {
        self.core.finish_internal()
    }

    /// Finish the WQE construction with completion signaling.
    #[inline]
    pub fn finish_signaled(mut self, entry: Entry) -> io::Result<WqeHandle> {
        self.core.entry = Some(entry);
        self.core.signaled = true;
        self.core.finish_internal()
    }
}

// =============================================================================
// DCI Entry Points
// =============================================================================

impl<Entry, OnComplete> DciIb<Entry, OnComplete> {
    /// Get a SQ WQE builder for InfiniBand transport.
    ///
    /// # Example
    /// ```ignore
    /// dci.sq_wqe(dc_key, dctn, dlid)?
    ///     .write(TxFlags::empty(), remote_addr, rkey)
    ///     .sge(local_addr, len, lkey)
    ///     .finish_signaled(entry)?;
    /// dci.ring_sq_doorbell();
    /// ```
    #[inline]
    pub fn sq_wqe(&self, dc_key: u64, dctn: u32, dlid: u16) -> io::Result<DciSqWqeEntryPoint<'_, Entry>> {
        let sq = self.sq()?;
        if sq.available() == 0 {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        Ok(DciSqWqeEntryPoint::new(sq, dc_key, dctn, dlid))
    }
}

impl<Entry, OnComplete> DciRoCE<Entry, OnComplete> {
    /// Get a SQ WQE builder for RoCE transport.
    ///
    /// # Example
    /// ```ignore
    /// dci.sq_wqe(dc_key, dctn, &grh)?
    ///     .write(TxFlags::empty(), remote_addr, rkey)
    ///     .sge(local_addr, len, lkey)
    ///     .finish_signaled(entry)?;
    /// dci.ring_sq_doorbell();
    /// ```
    #[inline]
    pub fn sq_wqe<'a>(&'a self, dc_key: u64, dctn: u32, grh: &'a GrhAttr) -> io::Result<DciRoceSqWqeEntryPoint<'a, Entry>> {
        let sq = self.sq()?;
        if sq.available() == 0 {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        Ok(DciRoceSqWqeEntryPoint::new(sq, dc_key, dctn, grh))
    }
}

// =============================================================================
// DCI BlueFlame Batch Builder
// =============================================================================

/// BlueFlame buffer size in bytes (256B doorbell window).
const BLUEFLAME_BUFFER_SIZE: usize = 256;

/// BlueFlame WQE batch builder for DCI (InfiniBand transport).
///
/// Allows building multiple WQEs that fit within the BlueFlame buffer (256 bytes).
/// Each WQE is copied to a contiguous buffer and submitted via BlueFlame doorbell
/// when `finish()` is called.
pub struct DciBlueflameWqeBatch<'a, Entry> {
    sq: &'a DciSendQueueState<Entry, OrderedWqeTable<Entry>>,
    dc_key: u64,
    dctn: u32,
    dlid: u16,
    buffer: [u8; BLUEFLAME_BUFFER_SIZE],
    offset: usize,
}

impl<'a, Entry> DciBlueflameWqeBatch<'a, Entry> {
    fn new(sq: &'a DciSendQueueState<Entry, OrderedWqeTable<Entry>>, dc_key: u64, dctn: u32, dlid: u16) -> Self {
        Self {
            sq,
            dc_key,
            dctn,
            dlid,
            buffer: [0u8; BLUEFLAME_BUFFER_SIZE],
            offset: 0,
        }
    }

    /// Get a WQE builder for the next WQE in the batch.
    ///
    /// # Errors
    /// Returns `SqFull` if the send queue doesn't have enough space.
    #[inline]
    pub fn wqe(&mut self) -> Result<DciBlueflameWqeEntryPoint<'_, 'a, Entry>, SubmissionError> {
        if self.sq.available() == 0 {
            return Err(SubmissionError::SqFull);
        }
        Ok(DciBlueflameWqeEntryPoint { batch: self })
    }

    /// Finish the batch and submit all WQEs via BlueFlame doorbell.
    #[inline]
    pub fn finish(self) {
        if self.offset == 0 {
            return;
        }

        mmio_flush_writes!();

        unsafe {
            std::ptr::write_volatile(
                self.sq.dbrec.add(1),
                (self.sq.pi.get() as u32).to_be(),
            );
        }

        udma_to_device_barrier!();

        if self.sq.bf_size > 0 {
            let bf_offset = self.sq.bf_offset.get();
            let bf = unsafe { self.sq.bf_reg.add(bf_offset as usize) };

            let mut src = self.buffer.as_ptr();
            let mut dst = bf;
            let mut remaining = self.offset;
            while remaining > 0 {
                unsafe {
                    mlx5_bf_copy!(dst, src);
                    src = src.add(WQEBB_SIZE);
                    dst = dst.add(WQEBB_SIZE);
                }
                remaining = remaining.saturating_sub(WQEBB_SIZE);
            }

            mmio_flush_writes!();
            self.sq.bf_offset.set(bf_offset ^ self.sq.bf_size);
        }
    }
}

/// BlueFlame WQE entry point for DCI (InfiniBand).
#[must_use = "WQE builder must be finished"]
pub struct DciBlueflameWqeEntryPoint<'b, 'a, Entry> {
    batch: &'b mut DciBlueflameWqeBatch<'a, Entry>,
}

impl<'b, 'a, Entry> DciBlueflameWqeEntryPoint<'b, 'a, Entry> {
    /// Start building a SEND WQE.
    #[inline]
    pub fn send(self, flags: TxFlags) -> Result<DciBlueflameWqeBuilder<'b, 'a, Entry, NoData>, SubmissionError> {
        let mut core = DciBlueflameWqeCore::new(self.batch)?;
        core.write_ctrl(WqeOpcode::Send, flags, 0);
        core.write_dc_av_ib()?;
        Ok(DciBlueflameWqeBuilder { core, _data: PhantomData })
    }

    /// Start building a SEND with immediate data WQE.
    #[inline]
    pub fn send_imm(self, flags: TxFlags, imm: u32) -> Result<DciBlueflameWqeBuilder<'b, 'a, Entry, NoData>, SubmissionError> {
        let mut core = DciBlueflameWqeCore::new(self.batch)?;
        core.write_ctrl(WqeOpcode::SendImm, flags, imm);
        core.write_dc_av_ib()?;
        Ok(DciBlueflameWqeBuilder { core, _data: PhantomData })
    }

    /// Start building an RDMA WRITE WQE.
    #[inline]
    pub fn write(self, flags: TxFlags, remote_addr: u64, rkey: u32) -> Result<DciBlueflameWqeBuilder<'b, 'a, Entry, NoData>, SubmissionError> {
        let mut core = DciBlueflameWqeCore::new(self.batch)?;
        core.write_ctrl(WqeOpcode::RdmaWrite, flags, 0);
        core.write_dc_av_ib()?;
        core.write_rdma(remote_addr, rkey)?;
        Ok(DciBlueflameWqeBuilder { core, _data: PhantomData })
    }

    /// Start building an RDMA WRITE with immediate data WQE.
    #[inline]
    pub fn write_imm(self, flags: TxFlags, remote_addr: u64, rkey: u32, imm: u32) -> Result<DciBlueflameWqeBuilder<'b, 'a, Entry, NoData>, SubmissionError> {
        let mut core = DciBlueflameWqeCore::new(self.batch)?;
        core.write_ctrl(WqeOpcode::RdmaWriteImm, flags, imm);
        core.write_dc_av_ib()?;
        core.write_rdma(remote_addr, rkey)?;
        Ok(DciBlueflameWqeBuilder { core, _data: PhantomData })
    }
}

/// Internal core for DCI BlueFlame WQE construction.
struct DciBlueflameWqeCore<'b, 'a, Entry> {
    batch: &'b mut DciBlueflameWqeBatch<'a, Entry>,
    wqe_start: usize,
    offset: usize,
    ds_count: u8,
    signaled: bool,
}

impl<'b, 'a, Entry> DciBlueflameWqeCore<'b, 'a, Entry> {
    #[inline]
    fn new(batch: &'b mut DciBlueflameWqeBatch<'a, Entry>) -> Result<Self, SubmissionError> {
        if batch.offset + CtrlSeg::SIZE > BLUEFLAME_BUFFER_SIZE {
            return Err(SubmissionError::BlueflameOverflow);
        }
        Ok(Self {
            wqe_start: batch.offset,
            offset: batch.offset,
            batch,
            ds_count: 0,
            signaled: false,
        })
    }

    #[inline]
    fn remaining(&self) -> usize {
        BLUEFLAME_BUFFER_SIZE - self.offset
    }

    #[inline]
    fn write_ctrl(&mut self, opcode: WqeOpcode, flags: TxFlags, imm: u32) {
        let wqe_idx = self.batch.sq.pi.get();
        let flags = WqeFlags::from_bits_truncate(flags.bits());
        unsafe {
            CtrlSeg::write(
                self.batch.buffer.as_mut_ptr().add(self.offset),
                0,
                opcode as u8,
                wqe_idx,
                self.batch.sq.sqn,
                0,
                flags.bits(),
                imm,
            );
        }
        self.offset += CtrlSeg::SIZE;
        self.ds_count = 1;
    }

    #[inline]
    fn write_dc_av_ib(&mut self) -> Result<(), SubmissionError> {
        if self.remaining() < AddressVector::SIZE {
            return Err(SubmissionError::BlueflameOverflow);
        }
        unsafe {
            AddressVector::write_ib(
                self.batch.buffer.as_mut_ptr().add(self.offset),
                self.batch.dc_key,
                self.batch.dctn,
                self.batch.dlid,
            );
        }
        self.offset += AddressVector::SIZE;
        self.ds_count += (AddressVector::SIZE / 16) as u8;
        Ok(())
    }

    #[inline]
    fn write_rdma(&mut self, addr: u64, rkey: u32) -> Result<(), SubmissionError> {
        if self.remaining() < RdmaSeg::SIZE {
            return Err(SubmissionError::BlueflameOverflow);
        }
        unsafe {
            RdmaSeg::write(self.batch.buffer.as_mut_ptr().add(self.offset), addr, rkey);
        }
        self.offset += RdmaSeg::SIZE;
        self.ds_count += 1;
        Ok(())
    }

    #[inline]
    fn write_sge(&mut self, addr: u64, len: u32, lkey: u32) -> Result<(), SubmissionError> {
        if self.remaining() < DataSeg::SIZE {
            return Err(SubmissionError::BlueflameOverflow);
        }
        unsafe {
            DataSeg::write(self.batch.buffer.as_mut_ptr().add(self.offset), len, lkey, addr);
        }
        self.offset += DataSeg::SIZE;
        self.ds_count += 1;
        Ok(())
    }

    #[inline]
    fn write_inline(&mut self, data: &[u8]) -> Result<(), SubmissionError> {
        let padded_size = ((4 + data.len()) + 15) & !15;
        if self.remaining() < padded_size {
            return Err(SubmissionError::BlueflameOverflow);
        }
        unsafe {
            let ptr = self.batch.buffer.as_mut_ptr().add(self.offset);
            InlineHeader::write(ptr, data.len() as u32);
            std::ptr::copy_nonoverlapping(data.as_ptr(), ptr.add(4), data.len());
        }
        self.offset += padded_size;
        self.ds_count += (padded_size / 16) as u8;
        Ok(())
    }

    #[inline]
    fn finish_internal(self, entry: Option<Entry>) -> Result<(), SubmissionError> {
        unsafe {
            CtrlSeg::update_ds_cnt(
                self.batch.buffer.as_mut_ptr().add(self.wqe_start),
                self.ds_count,
            );
            if self.signaled || entry.is_some() {
                CtrlSeg::set_completion_flag(
                    self.batch.buffer.as_mut_ptr().add(self.wqe_start),
                );
            }
        }

        let wqe_size = self.offset - self.wqe_start;
        let wqebb_cnt = calc_wqebb_cnt(wqe_size);

        let wqe_idx = self.batch.sq.pi.get();
        if let Some(entry) = entry {
            let ci_delta = wqe_idx.wrapping_add(wqebb_cnt);
            self.batch.sq.table.store(wqe_idx, entry, ci_delta);
        }

        self.batch.offset = self.offset;
        self.batch.sq.advance_pi(wqebb_cnt);

        Ok(())
    }
}

/// DCI BlueFlame WQE builder with type-state for data segments.
#[must_use = "WQE builder must be finished"]
pub struct DciBlueflameWqeBuilder<'b, 'a, Entry, DataState> {
    core: DciBlueflameWqeCore<'b, 'a, Entry>,
    _data: PhantomData<DataState>,
}

impl<'b, 'a, Entry> DciBlueflameWqeBuilder<'b, 'a, Entry, NoData> {
    /// Add a scatter/gather entry.
    #[inline]
    pub fn sge(mut self, addr: u64, len: u32, lkey: u32) -> Result<DciBlueflameWqeBuilder<'b, 'a, Entry, HasData>, SubmissionError> {
        self.core.write_sge(addr, len, lkey)?;
        Ok(DciBlueflameWqeBuilder { core: self.core, _data: PhantomData })
    }

    /// Add inline data.
    #[inline]
    pub fn inline(mut self, data: &[u8]) -> Result<DciBlueflameWqeBuilder<'b, 'a, Entry, HasData>, SubmissionError> {
        self.core.write_inline(data)?;
        Ok(DciBlueflameWqeBuilder { core: self.core, _data: PhantomData })
    }
}

impl<'b, 'a, Entry> DciBlueflameWqeBuilder<'b, 'a, Entry, HasData> {
    /// Add another scatter/gather entry.
    #[inline]
    pub fn sge(mut self, addr: u64, len: u32, lkey: u32) -> Result<Self, SubmissionError> {
        self.core.write_sge(addr, len, lkey)?;
        Ok(self)
    }

    /// Add more inline data.
    #[inline]
    pub fn inline(mut self, data: &[u8]) -> Result<Self, SubmissionError> {
        self.core.write_inline(data)?;
        Ok(self)
    }

    /// Finish the WQE construction (unsignaled).
    #[inline]
    pub fn finish(self) -> Result<(), SubmissionError> {
        self.core.finish_internal(None)
    }

    /// Finish the WQE construction with completion signaling.
    #[inline]
    pub fn finish_signaled(self, entry: Entry) -> Result<(), SubmissionError> {
        self.core.finish_internal(Some(entry))
    }
}

impl<Entry, OnComplete> DciIb<Entry, OnComplete> {
    /// Get a BlueFlame batch builder for low-latency WQE submission.
    ///
    /// Multiple WQEs can be accumulated in the BlueFlame buffer (up to 256 bytes)
    /// and submitted together via a single BlueFlame doorbell.
    ///
    /// # Example
    /// ```ignore
    /// let mut bf = dci.blueflame_sq_wqe(dc_key, dctn, dlid)?;
    /// bf.wqe()?.send(TxFlags::empty()).inline(&data).finish()?;
    /// bf.wqe()?.send(TxFlags::empty()).inline(&data).finish()?;
    /// bf.finish();
    /// ```
    ///
    /// # Errors
    /// Returns `BlueflameNotAvailable` if BlueFlame is not supported on this device.
    #[inline]
    pub fn blueflame_sq_wqe(&self, dc_key: u64, dctn: u32, dlid: u16) -> Result<DciBlueflameWqeBatch<'_, Entry>, SubmissionError> {
        let sq = self.sq.as_ref().ok_or(SubmissionError::SqFull)?;
        if sq.bf_size == 0 {
            return Err(SubmissionError::BlueflameNotAvailable);
        }
        Ok(DciBlueflameWqeBatch::new(sq, dc_key, dctn, dlid))
    }
}
