//! DC (Dynamically Connected) transport.
//!
//! DC provides scalable connectionless RDMA operations using:
//! - DCI (DC Initiator): Sends RDMA operations to DCTs
//! - DCT (DC Target): Receives RDMA operations via SRQ

pub mod builder;

use std::cell::{Cell, RefCell};
use std::rc::{Rc, Weak};
use std::{io, marker::PhantomData, mem::MaybeUninit, ptr::NonNull};

use crate::CompletionTarget;
use crate::cq::{Cq, Cqe};
use crate::device::Context;
use crate::mono_cq::MonoCq;
use crate::pd::Pd;
use crate::qp::QpInfo;
use crate::srq::Srq;
use crate::transport::IbRemoteDctInfo;
use crate::wqe::{
    CTRL_SEG_SIZE, InfiniBand, OrderedWqeTable, RoCE, WQEBB_SIZE, WqeOpcode,
    write_ctrl_seg,
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
pub(super) struct DciSendQueueState<Entry, TableType> {
    pub(super) buf: *mut u8,
    pub(super) wqe_cnt: u16,
    pub(super) sqn: u32,
    /// Maximum inline data size (from DCI config)
    pub(super) max_inline_data: u32,
    pub(super) pi: Cell<u16>,
    pub(super) ci: Cell<u16>,
    pub(super) last_wqe: Cell<Option<(*mut u8, usize)>>,
    pub(super) dbrec: *mut u32,
    pub(super) bf_reg: *mut u8,
    pub(super) bf_size: u32,
    pub(super) bf_offset: Cell<u32>,
    /// WQE table for tracking in-flight operations.
    /// Uses interior mutability (Cell<Option<Entry>>) so no RefCell needed.
    pub(super) table: TableType,
    _marker: PhantomData<Entry>,
}

impl<Entry, TableType> DciSendQueueState<Entry, TableType> {
    pub(super) fn available(&self) -> u16 {
        self.wqe_cnt - self.pi.get().wrapping_sub(self.ci.get())
    }

    pub(super) fn max_inline_data(&self) -> u32 {
        self.max_inline_data
    }

    pub(super) fn get_wqe_ptr(&self, idx: u16) -> *mut u8 {
        let offset = ((idx & (self.wqe_cnt - 1)) as usize) * WQEBB_SIZE;
        unsafe { self.buf.add(offset) }
    }

    /// Returns the number of WQEBBs from current PI to the end of the ring buffer.
    pub(super) fn slots_to_end(&self) -> u16 {
        self.wqe_cnt - (self.pi.get() & (self.wqe_cnt - 1))
    }

    pub(super) fn advance_pi(&self, count: u16) {
        self.pi.set(self.pi.get().wrapping_add(count));
    }

    pub(super) fn set_last_wqe(&self, ptr: *mut u8, size: usize) {
        self.last_wqe.set(Some((ptr, size)));
    }

    /// Post a NOP WQE to fill the remaining slots until the ring end.
    ///
    /// # Safety
    /// Caller must ensure there are enough available slots for the NOP WQE.
    pub(super) unsafe fn post_nop(&self, nop_wqebb_cnt: u16) {
        debug_assert!(nop_wqebb_cnt >= 1, "NOP must be at least 1 WQEBB");
        debug_assert!(
            self.available() >= nop_wqebb_cnt,
            "Not enough slots for NOP"
        );

        let wqe_idx = self.pi.get();
        let wqe_ptr = self.get_wqe_ptr(wqe_idx);

        // Write NOP control segment
        let ds_count = (nop_wqebb_cnt as u8) * 4;
        write_ctrl_seg(
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
    send_cq: Weak<Cq>,
    /// Keep the PD alive while this DCI exists.
    _pd: Pd,
    /// Phantom data for transport type parameter
    _transport: PhantomData<Transport>,
}

// =============================================================================
// DCI Builder (type-state pattern)
// =============================================================================

/// CQ not yet configured.
pub struct NoCq;
/// CQ configured.
pub struct CqSet;

/// DCI Builder with type-state pattern for CQ configuration.
pub struct DciBuilder<'a, Entry, T, CqState, OnComplete> {
    ctx: &'a Context,
    pd: &'a Pd,
    config: DciConfig,
    send_cq_ptr: *mut mlx5_sys::ibv_cq,
    send_cq_weak: Option<Weak<Cq>>,
    callback: OnComplete,
    _marker: PhantomData<(Entry, T, CqState)>,
}

impl Context {
    /// Create a DCI Builder.
    ///
    /// # Example
    /// ```ignore
    /// let dci = ctx.dci_builder::<u64>(&pd, &config)
    ///     .sq_cq(cq.clone(), |cqe, entry| { /* handle completion */ })
    ///     .build()?;
    /// ```
    pub fn dci_builder<'a, Entry>(
        &'a self,
        pd: &'a Pd,
        config: &DciConfig,
    ) -> DciBuilder<'a, Entry, InfiniBand, NoCq, ()> {
        DciBuilder {
            ctx: self,
            pd,
            config: config.clone(),
            send_cq_ptr: std::ptr::null_mut(),
            send_cq_weak: None,
            callback: (),
            _marker: PhantomData,
        }
    }
}

impl<'a, Entry, T> DciBuilder<'a, Entry, T, NoCq, ()> {
    /// Set normal CQ with callback.
    pub fn sq_cq<OnComplete>(
        self,
        cq: Rc<Cq>,
        callback: OnComplete,
    ) -> DciBuilder<'a, Entry, T, CqSet, OnComplete>
    where
        OnComplete: Fn(Cqe, Entry) + 'static,
    {
        DciBuilder {
            ctx: self.ctx,
            pd: self.pd,
            config: self.config,
            send_cq_ptr: cq.as_ptr(),
            send_cq_weak: Some(Rc::downgrade(&cq)),
            callback,
            _marker: PhantomData,
        }
    }

    /// Set MonoCq (callback is stored in MonoCq).
    pub fn sq_mono_cq<Q, F>(
        self,
        mono_cq: &Rc<MonoCq<Q, F>>,
    ) -> DciBuilder<'a, Entry, T, CqSet, ()>
    where
        F: Fn(Cqe, Q::Entry) + 'static,
        Q: crate::mono_cq::CompletionSource,
    {
        DciBuilder {
            ctx: self.ctx,
            pd: self.pd,
            config: self.config,
            send_cq_ptr: mono_cq.as_ptr(),
            send_cq_weak: None, // MonoCq doesn't need CQ registration
            callback: (),
            _marker: PhantomData,
        }
    }
}

impl<'a, Entry, T> DciBuilder<'a, Entry, T, NoCq, ()> {
    /// Switch to RoCE transport.
    pub fn for_roce(self) -> DciBuilder<'a, Entry, RoCE, NoCq, ()> {
        DciBuilder {
            ctx: self.ctx,
            pd: self.pd,
            config: self.config,
            send_cq_ptr: self.send_cq_ptr,
            send_cq_weak: self.send_cq_weak,
            callback: self.callback,
            _marker: PhantomData,
        }
    }
}

impl<'a, Entry, OnComplete> DciBuilder<'a, Entry, InfiniBand, CqSet, OnComplete>
where
    Entry: 'static,
    OnComplete: Fn(Cqe, Entry) + 'static,
{
    /// Build the DCI with normal CQ.
    pub fn build(self) -> io::Result<Rc<RefCell<DciWithTable<Entry, OnComplete>>>> {
        unsafe {
            let mut qp_attr: mlx5_sys::ibv_qp_init_attr_ex = MaybeUninit::zeroed().assume_init();
            qp_attr.qp_type = mlx5_sys::ibv_qp_type_IBV_QPT_DRIVER;
            qp_attr.send_cq = self.send_cq_ptr;
            qp_attr.recv_cq = self.send_cq_ptr;
            qp_attr.cap.max_send_wr = self.config.max_send_wr;
            qp_attr.cap.max_recv_wr = 0;
            qp_attr.cap.max_send_sge = self.config.max_send_sge;
            qp_attr.cap.max_recv_sge = 0;
            qp_attr.cap.max_inline_data = self.config.max_inline_data;
            qp_attr.comp_mask = mlx5_sys::ibv_qp_init_attr_mask_IBV_QP_INIT_ATTR_PD;
            qp_attr.pd = self.pd.as_ptr();

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
            let qp = NonNull::new(qp).ok_or_else(io::Error::last_os_error)?;

            let mut result = Dci {
                qp,
                state: Cell::new(DcQpState::Reset),
                max_inline_data: self.config.max_inline_data,
                sq: None,
                callback: self.callback,
                send_cq: self.send_cq_weak.clone().unwrap_or_else(|| Weak::new()),
                _pd: self.pd.clone(),
                _transport: PhantomData,
            };

            DciWithTable::<Entry, OnComplete>::init_direct_access_internal(&mut result)?;

            let dci_rc = Rc::new(RefCell::new(result));
            let qpn = dci_rc.borrow().qpn();

            // Register with CQ if using normal Cq
            if let Some(cq) = self.send_cq_weak.as_ref().and_then(|w| w.upgrade()) {
                cq.register_queue(qpn, Rc::downgrade(&dci_rc) as _);
            }

            Ok(dci_rc)
        }
    }
}

/// Type alias for DCI with MonoCq (no callback stored).
pub type DciForMonoCq<Entry> = Dci<Entry, InfiniBand, OrderedWqeTable<Entry>, ()>;

impl<'a, Entry> DciBuilder<'a, Entry, InfiniBand, CqSet, ()>
where
    Entry: 'static,
{
    /// Build the DCI for MonoCq.
    pub fn build(self) -> io::Result<Rc<RefCell<DciForMonoCq<Entry>>>> {
        unsafe {
            let mut qp_attr: mlx5_sys::ibv_qp_init_attr_ex = MaybeUninit::zeroed().assume_init();
            qp_attr.qp_type = mlx5_sys::ibv_qp_type_IBV_QPT_DRIVER;
            qp_attr.send_cq = self.send_cq_ptr;
            qp_attr.recv_cq = self.send_cq_ptr;
            qp_attr.cap.max_send_wr = self.config.max_send_wr;
            qp_attr.cap.max_recv_wr = 0;
            qp_attr.cap.max_send_sge = self.config.max_send_sge;
            qp_attr.cap.max_recv_sge = 0;
            qp_attr.cap.max_inline_data = self.config.max_inline_data;
            qp_attr.comp_mask = mlx5_sys::ibv_qp_init_attr_mask_IBV_QP_INIT_ATTR_PD;
            qp_attr.pd = self.pd.as_ptr();

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
            let qp = NonNull::new(qp).ok_or_else(io::Error::last_os_error)?;

            let mut result = Dci {
                qp,
                state: Cell::new(DcQpState::Reset),
                max_inline_data: self.config.max_inline_data,
                sq: None,
                callback: (),
                send_cq: Weak::new(),
                _pd: self.pd.clone(),
                _transport: PhantomData,
            };

            DciForMonoCq::<Entry>::init_direct_access_internal(&mut result)?;

            Ok(Rc::new(RefCell::new(result)))
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

// =============================================================================
// DCT Builder (type-state pattern)
// =============================================================================

/// DCT Builder with type-state pattern for CQ configuration.
pub struct DctBuilder<'a, Entry, CqState> {
    ctx: &'a Context,
    pd: &'a Pd,
    srq: &'a Srq<Entry>,
    config: DctConfig,
    recv_cq_ptr: *mut mlx5_sys::ibv_cq,
    _marker: PhantomData<CqState>,
}

impl Context {
    /// Create a DCT Builder.
    ///
    /// # Example
    /// ```ignore
    /// let dct = ctx.dct_builder(&pd, &srq, &config)
    ///     .recv_cq(&cq)
    ///     .build()?;
    /// ```
    pub fn dct_builder<'a, Entry>(
        &'a self,
        pd: &'a Pd,
        srq: &'a Srq<Entry>,
        config: &DctConfig,
    ) -> DctBuilder<'a, Entry, NoCq> {
        DctBuilder {
            ctx: self,
            pd,
            srq,
            config: config.clone(),
            recv_cq_ptr: std::ptr::null_mut(),
            _marker: PhantomData,
        }
    }
}

impl<'a, Entry> DctBuilder<'a, Entry, NoCq> {
    /// Set normal CQ for receive completions.
    pub fn recv_cq(self, cq: &Cq) -> DctBuilder<'a, Entry, CqSet> {
        DctBuilder {
            ctx: self.ctx,
            pd: self.pd,
            srq: self.srq,
            config: self.config,
            recv_cq_ptr: cq.as_ptr(),
            _marker: PhantomData,
        }
    }

    /// Set MonoCq for receive completions.
    pub fn recv_mono_cq<Q, F>(self, mono_cq: &MonoCq<Q, F>) -> DctBuilder<'a, Entry, CqSet>
    where
        F: Fn(Cqe, Q::Entry) + 'static,
        Q: crate::mono_cq::CompletionSource,
    {
        DctBuilder {
            ctx: self.ctx,
            pd: self.pd,
            srq: self.srq,
            config: self.config,
            recv_cq_ptr: mono_cq.as_ptr(),
            _marker: PhantomData,
        }
    }
}

impl<'a, Entry> DctBuilder<'a, Entry, CqSet> {
    /// Build the DCT.
    pub fn build(self) -> io::Result<Dct<Entry>> {
        unsafe {
            let mut qp_attr: mlx5_sys::ibv_qp_init_attr_ex = MaybeUninit::zeroed().assume_init();
            qp_attr.qp_type = mlx5_sys::ibv_qp_type_IBV_QPT_DRIVER;
            qp_attr.send_cq = self.recv_cq_ptr;
            qp_attr.recv_cq = self.recv_cq_ptr;
            qp_attr.srq = self.srq.as_ptr();
            qp_attr.cap.max_recv_wr = 0;
            qp_attr.cap.max_recv_sge = 0;
            qp_attr.comp_mask = mlx5_sys::ibv_qp_init_attr_mask_IBV_QP_INIT_ATTR_PD;
            qp_attr.pd = self.pd.as_ptr();

            let mut mlx5_attr: mlx5_sys::mlx5dv_qp_init_attr = MaybeUninit::zeroed().assume_init();
            mlx5_attr.comp_mask =
                mlx5_sys::mlx5dv_qp_init_attr_mask_MLX5DV_QP_INIT_ATTR_MASK_DC as u64;
            mlx5_attr.dc_init_attr.dc_type = mlx5_sys::mlx5dv_dc_type_MLX5DV_DCTYPE_DCT;
            mlx5_attr.dc_init_attr.__bindgen_anon_1.dct_access_key = self.config.dc_key;

            let qp = mlx5_sys::mlx5dv_create_qp(self.ctx.as_ptr(), &mut qp_attr, &mut mlx5_attr);
            NonNull::new(qp).map_or(Err(io::Error::last_os_error()), |qp| {
                Ok(Dct {
                    qp,
                    dc_key: self.config.dc_key,
                    state: DcQpState::Reset,
                    _pd: self.pd.clone(),
                    srq: self.srq.clone(),
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

