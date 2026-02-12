//! DC (Dynamically Connected) transport.
//!
//! DC provides scalable connectionless RDMA operations using:
//! - DCI (DC Initiator): Sends RDMA operations to DCTs
//! - DCT (DC Target): Receives RDMA operations via SRQ

use std::cell::{Cell, RefCell};
use std::rc::{Rc, Weak};
use std::{io, marker::PhantomData};

use crate::BuildResult;
use crate::CompletionTarget;
use crate::builder_common::{MaybeMonoCqRegister, register_with_send_cq};
use crate::cq::{Cq, Cqe};
use crate::device::Context;
use crate::devx::{DevxObj, DevxUmem};
use crate::mono_cq::CompletionSource;
use crate::mono_cq::MonoCq;
use crate::pd::Pd;
use crate::prm;
use crate::qp::AlignedBuf;
use crate::srq::Srq;
use crate::transport::IbRemoteDctInfo;
use crate::wqe::{
    InfiniBand, OrderedWqeTable, RoCE, WQEBB_SIZE,
    emit::{DciEmitContext, SendQueueState},
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

// =============================================================================
// DCI Send Queue State
// =============================================================================

// DciSendQueueState is now the unified SendQueueState from crate::wqe::emit.
// Type alias for backward compatibility.
pub(super) type DciSendQueueState<Entry, TableType> = SendQueueState<Entry, TableType>;

// =============================================================================
// DCI
// =============================================================================

/// Type alias for DCI with ordered WQE table (InfiniBand transport).
pub type DciWithTable<Entry, OnComplete> =
    Dci<Entry, InfiniBand, OrderedWqeTable<Entry>, OnComplete>;

/// Type alias for DCI with ordered WQE table (InfiniBand transport).
pub type DciIb<Entry, OnComplete> = Dci<Entry, InfiniBand, OrderedWqeTable<Entry>, OnComplete>;

/// Type alias for DCI with ordered WQE table (RoCE transport).
pub type DciRoCE<Entry, OnComplete> = Dci<Entry, RoCE, OrderedWqeTable<Entry>, OnComplete>;

/// DC Initiator (DCI).
///
/// Used for sending RDMA operations to DCTs.
/// Created using DevX PRM CREATE_QP with DC type.
///
/// Type parameter `Entry` is the entry type stored in the WQE table for tracking
/// in-flight operations. When a completion arrives, the associated entry is
/// returned via the callback.
/// Type parameter `Transport` determines InfiniBand vs RoCE transport.
/// Type parameter `TableType` determines sparse vs dense table behavior.
/// Type parameter `OnComplete` is the completion callback type.
pub struct Dci<Entry, Transport, TableType, OnComplete> {
    _devx_obj: DevxObj,
    _umem: DevxUmem,
    _wq_buf: AlignedBuf,
    qpn: u32,
    state: Cell<DcQpState>,
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
// DCI DevX creation
// =============================================================================

/// Resources created by DevX DCI creation.
struct DciResources {
    devx_obj: DevxObj,
    umem: DevxUmem,
    wq_buf: AlignedBuf,
    qpn: u32,
    sq_buf: *mut u8,
    dbrec: *mut u32,
    bf_reg: *mut u8,
    bf_size: u32,
    log_sq_size: u32,
}

/// Create a DCI via DevX PRM CREATE_QP command.
///
/// # Safety
/// The caller must ensure `ctx`, `pd` are valid and the context was opened with DevX.
unsafe fn create_devx_dci(
    ctx: &Context,
    pd: &Pd,
    send_cqn: u32,
    config: &DciConfig,
) -> io::Result<DciResources> {
    // DCI is SQ-only (no RQ). Layout: [SQ | DBREC]
    let log_sq_size = if config.max_send_wr == 0 {
        0
    } else {
        (config.max_send_wr as usize).next_power_of_two().trailing_zeros()
    };
    let sq_size = (1usize << log_sq_size) * WQEBB_SIZE;
    let dbrec_offset = (sq_size + 63) & !63;
    let total_size = (dbrec_offset + 64 + 4095) & !4095;

    let wq_buf = AlignedBuf::new(total_size);
    let umem = ctx.register_umem(wq_buf.as_ptr(), total_size, 7)?;
    let uar = ctx.devx_uar()?;

    // Build CREATE_QP PRM command (no PAS needed with UMEM mode)
    let cmd_in_size = prm::CREATE_QP_IN_BASE_SIZE;
    let mut cmd_in = vec![0u8; cmd_in_size];
    let mut cmd_out = vec![0u8; prm::CREATE_QP_OUT_SIZE];

    let umem_id = umem.umem_id();

    prm::prm_set(
        &mut cmd_in,
        prm::CREATE_QP_IN_OPCODE.0,
        prm::CREATE_QP_IN_OPCODE.1,
        prm::MLX5_CMD_OP_CREATE_QP,
    );

    // WQ UMEM fields
    prm::prm_set(
        &mut cmd_in,
        prm::CREATE_QP_IN_WQ_UMEM_VALID.0,
        prm::CREATE_QP_IN_WQ_UMEM_VALID.1,
        1,
    );
    prm::prm_set(
        &mut cmd_in,
        prm::CREATE_QP_IN_WQ_UMEM_ID.0,
        prm::CREATE_QP_IN_WQ_UMEM_ID.1,
        umem_id,
    );
    prm::prm_set64(&mut cmd_in, prm::CREATE_QP_IN_WQ_UMEM_OFFSET, 0);

    let q = prm::CREATE_QP_IN_QPC_OFF;
    prm::prm_set(&mut cmd_in, q + prm::QPC_ST.0, prm::QPC_ST.1, prm::QP_ST_DCI);
    prm::prm_set(&mut cmd_in, q + prm::QPC_PD.0, prm::QPC_PD.1, pd.pdn());
    prm::prm_set(&mut cmd_in, q + prm::QPC_UAR_PAGE.0, prm::QPC_UAR_PAGE.1, uar.page_id());
    prm::prm_set(&mut cmd_in, q + prm::QPC_LOG_SQ_SIZE.0, prm::QPC_LOG_SQ_SIZE.1, log_sq_size);
    prm::prm_set(&mut cmd_in, q + prm::QPC_LOG_RQ_SIZE.0, prm::QPC_LOG_RQ_SIZE.1, 0); // No RQ
    prm::prm_set(&mut cmd_in, q + prm::QPC_NO_SQ.0, prm::QPC_NO_SQ.1, 0); // Has SQ
    prm::prm_set(&mut cmd_in, q + prm::QPC_LOG_MSG_MAX.0, prm::QPC_LOG_MSG_MAX.1, 30);
    prm::prm_set(&mut cmd_in, q + prm::QPC_CQN_SND.0, prm::QPC_CQN_SND.1, send_cqn);
    prm::prm_set(&mut cmd_in, q + prm::QPC_CQN_RCV.0, prm::QPC_CQN_RCV.1, send_cqn);

    // DBR UMEM fields
    prm::prm_set(&mut cmd_in, q + prm::QPC_DBR_UMEM_VALID.0, prm::QPC_DBR_UMEM_VALID.1, 1);
    prm::prm_set(&mut cmd_in, q + prm::QPC_DBR_UMEM_ID.0, prm::QPC_DBR_UMEM_ID.1, umem_id);
    prm::prm_set64(
        &mut cmd_in,
        q + prm::QPC_DBR_ADDR,
        dbrec_offset as u64,
    );

    // Debug: dump CREATE_QP PRM command for DCI
    eprintln!("=== DCI CREATE_QP PRM command ({} bytes) ===", cmd_in.len());
    eprintln!("  sq_size={}, dbrec_offset={}, total={}", sq_size, dbrec_offset, total_size);
    eprintln!("  log_sq={}, umem_id={}, uar_page={}, pd={}", log_sq_size, umem_id, uar.page_id(), pd.pdn());
    eprintln!("  send_cqn={}", send_cqn);
    for i in (0..cmd_in.len()).step_by(16) {
        let end = (i + 16).min(cmd_in.len());
        let hex: Vec<String> = cmd_in[i..end].iter().map(|b| format!("{:02x}", b)).collect();
        if hex.iter().any(|s| s != "00") {
            eprintln!("  {:04x}: {}", i, hex.join(" "));
        }
    }

    let devx_obj = ctx.devx_obj_create(&cmd_in, &mut cmd_out)?;
    let qpn = prm::prm_get(&cmd_out, prm::CREATE_QP_OUT_QPN.0, prm::CREATE_QP_OUT_QPN.1);

    eprintln!("  DCI QPN: 0x{:x}", qpn);

    let sq_buf = wq_buf.as_ptr();
    let dbrec = wq_buf.as_ptr().add(dbrec_offset) as *mut u32;

    Ok(DciResources {
        devx_obj,
        umem,
        wq_buf,
        qpn,
        sq_buf,
        dbrec,
        bf_reg: uar.reg_addr(),
        bf_size: 256,
        log_sq_size,
    })
}

// =============================================================================
// DCI Builder (type-state pattern)
// =============================================================================

/// CQ not yet configured.
pub struct NoCq;
/// CQ configured.
pub struct CqSet;

/// DCI Builder with type-state pattern for CQ configuration.
pub struct DciBuilder<'a, Entry, T, CqState, OnComplete, SqMono = ()> {
    ctx: &'a Context,
    pd: &'a Pd,
    config: DciConfig,
    send_cqn: u32,
    send_cq_weak: Option<Weak<Cq>>,
    callback: OnComplete,
    sq_mono_cq_ref: SqMono,
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
    ) -> DciBuilder<'a, Entry, InfiniBand, NoCq, (), ()> {
        DciBuilder {
            ctx: self,
            pd,
            config: config.clone(),
            send_cqn: 0,
            send_cq_weak: None,
            callback: (),
            sq_mono_cq_ref: (),
            _marker: PhantomData,
        }
    }
}

impl<'a, Entry, T> DciBuilder<'a, Entry, T, NoCq, (), ()> {
    /// Set normal CQ with callback.
    pub fn sq_cq<OnComplete>(
        self,
        cq: Rc<Cq>,
        callback: OnComplete,
    ) -> DciBuilder<'a, Entry, T, CqSet, OnComplete, ()>
    where
        OnComplete: Fn(Cqe, Entry) + 'static,
    {
        DciBuilder {
            ctx: self.ctx,
            pd: self.pd,
            config: self.config,
            send_cqn: cq.cqn(),
            send_cq_weak: Some(Rc::downgrade(&cq)),
            callback,
            sq_mono_cq_ref: (),
            _marker: PhantomData,
        }
    }

    /// Set MonoCq (callback is stored in MonoCq).
    pub fn sq_mono_cq<Q>(
        self,
        mono_cq: &Rc<MonoCq<Q>>,
    ) -> DciBuilder<'a, Entry, T, CqSet, (), Rc<MonoCq<Q>>>
    where
        Q: crate::mono_cq::CompletionSource,
    {
        DciBuilder {
            ctx: self.ctx,
            pd: self.pd,
            config: self.config,
            send_cqn: mono_cq.cqn(),
            send_cq_weak: None, // MonoCq doesn't need CQ registration
            callback: (),
            sq_mono_cq_ref: Rc::clone(mono_cq),
            _marker: PhantomData,
        }
    }
}

impl<'a, Entry, T> DciBuilder<'a, Entry, T, NoCq, (), ()> {
    /// Switch to RoCE transport.
    pub fn for_roce(self) -> DciBuilder<'a, Entry, RoCE, NoCq, (), ()> {
        DciBuilder {
            ctx: self.ctx,
            pd: self.pd,
            config: self.config,
            send_cqn: self.send_cqn,
            send_cq_weak: self.send_cq_weak,
            callback: self.callback,
            sq_mono_cq_ref: self.sq_mono_cq_ref,
            _marker: PhantomData,
        }
    }
}

impl<'a, Entry, OnComplete> DciBuilder<'a, Entry, InfiniBand, CqSet, OnComplete, ()>
where
    Entry: 'static,
    OnComplete: Fn(Cqe, Entry) + 'static,
{
    /// Build the DCI with normal CQ.
    pub fn build(self) -> BuildResult<DciWithTable<Entry, OnComplete>> {
        unsafe {
            let res = create_devx_dci(self.ctx, self.pd, self.send_cqn, &self.config)?;

            let wqe_cnt = 1u16 << res.log_sq_size;
            let sq = Some(DciSendQueueState {
                buf: res.sq_buf,
                wqe_cnt,
                sqn: res.qpn, // SQN = QPN for DCI
                pi: Cell::new(0),
                ci: Cell::new(0),
                last_wqe: Cell::new(None),
                dbrec: res.dbrec,
                bf_reg: res.bf_reg,
                bf_size: res.bf_size,
                bf_offset: Cell::new(0),
                table: OrderedWqeTable::new(wqe_cnt),
                _marker: PhantomData,
            });

            let result = Dci {
                _devx_obj: res.devx_obj,
                _umem: res.umem,
                _wq_buf: res.wq_buf,
                qpn: res.qpn,
                state: Cell::new(DcQpState::Reset),
                sq,
                callback: self.callback,
                send_cq: self.send_cq_weak.clone().unwrap_or_default(),
                _pd: self.pd.clone(),
                _transport: PhantomData,
            };

            let dci_rc = Rc::new(RefCell::new(result));
            let qpn = dci_rc.borrow().qpn;

            // Register with CQ if using normal Cq
            register_with_send_cq(qpn, &self.send_cq_weak, &dci_rc);

            Ok(dci_rc)
        }
    }
}

/// Type alias for DCI with MonoCq (no callback stored).
pub type DciForMonoCq<Entry> = Dci<Entry, InfiniBand, OrderedWqeTable<Entry>, ()>;

impl<'a, Entry, SqMono> DciBuilder<'a, Entry, InfiniBand, CqSet, (), SqMono>
where
    Entry: 'static,
    SqMono: MaybeMonoCqRegister<DciForMonoCq<Entry>>,
{
    /// Build the DCI for MonoCq.
    pub fn build(self) -> BuildResult<DciForMonoCq<Entry>> {
        unsafe {
            let res = create_devx_dci(self.ctx, self.pd, self.send_cqn, &self.config)?;

            let wqe_cnt = 1u16 << res.log_sq_size;
            let sq = Some(DciSendQueueState {
                buf: res.sq_buf,
                wqe_cnt,
                sqn: res.qpn, // SQN = QPN for DCI
                pi: Cell::new(0),
                ci: Cell::new(0),
                last_wqe: Cell::new(None),
                dbrec: res.dbrec,
                bf_reg: res.bf_reg,
                bf_size: res.bf_size,
                bf_offset: Cell::new(0),
                table: OrderedWqeTable::new(wqe_cnt),
                _marker: PhantomData,
            });

            let result = Dci {
                _devx_obj: res.devx_obj,
                _umem: res.umem,
                _wq_buf: res.wq_buf,
                qpn: res.qpn,
                state: Cell::new(DcQpState::Reset),
                sq,
                callback: (),
                send_cq: Weak::new(),
                _pd: self.pd.clone(),
                _transport: PhantomData,
            };

            let qp_rc = Rc::new(RefCell::new(result));
            self.sq_mono_cq_ref.maybe_register(&qp_rc);
            Ok(qp_rc)
        }
    }
}

impl<Entry, Transport, TableType, OnComplete> Drop
    for Dci<Entry, Transport, TableType, OnComplete>
{
    fn drop(&mut self) {
        // Unregister from CQ before destroying QP
        if let Some(cq) = self.send_cq.upgrade() {
            cq.unregister_queue(self.qpn);
        }
        // DevxObj handles QP destruction
    }
}

impl<Entry, Transport, TableType, OnComplete> Dci<Entry, Transport, TableType, OnComplete> {
    /// Get the QP number.
    pub fn qpn(&self) -> u32 {
        self.qpn
    }

    /// Get the current QP state.
    pub fn state(&self) -> DcQpState {
        self.state.get()
    }

    /// Transition DCI from RESET to INIT.
    pub fn modify_to_init(&mut self, port: u8) -> io::Result<()> {
        if self.state.get() != DcQpState::Reset {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "DCI must be in RESET state",
            ));
        }

        let mut cmd_in = vec![0u8; prm::MODIFY_QP_IN_SIZE];
        let mut cmd_out = vec![0u8; prm::MODIFY_QP_OUT_SIZE];

        prm::prm_set(&mut cmd_in, prm::MODIFY_QP_IN_OPCODE.0, prm::MODIFY_QP_IN_OPCODE.1, prm::MLX5_CMD_OP_RST2INIT_QP);
        prm::prm_set(&mut cmd_in, prm::MODIFY_QP_IN_QPN.0, prm::MODIFY_QP_IN_QPN.1, self.qpn);

        let q = prm::MODIFY_QP_IN_QPC_OFF;
        let ads = q + prm::QPC_PRI_ADS;

        // PM state = migrated (required by FW)
        prm::prm_set(&mut cmd_in, q + prm::QPC_PM_STATE.0, prm::QPC_PM_STATE.1, 2);
        // Primary address path: port
        prm::prm_set(&mut cmd_in, ads + prm::ADS_VHCA_PORT_NUM.0, prm::ADS_VHCA_PORT_NUM.1, port as u32);

        let ret = self._devx_obj.modify(&cmd_in, &mut cmd_out);
        prm::check_prm_result(ret, &cmd_out, "RST2INIT_QP")?;

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

        let mut cmd_in = vec![0u8; prm::MODIFY_QP_IN_SIZE];
        let mut cmd_out = vec![0u8; prm::MODIFY_QP_OUT_SIZE];

        prm::prm_set(&mut cmd_in, prm::MODIFY_QP_IN_OPCODE.0, prm::MODIFY_QP_IN_OPCODE.1, prm::MLX5_CMD_OP_INIT2RTR_QP);
        prm::prm_set(&mut cmd_in, prm::MODIFY_QP_IN_QPN.0, prm::MODIFY_QP_IN_QPN.1, self.qpn);

        let q = prm::MODIFY_QP_IN_QPC_OFF;
        let ads = q + prm::QPC_PRI_ADS;

        prm::prm_set(&mut cmd_in, q + prm::QPC_MTU.0, prm::QPC_MTU.1, 5); // MTU 4096
        prm::prm_set(&mut cmd_in, ads + prm::ADS_VHCA_PORT_NUM.0, prm::ADS_VHCA_PORT_NUM.1, port as u32);

        let ret = self._devx_obj.modify(&cmd_in, &mut cmd_out);
        prm::check_prm_result(ret, &cmd_out, "INIT2RTR_QP")?;

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

        let mut cmd_in = vec![0u8; prm::MODIFY_QP_IN_SIZE];
        let mut cmd_out = vec![0u8; prm::MODIFY_QP_OUT_SIZE];

        prm::prm_set(&mut cmd_in, prm::MODIFY_QP_IN_OPCODE.0, prm::MODIFY_QP_IN_OPCODE.1, prm::MLX5_CMD_OP_RTR2RTS_QP);
        prm::prm_set(&mut cmd_in, prm::MODIFY_QP_IN_QPN.0, prm::MODIFY_QP_IN_QPN.1, self.qpn);

        let q = prm::MODIFY_QP_IN_QPC_OFF;
        let ads = q + prm::QPC_PRI_ADS;

        let log_sra = if max_rd_atomic > 0 {
            (max_rd_atomic as u32).next_power_of_two().trailing_zeros()
        } else {
            0
        };
        prm::prm_set(&mut cmd_in, q + prm::QPC_LOG_SRA_MAX.0, prm::QPC_LOG_SRA_MAX.1, log_sra);
        prm::prm_set(&mut cmd_in, q + prm::QPC_RETRY_COUNT.0, prm::QPC_RETRY_COUNT.1, 7);
        prm::prm_set(&mut cmd_in, q + prm::QPC_RNR_RETRY.0, prm::QPC_RNR_RETRY.1, 7);
        prm::prm_set(&mut cmd_in, q + prm::QPC_NEXT_SEND_PSN.0, prm::QPC_NEXT_SEND_PSN.1, local_psn);
        prm::prm_set(&mut cmd_in, ads + prm::ADS_ACK_TIMEOUT.0, prm::ADS_ACK_TIMEOUT.1, 14);

        let ret = self._devx_obj.modify(&cmd_in, &mut cmd_out);
        prm::check_prm_result(ret, &cmd_out, "RTR2RTS_QP")?;

        self.state.set(DcQpState::Rts);
        Ok(())
    }

    /// Get the number of available WQEBBs in the Send Queue.
    pub fn sq_available(&self) -> u16 {
        self.sq.as_ref().map(|sq| sq.available()).unwrap_or(0)
    }

    /// Ring the SQ doorbell with minimum 8-byte BlueFlame write.
    ///
    /// Updates DBREC and writes minimum 8 bytes (Control Segment) to BlueFlame register.
    /// The NIC fetches remaining WQE data via DMA.
    pub fn ring_sq_doorbell(&self) {
        if let Some(sq) = self.sq.as_ref() {
            sq.ring_doorbell();
        }
    }

    /// Ring the SQ doorbell with BlueFlame write of entire WQE.
    ///
    /// Copies the last WQE (up to bf_size bytes) to the BlueFlame register.
    /// For WQEs larger than bf_size, only the first bf_size bytes are copied via BF,
    /// and the NIC reads the rest from host memory.
    pub fn ring_sq_doorbell_bf(&self) {
        if let Some(sq) = self.sq.as_ref() {
            sq.ring_doorbell_bf();
        }
    }
}

impl<Entry, OnComplete> DciWithTable<Entry, OnComplete> {
    #[doc(hidden)]
    #[inline]
    pub fn emit_ctx(&self) -> io::Result<DciEmitContext<'_, Entry>> {
        let sq = self
            .sq
            .as_ref()
            .ok_or_else(|| io::Error::other("direct access not initialized"))?;
        Ok(DciEmitContext {
            buf: sq.buf,
            wqe_cnt: sq.wqe_cnt,
            sqn: sq.sqn,
            pi: &sq.pi,
            ci: &sq.ci,
            last_wqe: &sq.last_wqe,
            table: &sq.table,
        })
    }
}

impl<Entry, OnComplete> DciWithTable<Entry, OnComplete> {
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
// CompletionSource impl for DciForMonoCq
// =============================================================================

impl<Entry> CompletionSource for DciForMonoCq<Entry> {
    type Entry = Entry;

    fn qpn(&self) -> u32 {
        Dci::qpn(self)
    }

    fn process_cqe(&self, cqe: Cqe) -> Option<Entry> {
        // DCI only has SQ (no RQ)
        self.sq.as_ref()?.process_completion(cqe.wqe_counter)
    }
}

// =============================================================================
// DCT
// =============================================================================

/// DC Target (DCT).
///
/// Receives RDMA operations from DCIs via an SRQ.
/// Created using DevX PRM CREATE_QP with DC type.
///
/// Type parameter `T` is the entry type stored in the SRQ for tracking
/// in-flight receives.
pub struct Dct<T> {
    _devx_obj: DevxObj,
    _umem: DevxUmem,
    _wq_buf: AlignedBuf,
    dctn: u32,
    dc_key: u64,
    state: DcQpState,
    /// Keep the PD alive while this DCT exists.
    _pd: Pd,
    /// SRQ for receive processing.
    srq: Srq<T>,
}

// =============================================================================
// DCT DevX creation
// =============================================================================

/// Resources created by DevX DCT creation.
struct DctResources {
    devx_obj: DevxObj,
    umem: DevxUmem,
    wq_buf: AlignedBuf,
    qpn: u32,
}

/// Create a DCT via DevX PRM CREATE_QP command.
///
/// # Safety
/// The caller must ensure `ctx`, `pd` are valid and the context was opened with DevX.
unsafe fn create_devx_dct(
    ctx: &Context,
    pd: &Pd,
    recv_cqn: u32,
    srqn: u32,
    config: &DctConfig,
) -> io::Result<DctResources> {
    // DCT has no SQ/RQ buffers, only DBREC (minimal buffer)
    let dbrec_offset = 0usize;
    let total_size = 4096; // One page for DBREC

    let wq_buf = AlignedBuf::new(total_size);
    let umem = ctx.register_umem(wq_buf.as_ptr(), total_size, 7)?;
    let uar = ctx.devx_uar()?;

    // Build CREATE_QP PRM command (no PAS needed with UMEM mode)
    let cmd_in_size = prm::CREATE_QP_IN_BASE_SIZE;
    let mut cmd_in = vec![0u8; cmd_in_size];
    let mut cmd_out = vec![0u8; prm::CREATE_QP_OUT_SIZE];

    let umem_id = umem.umem_id();

    prm::prm_set(
        &mut cmd_in,
        prm::CREATE_QP_IN_OPCODE.0,
        prm::CREATE_QP_IN_OPCODE.1,
        prm::MLX5_CMD_OP_CREATE_QP,
    );

    // WQ UMEM fields
    prm::prm_set(
        &mut cmd_in,
        prm::CREATE_QP_IN_WQ_UMEM_VALID.0,
        prm::CREATE_QP_IN_WQ_UMEM_VALID.1,
        1,
    );
    prm::prm_set(
        &mut cmd_in,
        prm::CREATE_QP_IN_WQ_UMEM_ID.0,
        prm::CREATE_QP_IN_WQ_UMEM_ID.1,
        umem_id,
    );
    prm::prm_set64(&mut cmd_in, prm::CREATE_QP_IN_WQ_UMEM_OFFSET, 0);

    let q = prm::CREATE_QP_IN_QPC_OFF;
    prm::prm_set(&mut cmd_in, q + prm::QPC_ST.0, prm::QPC_ST.1, prm::QP_ST_DCR);
    prm::prm_set(&mut cmd_in, q + prm::QPC_PD.0, prm::QPC_PD.1, pd.pdn());
    prm::prm_set(&mut cmd_in, q + prm::QPC_UAR_PAGE.0, prm::QPC_UAR_PAGE.1, uar.page_id());
    prm::prm_set(&mut cmd_in, q + prm::QPC_LOG_SQ_SIZE.0, prm::QPC_LOG_SQ_SIZE.1, 0);
    prm::prm_set(&mut cmd_in, q + prm::QPC_LOG_RQ_SIZE.0, prm::QPC_LOG_RQ_SIZE.1, 0);
    prm::prm_set(&mut cmd_in, q + prm::QPC_NO_SQ.0, prm::QPC_NO_SQ.1, 1); // No SQ
    prm::prm_set(&mut cmd_in, q + prm::QPC_CQN_SND.0, prm::QPC_CQN_SND.1, recv_cqn);
    prm::prm_set(&mut cmd_in, q + prm::QPC_CQN_RCV.0, prm::QPC_CQN_RCV.1, recv_cqn);
    prm::prm_set(&mut cmd_in, q + prm::QPC_RQ_TYPE.0, prm::QPC_RQ_TYPE.1, 1); // SRQ
    prm::prm_set(&mut cmd_in, q + prm::QPC_SRQN_RMPN_XRQN.0, prm::QPC_SRQN_RMPN_XRQN.1, srqn);

    // DC access key (64-bit)
    prm::prm_set64(&mut cmd_in, q + prm::QPC_DC_ACCESS_KEY, config.dc_key);

    // DBR UMEM fields
    prm::prm_set(&mut cmd_in, q + prm::QPC_DBR_UMEM_VALID.0, prm::QPC_DBR_UMEM_VALID.1, 1);
    prm::prm_set(&mut cmd_in, q + prm::QPC_DBR_UMEM_ID.0, prm::QPC_DBR_UMEM_ID.1, umem_id);
    prm::prm_set64(
        &mut cmd_in,
        q + prm::QPC_DBR_ADDR,
        dbrec_offset as u64,
    );

    let devx_obj = ctx.devx_obj_create(&cmd_in, &mut cmd_out)?;
    let qpn = prm::prm_get(&cmd_out, prm::CREATE_QP_OUT_QPN.0, prm::CREATE_QP_OUT_QPN.1);

    Ok(DctResources {
        devx_obj,
        umem,
        wq_buf,
        qpn,
    })
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
    recv_cqn: u32,
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
            recv_cqn: 0,
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
            recv_cqn: cq.cqn(),
            _marker: PhantomData,
        }
    }

    /// Set MonoCq for receive completions.
    pub fn recv_mono_cq<Q>(self, mono_cq: &MonoCq<Q>) -> DctBuilder<'a, Entry, CqSet>
    where
        Q: crate::mono_cq::CompletionSource,
    {
        DctBuilder {
            ctx: self.ctx,
            pd: self.pd,
            srq: self.srq,
            config: self.config,
            recv_cqn: mono_cq.cqn(),
            _marker: PhantomData,
        }
    }
}

impl<'a, Entry> DctBuilder<'a, Entry, CqSet> {
    /// Build the DCT.
    pub fn build(self) -> io::Result<Dct<Entry>> {
        let srqn = self.srq.srq_number()?;
        unsafe {
            let res = create_devx_dct(self.ctx, self.pd, self.recv_cqn, srqn, &self.config)?;
            Ok(Dct {
                _devx_obj: res.devx_obj,
                _umem: res.umem,
                _wq_buf: res.wq_buf,
                dctn: res.qpn,
                dc_key: self.config.dc_key,
                state: DcQpState::Reset,
                _pd: self.pd.clone(),
                srq: self.srq.clone(),
            })
        }
    }
}

impl<T> Drop for Dct<T> {
    fn drop(&mut self) {
        // DevxObj handles QP destruction
    }
}

impl<T> Dct<T> {
    /// Get the DCT number.
    pub fn dctn(&self) -> u32 {
        self.dctn
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

        let mut cmd_in = vec![0u8; prm::MODIFY_QP_IN_SIZE];
        let mut cmd_out = vec![0u8; prm::MODIFY_QP_OUT_SIZE];

        prm::prm_set(&mut cmd_in, prm::MODIFY_QP_IN_OPCODE.0, prm::MODIFY_QP_IN_OPCODE.1, prm::MLX5_CMD_OP_RST2INIT_QP);
        prm::prm_set(&mut cmd_in, prm::MODIFY_QP_IN_QPN.0, prm::MODIFY_QP_IN_QPN.1, self.dctn);

        let q = prm::MODIFY_QP_IN_QPC_OFF;
        let ads = q + prm::QPC_PRI_ADS;

        // PM state = migrated (required by FW)
        prm::prm_set(&mut cmd_in, q + prm::QPC_PM_STATE.0, prm::QPC_PM_STATE.1, 2);
        // Primary address path: port
        prm::prm_set(&mut cmd_in, ads + prm::ADS_VHCA_PORT_NUM.0, prm::ADS_VHCA_PORT_NUM.1, port as u32);

        // Access flags: RRE, RWE, RAE
        if access_flags & 1 != 0 {
            prm::prm_set(&mut cmd_in, q + prm::QPC_RRE.0, prm::QPC_RRE.1, 1);
        }
        if access_flags & 2 != 0 {
            prm::prm_set(&mut cmd_in, q + prm::QPC_RWE.0, prm::QPC_RWE.1, 1);
        }
        if access_flags & 4 != 0 {
            prm::prm_set(&mut cmd_in, q + prm::QPC_RAE.0, prm::QPC_RAE.1, 1);
        }

        let ret = self._devx_obj.modify(&cmd_in, &mut cmd_out);
        prm::check_prm_result(ret, &cmd_out, "RST2INIT_QP")?;

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

        let mut cmd_in = vec![0u8; prm::MODIFY_QP_IN_SIZE];
        let mut cmd_out = vec![0u8; prm::MODIFY_QP_OUT_SIZE];

        prm::prm_set(&mut cmd_in, prm::MODIFY_QP_IN_OPCODE.0, prm::MODIFY_QP_IN_OPCODE.1, prm::MLX5_CMD_OP_INIT2RTR_QP);
        prm::prm_set(&mut cmd_in, prm::MODIFY_QP_IN_QPN.0, prm::MODIFY_QP_IN_QPN.1, self.dctn);

        let q = prm::MODIFY_QP_IN_QPC_OFF;
        let ads = q + prm::QPC_PRI_ADS;

        prm::prm_set(&mut cmd_in, q + prm::QPC_MTU.0, prm::QPC_MTU.1, 5); // MTU 4096
        prm::prm_set(&mut cmd_in, q + prm::QPC_MIN_RNR_NAK.0, prm::QPC_MIN_RNR_NAK.1, min_rnr_timer as u32);
        prm::prm_set(&mut cmd_in, ads + prm::ADS_VHCA_PORT_NUM.0, prm::ADS_VHCA_PORT_NUM.1, port as u32);

        let ret = self._devx_obj.modify(&cmd_in, &mut cmd_out);
        prm::check_prm_result(ret, &cmd_out, "INIT2RTR_QP")?;

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
    pub fn remote_info(&self, local_identifier: u16) -> IbRemoteDctInfo {
        IbRemoteDctInfo {
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
