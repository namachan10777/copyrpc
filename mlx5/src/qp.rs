//! Queue Pair (QP) management.
//!
//! Queue Pairs are the fundamental communication endpoints in RDMA.
//! This module provides RC (Reliable Connection) QP creation using mlx5dv_create_qp.

use std::{io, mem::MaybeUninit, ptr::NonNull};

use crate::cq::CompletionQueue;
use crate::device::Context;
use crate::pd::ProtectionDomain;

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

/// RC (Reliable Connection) Queue Pair.
///
/// Created using mlx5dv_create_qp for direct hardware access.
pub struct RcQp {
    qp: NonNull<mlx5_sys::ibv_qp>,
    state: QpState,
}

impl Context {
    /// Create an RC Queue Pair using mlx5dv_create_qp.
    ///
    /// # Arguments
    /// * `pd` - Protection Domain
    /// * `send_cq` - Completion Queue for send completions
    /// * `recv_cq` - Completion Queue for receive completions
    /// * `config` - QP configuration
    ///
    /// # Errors
    /// Returns an error if the QP cannot be created.
    pub fn create_rc_qp(
        &self,
        pd: &ProtectionDomain,
        send_cq: &CompletionQueue,
        recv_cq: &CompletionQueue,
        config: &RcQpConfig,
    ) -> io::Result<RcQp> {
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
            // No special mlx5 flags needed for basic RC QP

            let qp = mlx5_sys::mlx5dv_create_qp(self.ctx.as_ptr(), &mut qp_attr, &mut mlx5_attr);
            NonNull::new(qp).map_or(Err(io::Error::last_os_error()), |qp| {
                Ok(RcQp {
                    qp,
                    state: QpState::Reset,
                })
            })
        }
    }
}

impl Drop for RcQp {
    fn drop(&mut self) {
        unsafe {
            mlx5_sys::ibv_destroy_qp(self.qp.as_ptr());
        }
    }
}

impl RcQp {
    /// Get the QP number.
    pub fn qpn(&self) -> u32 {
        unsafe { (*self.qp.as_ptr()).qp_num }
    }

    /// Get the current QP state.
    pub fn state(&self) -> QpState {
        self.state
    }

    /// Get mlx5-specific QP information for direct WQE access.
    ///
    /// Returns buffer pointers and metadata needed for posting WQEs directly.
    pub fn info(&self) -> io::Result<QpInfo> {
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
    ///
    /// # Arguments
    /// * `port` - Port number (1-based)
    /// * `access_flags` - QP access flags for remote operations
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
    ///
    /// # Arguments
    /// * `remote` - Remote QP information
    /// * `port` - Port number (1-based)
    /// * `max_dest_rd_atomic` - Maximum outstanding RDMA read/atomic as responder
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
    ///
    /// # Arguments
    /// * `local_psn` - Local packet sequence number
    /// * `max_rd_atomic` - Maximum outstanding RDMA read/atomic as initiator
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

    /// Connect to a remote QP.
    ///
    /// Transitions the QP through RESET -> INIT -> RTR -> RTS.
    ///
    /// # Arguments
    /// * `remote` - Remote QP information
    /// * `port` - Port number (1-based)
    /// * `local_psn` - Local packet sequence number
    /// * `max_rd_atomic` - Maximum outstanding RDMA read/atomic as initiator
    /// * `max_dest_rd_atomic` - Maximum outstanding RDMA read/atomic as responder
    /// * `access_flags` - QP access flags for remote operations
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
        Ok(())
    }
}
