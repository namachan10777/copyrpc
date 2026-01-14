//! Tag Matching SRQ (TM-SRQ) management.
//!
//! TM-SRQ enables hardware-accelerated tag matching for RPC-style messaging.
//! Incoming messages are matched to posted receive buffers based on 64-bit tags.

use std::{io, mem::MaybeUninit, ptr::NonNull};

use crate::cq::CompletionQueue;
use crate::device::Context;
use crate::pd::ProtectionDomain;
use crate::srq::SrqInfo;

/// TM-SRQ configuration.
#[derive(Debug, Clone)]
pub struct TmSrqConfig {
    /// Maximum number of outstanding receive WRs.
    pub max_wr: u32,
    /// Maximum number of SGEs per WR.
    pub max_sge: u32,
    /// Maximum number of tags that can be posted.
    pub max_num_tags: u32,
    /// Maximum number of outstanding TM operations.
    pub max_ops: u32,
}

impl Default for TmSrqConfig {
    fn default() -> Self {
        Self {
            max_wr: 1024,
            max_sge: 1,
            max_num_tags: 64,
            max_ops: 16,
        }
    }
}

/// Tag Matching Shared Receive Queue.
///
/// TM-SRQ provides hardware-accelerated tag matching. When a message arrives,
/// the hardware matches it against posted receive tags and delivers to the
/// corresponding buffer.
pub struct TagMatchingSrq {
    srq: NonNull<mlx5_sys::ibv_srq>,
}

impl Context {
    /// Create a Tag Matching SRQ.
    ///
    /// # Arguments
    /// * `pd` - Protection Domain
    /// * `cq` - Completion Queue for TM operation completions
    /// * `config` - TM-SRQ configuration
    ///
    /// # Errors
    /// Returns an error if the TM-SRQ cannot be created.
    pub fn create_tm_srq(
        &self,
        pd: &ProtectionDomain,
        cq: &CompletionQueue,
        config: &TmSrqConfig,
    ) -> io::Result<TagMatchingSrq> {
        unsafe {
            let mut attr: mlx5_sys::ibv_srq_init_attr_ex = MaybeUninit::zeroed().assume_init();
            attr.attr.max_wr = config.max_wr;
            attr.attr.max_sge = config.max_sge;
            attr.srq_type = mlx5_sys::ibv_srq_type_IBV_SRQT_TM;
            attr.pd = pd.as_ptr();
            attr.cq = cq.as_ptr();
            attr.tm_cap.max_num_tags = config.max_num_tags;
            attr.tm_cap.max_ops = config.max_ops;
            attr.comp_mask = mlx5_sys::ibv_srq_init_attr_mask_IBV_SRQ_INIT_ATTR_TYPE
                | mlx5_sys::ibv_srq_init_attr_mask_IBV_SRQ_INIT_ATTR_PD
                | mlx5_sys::ibv_srq_init_attr_mask_IBV_SRQ_INIT_ATTR_CQ
                | mlx5_sys::ibv_srq_init_attr_mask_IBV_SRQ_INIT_ATTR_TM;

            let srq = mlx5_sys::ibv_create_srq_ex_ex(self.ctx.as_ptr(), &mut attr);
            NonNull::new(srq).map_or(Err(io::Error::last_os_error()), |srq| {
                Ok(TagMatchingSrq { srq })
            })
        }
    }
}

impl Drop for TagMatchingSrq {
    fn drop(&mut self) {
        unsafe {
            mlx5_sys::ibv_destroy_srq(self.srq.as_ptr());
        }
    }
}

impl TagMatchingSrq {
    /// Get the raw ibv_srq pointer.
    pub(crate) fn as_ptr(&self) -> *mut mlx5_sys::ibv_srq {
        self.srq.as_ptr()
    }

    /// Get mlx5-specific SRQ information for direct WQE access.
    pub fn info(&self) -> io::Result<SrqInfo> {
        unsafe {
            let mut dv_srq: MaybeUninit<mlx5_sys::mlx5dv_srq> = MaybeUninit::zeroed();
            let mut obj: MaybeUninit<mlx5_sys::mlx5dv_obj> = MaybeUninit::zeroed();

            let obj_ptr = obj.as_mut_ptr();
            (*obj_ptr).srq.in_ = self.srq.as_ptr();
            (*obj_ptr).srq.out = dv_srq.as_mut_ptr();

            let ret =
                mlx5_sys::mlx5dv_init_obj(obj_ptr, mlx5_sys::mlx5dv_obj_type_MLX5DV_OBJ_SRQ as u64);
            if ret != 0 {
                return Err(io::Error::from_raw_os_error(-ret));
            }

            let dv_srq = dv_srq.assume_init();

            Ok(SrqInfo {
                buf: dv_srq.buf as *mut u8,
                dbrec: dv_srq.dbrec as *mut u32,
                stride: dv_srq.stride,
                srqn: dv_srq.srqn,
            })
        }
    }

    /// Get the SRQ number.
    pub fn srqn(&self) -> io::Result<u32> {
        self.info().map(|info| info.srqn)
    }
}
