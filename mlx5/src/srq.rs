//! Shared Receive Queue (SRQ) management.
//!
//! An SRQ allows multiple Queue Pairs to share a common pool of receive buffers,
//! reducing memory usage when many connections are needed.

use std::{io, mem::MaybeUninit, ptr::NonNull};

use crate::pd::ProtectionDomain;

/// SRQ configuration.
#[derive(Debug, Clone)]
pub struct SrqConfig {
    /// Maximum number of outstanding receive WRs.
    pub max_wr: u32,
    /// Maximum number of SGEs per WR.
    pub max_sge: u32,
}

impl Default for SrqConfig {
    fn default() -> Self {
        Self {
            max_wr: 1024,
            max_sge: 1,
        }
    }
}

/// SRQ internal info obtained from mlx5dv_init_obj.
#[derive(Debug)]
pub struct SrqInfo {
    /// SRQ buffer pointer.
    pub buf: *mut u8,
    /// Doorbell record pointer.
    pub dbrec: *mut u32,
    /// WQE stride (bytes per slot).
    pub stride: u32,
    /// SRQ number.
    pub srqn: u32,
}

/// Shared Receive Queue.
///
/// Allows multiple QPs to share receive buffers. This is useful for DC
/// (Dynamically Connected) transport where many connections share one SRQ.
pub struct SharedReceiveQueue {
    srq: NonNull<mlx5_sys::ibv_srq>,
}

impl ProtectionDomain {
    /// Create a Shared Receive Queue.
    ///
    /// # Arguments
    /// * `config` - SRQ configuration
    ///
    /// # Errors
    /// Returns an error if the SRQ cannot be created.
    pub fn create_srq(&self, config: &SrqConfig) -> io::Result<SharedReceiveQueue> {
        unsafe {
            let attr = mlx5_sys::ibv_srq_init_attr {
                srq_context: std::ptr::null_mut(),
                attr: mlx5_sys::ibv_srq_attr {
                    max_wr: config.max_wr,
                    max_sge: config.max_sge,
                    srq_limit: 0,
                },
            };

            let srq = mlx5_sys::ibv_create_srq(self.as_ptr(), &attr as *const _ as *mut _);
            NonNull::new(srq).map_or(Err(io::Error::last_os_error()), |srq| {
                Ok(SharedReceiveQueue { srq })
            })
        }
    }
}

impl Drop for SharedReceiveQueue {
    fn drop(&mut self) {
        unsafe {
            mlx5_sys::ibv_destroy_srq(self.srq.as_ptr());
        }
    }
}

impl SharedReceiveQueue {
    /// Get the raw ibv_srq pointer.
    pub(crate) fn as_ptr(&self) -> *mut mlx5_sys::ibv_srq {
        self.srq.as_ptr()
    }

    /// Get mlx5-specific SRQ information for direct WQE access.
    ///
    /// Returns buffer pointers and metadata needed for posting receive WQEs directly.
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
