//! Completion Queue (CQ) management.
//!
//! A Completion Queue is used to notify the application when work requests
//! have completed. CQs can be shared across multiple Queue Pairs.

use std::{io, mem::MaybeUninit, ptr::NonNull};

use crate::device::Context;

/// Completion Queue.
///
/// Used to receive completion notifications for send and receive operations.
/// Multiple QPs can share the same CQ.
pub struct CompletionQueue {
    cq: NonNull<mlx5_sys::ibv_cq>,
}

/// CQ internal info obtained from mlx5dv_init_obj.
#[derive(Debug)]
pub struct CqInfo {
    /// CQ buffer pointer.
    pub buf: *mut u8,
    /// Doorbell record pointer.
    pub dbrec: *mut u32,
    /// Number of CQEs.
    pub cqe_cnt: u32,
    /// CQE size in bytes.
    pub cqe_size: u32,
    /// CQ UAR (User Access Region) pointer.
    pub cq_uar: *mut u8,
    /// CQ number.
    pub cqn: u32,
}

impl Context {
    /// Create a Completion Queue.
    ///
    /// # Arguments
    /// * `cqe` - Minimum number of CQ entries (actual may be larger)
    ///
    /// # Errors
    /// Returns an error if the CQ cannot be created.
    pub fn create_cq(&self, cqe: i32) -> io::Result<CompletionQueue> {
        unsafe {
            let cq = mlx5_sys::ibv_create_cq(
                self.ctx.as_ptr(),
                cqe,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                0,
            );
            NonNull::new(cq).map_or(Err(io::Error::last_os_error()), |cq| {
                Ok(CompletionQueue { cq })
            })
        }
    }
}

impl Drop for CompletionQueue {
    fn drop(&mut self) {
        unsafe {
            mlx5_sys::ibv_destroy_cq(self.cq.as_ptr());
        }
    }
}

impl CompletionQueue {
    /// Get the raw ibv_cq pointer.
    pub(crate) fn as_ptr(&self) -> *mut mlx5_sys::ibv_cq {
        self.cq.as_ptr()
    }

    /// Get mlx5-specific CQ information for direct CQE access.
    ///
    /// Returns buffer pointers and metadata needed for polling CQEs directly.
    pub fn info(&self) -> io::Result<CqInfo> {
        unsafe {
            let mut dv_cq: MaybeUninit<mlx5_sys::mlx5dv_cq> = MaybeUninit::zeroed();
            let mut obj: MaybeUninit<mlx5_sys::mlx5dv_obj> = MaybeUninit::zeroed();

            let obj_ptr = obj.as_mut_ptr();
            (*obj_ptr).cq.in_ = self.cq.as_ptr();
            (*obj_ptr).cq.out = dv_cq.as_mut_ptr();

            let ret =
                mlx5_sys::mlx5dv_init_obj(obj_ptr, mlx5_sys::mlx5dv_obj_type_MLX5DV_OBJ_CQ as u64);
            if ret != 0 {
                return Err(io::Error::from_raw_os_error(-ret));
            }

            let dv_cq = dv_cq.assume_init();

            Ok(CqInfo {
                buf: dv_cq.buf as *mut u8,
                dbrec: dv_cq.dbrec as *mut u32,
                cqe_cnt: dv_cq.cqe_cnt,
                cqe_size: dv_cq.cqe_size,
                cq_uar: dv_cq.cq_uar as *mut u8,
                cqn: dv_cq.cqn,
            })
        }
    }

    /// Poll for completions.
    ///
    /// # Arguments
    /// * `wc` - Slice to store work completions
    ///
    /// # Returns
    /// Number of completions polled, or error if polling failed.
    pub fn poll(&self, wc: &mut [mlx5_sys::ibv_wc]) -> io::Result<usize> {
        unsafe {
            let ret = mlx5_sys::ibv_poll_cq_ex(self.cq.as_ptr(), wc.len() as i32, wc.as_mut_ptr());
            if ret < 0 {
                Err(io::Error::from_raw_os_error(-ret))
            } else {
                Ok(ret as usize)
            }
        }
    }
}
