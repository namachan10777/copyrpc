//! Shared Receive Queue (SRQ) management.
//!
//! An SRQ allows multiple Queue Pairs to share a common pool of receive buffers,
//! reducing memory usage when many connections are needed.

use std::rc::Rc;
use std::{io, mem::MaybeUninit, ptr::NonNull};

use crate::pd::Pd;
use crate::wqe::DataSeg;

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

// =============================================================================
// SRQ State
// =============================================================================

/// SRQ state for direct WQE posting.
struct SrqState {
    buf: *mut u8,
    wqe_cnt: u32,
    stride: u32,
    head: u32,
    dbrec: *mut u32,
}

impl SrqState {
    fn get_wqe_ptr(&self, idx: u32) -> *mut u8 {
        let offset = (idx & (self.wqe_cnt - 1)) * self.stride;
        unsafe { self.buf.add(offset as usize) }
    }

    unsafe fn post(&mut self, addr: u64, len: u32, lkey: u32) {
        let wqe_ptr = self.get_wqe_ptr(self.head);

        // SRQ WQE format: Next Segment (16 bytes) + Data Segment (16 bytes)
        std::ptr::write_bytes(wqe_ptr, 0, 16);

        // Write Data Segment at offset 16
        DataSeg::write(wqe_ptr.add(16), len, lkey, addr);

        self.head = self.head.wrapping_add(1);
    }

    fn ring_doorbell(&mut self) {
        mmio_flush_writes!();
        unsafe {
            std::ptr::write_volatile(self.dbrec, self.head.to_be());
        }
    }
}

// =============================================================================
// Shared Receive Queue
// =============================================================================

/// Internal SRQ structure.
///
/// This is wrapped in Rc to ensure proper resource lifetime management.
pub(crate) struct SrqInner {
    srq: NonNull<mlx5_sys::ibv_srq>,
    wqe_cnt: u32,
    state: Option<SrqState>,
    /// Keep the PD alive while this SRQ exists.
    _pd: Pd,
}

impl Drop for SrqInner {
    fn drop(&mut self) {
        unsafe {
            mlx5_sys::ibv_destroy_srq(self.srq.as_ptr());
        }
    }
}

/// Shared Receive Queue.
///
/// Allows multiple QPs to share receive buffers. This is useful for DC
/// (Dynamically Connected) transport where many connections share one SRQ.
///
/// This type uses `Rc` internally and can be cheaply cloned.
#[derive(Clone)]
pub struct Srq(Rc<std::cell::RefCell<SrqInner>>);

impl Pd {
    /// Create a Shared Receive Queue.
    ///
    /// # Arguments
    /// * `config` - SRQ configuration
    ///
    /// # Errors
    /// Returns an error if the SRQ cannot be created.
    pub fn create_srq(&self, config: &SrqConfig) -> io::Result<Srq> {
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
                Ok(Srq(Rc::new(std::cell::RefCell::new(SrqInner {
                    srq,
                    wqe_cnt: config.max_wr.next_power_of_two(),
                    state: None,
                    _pd: self.clone(),
                }))))
            })
        }
    }
}

impl Srq {
    /// Get the raw ibv_srq pointer.
    pub(crate) fn as_ptr(&self) -> *mut mlx5_sys::ibv_srq {
        self.0.borrow().srq.as_ptr()
    }

    /// Get mlx5-specific SRQ information for direct WQE access.
    fn query_info(&self) -> io::Result<SrqInfo> {
        unsafe {
            let mut dv_srq: MaybeUninit<mlx5_sys::mlx5dv_srq> = MaybeUninit::zeroed();
            let mut obj: MaybeUninit<mlx5_sys::mlx5dv_obj> = MaybeUninit::zeroed();

            let obj_ptr = obj.as_mut_ptr();
            (*obj_ptr).srq.in_ = self.as_ptr();
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

    /// Initialize direct access for the SRQ.
    ///
    /// Call this before using post_recv/ring_doorbell.
    pub fn init_direct_access(&self) -> io::Result<()> {
        let info = self.query_info()?;
        let mut inner = self.0.borrow_mut();

        inner.state = Some(SrqState {
            buf: info.buf,
            wqe_cnt: inner.wqe_cnt,
            stride: info.stride,
            head: 0,
            dbrec: info.dbrec,
        });

        Ok(())
    }

    /// Get the SRQ number.
    pub fn srqn(&self) -> io::Result<u32> {
        self.query_info().map(|info| info.srqn)
    }

    /// Post a receive WQE.
    ///
    /// # Safety
    /// - The buffer must be registered and valid
    /// - There must be available slots in the SRQ
    pub unsafe fn post_recv(&self, addr: u64, len: u32, lkey: u32) {
        let mut inner = self.0.borrow_mut();
        if let Some(state) = inner.state.as_mut() {
            state.post(addr, len, lkey);
        }
    }

    /// Ring the SRQ doorbell to notify HCA of new WQEs.
    pub fn ring_doorbell(&self) {
        let mut inner = self.0.borrow_mut();
        if let Some(state) = inner.state.as_mut() {
            state.ring_doorbell();
        }
    }
}
