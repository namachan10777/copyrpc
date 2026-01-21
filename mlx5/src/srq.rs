//! Shared Receive Queue (SRQ) management.
//!
//! An SRQ allows multiple Queue Pairs to share a common pool of receive buffers,
//! reducing memory usage when many connections are needed.

use std::cell::{Cell, RefCell};
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
    pub doorbell_record: *mut u32,
    /// WQE stride (bytes per slot).
    pub stride: u32,
    /// SRQ number.
    pub srq_number: u32,
}

// =============================================================================
// SRQ State
// =============================================================================

/// SRQ state for direct WQE posting.
struct SrqState<T> {
    buf: *mut u8,
    wqe_cnt: u32,
    stride: u32,
    pi: Cell<u32>,
    ci: Cell<u32>,
    dbrec: *mut u32,
    /// Entry table for tracking in-flight receives.
    table: Box<[Cell<Option<T>>]>,
}

impl<T> SrqState<T> {
    fn get_wqe_ptr(&self, idx: u32) -> *mut u8 {
        let offset = (idx & (self.wqe_cnt - 1)) * self.stride;
        unsafe { self.buf.add(offset as usize) }
    }

    fn ring_doorbell(&self) {
        mmio_flush_writes!();
        unsafe {
            std::ptr::write_volatile(self.dbrec, self.pi.get().to_be());
        }
    }

    /// Available slots based on pi - ci difference.
    fn available(&self) -> u32 {
        self.wqe_cnt - self.pi.get().wrapping_sub(self.ci.get())
    }

    /// Process a receive completion and return the associated entry.
    fn process_completion(&self, wqe_idx: u16) -> Option<T> {
        self.ci.set(self.ci.get().wrapping_add(1));
        let idx = (wqe_idx as usize) & ((self.wqe_cnt - 1) as usize);
        self.table[idx].take()
    }
}

// =============================================================================
// SRQ Receive WQE Builder
// =============================================================================

/// Builder for posting receive WQEs to SRQ.
pub struct SrqRecvWqeBuilder<'a, T> {
    state: &'a SrqState<T>,
    entry: T,
    wqe_idx: u32,
}

impl<'a, T> SrqRecvWqeBuilder<'a, T> {
    /// Add an SGE (Scatter/Gather Element) to the receive WQE.
    pub fn sge(self, addr: u64, len: u32, lkey: u32) -> Self {
        unsafe {
            let wqe_ptr = self.state.get_wqe_ptr(self.wqe_idx);

            // SRQ WQE format: Next Segment (16 bytes) + Data Segment (16 bytes)
            std::ptr::write_bytes(wqe_ptr, 0, 16);

            // Write Data Segment at offset 16
            DataSeg::write(wqe_ptr.add(16), len, lkey, addr);
        }
        self
    }

    /// Finish the WQE construction.
    pub fn finish(self) {
        let idx = (self.wqe_idx as usize) & ((self.state.wqe_cnt - 1) as usize);
        self.state.table[idx].set(Some(self.entry));
        self.state.pi.set(self.state.pi.get().wrapping_add(1));
    }
}

// =============================================================================
// Shared Receive Queue
// =============================================================================

/// Internal SRQ structure.
///
/// This is wrapped in Rc to ensure proper resource lifetime management.
pub(crate) struct SrqInner<T> {
    srq: NonNull<mlx5_sys::ibv_srq>,
    wqe_cnt: u32,
    state: Option<SrqState<T>>,
    /// Keep the PD alive while this SRQ exists.
    _pd: Pd,
}

impl<T> Drop for SrqInner<T> {
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
/// Type parameter `T` is the entry type stored for tracking in-flight receives.
/// When a receive completion arrives, call `process_recv_completion()` to retrieve
/// the associated entry.
///
/// This type uses `Rc` internally and can be cheaply cloned.
pub struct Srq<T>(Rc<RefCell<SrqInner<T>>>);

impl<T> Clone for Srq<T> {
    fn clone(&self) -> Self {
        Srq(Rc::clone(&self.0))
    }
}

impl Pd {
    /// Create a Shared Receive Queue.
    ///
    /// # Arguments
    /// * `config` - SRQ configuration
    ///
    /// # Errors
    /// Returns an error if the SRQ cannot be created.
    pub fn create_srq<T>(&self, config: &SrqConfig) -> io::Result<Srq<T>> {
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
            let srq = NonNull::new(srq).ok_or_else(io::Error::last_os_error)?;

            let result = Srq(Rc::new(RefCell::new(SrqInner {
                srq,
                wqe_cnt: config.max_wr.next_power_of_two(),
                state: None,
                _pd: self.clone(),
            })));

            // Auto-initialize direct access
            result.init_direct_access_internal()?;

            Ok(result)
        }
    }
}

impl<T> Srq<T> {
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
                doorbell_record: dv_srq.dbrec,
                stride: dv_srq.stride,
                srq_number: dv_srq.srqn,
            })
        }
    }

    /// Initialize direct access for the SRQ (internal implementation).
    fn init_direct_access_internal(&self) -> io::Result<()> {
        {
            let inner = self.0.borrow();
            if inner.state.is_some() {
                return Ok(()); // Already initialized
            }
        }

        let info = self.query_info()?;
        let mut inner = self.0.borrow_mut();
        let wqe_cnt = inner.wqe_cnt;

        inner.state = Some(SrqState {
            buf: info.buf,
            wqe_cnt,
            stride: info.stride,
            pi: Cell::new(0),
            ci: Cell::new(0),
            dbrec: info.doorbell_record,
            table: (0..wqe_cnt).map(|_| Cell::new(None)).collect(),
        });

        Ok(())
    }

    /// Initialize direct access for the SRQ.
    ///
    /// # Deprecated
    /// Direct access is now auto-initialized at SRQ creation.
    /// This method is kept for backwards compatibility and is a no-op if already initialized.
    #[deprecated(note = "Direct access is now auto-initialized at creation")]
    pub fn init_direct_access(&self) -> io::Result<()> {
        self.init_direct_access_internal()
    }

    /// Get the SRQ number.
    pub fn srq_number(&self) -> io::Result<u32> {
        self.query_info().map(|info| info.srq_number)
    }

    /// Get a WQE builder for posting a receive.
    ///
    /// # Arguments
    /// * `entry` - Entry to associate with this receive WQE
    ///
    /// # Errors
    /// Returns `WouldBlock` if the SRQ is full.
    pub fn recv_builder(&self, entry: T) -> io::Result<SrqRecvWqeBuilder<'_, T>> {
        let inner = self.0.borrow();
        let state = inner
            .state
            .as_ref()
            .ok_or_else(|| io::Error::other("direct access not initialized"))?;

        if state.available() == 0 {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SRQ full"));
        }

        let wqe_idx = state.pi.get();

        // SAFETY: We're returning a reference that borrows self, and state lives as long as inner
        // which lives as long as self.0. This is a bit unsafe but the borrow checker ensures
        // we can't drop self while the builder exists.
        let state_ptr = state as *const SrqState<T>;
        drop(inner);

        Ok(SrqRecvWqeBuilder {
            state: unsafe { &*state_ptr },
            entry,
            wqe_idx,
        })
    }

    /// Get available slot count.
    pub fn available(&self) -> u32 {
        self.0
            .borrow()
            .state
            .as_ref()
            .map(|s| s.available())
            .unwrap_or(0)
    }

    /// Ring the SRQ doorbell to notify HCA of new WQEs.
    pub fn ring_doorbell(&self) {
        let inner = self.0.borrow();
        if let Some(state) = inner.state.as_ref() {
            state.ring_doorbell();
        }
    }

    /// Process a receive completion and return the associated entry.
    ///
    /// Call this when a receive CQE is received to retrieve the entry
    /// associated with the completed receive.
    ///
    /// # Arguments
    /// * `wqe_idx` - WQE index from the CQE (wqe_counter field)
    pub fn process_recv_completion(&self, wqe_idx: u16) -> Option<T> {
        self.0
            .borrow()
            .state
            .as_ref()
            .and_then(|s| s.process_completion(wqe_idx))
    }

    /// Get a RQ WQE builder using the new trait-based API.
    ///
    /// The entry will be stored and returned via callback on RQ completion.
    ///
    /// # Example
    /// ```ignore
    /// srq.rq_wqe(entry)
    ///     .sge(addr, len, lkey)
    ///     .finish();
    /// srq.ring_doorbell();
    /// ```
    #[inline]
    pub fn rq_wqe(&self, entry: T) -> io::Result<SrqRecvWqeBuilder<'_, T>> {
        self.recv_builder(entry)
    }
}

