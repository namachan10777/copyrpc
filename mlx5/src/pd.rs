//! Protection Domain and Memory Region management.
//!
//! A Protection Domain (PD) defines a protection scope for RDMA resources.
//! Memory Regions (MRs) must be registered within a PD before they can be
//! used for RDMA operations.

use std::rc::Rc;
use std::{io, ptr::NonNull};

use bitflags::bitflags;

use crate::device::Context;

bitflags! {
    /// Memory access flags for Memory Region registration.
    ///
    /// These flags describe the desired memory protection attributes for an MR.
    /// Local read access is always enabled for the MR.
    ///
    /// # Important
    /// If `REMOTE_WRITE` or `REMOTE_ATOMIC` is set, then `LOCAL_WRITE` must also be set.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct AccessFlags: u32 {
        /// Enable local write access.
        const LOCAL_WRITE = mlx5_sys::ibv_access_flags_IBV_ACCESS_LOCAL_WRITE;

        /// Enable remote write access.
        /// Requires `LOCAL_WRITE` to be set.
        const REMOTE_WRITE = mlx5_sys::ibv_access_flags_IBV_ACCESS_REMOTE_WRITE;

        /// Enable remote read access.
        const REMOTE_READ = mlx5_sys::ibv_access_flags_IBV_ACCESS_REMOTE_READ;

        /// Enable remote atomic operation access (if supported).
        /// Requires `LOCAL_WRITE` to be set.
        const REMOTE_ATOMIC = mlx5_sys::ibv_access_flags_IBV_ACCESS_REMOTE_ATOMIC;

        /// Enable Memory Window binding.
        const MW_BIND = mlx5_sys::ibv_access_flags_IBV_ACCESS_MW_BIND;

        /// Use byte offset from beginning of MR to access this MR,
        /// instead of a pointer address.
        const ZERO_BASED = mlx5_sys::ibv_access_flags_IBV_ACCESS_ZERO_BASED;

        /// Create an on-demand paging MR.
        /// To create an implicit ODP MR, set this flag with addr=0 and length=SIZE_MAX.
        const ON_DEMAND = mlx5_sys::ibv_access_flags_IBV_ACCESS_ON_DEMAND;

        /// Huge pages are guaranteed to be used for this MR.
        /// Only applicable with `ON_DEMAND` in explicit mode.
        /// Application must ensure all pages are huge and never break huge pages.
        const HUGETLB = mlx5_sys::ibv_access_flags_IBV_ACCESS_HUGETLB;

        /// Enable remote flush operation with global visibility placement type (if supported).
        const FLUSH_GLOBAL = mlx5_sys::ibv_access_flags_IBV_ACCESS_FLUSH_GLOBAL;

        /// Enable remote flush operation with persistence placement type (if supported).
        const FLUSH_PERSISTENT = mlx5_sys::ibv_access_flags_IBV_ACCESS_FLUSH_PERSISTENT;

        /// Allow the NIC to relax the order of data transfer between the network
        /// and the target memory region.
        ///
        /// This can improve performance but has the following impacts:
        /// - RDMA write-after-write message order is no longer guaranteed
        ///   (send messages still match posted receive buffers in order)
        /// - Back-to-back network writes targeting the same memory region
        ///   leave the region in an unknown state
        ///
        /// Completion semantics are unchanged: a completion still ensures all data
        /// is visible, including data from prior transfers. Relaxed ordered operations
        /// will not bypass atomic operations.
        const RELAXED_ORDERING = mlx5_sys::ibv_access_flags_IBV_ACCESS_RELAXED_ORDERING;
    }
}

/// Internal PD structure holding the raw ibv_pd pointer.
///
/// This is wrapped in Rc to ensure proper resource lifetime management.
pub(crate) struct PdInner {
    pd: NonNull<mlx5_sys::ibv_pd>,
    /// Keep the context alive while this PD exists.
    _ctx: Context,
}

impl Drop for PdInner {
    fn drop(&mut self) {
        unsafe {
            mlx5_sys::ibv_dealloc_pd(self.pd.as_ptr());
        }
    }
}

/// Protection Domain for RDMA resources.
///
/// A PD defines a protection scope for RDMA resources. All resources
/// (QPs, MRs, MWs, SRQs, AHs) that need to work together must be created
/// within the same PD.
///
/// The PD will be deallocated when dropped. Deallocation may fail if any
/// other resource is still associated with the PD.
///
/// This type uses `Rc` internally and can be cheaply cloned.
#[derive(Clone)]
pub struct Pd(Rc<PdInner>);

impl Context {
    /// Allocate a Protection Domain for this RDMA device context.
    ///
    /// # Errors
    /// Returns an error if the allocation fails.
    pub fn alloc_pd(&self) -> io::Result<Pd> {
        unsafe {
            let pd = mlx5_sys::ibv_alloc_pd(self.as_ptr());
            NonNull::new(pd).map_or(Err(io::Error::last_os_error()), |pd| {
                Ok(Pd(Rc::new(PdInner {
                    pd,
                    _ctx: self.clone(),
                })))
            })
        }
    }
}

impl Pd {
    /// Get the raw ibv_pd pointer.
    pub fn as_ptr(&self) -> *mut mlx5_sys::ibv_pd {
        self.0.pd.as_ptr()
    }

    /// Get the firmware PD number (PDN) for use in DevX QPC fields.
    ///
    /// Note: This is the firmware-internal PDN obtained via mlx5dv_init_obj,
    /// NOT the verbs handle (`ibv_pd.handle`).
    pub fn pdn(&self) -> u32 {
        unsafe {
            let mut dv_pd: std::mem::MaybeUninit<mlx5_sys::mlx5dv_pd> =
                std::mem::MaybeUninit::zeroed();
            let mut obj: std::mem::MaybeUninit<mlx5_sys::mlx5dv_obj> =
                std::mem::MaybeUninit::zeroed();
            let obj_ptr = obj.as_mut_ptr();
            (*obj_ptr).pd.in_ = self.0.pd.as_ptr();
            (*obj_ptr).pd.out = dv_pd.as_mut_ptr();
            let ret = mlx5_sys::mlx5dv_init_obj(
                obj_ptr,
                mlx5_sys::mlx5dv_obj_type_MLX5DV_OBJ_PD as u64,
            );
            assert_eq!(ret, 0, "mlx5dv_init_obj(PD) failed");
            dv_pd.assume_init().pdn
        }
    }
}

/// Memory Region registered with the HCA.
///
/// An MR allows the HCA to access a memory buffer. The MR provides:
/// - `lkey`: Local key used in `ibv_sge` when posting buffers with `ibv_post_*` verbs
/// - `rkey`: Remote key used by remote processes to perform RDMA and atomic operations
///
/// The MR will be deregistered when dropped. Deregistration fails if any
/// Memory Window is still bound to this MR.
pub struct MemoryRegion {
    mr: NonNull<mlx5_sys::ibv_mr>,
    /// Keep the PD alive while this MR exists.
    _pd: Pd,
}

impl Pd {
    /// Register a memory region with the HCA.
    ///
    /// Registers a memory buffer starting at `addr` with size `len` bytes.
    /// The `access` flags describe the desired memory protection attributes.
    ///
    /// # Safety
    /// The caller must ensure that:
    /// - The memory region pointed to by `addr` with `len` bytes is valid
    /// - The memory region remains valid for the lifetime of the returned `MemoryRegion`
    /// - If `REMOTE_WRITE` or `REMOTE_ATOMIC` is set, `LOCAL_WRITE` must also be set
    ///
    /// # Errors
    /// Returns an error if the registration fails.
    pub unsafe fn register(
        &self,
        addr: *mut u8,
        len: usize,
        access: AccessFlags,
    ) -> io::Result<MemoryRegion> {
        let mr = unsafe {
            mlx5_sys::ibv_reg_mr(
                self.as_ptr(),
                addr as *mut std::ffi::c_void,
                len,
                access.bits() as i32,
            )
        };
        NonNull::new(mr).map_or(Err(io::Error::last_os_error()), |mr| {
            Ok(MemoryRegion {
                mr,
                _pd: self.clone(),
            })
        })
    }
}

impl Drop for MemoryRegion {
    fn drop(&mut self) {
        unsafe {
            mlx5_sys::ibv_dereg_mr(self.mr.as_ptr());
        }
    }
}

impl MemoryRegion {
    /// Returns the local key (L_Key) for this memory region.
    ///
    /// The lkey is used in `ibv_sge` when posting buffers with `ibv_post_*` verbs.
    pub fn lkey(&self) -> u32 {
        unsafe { (*self.mr.as_ptr()).lkey }
    }

    /// Returns the remote key (R_Key) for this memory region.
    ///
    /// The rkey is used by remote processes to perform RDMA read/write
    /// and atomic operations on this memory region.
    pub fn rkey(&self) -> u32 {
        unsafe { (*self.mr.as_ptr()).rkey }
    }

    /// Returns the starting address of the registered memory region.
    pub fn addr(&self) -> *mut u8 {
        unsafe { (*self.mr.as_ptr()).addr as *mut u8 }
    }

    /// Returns the length of the registered memory region in bytes.
    pub fn len(&self) -> usize {
        unsafe { (*self.mr.as_ptr()).length }
    }

    /// Returns true if the memory region is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// =============================================================================
// Address Handle
// =============================================================================

/// Remote UD QP information for creating Address Handle.
#[derive(Debug, Clone, Copy)]
pub struct RemoteUdQpInfo {
    /// Remote QP number.
    pub qpn: u32,
    /// Remote Q_Key.
    pub qkey: u32,
    /// Remote LID.
    pub lid: u16,
}

/// Address Handle for UD (Unreliable Datagram) transport.
///
/// An AH defines the path (route) to a remote UD QP.
/// It encapsulates addressing information required to send messages.
pub struct AddressHandle {
    ah: NonNull<mlx5_sys::ibv_ah>,
    /// Remote QP number.
    qpn: u32,
    /// Q_Key for this destination.
    qkey: u32,
    /// Destination LID.
    dlid: u16,
    /// Keep the PD alive while this AH exists.
    _pd: Pd,
}

impl Pd {
    /// Create an Address Handle for UD transport.
    ///
    /// # Arguments
    /// * `port` - Local port number
    /// * `remote` - Remote UD QP information
    ///
    /// # Errors
    /// Returns an error if the AH cannot be created.
    pub fn create_ah(&self, port: u8, remote: &RemoteUdQpInfo) -> io::Result<AddressHandle> {
        use std::mem::MaybeUninit;

        unsafe {
            let mut ah_attr: mlx5_sys::ibv_ah_attr = MaybeUninit::zeroed().assume_init();
            ah_attr.dlid = remote.lid;
            ah_attr.sl = 0;
            ah_attr.src_path_bits = 0;
            ah_attr.port_num = port;

            let ah = mlx5_sys::ibv_create_ah(self.as_ptr(), &mut ah_attr);
            NonNull::new(ah).map_or(Err(io::Error::last_os_error()), |ah| {
                Ok(AddressHandle {
                    ah,
                    qpn: remote.qpn,
                    qkey: remote.qkey,
                    dlid: remote.lid,
                    _pd: self.clone(),
                })
            })
        }
    }
}

impl Drop for AddressHandle {
    fn drop(&mut self) {
        unsafe {
            mlx5_sys::ibv_destroy_ah(self.ah.as_ptr());
        }
    }
}

impl AddressHandle {
    /// Get the raw ibv_ah pointer.
    #[allow(dead_code)]
    pub(crate) fn as_ptr(&self) -> *mut mlx5_sys::ibv_ah {
        self.ah.as_ptr()
    }

    /// Get the remote QP number.
    pub fn qpn(&self) -> u32 {
        self.qpn
    }

    /// Get the Q_Key.
    pub fn qkey(&self) -> u32 {
        self.qkey
    }

    /// Get the destination LID.
    pub fn dlid(&self) -> u16 {
        self.dlid
    }
}
