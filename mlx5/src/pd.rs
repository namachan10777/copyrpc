use std::{io, ptr::NonNull};

use bitflags::bitflags;

use crate::device::Context;

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct AccessFlags: u32 {
        const LOCAL_WRITE = mlx5_sys::ibv_access_flags_IBV_ACCESS_LOCAL_WRITE;
        const REMOTE_WRITE = mlx5_sys::ibv_access_flags_IBV_ACCESS_REMOTE_WRITE;
        const REMOTE_READ = mlx5_sys::ibv_access_flags_IBV_ACCESS_REMOTE_READ;
        const REMOTE_ATOMIC = mlx5_sys::ibv_access_flags_IBV_ACCESS_REMOTE_ATOMIC;
        const MW_BIND = mlx5_sys::ibv_access_flags_IBV_ACCESS_MW_BIND;
        const ZERO_BASED = mlx5_sys::ibv_access_flags_IBV_ACCESS_ZERO_BASED;
        const ON_DEMAND = mlx5_sys::ibv_access_flags_IBV_ACCESS_ON_DEMAND;
        const HUGETLB = mlx5_sys::ibv_access_flags_IBV_ACCESS_HUGETLB;
        const FLUSH_GLOBAL = mlx5_sys::ibv_access_flags_IBV_ACCESS_FLUSH_GLOBAL;
        const FLUSH_PERSISTENT = mlx5_sys::ibv_access_flags_IBV_ACCESS_FLUSH_PERSISTENT;
        const RELAXED_ORDERING = mlx5_sys::ibv_access_flags_IBV_ACCESS_RELAXED_ORDERING;
    }
}

pub struct ProtectionDomain {
    pd: NonNull<mlx5_sys::ibv_pd>,
}

impl Context {
    pub fn alloc_pd(&self) -> io::Result<ProtectionDomain> {
        unsafe {
            let pd = mlx5_sys::ibv_alloc_pd(self.ctx.as_ptr());
            NonNull::new(pd).map_or(Err(io::Error::last_os_error()), |pd| {
                Ok(ProtectionDomain { pd })
            })
        }
    }
}

impl Drop for ProtectionDomain {
    fn drop(&mut self) {
        unsafe {
            mlx5_sys::ibv_dealloc_pd(self.pd.as_ptr());
        }
    }
}

pub struct MemoryRegion {
    mr: NonNull<mlx5_sys::ibv_mr>,
}

impl ProtectionDomain {
    /// Register a memory region.
    ///
    /// # Safety
    /// The caller must ensure that the memory region pointed to by `addr` with `len` bytes
    /// remains valid for the lifetime of the returned `MemoryRegion`.
    pub unsafe fn register(
        &self,
        addr: *mut u8,
        len: usize,
        access: AccessFlags,
    ) -> io::Result<MemoryRegion> {
        let mr = unsafe {
            mlx5_sys::ibv_reg_mr(
                self.pd.as_ptr(),
                addr as *mut std::ffi::c_void,
                len,
                access.bits() as i32,
            )
        };
        NonNull::new(mr).map_or(Err(io::Error::last_os_error()), |mr| {
            Ok(MemoryRegion { mr })
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
    pub fn lkey(&self) -> u32 {
        unsafe { (*self.mr.as_ptr()).lkey }
    }

    pub fn rkey(&self) -> u32 {
        unsafe { (*self.mr.as_ptr()).rkey }
    }

    pub fn addr(&self) -> *mut u8 {
        unsafe { (*self.mr.as_ptr()).addr as *mut u8 }
    }

    pub fn len(&self) -> usize {
        unsafe { (*self.mr.as_ptr()).length }
    }
}
