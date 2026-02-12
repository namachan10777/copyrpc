//! DevX resource wrappers.
//!
//! RAII wrappers for mlx5 DevX objects: UAR, UMEM, and generic DevX objects (QP/CQ/SRQ).

use std::io;
use std::ptr::NonNull;

use crate::device::Context;

// ============================================================================
// DevX UAR (User Access Region)
// ============================================================================

/// UAR (User Access Region) — doorbell/BlueFlame register page.
///
/// A UAR provides the register address for writing doorbells and BlueFlame WQEs.
/// UAR pages can be shared across multiple resources (QP, CQ, SRQ).
pub struct DevxUar {
    uar: NonNull<mlx5_sys::mlx5dv_devx_uar>,
}

impl DevxUar {
    /// Get the UAR page ID for use in PRM contexts (QPC, CQC).
    pub fn page_id(&self) -> u32 {
        unsafe { self.uar.as_ref().page_id }
    }

    /// Get the BlueFlame register address.
    pub fn reg_addr(&self) -> *mut u8 {
        unsafe { self.uar.as_ref().reg_addr as *mut u8 }
    }

    /// Get the base address of the UAR page.
    pub fn base_addr(&self) -> *mut u8 {
        unsafe { self.uar.as_ref().base_addr as *mut u8 }
    }
}

impl Drop for DevxUar {
    fn drop(&mut self) {
        unsafe {
            mlx5_sys::mlx5dv_devx_free_uar(self.uar.as_ptr());
        }
    }
}

// ============================================================================
// DevX UMEM (User Memory)
// ============================================================================

/// UMEM — user memory region registered with the device.
///
/// Allows the HCA to access user-allocated memory buffers for CQ, SRQ, and QP
/// work queue data. The UMEM ID is used in PRM commands (PAS entries, DBR encoding).
pub struct DevxUmem {
    umem: NonNull<mlx5_sys::mlx5dv_devx_umem>,
}

impl DevxUmem {
    /// Get the UMEM ID for use in PRM commands.
    pub fn umem_id(&self) -> u32 {
        unsafe { self.umem.as_ref().umem_id }
    }
}

impl Drop for DevxUmem {
    fn drop(&mut self) {
        unsafe {
            mlx5_sys::mlx5dv_devx_umem_dereg(self.umem.as_ptr());
        }
    }
}

// ============================================================================
// DevX Object (QP, CQ, SRQ)
// ============================================================================

/// Generic DevX object (QP, CQ, SRQ).
///
/// Wraps a `mlx5dv_devx_obj` and provides modify capability.
/// The object is destroyed when dropped.
pub struct DevxObj {
    obj: NonNull<mlx5_sys::mlx5dv_devx_obj>,
}

impl DevxObj {
    /// Modify the DevX object using a PRM command.
    ///
    /// Returns the result code from `mlx5dv_devx_obj_modify`.
    pub fn modify(&self, cmd_in: &[u8], cmd_out: &mut [u8]) -> i32 {
        unsafe {
            mlx5_sys::mlx5dv_devx_obj_modify(
                self.obj.as_ptr(),
                cmd_in.as_ptr() as *const _,
                cmd_in.len(),
                cmd_out.as_mut_ptr() as *mut _,
                cmd_out.len(),
            )
        }
    }
}

impl Drop for DevxObj {
    fn drop(&mut self) {
        unsafe {
            mlx5_sys::mlx5dv_devx_obj_destroy(self.obj.as_ptr());
        }
    }
}

// ============================================================================
// Context DevX methods
// ============================================================================

impl Context {
    /// Allocate a DevX UAR (User Access Region).
    pub fn alloc_uar(&self) -> io::Result<DevxUar> {
        unsafe {
            let uar = mlx5_sys::mlx5dv_devx_alloc_uar(self.as_ptr(), 0);
            NonNull::new(uar).map_or(Err(io::Error::last_os_error()), |uar| {
                Ok(DevxUar { uar })
            })
        }
    }

    /// Register user memory with the device (UMEM).
    ///
    /// # Safety
    /// The caller must ensure that the memory pointed to by `addr` with `len` bytes
    /// remains valid for the lifetime of the returned `DevxUmem`.
    pub unsafe fn register_umem(
        &self,
        addr: *mut u8,
        len: usize,
        access: u32,
    ) -> io::Result<DevxUmem> {
        let umem = mlx5_sys::mlx5dv_devx_umem_reg(
            self.as_ptr(),
            addr as *mut _,
            len,
            access,
        );
        NonNull::new(umem).map_or(Err(io::Error::last_os_error()), |umem| {
            Ok(DevxUmem { umem })
        })
    }

    /// Query the Event Queue Number (EQN) for a completion vector.
    ///
    /// The EQN is required when creating a CQ via DevX PRM (c_eqn field in CQC).
    pub fn query_eqn(&self, vector: u32) -> io::Result<u32> {
        unsafe {
            let mut eqn: u32 = 0;
            let ret = mlx5_sys::mlx5dv_devx_query_eqn(self.as_ptr(), vector, &mut eqn);
            if ret != 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(eqn)
        }
    }

    /// Create a DevX object via PRM command.
    ///
    /// This is the low-level interface for creating QPs, CQs, SRQs via DevX.
    /// Callers should use the higher-level create methods instead.
    pub(crate) fn devx_obj_create(
        &self,
        cmd_in: &[u8],
        cmd_out: &mut [u8],
    ) -> io::Result<DevxObj> {
        unsafe {
            let obj = mlx5_sys::mlx5dv_devx_obj_create(
                self.as_ptr(),
                cmd_in.as_ptr() as *const _,
                cmd_in.len(),
                cmd_out.as_mut_ptr() as *mut _,
                cmd_out.len(),
            );
            NonNull::new(obj).map_or_else(
                || {
                    let errno = io::Error::last_os_error();
                    // Try to extract PRM status/syndrome from output buffer
                    let (status, syndrome) = if cmd_out.len() >= 8 {
                        let status = crate::prm::prm_get(cmd_out, 0x00, 8);
                        let syndrome = crate::prm::prm_get(cmd_out, 0x20, 0x20);
                        (status, syndrome)
                    } else {
                        (0, 0)
                    };
                    Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!(
                            "devx_obj_create failed: errno={}, prm_status=0x{:x}, prm_syndrome=0x{:x}",
                            errno, status, syndrome
                        ),
                    ))
                },
                |obj| Ok(DevxObj { obj }),
            )
        }
    }
}
