//! Shared memory management using `/dev/shm`.

use nix::fcntl::OFlag;
use nix::sys::mman::{MapFlags, ProtFlags, mmap, munmap, shm_open, shm_unlink};
use nix::sys::stat::Mode;
use nix::unistd::{close, ftruncate};
use std::ffi::CString;
use std::io;
use std::num::NonZeroUsize;
use std::os::fd::{AsRawFd, IntoRawFd, RawFd};
use std::path::Path;
use std::ptr::NonNull;

/// A region of shared memory backed by `/dev/shm`.
pub struct SharedMemory {
    ptr: NonNull<u8>,
    size: usize,
    name: CString,
    fd: RawFd,
    is_owner: bool,
}

unsafe impl Send for SharedMemory {}
unsafe impl Sync for SharedMemory {}

impl SharedMemory {
    /// Creates a new shared memory region.
    ///
    /// The caller becomes the owner and is responsible for unlinking
    /// the shared memory when done.
    ///
    /// # Safety
    /// The caller must ensure that `size` is appropriate for the intended use
    /// and that the path is valid.
    pub unsafe fn create<P: AsRef<Path>>(path: P, size: usize) -> io::Result<Self> {
        let name = path_to_cstring(path)?;

        // Create shared memory object
        let fd = shm_open(
            name.as_c_str(),
            OFlag::O_CREAT | OFlag::O_EXCL | OFlag::O_RDWR,
            Mode::S_IRUSR | Mode::S_IWUSR,
        )
        .map_err(|e| io::Error::from_raw_os_error(e as i32))?;

        let raw_fd = fd.as_raw_fd();

        // Set size
        if let Err(e) = ftruncate(&fd, size as i64) {
            let _ = close(raw_fd);
            let _ = shm_unlink(name.as_c_str());
            return Err(io::Error::from_raw_os_error(e as i32));
        }

        // Map into memory
        let ptr = match unsafe {
            mmap(
                None,
                NonZeroUsize::new(size).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "size must be non-zero")
                })?,
                ProtFlags::PROT_READ | ProtFlags::PROT_WRITE,
                MapFlags::MAP_SHARED,
                &fd,
                0,
            )
        } {
            Ok(p) => p,
            Err(e) => {
                let _ = close(raw_fd);
                let _ = shm_unlink(name.as_c_str());
                return Err(io::Error::from_raw_os_error(e as i32));
            }
        };

        Ok(Self {
            ptr: unsafe { NonNull::new_unchecked(ptr.as_ptr().cast()) },
            size,
            name,
            fd: fd.into_raw_fd(),
            is_owner: true,
        })
    }

    /// Opens an existing shared memory region.
    ///
    /// # Safety
    /// The caller must ensure that the shared memory exists and has the
    /// expected size and layout.
    pub unsafe fn open<P: AsRef<Path>>(path: P, size: usize) -> io::Result<Self> {
        let name = path_to_cstring(path)?;

        // Open existing shared memory object
        let fd = shm_open(name.as_c_str(), OFlag::O_RDWR, Mode::empty())
            .map_err(|e| io::Error::from_raw_os_error(e as i32))?;

        let raw_fd = fd.as_raw_fd();

        // Map into memory
        let ptr = match unsafe {
            mmap(
                None,
                NonZeroUsize::new(size).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "size must be non-zero")
                })?,
                ProtFlags::PROT_READ | ProtFlags::PROT_WRITE,
                MapFlags::MAP_SHARED,
                &fd,
                0,
            )
        } {
            Ok(p) => p,
            Err(e) => {
                let _ = close(raw_fd);
                return Err(io::Error::from_raw_os_error(e as i32));
            }
        };

        Ok(Self {
            ptr: unsafe { NonNull::new_unchecked(ptr.as_ptr().cast()) },
            size,
            name,
            fd: fd.into_raw_fd(),
            is_owner: false,
        })
    }

    /// Returns a pointer to the start of the shared memory region.
    pub fn as_ptr(&self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    /// Returns the size of the shared memory region.
    pub fn size(&self) -> usize {
        self.size
    }
}

impl Drop for SharedMemory {
    fn drop(&mut self) {
        unsafe {
            let _ = munmap(
                NonNull::new_unchecked(self.ptr.as_ptr() as *mut _),
                self.size,
            );
            let _ = close(self.fd);

            if self.is_owner {
                let _ = shm_unlink(self.name.as_c_str());
            }
        }
    }
}

fn path_to_cstring<P: AsRef<Path>>(path: P) -> io::Result<CString> {
    let path_str = path.as_ref().to_str().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "path contains invalid UTF-8")
    })?;

    // Ensure the path starts with /
    let name = if path_str.starts_with('/') {
        path_str.to_string()
    } else {
        format!("/{}", path_str)
    };

    CString::new(name)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains null byte"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_and_open() {
        let name = format!("/shm_test_{}", std::process::id());
        let size = 4096;

        unsafe {
            // Create
            let shm1 = SharedMemory::create(&name, size).unwrap();
            assert_eq!(shm1.size(), size);

            // Write some data
            std::ptr::write_volatile(shm1.as_ptr(), 42u8);

            // Open from another "process" (simulated)
            let shm2 = SharedMemory::open(&name, size).unwrap();

            // Read the data
            let value = std::ptr::read_volatile(shm2.as_ptr());
            assert_eq!(value, 42u8);

            // shm1 is owner, will unlink on drop
            drop(shm2);
            drop(shm1);
        }
    }
}
