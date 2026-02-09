use nix::fcntl::{OFlag, open};
use nix::sys::mman::{MapFlags, ProtFlags, mmap, munmap};
use nix::sys::stat::Mode;
use nix::unistd::close;
use std::io;
use std::num::NonZeroUsize;
use std::os::fd::{AsRawFd, IntoRawFd, RawFd};
use std::path::Path;
use std::ptr::NonNull;

pub struct DevDaxRegion {
    ptr: NonNull<u8>,
    len: usize,
    fd: RawFd,
}

unsafe impl Send for DevDaxRegion {}
unsafe impl Sync for DevDaxRegion {}

impl DevDaxRegion {
    /// Open a DevDax device and map it into memory.
    ///
    /// # Safety
    ///
    /// The caller must ensure that:
    /// - The path points to a valid DevDax device
    /// - No other process is accessing the device in an incompatible way
    /// - The device supports MAP_SYNC (i.e., is in devdax mode, not fsdax)
    pub unsafe fn open(path: &Path) -> io::Result<Self> {
        // Open the device
        let owned_fd = open(path, OFlag::O_RDWR, Mode::empty())
            .map_err(|e| io::Error::from_raw_os_error(e as i32))?;
        let raw_fd = owned_fd.as_raw_fd();

        // Get the device size using fstat
        let mut stat_buf: libc::stat = unsafe { std::mem::zeroed() };
        let stat_result = unsafe { libc::fstat(raw_fd, &mut stat_buf) };
        if stat_result != 0 {
            let err = io::Error::last_os_error();
            drop(owned_fd); // Close the fd before returning
            return Err(err);
        }
        let len = stat_buf.st_size as usize;

        // Prepare mmap flags
        // MAP_SHARED_VALIDATE ensures the kernel validates the flags
        let map_shared_validate = MapFlags::from_bits_truncate(libc::MAP_SHARED_VALIDATE);
        // MAP_SYNC enables synchronous page faults for persistent memory
        let map_sync = MapFlags::from_bits_truncate(0x80000);
        let flags = map_shared_validate | map_sync;

        // Map the device
        let ptr = unsafe {
            mmap(
                None,
                NonZeroUsize::new(len).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "device size is zero")
                })?,
                ProtFlags::PROT_READ | ProtFlags::PROT_WRITE,
                flags,
                &owned_fd,
                0,
            )
        }
        .map_err(|e| {
            let err = io::Error::from_raw_os_error(e as i32);
            // Close the fd before returning
            let _ = close(raw_fd);
            err
        })?;

        // Convert to raw fd and store it (we now own it)
        let fd = owned_fd.into_raw_fd();

        Ok(Self {
            ptr: NonNull::new(ptr.as_ptr().cast::<u8>()).unwrap(),
            len,
            fd,
        })
    }

    pub fn as_ptr(&self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.len
    }
}

impl Drop for DevDaxRegion {
    fn drop(&mut self) {
        unsafe {
            // Unmap the memory region
            let _ = munmap(self.ptr.cast(), self.len);
            // Close the file descriptor
            let _ = close(self.fd);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore = "requires devdax hardware"]
    fn test_devdax_open() {
        // This test requires a real devdax device like /dev/dax0.0
        let path = Path::new("/dev/dax0.0");
        if !path.exists() {
            eprintln!("DevDax device not found, skipping test");
            return;
        }

        unsafe {
            let region = DevDaxRegion::open(path).expect("failed to open devdax device");
            assert!(region.len() > 0, "device size should be non-zero");
            assert!(
                !region.as_ptr().is_null(),
                "mapped pointer should be non-null"
            );

            // Try writing and reading back
            let ptr = region.as_ptr();
            *ptr = 42;
            assert_eq!(*ptr, 42, "write/read should work");
        }
    }
}
