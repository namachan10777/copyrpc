use nix::fcntl::{open, OFlag};
use nix::sys::mman::{mmap, munmap, MapFlags, ProtFlags};
use nix::sys::stat::Mode;
use nix::unistd::{close, ftruncate};
use std::io;
use std::num::NonZeroUsize;
use std::os::fd::{AsRawFd, IntoRawFd, RawFd};
use std::path::Path;
use std::ptr::NonNull;

pub struct FileRegion {
    ptr: NonNull<u8>,
    len: usize,
    fd: RawFd,
}

unsafe impl Send for FileRegion {}
unsafe impl Sync for FileRegion {}

impl FileRegion {
    /// Creates a new file-backed memory region.
    ///
    /// # Safety
    ///
    /// The caller must ensure that:
    /// - The path is valid and writable
    /// - The length is appropriate for the intended use
    /// - No other process is accessing the file in an incompatible way
    pub unsafe fn create(path: &Path, len: usize) -> io::Result<Self> {
        // Open the file with O_CREAT | O_RDWR
        let owned_fd = open(
            path,
            OFlag::O_CREAT | OFlag::O_RDWR,
            Mode::S_IRUSR | Mode::S_IWUSR,
        )
        .map_err(|e| io::Error::from_raw_os_error(e as i32))?;

        let raw_fd = owned_fd.as_raw_fd();

        // Set the file size
        if let Err(e) = ftruncate(&owned_fd, len as i64) {
            drop(owned_fd); // Close the fd before returning
            return Err(io::Error::from_raw_os_error(e as i32));
        }

        // Map the file into memory
        let ptr = unsafe {
            mmap(
                None,
                NonZeroUsize::new(len).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "length must be non-zero")
                })?,
                ProtFlags::PROT_READ | ProtFlags::PROT_WRITE,
                MapFlags::MAP_SHARED,
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

    /// Opens an existing file-backed memory region.
    ///
    /// # Safety
    ///
    /// The caller must ensure that:
    /// - The file exists and is accessible
    /// - No other process is accessing the file in an incompatible way
    pub unsafe fn open(path: &Path) -> io::Result<Self> {
        // Open the file
        let owned_fd = open(path, OFlag::O_RDWR, Mode::empty())
            .map_err(|e| io::Error::from_raw_os_error(e as i32))?;

        let raw_fd = owned_fd.as_raw_fd();

        // Get the file size using fstat
        let mut stat_buf: libc::stat = unsafe { std::mem::zeroed() };
        let stat_result = unsafe { libc::fstat(raw_fd, &mut stat_buf) };
        if stat_result != 0 {
            let err = io::Error::last_os_error();
            drop(owned_fd); // Close the fd before returning
            return Err(err);
        }
        let len = stat_buf.st_size as usize;

        // Map the file into memory
        let ptr = unsafe {
            mmap(
                None,
                NonZeroUsize::new(len).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "file size is zero")
                })?,
                ProtFlags::PROT_READ | ProtFlags::PROT_WRITE,
                MapFlags::MAP_SHARED,
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

impl Drop for FileRegion {
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
    fn test_create_write_reopen() {
        let pid = std::process::id();
        let path = format!("/tmp/pmem_test_{}", pid);
        let path = Path::new(&path);
        let len = 4096;

        unsafe {
            // Create a new file region
            let region = FileRegion::create(path, len).expect("failed to create file region");
            assert_eq!(region.len(), len);

            // Write some test data
            let test_value = 42u64;
            std::ptr::write_volatile(region.as_ptr() as *mut u64, test_value);

            // Persist the data
            crate::flush::persist(region.as_ptr(), std::mem::size_of::<u64>());

            // Drop the region
            drop(region);

            // Reopen the file
            let region2 = FileRegion::open(path).expect("failed to reopen file region");
            assert_eq!(region2.len(), len);

            // Read back the data
            let read_value = std::ptr::read_volatile(region2.as_ptr() as *const u64);
            assert_eq!(read_value, test_value, "data should persist across reopen");

            // Cleanup
            drop(region2);
        }

        // Remove the test file
        std::fs::remove_file(path).expect("failed to remove test file");
    }

    #[test]
    fn test_as_ptr_and_len() {
        let pid = std::process::id();
        let path = format!("/tmp/pmem_test_ptr_{}", pid);
        let path = Path::new(&path);
        let len = 8192;

        unsafe {
            let region = FileRegion::create(path, len).expect("failed to create file region");

            // Verify as_ptr returns a non-null pointer
            assert!(!region.as_ptr().is_null(), "as_ptr should return non-null");

            // Verify len returns the correct size
            assert_eq!(region.len(), len, "len should return the correct size");

            drop(region);
        }

        // Cleanup
        std::fs::remove_file(path).expect("failed to remove test file");
    }
}
