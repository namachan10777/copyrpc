//! Shared memory region via /dev/shm for benchkv client-daemon communication.
//!
//! Layout:
//!   Offset 0:    Header (64B)
//!     [max_clients: u32] [_pad: 28B] [connected: AtomicU32]
//!   Offset 64:   SlotPair[0] (128B)
//!     [req: 64B] [resp: 64B]
//!   Offset 192:  SlotPair[1] (128B)
//!   ...
//!   Total: 64 + max_clients * 128

use std::os::fd::OwnedFd;
use std::os::unix::io::FromRawFd;
use std::sync::atomic::{AtomicU32, Ordering};

use crate::slot::SLOT_SIZE;

const HEADER_SIZE: usize = 64;
const SLOT_PAIR_SIZE: usize = SLOT_SIZE * 2; // req + resp = 128B
const CONNECTED_OFFSET: usize = 32; // offset of AtomicU32 within header

fn shm_total_size(max_clients: u32) -> usize {
    HEADER_SIZE + max_clients as usize * SLOT_PAIR_SIZE
}

pub struct ShmSlot {
    pub req: *mut u8,
    pub resp: *mut u8,
}

pub struct ShmServer {
    ptr: *mut u8,
    max_clients: u32,
    _fd: OwnedFd,
    path: String,
}

unsafe impl Send for ShmServer {}

impl ShmServer {
    pub fn create(path: &str, max_clients: u32) -> Self {
        let total = shm_total_size(max_clients);
        let c_path = std::ffi::CString::new(path).expect("invalid shm path");

        unsafe {
            let fd = libc::shm_open(
                c_path.as_ptr(),
                libc::O_CREAT | libc::O_RDWR | libc::O_TRUNC,
                0o600,
            );
            assert!(
                fd >= 0,
                "shm_open failed: {}",
                std::io::Error::last_os_error()
            );
            let ret = libc::ftruncate(fd, total as libc::off_t);
            assert!(ret == 0, "ftruncate failed");

            let ptr = libc::mmap(
                std::ptr::null_mut(),
                total,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd,
                0,
            );
            assert!(ptr != libc::MAP_FAILED, "mmap failed");
            let ptr = ptr as *mut u8;

            // Zero entire region
            std::ptr::write_bytes(ptr, 0, total);

            // Write max_clients into header
            (ptr as *mut u32).write(max_clients);

            ShmServer {
                ptr,
                max_clients,
                _fd: OwnedFd::from_raw_fd(fd),
                path: path.to_string(),
            }
        }
    }

    pub fn slot(&self, client_id: u32) -> ShmSlot {
        assert!(client_id < self.max_clients);
        unsafe {
            let base = self
                .ptr
                .add(HEADER_SIZE + client_id as usize * SLOT_PAIR_SIZE);
            ShmSlot {
                req: base,
                resp: base.add(SLOT_SIZE),
            }
        }
    }

    pub fn max_clients(&self) -> u32 {
        self.max_clients
    }

    pub fn connected_count(&self) -> u32 {
        unsafe {
            let atom = &*(self.ptr.add(CONNECTED_OFFSET) as *const AtomicU32);
            atom.load(Ordering::Acquire)
        }
    }

}

impl Drop for ShmServer {
    fn drop(&mut self) {
        let total = shm_total_size(self.max_clients);
        unsafe {
            libc::munmap(self.ptr as *mut libc::c_void, total);
        }
        let c_path = std::ffi::CString::new(self.path.as_str()).unwrap();
        unsafe {
            libc::shm_unlink(c_path.as_ptr());
        }
    }
}

pub struct ShmClient {
    pub req: *mut u8,
    pub resp: *mut u8,
}

unsafe impl Send for ShmClient {}

impl ShmClient {
    pub fn connect(path: &str, client_id: u32) -> Self {
        let c_path = std::ffi::CString::new(path).expect("invalid shm path");

        unsafe {
            let fd = libc::shm_open(c_path.as_ptr(), libc::O_RDWR, 0);
            assert!(
                fd >= 0,
                "shm_open connect failed: {}",
                std::io::Error::last_os_error()
            );

            // Read max_clients from header to compute size
            let header_buf = [0u8; HEADER_SIZE];
            let ret = libc::read(fd, header_buf.as_ptr() as *mut libc::c_void, HEADER_SIZE);
            assert!(ret == HEADER_SIZE as isize, "read header failed");
            let max_clients = *(header_buf.as_ptr() as *const u32);
            assert!(client_id < max_clients, "client_id out of range");

            let total = shm_total_size(max_clients);
            let ptr = libc::mmap(
                std::ptr::null_mut(),
                total,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd,
                0,
            );
            assert!(ptr != libc::MAP_FAILED, "mmap connect failed");
            libc::close(fd);

            let ptr = ptr as *mut u8;

            // Increment connected count
            let atom = &*(ptr.add(CONNECTED_OFFSET) as *const AtomicU32);
            atom.fetch_add(1, Ordering::Release);

            let base = ptr.add(HEADER_SIZE + client_id as usize * SLOT_PAIR_SIZE);
            ShmClient {
                req: base,
                resp: base.add(SLOT_SIZE),
            }
        }
    }
}
