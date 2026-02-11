//! C FFI for libcopyrpcfs — used by IOR AIORI plugin.

use std::ffi::CStr;
use std::os::raw::c_char;

use crate::client::FsClient;

/// Connect to a copyrpc-fsd daemon via shared memory.
///
/// Returns a pointer to an opaque `FsClient`, or null on failure.
///
/// # Safety
/// `shm_path` must be a valid null-terminated C string pointing to a live
/// copyrpc-fsd server.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn copyrpcfs_connect(
    shm_path: *const c_char,
    chunk_size: usize,
) -> *mut FsClient {
    let path = unsafe { CStr::from_ptr(shm_path) };
    let Ok(path_str) = path.to_str() else {
        return std::ptr::null_mut();
    };
    match unsafe { FsClient::connect(path_str, chunk_size) } {
        Ok(client) => Box::into_raw(Box::new(client)),
        Err(e) => {
            eprintln!("copyrpcfs_connect: {:?}", e);
            std::ptr::null_mut()
        }
    }
}

/// Disconnect and free the client.
///
/// # Safety
/// `client` must be a valid pointer returned by `copyrpcfs_connect`, and must
/// not be used after this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn copyrpcfs_disconnect(client: *mut FsClient) {
    if !client.is_null() {
        drop(unsafe { Box::from_raw(client) });
    }
}

/// Create a file.
///
/// Returns 0 on success, negative errno on error.
///
/// # Safety
/// `client` must be a valid pointer. `path` must be null-terminated.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn copyrpcfs_create(
    client: *mut FsClient,
    path: *const c_char,
    mode: u32,
) -> i32 {
    let client = unsafe { &mut *client };
    let path = unsafe { CStr::from_ptr(path) };
    let Ok(path_str) = path.to_str() else {
        return -libc::EINVAL;
    };
    match client.create(path_str, mode) {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("copyrpcfs_create: {}", e);
            -libc::EIO
        }
    }
}

/// Open a file and return a file descriptor, or -1 on error.
///
/// # Safety
/// `client` must be a valid pointer. `path` must be null-terminated.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn copyrpcfs_open(client: *mut FsClient, path: *const c_char) -> i64 {
    let client = unsafe { &mut *client };
    let path = unsafe { CStr::from_ptr(path) };
    let Ok(path_str) = path.to_str() else {
        return -1;
    };
    match client.open(path_str) {
        Ok(fd) => fd as i64,
        Err(e) => {
            eprintln!("copyrpcfs_open: {}", e);
            -1
        }
    }
}

/// Close a file descriptor.
///
/// Returns 0 on success, negative errno on error.
///
/// # Safety
/// `client` must be a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn copyrpcfs_close(client: *mut FsClient, fd: u32) -> i32 {
    let client = unsafe { &mut *client };
    match client.close(fd) {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("copyrpcfs_close: {}", e);
            -libc::EBADF
        }
    }
}

/// Write data at a given offset (pwrite semantics).
///
/// Returns bytes written, or negative errno on error.
///
/// # Safety
/// `client` must be valid. `buf` must point to at least `len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn copyrpcfs_pwrite(
    client: *mut FsClient,
    fd: u32,
    buf: *const u8,
    len: usize,
    offset: u64,
) -> i64 {
    let client = unsafe { &mut *client };
    let data = unsafe { std::slice::from_raw_parts(buf, len) };
    match client.pwrite(fd, data, offset) {
        Ok(n) => n as i64,
        Err(e) => {
            eprintln!("copyrpcfs_pwrite: {}", e);
            -(libc::EIO as i64)
        }
    }
}

/// Read data at a given offset (pread semantics).
///
/// Returns bytes read, or negative errno on error.
///
/// # Safety
/// `client` must be valid. `buf` must point to at least `len` writable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn copyrpcfs_pread(
    client: *mut FsClient,
    fd: u32,
    buf: *mut u8,
    len: usize,
    offset: u64,
) -> i64 {
    let client = unsafe { &mut *client };
    let data = unsafe { std::slice::from_raw_parts_mut(buf, len) };
    match client.pread(fd, data, offset) {
        Ok(n) => n as i64,
        Err(e) => {
            eprintln!("copyrpcfs_pread: {}", e);
            -(libc::EIO as i64)
        }
    }
}

/// Get file size via stat.
///
/// Returns 0 on success (size written to `size_out`), negative errno on error.
///
/// # Safety
/// `client` must be valid. `path` must be null-terminated. `size_out` must be
/// a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn copyrpcfs_stat(
    client: *mut FsClient,
    path: *const c_char,
    size_out: *mut u64,
) -> i32 {
    let client = unsafe { &mut *client };
    let path = unsafe { CStr::from_ptr(path) };
    let Ok(path_str) = path.to_str() else {
        return -libc::EINVAL;
    };
    match client.stat(path_str) {
        Ok(header) => {
            unsafe { *size_out = header.size };
            0
        }
        Err(crate::client::FsError::NotFound) => -libc::ENOENT,
        Err(e) => {
            eprintln!("copyrpcfs_stat: {}", e);
            -libc::EIO
        }
    }
}

/// Remove (unlink) a file.
///
/// Returns 0 on success, negative errno on error.
///
/// # Safety
/// `client` must be valid. `path` must be null-terminated.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn copyrpcfs_unlink(client: *mut FsClient, path: *const c_char) -> i32 {
    let client = unsafe { &mut *client };
    let path = unsafe { CStr::from_ptr(path) };
    let Ok(path_str) = path.to_str() else {
        return -libc::EINVAL;
    };
    match client.unlink(path_str) {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("copyrpcfs_unlink: {}", e);
            -libc::EIO
        }
    }
}
