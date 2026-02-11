//! Integration tests for copyrpc-fs: single-node loopback tests.
//!
//! Spawns a daemon thread (ipc::Server + PmemStore) and connects
//! an FsClient to it, then exercises create/write/read/stat/unlink.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};

use copyrpc_fs::client::{FsClient, FsError};
use copyrpc_fs::message::{DATA_AREA_OFFSET, FsRequest, FsResponse};
use copyrpc_fs::store::PmemStore;

const CHUNK_SIZE: usize = 4096;
const MAX_CLIENTS: u32 = 4;
const QUEUE_DEPTH: u32 = 8;
const EXTRA_BUF_SIZE: u32 = 8192; // PATH_AREA(4096) + DATA_AREA(4096)
const PMEM_SIZE: usize = 1 << 20; // 1 MiB

/// Run a minimal daemon event loop (single-node, no copyrpc).
fn run_test_daemon(
    mut server: ipc::Server<FsRequest, FsResponse>,
    mut store: PmemStore,
    stop: Arc<AtomicBool>,
    ready: Arc<Barrier>,
) {
    ready.wait();

    while !stop.load(Ordering::Relaxed) {
        server.poll();

        // Cache extra buffer pointers before recv loop
        let extra_buf_ptrs: Vec<Option<*mut u8>> = (0..MAX_CLIENTS)
            .map(|i| server.client_extra_buffer(ipc::ClientId(i)))
            .collect();

        while let Some(ipc_req) = server.recv() {
            let client_id = ipc_req.client_id();
            let client_idx = client_id.0 as usize;
            let req_data = *ipc_req.data();

            match req_data {
                FsRequest::Write {
                    path_hash,
                    chunk_index,
                    offset,
                    len,
                } => {
                    if let Some(buf_ptr) = extra_buf_ptrs.get(client_idx).copied().flatten() {
                        let data = unsafe {
                            std::slice::from_raw_parts(buf_ptr.add(DATA_AREA_OFFSET), len as usize)
                        };
                        store.write(path_hash, chunk_index, offset as usize, data);
                    }
                    ipc_req.reply(FsResponse::Ok);
                }

                FsRequest::Read {
                    path_hash,
                    chunk_index,
                    offset,
                    len,
                } => {
                    if let Some(buf_ptr) = extra_buf_ptrs.get(client_idx).copied().flatten() {
                        let dst = unsafe {
                            std::slice::from_raw_parts_mut(
                                buf_ptr.add(DATA_AREA_OFFSET),
                                len as usize,
                            )
                        };
                        let actual = store.read(path_hash, chunk_index, offset as usize, dst);
                        ipc_req.reply(FsResponse::ReadOk { len: actual as u32 });
                    } else {
                        ipc_req.reply(FsResponse::Error { code: -1 });
                    }
                }

                FsRequest::Create {
                    path_hash,
                    mode,
                    chunk_size: cs,
                    ..
                } => {
                    let header = copyrpc_fs::InodeHeader {
                        mode,
                        chunk_size: cs,
                        ..Default::default()
                    };
                    let header_bytes = unsafe {
                        std::slice::from_raw_parts(
                            &header as *const copyrpc_fs::InodeHeader as *const u8,
                            std::mem::size_of::<copyrpc_fs::InodeHeader>(),
                        )
                    };
                    store.write(path_hash, 0, 0, header_bytes);
                    ipc_req.reply(FsResponse::Ok);
                }

                FsRequest::Stat { path_hash, .. } => {
                    let mut header_buf = vec![0u8; std::mem::size_of::<copyrpc_fs::InodeHeader>()];
                    let read = store.read(path_hash, 0, 0, &mut header_buf);
                    if read >= std::mem::size_of::<copyrpc_fs::InodeHeader>() {
                        let header: copyrpc_fs::InodeHeader = unsafe {
                            std::ptr::read_unaligned(
                                header_buf.as_ptr() as *const copyrpc_fs::InodeHeader
                            )
                        };
                        ipc_req.reply(FsResponse::StatOk { header });
                    } else {
                        ipc_req.reply(FsResponse::NotFound);
                    }
                }

                FsRequest::Unlink { path_hash, .. } => {
                    store.remove(path_hash, 0);
                    ipc_req.reply(FsResponse::Ok);
                }

                FsRequest::Mkdir {
                    path_hash, mode, ..
                } => {
                    let header = copyrpc_fs::InodeHeader {
                        mode: mode | 0o040000,
                        ..Default::default()
                    };
                    let header_bytes = unsafe {
                        std::slice::from_raw_parts(
                            &header as *const copyrpc_fs::InodeHeader as *const u8,
                            std::mem::size_of::<copyrpc_fs::InodeHeader>(),
                        )
                    };
                    store.write(path_hash, 0, 0, header_bytes);
                    ipc_req.reply(FsResponse::Ok);
                }

                FsRequest::Readdir { .. } => {
                    ipc_req.reply(FsResponse::Error { code: -38 });
                }
            }
        }

        std::thread::yield_now();
    }
}

/// Helper to create a daemon + client pair for testing.
struct TestFixture {
    client: FsClient,
    stop: Arc<AtomicBool>,
    daemon_handle: Option<std::thread::JoinHandle<()>>,
}

impl TestFixture {
    fn new(shm_name: &str) -> Self {
        let shm_path = format!("/copyrpc_fs_test_{}", shm_name);

        let server = unsafe {
            ipc::Server::<FsRequest, FsResponse>::create(
                &shm_path,
                MAX_CLIENTS,
                QUEUE_DEPTH,
                EXTRA_BUF_SIZE,
            )
        }
        .expect("create server");

        let pmem_path = format!("/tmp/copyrpc_fs_test_{}", shm_name);
        let region: Box<dyn pmem::PmemRegion + Send> = Box::new(
            unsafe { pmem::FileRegion::create(std::path::Path::new(&pmem_path), PMEM_SIZE) }
                .expect("create pmem"),
        );
        let store = PmemStore::new(region, CHUNK_SIZE);

        let stop = Arc::new(AtomicBool::new(false));
        let ready = Arc::new(Barrier::new(2));

        let stop_clone = stop.clone();
        let ready_clone = ready.clone();
        let daemon_handle = std::thread::spawn(move || {
            run_test_daemon(server, store, stop_clone, ready_clone);
        });

        ready.wait();

        // Small delay to let daemon start polling
        std::thread::sleep(std::time::Duration::from_millis(10));

        let client = unsafe { FsClient::connect(&shm_path, CHUNK_SIZE) }.expect("connect client");

        TestFixture {
            client,
            stop,
            daemon_handle: Some(daemon_handle),
        }
    }
}

impl Drop for TestFixture {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.daemon_handle.take() {
            handle.join().ok();
        }
    }
}

#[test]
fn test_create_and_stat() {
    let mut fix = TestFixture::new("create_stat");

    fix.client.create("/test_file.txt", 0o644).unwrap();

    let header = fix.client.stat("/test_file.txt").unwrap();
    assert_eq!(header.mode, 0o644);
    assert_eq!(header.chunk_size, CHUNK_SIZE as u32);
}

#[test]
fn test_write_and_read() {
    let mut fix = TestFixture::new("write_read");

    fix.client.create("/data.bin", 0o644).unwrap();
    let fd = fix.client.open("/data.bin").unwrap();

    // Write some data
    let write_data = b"Hello, copyrpc-fs!";
    fix.client.pwrite(fd, write_data, 0).unwrap();

    // Read it back
    let mut read_buf = vec![0u8; write_data.len()];
    let n = fix.client.pread(fd, &mut read_buf, 0).unwrap();
    assert_eq!(n, write_data.len());
    assert_eq!(&read_buf, write_data);

    fix.client.close(fd).unwrap();
}

#[test]
fn test_write_at_offset() {
    let mut fix = TestFixture::new("write_offset");

    fix.client.create("/offset.bin", 0o644).unwrap();
    let fd = fix.client.open("/offset.bin").unwrap();

    // Write at offset 100
    let data = b"offset data";
    fix.client.pwrite(fd, data, 100).unwrap();

    // Read back at offset 100
    let mut buf = vec![0u8; data.len()];
    let n = fix.client.pread(fd, &mut buf, 100).unwrap();
    assert_eq!(n, data.len());
    assert_eq!(&buf, data);

    // Read at offset 0 should return some data (from the valid_len perspective)
    let mut buf2 = vec![0u8; 50];
    let n2 = fix.client.pread(fd, &mut buf2, 0).unwrap();
    assert_eq!(n2, 50);

    fix.client.close(fd).unwrap();
}

#[test]
fn test_unlink() {
    let mut fix = TestFixture::new("unlink");

    fix.client.create("/to_delete.txt", 0o644).unwrap();
    fix.client.stat("/to_delete.txt").unwrap();

    fix.client.unlink("/to_delete.txt").unwrap();

    // After unlink, stat should return NotFound
    match fix.client.stat("/to_delete.txt") {
        Err(FsError::NotFound) => {} // expected
        other => panic!("expected NotFound, got {:?}", other),
    }
}

#[test]
fn test_mkdir() {
    let mut fix = TestFixture::new("mkdir");

    fix.client.mkdir("/mydir", 0o755).unwrap();

    let header = fix.client.stat("/mydir").unwrap();
    assert_eq!(header.mode, 0o755 | 0o040000); // S_IFDIR
}

#[test]
fn test_fd_lifecycle() {
    let mut fix = TestFixture::new("fd_lifecycle");

    fix.client.create("/fd_test.bin", 0o644).unwrap();
    let fd = fix.client.open("/fd_test.bin").unwrap();

    // Close and reopen
    fix.client.close(fd).unwrap();
    let fd2 = fix.client.open("/fd_test.bin").unwrap();

    // Fd slot should be reused
    assert_eq!(fd, fd2);

    // Double close should fail
    fix.client.close(fd2).unwrap();
    match fix.client.close(fd2) {
        Err(FsError::NoSuchFd) => {}
        other => panic!("expected NoSuchFd, got {:?}", other),
    }
}

#[test]
fn test_large_write_multi_chunk() {
    let mut fix = TestFixture::new("large_write");

    fix.client.create("/large.bin", 0o644).unwrap();
    let fd = fix.client.open("/large.bin").unwrap();

    // Write data larger than one chunk (4096 bytes)
    // But limited by extra_buffer data area (also 4096 bytes)
    // So each chunk write is at most 4096 bytes
    let data: Vec<u8> = (0..CHUNK_SIZE * 2).map(|i| (i % 256) as u8).collect();
    let written = fix.client.pwrite(fd, &data, 0).unwrap();
    assert_eq!(written, data.len());

    // Read back
    let mut buf = vec![0u8; data.len()];
    let n = fix.client.pread(fd, &mut buf, 0).unwrap();
    assert_eq!(n, data.len());
    assert_eq!(buf, data);

    fix.client.close(fd).unwrap();
}
