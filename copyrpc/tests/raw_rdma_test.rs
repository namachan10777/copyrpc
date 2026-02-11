//! Tests for raw RDMA READ/WRITE operations via copyrpc.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use copyrpc::{Context, ContextBuilder, EndpointConfig, RemoteEndpointInfo};
use mlx5::pd::AccessFlags;
use mlx5::srq::SrqConfig;

/// Connection info exchanged between client and server.
#[derive(Clone)]
struct ConnectionInfo {
    qp_number: u32,
    packet_sequence_number: u32,
    local_identifier: u16,
    recv_ring_addr: u64,
    recv_ring_rkey: u32,
    recv_ring_size: u64,
    initial_credit: u64,
    /// MR info for raw RDMA target buffer.
    mr_addr: u64,
    mr_rkey: u32,
    #[allow(dead_code)]
    mr_lkey: u32,
}

/// Helper to build a context with default settings.
fn make_context<U>() -> Context<U> {
    ContextBuilder::new()
        .device_index(0)
        .port(1)
        .srq_config(SrqConfig {
            max_wr: 256,
            max_sge: 1,
        })
        .cq_size(1024)
        .build()
        .expect("Failed to create context")
}

// =============================================================================
// Test: RDMA WRITE to remote buffer
// =============================================================================

#[test]
fn test_raw_rdma_write() {
    let stop = Arc::new(AtomicBool::new(false));
    let stop_server = stop.clone();

    let (s_info_tx, s_info_rx) = mpsc::channel::<ConnectionInfo>();
    let (c_info_tx, c_info_rx) = mpsc::channel::<ConnectionInfo>();

    // Server thread: owns the target buffer
    let server_handle = thread::spawn(move || {
        let ctx: Context<()> = make_context();

        // Allocate target buffer and register MR with remote write access
        let mut buffer = vec![0u8; 4096];
        let mr = unsafe {
            ctx.pd().register(
                buffer.as_mut_ptr(),
                buffer.len(),
                AccessFlags::LOCAL_WRITE | AccessFlags::REMOTE_WRITE | AccessFlags::REMOTE_READ,
            )
        }
        .expect("Failed to register MR");

        let ep_config = EndpointConfig::default();
        let mut ep = ctx.create_endpoint(&ep_config).expect("create_endpoint");
        let (info, lid, _port) = ep.local_info(ctx.lid(), ctx.port());

        s_info_tx
            .send(ConnectionInfo {
                qp_number: info.qp_number,
                packet_sequence_number: 0,
                local_identifier: lid,
                recv_ring_addr: info.recv_ring_addr,
                recv_ring_rkey: info.recv_ring_rkey,
                recv_ring_size: info.recv_ring_size,
                initial_credit: info.initial_credit,
                mr_addr: mr.addr() as u64,
                mr_rkey: mr.rkey(),
                mr_lkey: mr.lkey(),
            })
            .unwrap();

        let client_info = c_info_rx.recv().unwrap();
        let remote = RemoteEndpointInfo {
            qp_number: client_info.qp_number,
            packet_sequence_number: client_info.packet_sequence_number,
            local_identifier: client_info.local_identifier,
            recv_ring_addr: client_info.recv_ring_addr,
            recv_ring_rkey: client_info.recv_ring_rkey,
            recv_ring_size: client_info.recv_ring_size,
            initial_credit: client_info.initial_credit,
        };
        ep.connect_ex(&remote, 0, ctx.port(), true)
            .expect("connect");

        // Poll until stop (process send CQ completions)
        while !stop_server.load(Ordering::Relaxed) {
            ctx.poll(|_, _| {});
            std::hint::spin_loop();
        }

        // Verify the buffer was written
        buffer
    });

    // Client thread: posts RDMA WRITE to server's buffer
    let ctx: Context<()> = make_context();

    let ep_config = EndpointConfig::default();
    let mut ep = ctx.create_endpoint(&ep_config).expect("create_endpoint");
    let (info, lid, _port) = ep.local_info(ctx.lid(), ctx.port());

    // Source buffer for RDMA WRITE
    let mut src_buffer = vec![0u8; 256];
    for (i, byte) in src_buffer.iter_mut().enumerate() {
        *byte = (i % 251) as u8; // distinct pattern
    }
    let src_mr = unsafe {
        ctx.pd().register(
            src_buffer.as_mut_ptr(),
            src_buffer.len(),
            AccessFlags::LOCAL_WRITE,
        )
    }
    .expect("register src MR");

    c_info_tx
        .send(ConnectionInfo {
            qp_number: info.qp_number,
            packet_sequence_number: 0,
            local_identifier: lid,
            recv_ring_addr: info.recv_ring_addr,
            recv_ring_rkey: info.recv_ring_rkey,
            recv_ring_size: info.recv_ring_size,
            initial_credit: info.initial_credit,
            mr_addr: 0,
            mr_rkey: 0,
            mr_lkey: 0,
        })
        .unwrap();

    let server_info = s_info_rx.recv().unwrap();
    let remote = RemoteEndpointInfo {
        qp_number: server_info.qp_number,
        packet_sequence_number: server_info.packet_sequence_number,
        local_identifier: server_info.local_identifier,
        recv_ring_addr: server_info.recv_ring_addr,
        recv_ring_rkey: server_info.recv_ring_rkey,
        recv_ring_size: server_info.recv_ring_size,
        initial_credit: server_info.initial_credit,
    };
    ep.connect_ex(&remote, 0, ctx.port(), true)
        .expect("connect");

    // Post RDMA WRITE: write src_buffer to offset 128 of server's buffer
    let remote_offset = 128u64;
    ep.post_rdma_write(
        server_info.mr_addr + remote_offset,
        server_info.mr_rkey,
        src_mr.addr() as u64,
        src_mr.lkey(),
        src_buffer.len() as u32,
    )
    .expect("post_rdma_write");

    assert_eq!(ep.raw_rdma_pending(), 1);

    // Poll until completion
    let start = std::time::Instant::now();
    let timeout = Duration::from_secs(5);
    while ep.raw_rdma_pending() > 0 {
        ctx.poll(|_, _| {});
        assert!(start.elapsed() < timeout, "RDMA WRITE timed out");
    }

    assert_eq!(ep.raw_rdma_pending(), 0);

    // Allow server to see the write (small delay for RDMA visibility)
    std::thread::sleep(Duration::from_millis(10));
    stop.store(true, Ordering::SeqCst);

    let server_buffer = server_handle.join().expect("server panicked");

    // Verify: bytes at offset 128..384 should match src_buffer
    assert_eq!(
        &server_buffer[128..128 + 256],
        &src_buffer[..],
        "RDMA WRITE data mismatch"
    );
    // Bytes before offset 128 should be zero
    assert!(server_buffer[..128].iter().all(|&b| b == 0));
}

// =============================================================================
// Test: RDMA READ from remote buffer
// =============================================================================

#[test]
fn test_raw_rdma_read() {
    let stop = Arc::new(AtomicBool::new(false));
    let stop_server = stop.clone();

    let (s_info_tx, s_info_rx) = mpsc::channel::<ConnectionInfo>();
    let (c_info_tx, c_info_rx) = mpsc::channel::<ConnectionInfo>();

    // Server thread: owns the source buffer with known data
    let server_handle = thread::spawn(move || {
        let ctx: Context<()> = make_context();

        let mut buffer = vec![0u8; 4096];
        // Fill with a known pattern at offset 64
        for i in 0..512 {
            buffer[64 + i] = ((i * 7 + 3) % 256) as u8;
        }

        let mr = unsafe {
            ctx.pd().register(
                buffer.as_mut_ptr(),
                buffer.len(),
                AccessFlags::LOCAL_WRITE | AccessFlags::REMOTE_WRITE | AccessFlags::REMOTE_READ,
            )
        }
        .expect("register MR");

        let ep_config = EndpointConfig::default();
        let mut ep = ctx.create_endpoint(&ep_config).expect("create_endpoint");
        let (info, lid, _port) = ep.local_info(ctx.lid(), ctx.port());

        s_info_tx
            .send(ConnectionInfo {
                qp_number: info.qp_number,
                packet_sequence_number: 0,
                local_identifier: lid,
                recv_ring_addr: info.recv_ring_addr,
                recv_ring_rkey: info.recv_ring_rkey,
                recv_ring_size: info.recv_ring_size,
                initial_credit: info.initial_credit,
                mr_addr: mr.addr() as u64,
                mr_rkey: mr.rkey(),
                mr_lkey: mr.lkey(),
            })
            .unwrap();

        let client_info = c_info_rx.recv().unwrap();
        let remote = RemoteEndpointInfo {
            qp_number: client_info.qp_number,
            packet_sequence_number: client_info.packet_sequence_number,
            local_identifier: client_info.local_identifier,
            recv_ring_addr: client_info.recv_ring_addr,
            recv_ring_rkey: client_info.recv_ring_rkey,
            recv_ring_size: client_info.recv_ring_size,
            initial_credit: client_info.initial_credit,
        };
        ep.connect_ex(&remote, 0, ctx.port(), true)
            .expect("connect");

        while !stop_server.load(Ordering::Relaxed) {
            ctx.poll(|_, _| {});
            std::hint::spin_loop();
        }
    });

    // Client thread: posts RDMA READ from server's buffer
    let ctx: Context<()> = make_context();

    let ep_config = EndpointConfig::default();
    let mut ep = ctx.create_endpoint(&ep_config).expect("create_endpoint");
    let (info, lid, _port) = ep.local_info(ctx.lid(), ctx.port());

    // Destination buffer for RDMA READ
    let mut dst_buffer = vec![0xFFu8; 512];
    let dst_mr = unsafe {
        ctx.pd().register(
            dst_buffer.as_mut_ptr(),
            dst_buffer.len(),
            AccessFlags::LOCAL_WRITE,
        )
    }
    .expect("register dst MR");

    c_info_tx
        .send(ConnectionInfo {
            qp_number: info.qp_number,
            packet_sequence_number: 0,
            local_identifier: lid,
            recv_ring_addr: info.recv_ring_addr,
            recv_ring_rkey: info.recv_ring_rkey,
            recv_ring_size: info.recv_ring_size,
            initial_credit: info.initial_credit,
            mr_addr: 0,
            mr_rkey: 0,
            mr_lkey: 0,
        })
        .unwrap();

    let server_info = s_info_rx.recv().unwrap();
    let remote = RemoteEndpointInfo {
        qp_number: server_info.qp_number,
        packet_sequence_number: server_info.packet_sequence_number,
        local_identifier: server_info.local_identifier,
        recv_ring_addr: server_info.recv_ring_addr,
        recv_ring_rkey: server_info.recv_ring_rkey,
        recv_ring_size: server_info.recv_ring_size,
        initial_credit: server_info.initial_credit,
    };
    ep.connect_ex(&remote, 0, ctx.port(), true)
        .expect("connect");

    // Post RDMA READ: read 512 bytes from offset 64 of server's buffer
    ep.post_rdma_read(
        server_info.mr_addr + 64,
        server_info.mr_rkey,
        dst_mr.addr() as u64,
        dst_mr.lkey(),
        512,
    )
    .expect("post_rdma_read");

    assert_eq!(ep.raw_rdma_pending(), 1);

    // Poll until completion
    let start = std::time::Instant::now();
    let timeout = Duration::from_secs(5);
    while ep.raw_rdma_pending() > 0 {
        ctx.poll(|_, _| {});
        assert!(start.elapsed() < timeout, "RDMA READ timed out");
    }

    assert_eq!(ep.raw_rdma_pending(), 0);

    // Verify data matches the server's pattern
    let expected: Vec<u8> = (0..512).map(|i| ((i * 7 + 3) % 256) as u8).collect();
    assert_eq!(&dst_buffer[..], &expected[..], "RDMA READ data mismatch");

    stop.store(true, Ordering::SeqCst);
    server_handle.join().expect("server panicked");
}

// =============================================================================
// Test: Multiple RDMA operations
// =============================================================================

#[test]
fn test_raw_rdma_multiple_ops() {
    let stop = Arc::new(AtomicBool::new(false));
    let stop_server = stop.clone();

    let (s_info_tx, s_info_rx) = mpsc::channel::<ConnectionInfo>();
    let (c_info_tx, c_info_rx) = mpsc::channel::<ConnectionInfo>();

    let server_handle = thread::spawn(move || {
        let ctx: Context<()> = make_context();

        let mut buffer = vec![0u8; 4096];
        let mr = unsafe {
            ctx.pd().register(
                buffer.as_mut_ptr(),
                buffer.len(),
                AccessFlags::LOCAL_WRITE | AccessFlags::REMOTE_WRITE | AccessFlags::REMOTE_READ,
            )
        }
        .expect("register MR");

        let ep_config = EndpointConfig::default();
        let mut ep = ctx.create_endpoint(&ep_config).expect("create_endpoint");
        let (info, lid, _port) = ep.local_info(ctx.lid(), ctx.port());

        s_info_tx
            .send(ConnectionInfo {
                qp_number: info.qp_number,
                packet_sequence_number: 0,
                local_identifier: lid,
                recv_ring_addr: info.recv_ring_addr,
                recv_ring_rkey: info.recv_ring_rkey,
                recv_ring_size: info.recv_ring_size,
                initial_credit: info.initial_credit,
                mr_addr: mr.addr() as u64,
                mr_rkey: mr.rkey(),
                mr_lkey: mr.lkey(),
            })
            .unwrap();

        let client_info = c_info_rx.recv().unwrap();
        let remote = RemoteEndpointInfo {
            qp_number: client_info.qp_number,
            packet_sequence_number: client_info.packet_sequence_number,
            local_identifier: client_info.local_identifier,
            recv_ring_addr: client_info.recv_ring_addr,
            recv_ring_rkey: client_info.recv_ring_rkey,
            recv_ring_size: client_info.recv_ring_size,
            initial_credit: client_info.initial_credit,
        };
        ep.connect_ex(&remote, 0, ctx.port(), true)
            .expect("connect");

        while !stop_server.load(Ordering::Relaxed) {
            ctx.poll(|_, _| {});
            std::hint::spin_loop();
        }

        buffer
    });

    // Client: write → read back → verify
    let ctx: Context<()> = make_context();
    let ep_config = EndpointConfig::default();
    let mut ep = ctx.create_endpoint(&ep_config).expect("create_endpoint");
    let (info, lid, _port) = ep.local_info(ctx.lid(), ctx.port());

    let mut local_buf = vec![0u8; 1024];
    let local_mr = unsafe {
        ctx.pd().register(
            local_buf.as_mut_ptr(),
            local_buf.len(),
            AccessFlags::LOCAL_WRITE,
        )
    }
    .expect("register local MR");

    c_info_tx
        .send(ConnectionInfo {
            qp_number: info.qp_number,
            packet_sequence_number: 0,
            local_identifier: lid,
            recv_ring_addr: info.recv_ring_addr,
            recv_ring_rkey: info.recv_ring_rkey,
            recv_ring_size: info.recv_ring_size,
            initial_credit: info.initial_credit,
            mr_addr: 0,
            mr_rkey: 0,
            mr_lkey: 0,
        })
        .unwrap();

    let server_info = s_info_rx.recv().unwrap();
    let remote = RemoteEndpointInfo {
        qp_number: server_info.qp_number,
        packet_sequence_number: server_info.packet_sequence_number,
        local_identifier: server_info.local_identifier,
        recv_ring_addr: server_info.recv_ring_addr,
        recv_ring_rkey: server_info.recv_ring_rkey,
        recv_ring_size: server_info.recv_ring_size,
        initial_credit: server_info.initial_credit,
    };
    ep.connect_ex(&remote, 0, ctx.port(), true)
        .expect("connect");

    let timeout = Duration::from_secs(5);

    // Write pattern A to offset 0
    local_buf[..256].fill(0xAA);
    ep.post_rdma_write(
        server_info.mr_addr,
        server_info.mr_rkey,
        local_mr.addr() as u64,
        local_mr.lkey(),
        256,
    )
    .expect("write A");

    // Write pattern B to offset 512
    local_buf[256..512].fill(0xBB);
    ep.post_rdma_write(
        server_info.mr_addr + 512,
        server_info.mr_rkey,
        local_mr.addr() as u64 + 256,
        local_mr.lkey(),
        256,
    )
    .expect("write B");

    assert_eq!(ep.raw_rdma_pending(), 2);

    // Wait for both writes to complete
    let start = std::time::Instant::now();
    while ep.raw_rdma_pending() > 0 {
        ctx.poll(|_, _| {});
        assert!(start.elapsed() < timeout, "RDMA WRITEs timed out");
    }

    // Now read back both regions
    // Clear local buffer first
    local_buf.fill(0);

    ep.post_rdma_read(
        server_info.mr_addr,
        server_info.mr_rkey,
        local_mr.addr() as u64,
        local_mr.lkey(),
        256,
    )
    .expect("read A");

    let start = std::time::Instant::now();
    while ep.raw_rdma_pending() > 0 {
        ctx.poll(|_, _| {});
        assert!(start.elapsed() < timeout, "RDMA READ A timed out");
    }

    assert!(
        local_buf[..256].iter().all(|&b| b == 0xAA),
        "pattern A mismatch"
    );

    ep.post_rdma_read(
        server_info.mr_addr + 512,
        server_info.mr_rkey,
        local_mr.addr() as u64 + 256,
        local_mr.lkey(),
        256,
    )
    .expect("read B");

    let start = std::time::Instant::now();
    while ep.raw_rdma_pending() > 0 {
        ctx.poll(|_, _| {});
        assert!(start.elapsed() < timeout, "RDMA READ B timed out");
    }

    assert!(
        local_buf[256..512].iter().all(|&b| b == 0xBB),
        "pattern B mismatch"
    );

    stop.store(true, Ordering::SeqCst);
    let server_buffer = server_handle.join().expect("server panicked");

    // Also verify server-side
    assert!(server_buffer[..256].iter().all(|&b| b == 0xAA));
    assert!(server_buffer[256..512].iter().all(|&b| b == 0));
    assert!(server_buffer[512..768].iter().all(|&b| b == 0xBB));
}
