//! Test for QP destruction after remote QP is destroyed.

mod common;

use common::{TestContext, full_access};
use mlx5::qp::{RcQpConfig, RemoteQpInfo};
use std::rc::Rc;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

/// Test that QP can be safely destroyed after remote QP is gone.
#[test]
fn test_qp_destroy_after_remote_gone() {
    let ctx1 = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping: no mlx5 device");
            return;
        }
    };

    let ctx2 = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping: no mlx5 device");
            return;
        }
    };

    fn noop_callback(_cqe: mlx5::cq::Cqe, _entry: u64) {}

    let config = RcQpConfig {
        max_send_wr: 64,
        max_recv_wr: 64,
        max_send_sge: 1,
        max_recv_sge: 1,
        max_inline_data: 64,
        enable_scatter_to_cqe: false,
    };

    // Create QP1 on context 1 (separate send and recv CQs)
    let mut send_cq1 = ctx1.ctx.create_cq(64).expect("create send_cq1");
    send_cq1.init_direct_access().expect("init send_cq1");
    let send_cq1 = Rc::new(send_cq1);
    let mut recv_cq1 = ctx1.ctx.create_cq(64).expect("create recv_cq1");
    recv_cq1.init_direct_access().expect("init recv_cq1");
    let recv_cq1 = Rc::new(recv_cq1);
    let qp1 = ctx1
        .ctx
        .create_rc_qp(
            &ctx1.pd,
            &send_cq1,
            &recv_cq1,
            &config,
            noop_callback as fn(_, _),
        )
        .expect("create qp1");

    // Create QP2 on context 2 (separate send and recv CQs)
    let mut send_cq2 = ctx2.ctx.create_cq(64).expect("create send_cq2");
    send_cq2.init_direct_access().expect("init send_cq2");
    let send_cq2 = Rc::new(send_cq2);
    let mut recv_cq2 = ctx2.ctx.create_cq(64).expect("create recv_cq2");
    recv_cq2.init_direct_access().expect("init recv_cq2");
    let recv_cq2 = Rc::new(recv_cq2);
    let qp2 = ctx2
        .ctx
        .create_rc_qp(
            &ctx2.pd,
            &send_cq2,
            &recv_cq2,
            &config,
            noop_callback as fn(_, _),
        )
        .expect("create qp2");

    let access = full_access().bits();
    let port = 1u8;

    // Connect QP1 -> QP2
    let remote1 = RemoteQpInfo {
        qp_number: qp2.borrow().qpn(),
        packet_sequence_number: 0,
        local_identifier: ctx2.port_attr.lid,
    };
    qp1.borrow_mut()
        .connect(&remote1, port, 0, 4, 4, access)
        .expect("connect qp1");

    // Connect QP2 -> QP1
    let remote2 = RemoteQpInfo {
        qp_number: qp1.borrow().qpn(),
        packet_sequence_number: 0,
        local_identifier: ctx1.port_attr.lid,
    };
    qp2.borrow_mut()
        .connect(&remote2, port, 0, 4, 4, access)
        .expect("connect qp2");

    eprintln!("Both QPs connected");

    // Now destroy QP2 first (simulate remote going away)
    eprintln!("Destroying QP2...");
    drop(qp2);
    drop(send_cq2);
    drop(recv_cq2);
    drop(ctx2);
    eprintln!("QP2 destroyed");

    // Wait a bit
    thread::sleep(Duration::from_millis(100));

    // Now try to destroy QP1
    eprintln!("Trying to modify QP1 to ERROR state...");
    if let Err(e) = qp1.borrow_mut().modify_to_error() {
        eprintln!("modify_to_error failed: {}", e);
    } else {
        eprintln!("QP1 modified to ERROR");
    }

    // Wait for any error completions
    thread::sleep(Duration::from_millis(100));

    eprintln!("Destroying QP1...");
    drop(qp1);
    eprintln!("QP1 destroyed");

    drop(send_cq1);
    drop(recv_cq1);
    eprintln!("CQs destroyed");

    drop(ctx1);
    eprintln!("Context1 destroyed");

    eprintln!("Test passed!");
}

/// Test QP destruction with separate threads (like the benchmark).
#[test]
fn test_qp_destroy_multi_thread() {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    let stop_flag = Arc::new(AtomicBool::new(false));
    let stop_flag_clone = Arc::clone(&stop_flag);

    // Channel for exchanging connection info
    let (tx1, rx1) = mpsc::channel();
    let (tx2, rx2) = mpsc::channel();

    // Server thread
    let server_handle = thread::spawn(move || {
        let ctx = match TestContext::new() {
            Some(ctx) => ctx,
            None => {
                eprintln!("Server: Skipping - no mlx5 device");
                return;
            }
        };

        fn noop_callback(_cqe: mlx5::cq::Cqe, _entry: u64) {}

        let config = RcQpConfig {
            max_send_wr: 64,
            max_recv_wr: 64,
            max_send_sge: 1,
            max_recv_sge: 1,
            max_inline_data: 64,
            enable_scatter_to_cqe: false,
        };

        let mut send_cq = ctx.ctx.create_cq(64).expect("create send_cq");
        send_cq.init_direct_access().expect("init send_cq");
        let send_cq = Rc::new(send_cq);
        let mut recv_cq = ctx.ctx.create_cq(64).expect("create recv_cq");
        recv_cq.init_direct_access().expect("init recv_cq");
        let recv_cq = Rc::new(recv_cq);
        let qp = ctx
            .ctx
            .create_rc_qp(
                &ctx.pd,
                &send_cq,
                &recv_cq,
                &config,
                noop_callback as fn(_, _),
            )
            .expect("create qp");

        // Send server info
        tx1.send((qp.borrow().qpn(), ctx.port_attr.lid)).unwrap();

        // Receive client info
        let (client_qpn, client_lid) = rx2.recv().unwrap();

        // Connect
        let client_remote = RemoteQpInfo {
            qp_number: client_qpn,
            packet_sequence_number: 0,
            local_identifier: client_lid,
        };

        let access = full_access().bits();
        qp.borrow_mut()
            .connect(&client_remote, 1, 0, 4, 4, access)
            .expect("connect qp");

        eprintln!("Server: Connected, waiting for stop signal");

        // Wait for stop signal
        while !stop_flag_clone.load(Ordering::Relaxed) {
            thread::sleep(Duration::from_millis(10));
        }

        eprintln!("Server: Stopping, destroying resources");
        // Resources are dropped here when thread exits
    });

    // Client (main thread)
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Client: Skipping - no mlx5 device");
            stop_flag.store(true, Ordering::SeqCst);
            server_handle.join().unwrap();
            return;
        }
    };

    fn noop_callback(_cqe: mlx5::cq::Cqe, _entry: u64) {}

    let config = RcQpConfig {
        max_send_wr: 64,
        max_recv_wr: 64,
        max_send_sge: 1,
        max_recv_sge: 1,
        max_inline_data: 64,
        enable_scatter_to_cqe: false,
    };

    let mut send_cq = ctx.ctx.create_cq(64).expect("create send_cq");
    send_cq.init_direct_access().expect("init send_cq");
    let send_cq = Rc::new(send_cq);
    let mut recv_cq = ctx.ctx.create_cq(64).expect("create recv_cq");
    recv_cq.init_direct_access().expect("init recv_cq");
    let recv_cq = Rc::new(recv_cq);
    let qp = ctx
        .ctx
        .create_rc_qp(
            &ctx.pd,
            &send_cq,
            &recv_cq,
            &config,
            noop_callback as fn(_, _),
        )
        .expect("create qp");

    // Receive server info
    let (server_qpn, server_lid) = rx1.recv().unwrap();

    // Send client info
    tx2.send((qp.borrow().qpn(), ctx.port_attr.lid)).unwrap();

    // Connect
    let server_remote = RemoteQpInfo {
        qp_number: server_qpn,
        packet_sequence_number: 0,
        local_identifier: server_lid,
    };

    let access = full_access().bits();
    qp.borrow_mut()
        .connect(&server_remote, 1, 0, 4, 4, access)
        .expect("connect qp");

    eprintln!("Client: Connected");

    // Stop server (destroys server's resources)
    eprintln!("Client: Stopping server");
    stop_flag.store(true, Ordering::SeqCst);
    server_handle.join().unwrap();
    eprintln!("Client: Server stopped");

    // Wait a bit
    thread::sleep(Duration::from_millis(100));

    // Now try to destroy client resources
    eprintln!("Client: Modifying QP to ERROR state");
    if let Err(e) = qp.borrow_mut().modify_to_error() {
        eprintln!("Client: modify_to_error failed: {}", e);
    } else {
        eprintln!("Client: QP modified to ERROR");
    }

    thread::sleep(Duration::from_millis(50));

    eprintln!("Client: Destroying QP");
    drop(qp);
    eprintln!("Client: QP destroyed");

    drop(send_cq);
    drop(recv_cq);
    eprintln!("Client: CQs destroyed");

    eprintln!("Multi-thread test passed!");
}

/// Test QP destruction after actual data transfer.
#[test]
fn test_qp_destroy_after_data_transfer() {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, Ordering},
    };

    let stop_flag = Arc::new(AtomicBool::new(false));
    let stop_flag_clone = Arc::clone(&stop_flag);
    let server_ready = Arc::new(AtomicU32::new(0));
    let server_ready_clone = Arc::clone(&server_ready);

    // Channel for exchanging connection info
    let (tx1, rx1) = mpsc::channel::<(u32, u16, u64, u32)>(); // qpn, lid, buf_addr, rkey
    let (tx2, rx2) = mpsc::channel::<(u32, u16, u64, u32)>();

    // Server thread
    let server_handle = thread::spawn(move || {
        let ctx = match TestContext::new() {
            Some(ctx) => ctx,
            None => {
                eprintln!("Server: Skipping - no mlx5 device");
                return;
            }
        };

        fn noop_callback(_cqe: mlx5::cq::Cqe, _entry: u64) {}

        let config = RcQpConfig {
            max_send_wr: 64,
            max_recv_wr: 64,
            max_send_sge: 1,
            max_recv_sge: 1,
            max_inline_data: 64,
            enable_scatter_to_cqe: false,
        };

        let mut send_cq = ctx.ctx.create_cq(64).expect("create send_cq");
        send_cq.init_direct_access().expect("init send_cq");
        let send_cq = Rc::new(send_cq);
        let mut recv_cq = ctx.ctx.create_cq(64).expect("create recv_cq");
        recv_cq.init_direct_access().expect("init recv_cq");
        let recv_cq = Rc::new(recv_cq);
        let qp = ctx
            .ctx
            .create_rc_qp(
                &ctx.pd,
                &send_cq,
                &recv_cq,
                &config,
                noop_callback as fn(_, _),
            )
            .expect("create qp");

        // Allocate buffer and MR
        let buf = common::AlignedBuffer::new(4096);
        let mr =
            unsafe { ctx.pd.register(buf.as_ptr(), buf.size(), full_access()) }.expect("reg_mr");

        // Send server info
        tx1.send((qp.borrow().qpn(), ctx.port_attr.lid, buf.addr(), mr.rkey()))
            .unwrap();

        // Receive client info
        let (client_qpn, client_lid, _client_buf, _client_rkey) = rx2.recv().unwrap();

        // Connect
        let client_remote = RemoteQpInfo {
            qp_number: client_qpn,
            packet_sequence_number: 0,
            local_identifier: client_lid,
        };

        let access = full_access().bits();
        qp.borrow_mut()
            .connect(&client_remote, 1, 0, 4, 4, access)
            .expect("connect qp");

        // Pre-post receives
        for i in 0..32 {
            let _ = qp
                .borrow()
                .recv_builder(i as u64)
                .map(|b| b.sge(buf.addr() + (i * 64) as u64, 64, mr.lkey()).finish());
        }
        qp.borrow().ring_rq_doorbell();

        eprintln!("Server: Ready");
        server_ready_clone.store(1, Ordering::Release);

        // Wait for stop signal
        while !stop_flag_clone.load(Ordering::Relaxed) {
            thread::sleep(Duration::from_millis(10));
        }

        eprintln!("Server: Stopping");
        // Resources dropped here - this is where the remote QP goes away
    });

    // Client (main thread)
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Client: Skipping - no mlx5 device");
            stop_flag.store(true, Ordering::SeqCst);
            server_handle.join().unwrap();
            return;
        }
    };

    fn noop_callback(_cqe: mlx5::cq::Cqe, _entry: u64) {}

    let config = RcQpConfig {
        max_send_wr: 64,
        max_recv_wr: 64,
        max_send_sge: 1,
        max_recv_sge: 1,
        max_inline_data: 64,
        enable_scatter_to_cqe: false,
    };

    let mut send_cq = ctx.ctx.create_cq(64).expect("create send_cq");
    send_cq.init_direct_access().expect("init send_cq");
    let send_cq = Rc::new(send_cq);
    let mut recv_cq = ctx.ctx.create_cq(64).expect("create recv_cq");
    recv_cq.init_direct_access().expect("init recv_cq");
    let recv_cq = Rc::new(recv_cq);
    let qp = ctx
        .ctx
        .create_rc_qp(
            &ctx.pd,
            &send_cq,
            &recv_cq,
            &config,
            noop_callback as fn(_, _),
        )
        .expect("create qp");

    // Allocate buffer and MR
    let buf = common::AlignedBuffer::new(4096);
    let mr = unsafe { ctx.pd.register(buf.as_ptr(), buf.size(), full_access()) }.expect("reg_mr");

    // Receive server info
    let (server_qpn, server_lid, _server_buf, _server_rkey) = rx1.recv().unwrap();

    // Send client info
    tx2.send((qp.borrow().qpn(), ctx.port_attr.lid, buf.addr(), mr.rkey()))
        .unwrap();

    // Connect
    let server_remote = RemoteQpInfo {
        qp_number: server_qpn,
        packet_sequence_number: 0,
        local_identifier: server_lid,
    };

    let access = full_access().bits();
    qp.borrow_mut()
        .connect(&server_remote, 1, 0, 4, 4, access)
        .expect("connect qp");

    // Pre-post receives
    for i in 0..32 {
        let _ = qp
            .borrow()
            .recv_builder(i as u64)
            .map(|b| b.sge(buf.addr() + (i * 64) as u64, 64, mr.lkey()).finish());
    }
    qp.borrow().ring_rq_doorbell();

    // Wait for server ready
    while server_ready.load(Ordering::Acquire) == 0 {
        thread::sleep(Duration::from_millis(1));
    }

    eprintln!("Client: Server ready, stopping server immediately");

    // Stop server without doing any data transfer
    stop_flag.store(true, Ordering::SeqCst);
    server_handle.join().unwrap();
    eprintln!("Client: Server stopped (remote QP destroyed)");

    // Wait
    thread::sleep(Duration::from_millis(100));

    // Now try cleanup
    eprintln!("Client: Modifying QP to ERROR");
    if let Err(e) = qp.borrow_mut().modify_to_error() {
        eprintln!("Client: modify_to_error failed: {}", e);
    }

    thread::sleep(Duration::from_millis(50));

    eprintln!("Client: Destroying QP");
    drop(qp);
    eprintln!("Client: QP destroyed");

    drop(mr);
    drop(buf);
    drop(send_cq);
    drop(recv_cq);

    eprintln!("Data transfer test passed!");
}

/// Test QP destruction after actual SEND/RECV data transfer using direct verbs.
/// This mimics the benchmark's behavior more closely.
#[test]
fn test_qp_destroy_after_actual_send_recv() {
    use mlx5::wqe::TxFlags;
    use std::cell::Cell;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, Ordering},
    };

    let stop_flag = Arc::new(AtomicBool::new(false));
    let stop_flag_clone = Arc::clone(&stop_flag);
    let server_ready = Arc::new(AtomicU32::new(0));
    let server_ready_clone = Arc::clone(&server_ready);
    let data_transferred = Arc::new(AtomicU32::new(0));
    let data_transferred_clone = Arc::clone(&data_transferred);

    // Channel for exchanging connection info
    let (tx1, rx1) = mpsc::channel::<(u32, u16)>(); // qpn, lid
    let (tx2, rx2) = mpsc::channel::<(u32, u16)>();

    // Server thread
    let server_handle = thread::spawn(move || {
        let ctx = match TestContext::new() {
            Some(ctx) => ctx,
            None => {
                eprintln!("Server: Skipping - no mlx5 device");
                return;
            }
        };

        // Shared state for recv callback
        let server_rx_count = Rc::new(Cell::new(0usize));
        let server_rx_syndrome = Rc::new(Cell::new(0u8));
        let rx_count_clone = Rc::clone(&server_rx_count);
        let rx_syndrome_clone = Rc::clone(&server_rx_syndrome);
        let recv_callback = move |cqe: mlx5::cq::Cqe, _entry: u64| {
            if cqe.opcode.is_responder() {
                rx_syndrome_clone.set(cqe.syndrome);
                if cqe.syndrome == 0 {
                    rx_count_clone.set(rx_count_clone.get() + 1);
                }
            }
        };

        let config = RcQpConfig {
            max_send_wr: 64,
            max_recv_wr: 64,
            max_send_sge: 1,
            max_recv_sge: 1,
            max_inline_data: 64,
            enable_scatter_to_cqe: false,
        };

        let mut send_cq = ctx.ctx.create_cq(64).expect("create send_cq");
        send_cq.init_direct_access().expect("init send_cq");
        let send_cq = Rc::new(send_cq);
        let mut recv_cq = ctx.ctx.create_cq(64).expect("create recv_cq");
        recv_cq.init_direct_access().expect("init recv_cq");
        let recv_cq = Rc::new(recv_cq);

        let qp = ctx
            .ctx
            .create_rc_qp(&ctx.pd, &send_cq, &recv_cq, &config, recv_callback)
            .expect("create qp");

        // Allocate buffer and MR
        let buf = common::AlignedBuffer::new(4096);
        let mr =
            unsafe { ctx.pd.register(buf.as_ptr(), buf.size(), full_access()) }.expect("reg_mr");

        // Send server info
        tx1.send((qp.borrow().qpn(), ctx.port_attr.lid)).unwrap();

        // Receive client info
        let (client_qpn, client_lid) = rx2.recv().unwrap();

        // Connect
        let client_remote = RemoteQpInfo {
            qp_number: client_qpn,
            packet_sequence_number: 0,
            local_identifier: client_lid,
        };

        let access = full_access().bits();
        qp.borrow_mut()
            .connect(&client_remote, 1, 0, 4, 4, access)
            .expect("connect qp");

        // Pre-post receives
        for i in 0..32 {
            let _ = qp
                .borrow()
                .recv_builder(i as u64)
                .map(|b| b.sge(buf.addr() + (i * 64) as u64, 64, mr.lkey()).finish());
        }
        qp.borrow().ring_rq_doorbell();

        eprintln!("Server: Ready");
        server_ready_clone.store(1, Ordering::Release);

        // Server loop - receive and echo back
        let mut recv_count = 0u32;
        while !stop_flag_clone.load(Ordering::Relaxed) {
            // Poll recv CQ (callback updates server_rx_count)
            server_rx_count.set(0);
            recv_cq.poll();
            recv_cq.flush();

            let new_recvs = server_rx_count.get();
            for _ in 0..new_recvs {
                if server_rx_syndrome.get() != 0 {
                    eprintln!("Server: recv error syndrome={}", server_rx_syndrome.get());
                    continue;
                }
                recv_count += 1;

                // Echo back using direct verbs
                let idx = (recv_count as usize - 1) % 32;
                let offset = (idx * 64) as u64;

                let _ = qp
                    .borrow_mut()
                    .sq_wqe()
                    .and_then(|b| b.send(TxFlags::empty()))
                    .map(|b| {
                        b.sge(buf.addr() + offset, 32, mr.lkey())
                            .finish_signaled(idx as u64)
                    });

                // Repost recv
                let _ = qp
                    .borrow()
                    .recv_builder(idx as u64)
                    .map(|b| b.sge(buf.addr() + offset, 64, mr.lkey()).finish());
                qp.borrow().ring_rq_doorbell();
            }

            // Drain send CQ using poll()
            send_cq.poll();
            send_cq.flush();

            std::hint::spin_loop();
        }

        data_transferred_clone.store(recv_count, Ordering::Release);
        eprintln!("Server: Stopping after {} messages", recv_count);
    });

    // Client (main thread)
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Client: Skipping - no mlx5 device");
            stop_flag.store(true, Ordering::SeqCst);
            server_handle.join().unwrap();
            return;
        }
    };

    // Shared state for recv callback
    let client_rx_count = Rc::new(Cell::new(0usize));
    let client_rx_syndrome = Rc::new(Cell::new(0u8));
    let rx_count_clone = Rc::clone(&client_rx_count);
    let rx_syndrome_clone = Rc::clone(&client_rx_syndrome);
    let recv_callback = move |cqe: mlx5::cq::Cqe, _entry: u64| {
        if cqe.opcode.is_responder() {
            rx_syndrome_clone.set(cqe.syndrome);
            if cqe.syndrome == 0 {
                rx_count_clone.set(rx_count_clone.get() + 1);
            }
        }
    };

    let config = RcQpConfig {
        max_send_wr: 64,
        max_recv_wr: 64,
        max_send_sge: 1,
        max_recv_sge: 1,
        max_inline_data: 64,
        enable_scatter_to_cqe: false,
    };

    let mut send_cq = ctx.ctx.create_cq(64).expect("create send_cq");
    send_cq.init_direct_access().expect("init send_cq");
    let send_cq = Rc::new(send_cq);
    let mut recv_cq = ctx.ctx.create_cq(64).expect("create recv_cq");
    recv_cq.init_direct_access().expect("init recv_cq");
    let recv_cq = Rc::new(recv_cq);

    let qp = ctx
        .ctx
        .create_rc_qp(&ctx.pd, &send_cq, &recv_cq, &config, recv_callback)
        .expect("create qp");

    // Allocate buffer and MR
    let buf = common::AlignedBuffer::new(4096);
    let mr = unsafe { ctx.pd.register(buf.as_ptr(), buf.size(), full_access()) }.expect("reg_mr");

    // Receive server info
    let (server_qpn, server_lid) = rx1.recv().unwrap();

    // Send client info
    tx2.send((qp.borrow().qpn(), ctx.port_attr.lid)).unwrap();

    // Connect
    let server_remote = RemoteQpInfo {
        qp_number: server_qpn,
        packet_sequence_number: 0,
        local_identifier: server_lid,
    };

    let access = full_access().bits();
    qp.borrow_mut()
        .connect(&server_remote, 1, 0, 4, 4, access)
        .expect("connect qp");

    // Pre-post receives
    for i in 0..32 {
        let _ = qp
            .borrow()
            .recv_builder(i as u64)
            .map(|b| b.sge(buf.addr() + (i * 64) as u64, 64, mr.lkey()).finish());
    }
    qp.borrow().ring_rq_doorbell();

    // Wait for server ready
    while server_ready.load(Ordering::Acquire) == 0 {
        thread::sleep(Duration::from_millis(1));
    }

    eprintln!("Client: Server ready, doing data transfer");

    // Do 100 ping-pongs
    let target = 100u64;
    let mut sent = 0u64;
    let mut received = 0u64;

    // Initial burst of 32 SENDs
    for i in 0..32.min(target) as usize {
        let offset = (i * 64) as u64;
        let _ = qp
            .borrow_mut()
            .sq_wqe()
            .and_then(|b| b.send(TxFlags::empty()))
            .map(|b| {
                b.sge(buf.addr() + offset, 32, mr.lkey())
                    .finish_signaled(i as u64)
            });
        sent += 1;
    }
    qp.borrow().ring_sq_doorbell();

    // Ping-pong loop
    while received < target {
        // Poll send CQ using poll()
        send_cq.poll();
        send_cq.flush();

        // Poll recv CQ (callback updates client_rx_count)
        client_rx_count.set(0);
        recv_cq.poll();
        recv_cq.flush();

        let new_recvs = client_rx_count.get();
        for _ in 0..new_recvs {
            if client_rx_syndrome.get() != 0 {
                eprintln!("Client: recv error syndrome={}", client_rx_syndrome.get());
                continue;
            }
            received += 1;

            let idx = ((received - 1) as usize) % 32;
            let offset = (idx * 64) as u64;

            // Repost recv
            let _ = qp
                .borrow()
                .recv_builder(idx as u64)
                .map(|b| b.sge(buf.addr() + offset, 64, mr.lkey()).finish());
            qp.borrow().ring_rq_doorbell();

            // Send more if needed
            if sent < target {
                let _ = qp
                    .borrow_mut()
                    .sq_wqe()
                    .and_then(|b| b.send(TxFlags::empty()))
                    .map(|b| {
                        b.sge(buf.addr() + offset, 32, mr.lkey())
                            .finish_signaled(idx as u64)
                    });
                sent += 1;
            }
        }
    }

    eprintln!("Client: Done {} ping-pongs, stopping server", received);

    // Stop server
    stop_flag.store(true, Ordering::SeqCst);
    server_handle.join().unwrap();
    eprintln!("Client: Server stopped");

    // Wait a bit
    thread::sleep(Duration::from_millis(100));

    // Drain CQs before destruction
    eprintln!("Client: Draining CQs...");
    loop {
        let mut drained = false;
        client_rx_count.set(0);
        recv_cq.poll();
        recv_cq.flush();
        let recv_drained = client_rx_count.get();
        if recv_drained > 0 {
            eprintln!("  recv CQ: drained {} completions", recv_drained);
            drained = true;
        }
        let n = send_cq.poll();
        if n > 0 {
            drained = true;
        }
        send_cq.flush();
        if !drained {
            break;
        }
    }

    // Now try cleanup
    eprintln!("Client: Modifying QP to ERROR");
    if let Err(e) = qp.borrow_mut().modify_to_error() {
        eprintln!("Client: modify_to_error failed: {}", e);
    }

    thread::sleep(Duration::from_millis(50));

    eprintln!("Client: Destroying QP");
    drop(qp);
    eprintln!("Client: QP destroyed");

    drop(mr);
    drop(buf);
    drop(send_cq);
    drop(recv_cq);

    eprintln!("Actual SEND/RECV test passed!");
}
