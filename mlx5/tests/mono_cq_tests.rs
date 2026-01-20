//! Tests for MonoCq (Monomorphic Completion Queue).
//!
//! MonoCq provides inlined callback dispatch for better performance
//! by eliminating vtable overhead.

mod common;

use std::cell::{Cell, RefCell};
use std::rc::Rc;

use mlx5::cq::{CqConfig, CqeCompressionFormat, Cqe};
use mlx5::qp::{RcQpConfig, RcQpForMonoCq, RemoteQpInfo};
use mlx5::wqe::{WqeFlags, WqeOpcode};

use common::{AlignedBuffer, TestContext, full_access};

/// Test MonoCq creation and basic properties.
#[test]
fn test_mono_cq_creation() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    fn callback(_cqe: Cqe, _entry: u64) {}

    let mono_cq = ctx
        .ctx
        .create_mono_cq::<RcQpForMonoCq<u64>, _>(256, callback as fn(_, _))
        .expect("Failed to create MonoCq");

    // Verify as_ptr returns non-null
    assert!(
        !mono_cq.as_ptr().is_null(),
        "MonoCq as_ptr should not be null"
    );

    println!("MonoCq creation test passed!");
}

/// Test MonoCq with RC QP: register, poll, unregister.
#[test]
fn test_mono_cq_with_rc_qp() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    // Track completions with callback
    let completions: Rc<RefCell<Vec<(Cqe, u64)>>> = Rc::new(RefCell::new(Vec::new()));
    let completions_clone = completions.clone();

    let callback = move |cqe: Cqe, entry: u64| {
        completions_clone.borrow_mut().push((cqe, entry));
    };

    // Create send and recv MonoCqs
    let send_cq = ctx
        .ctx
        .create_mono_cq::<RcQpForMonoCq<u64>, _>(256, callback)
        .expect("Failed to create send MonoCq");

    // For recv CQ, we need a separate callback
    let recv_completions: Rc<RefCell<Vec<(Cqe, u64)>>> = Rc::new(RefCell::new(Vec::new()));
    let recv_completions_clone = recv_completions.clone();
    let recv_callback = move |cqe: Cqe, entry: u64| {
        recv_completions_clone.borrow_mut().push((cqe, entry));
    };

    let recv_cq1 = ctx
        .ctx
        .create_mono_cq::<RcQpForMonoCq<u64>, _>(256, recv_callback)
        .expect("Failed to create recv MonoCq 1");

    let recv_completions2: Rc<RefCell<Vec<(Cqe, u64)>>> = Rc::new(RefCell::new(Vec::new()));
    let recv_completions2_clone = recv_completions2.clone();
    let recv_callback2 = move |cqe: Cqe, entry: u64| {
        recv_completions2_clone.borrow_mut().push((cqe, entry));
    };

    let recv_cq2 = ctx
        .ctx
        .create_mono_cq::<RcQpForMonoCq<u64>, _>(256, recv_callback2)
        .expect("Failed to create recv MonoCq 2");

    let config = RcQpConfig::default();

    // Create QPs for MonoCq
    let qp1 = ctx
        .ctx
        .create_rc_qp_for_mono_cq::<u64, _, _, _>(&ctx.pd, &send_cq, &recv_cq1, &config)
        .expect("Failed to create QP1");
    let qp2 = ctx
        .ctx
        .create_rc_qp_for_mono_cq::<u64, _, _, _>(&ctx.pd, &send_cq, &recv_cq2, &config)
        .expect("Failed to create QP2");

    // Register QPs with their CQs
    send_cq.register(&qp1);
    send_cq.register(&qp2);
    recv_cq1.register(&qp1);
    recv_cq2.register(&qp2);

    // Connect QPs
    let remote1 = RemoteQpInfo {
        qp_number: qp1.borrow().qpn(),
        packet_sequence_number: 0,
        local_identifier: ctx.port_attr.lid,
    };
    let remote2 = RemoteQpInfo {
        qp_number: qp2.borrow().qpn(),
        packet_sequence_number: 0,
        local_identifier: ctx.port_attr.lid,
    };

    let access = full_access().bits();
    qp1.borrow_mut()
        .connect(&remote2, ctx.port, 0, 4, 4, access)
        .expect("connect QP1");
    qp2.borrow_mut()
        .connect(&remote1, ctx.port, 0, 4, 4, access)
        .expect("connect QP2");

    // Allocate buffers
    let mut local_buf = AlignedBuffer::new(4096);
    let mut remote_buf = AlignedBuffer::new(4096);

    let local_mr = unsafe {
        ctx.pd
            .register(local_buf.as_ptr(), local_buf.size(), full_access())
    }
    .expect("Failed to register local MR");
    let remote_mr = unsafe {
        ctx.pd
            .register(remote_buf.as_ptr(), remote_buf.size(), full_access())
    }
    .expect("Failed to register remote MR");

    let test_data = b"MonoCq test data";
    local_buf.fill_bytes(test_data);
    remote_buf.fill(0);

    // Post RDMA WRITE with entry = 42
    qp1.borrow_mut()
        .wqe_builder(42u64)
        .expect("wqe_builder failed")
        .ctrl_rdma_write(WqeFlags::COMPLETION)
        .rdma(remote_buf.addr(), remote_mr.rkey())
        .sge(local_buf.addr(), test_data.len() as u32, local_mr.lkey())
        .finish_with_blueflame()
        .expect("finish_with_blueflame failed");

    // Poll for completion
    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(5);

    loop {
        let count = send_cq.poll();
        if count > 0 {
            send_cq.flush();
            break;
        }
        if start.elapsed() > timeout {
            panic!("Timeout waiting for completion");
        }
        std::hint::spin_loop();
    }

    // Verify callback was invoked
    let comps = completions.borrow();
    assert_eq!(comps.len(), 1, "Should have exactly 1 completion");
    let (cqe, entry) = &comps[0];
    assert_eq!(*entry, 42, "Entry should be 42");
    assert_eq!(cqe.syndrome, 0, "CQE should have no error");
    println!("Completion: entry={}, syndrome={}", entry, cqe.syndrome);

    // Verify data was written
    let written = remote_buf.read_bytes(test_data.len());
    assert_eq!(&written[..], test_data, "Data mismatch");

    // Test unregister
    send_cq.unregister(qp1.borrow().qpn());
    println!("QP1 unregistered from send_cq");

    println!("MonoCq with RC QP test passed!");
}

/// Test MonoCq poll returns 0 when no completions.
#[test]
fn test_mono_cq_poll_empty() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    fn callback(_cqe: Cqe, _entry: u64) {
        panic!("Callback should not be called");
    }

    let mono_cq = ctx
        .ctx
        .create_mono_cq::<RcQpForMonoCq<u64>, _>(256, callback as fn(_, _))
        .expect("Failed to create MonoCq");

    // Poll should return 0 when no completions
    let count = mono_cq.poll();
    assert_eq!(count, 0, "Empty CQ should return 0 completions");

    // flush() should be safe to call even with no completions
    mono_cq.flush();

    println!("MonoCq poll empty test passed!");
}

/// Test MonoCq with multiple completions.
#[test]
fn test_mono_cq_multiple_completions() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    let completions: Rc<RefCell<Vec<u64>>> = Rc::new(RefCell::new(Vec::new()));
    let completions_clone = completions.clone();

    let callback = move |_cqe: Cqe, entry: u64| {
        completions_clone.borrow_mut().push(entry);
    };

    let send_cq = ctx
        .ctx
        .create_mono_cq::<RcQpForMonoCq<u64>, _>(256, callback)
        .expect("Failed to create send MonoCq");

    let recv_completions: Rc<RefCell<Vec<u64>>> = Rc::new(RefCell::new(Vec::new()));
    let recv_completions_clone = recv_completions.clone();
    let recv_callback = move |_cqe: Cqe, entry: u64| {
        recv_completions_clone.borrow_mut().push(entry);
    };

    let recv_cq1 = ctx
        .ctx
        .create_mono_cq::<RcQpForMonoCq<u64>, _>(256, recv_callback)
        .expect("Failed to create recv MonoCq 1");

    let recv_completions2: Rc<RefCell<Vec<u64>>> = Rc::new(RefCell::new(Vec::new()));
    let recv_completions2_clone = recv_completions2.clone();
    let recv_callback2 = move |_cqe: Cqe, entry: u64| {
        recv_completions2_clone.borrow_mut().push(entry);
    };

    let recv_cq2 = ctx
        .ctx
        .create_mono_cq::<RcQpForMonoCq<u64>, _>(256, recv_callback2)
        .expect("Failed to create recv MonoCq 2");

    let config = RcQpConfig::default();

    let qp1 = ctx
        .ctx
        .create_rc_qp_for_mono_cq::<u64, _, _, _>(&ctx.pd, &send_cq, &recv_cq1, &config)
        .expect("Failed to create QP1");
    let qp2 = ctx
        .ctx
        .create_rc_qp_for_mono_cq::<u64, _, _, _>(&ctx.pd, &send_cq, &recv_cq2, &config)
        .expect("Failed to create QP2");

    send_cq.register(&qp1);
    send_cq.register(&qp2);
    recv_cq1.register(&qp1);
    recv_cq2.register(&qp2);

    let remote1 = RemoteQpInfo {
        qp_number: qp1.borrow().qpn(),
        packet_sequence_number: 0,
        local_identifier: ctx.port_attr.lid,
    };
    let remote2 = RemoteQpInfo {
        qp_number: qp2.borrow().qpn(),
        packet_sequence_number: 0,
        local_identifier: ctx.port_attr.lid,
    };

    let access = full_access().bits();
    qp1.borrow_mut()
        .connect(&remote2, ctx.port, 0, 4, 4, access)
        .expect("connect QP1");
    qp2.borrow_mut()
        .connect(&remote1, ctx.port, 0, 4, 4, access)
        .expect("connect QP2");

    let mut local_buf = AlignedBuffer::new(4096);
    let remote_buf = AlignedBuffer::new(4096);

    let local_mr = unsafe {
        ctx.pd
            .register(local_buf.as_ptr(), local_buf.size(), full_access())
    }
    .expect("Failed to register local MR");
    let remote_mr = unsafe {
        ctx.pd
            .register(remote_buf.as_ptr(), remote_buf.size(), full_access())
    }
    .expect("Failed to register remote MR");

    let test_data = b"Multi completion test";
    local_buf.fill_bytes(test_data);

    // Post multiple WQEs
    let num_ops = 5;
    for i in 0..num_ops {
        qp1.borrow_mut()
            .wqe_builder(100 + i as u64)
            .expect("wqe_builder failed")
            .ctrl_rdma_write(WqeFlags::COMPLETION)
            .rdma(remote_buf.addr(), remote_mr.rkey())
            .sge(local_buf.addr(), test_data.len() as u32, local_mr.lkey())
            .finish_with_blueflame()
            .expect("finish_with_blueflame failed");
    }

    // Poll until all completions are received
    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(5);

    while completions.borrow().len() < num_ops {
        let count = send_cq.poll();
        if count > 0 {
            send_cq.flush();
        }
        if start.elapsed() > timeout {
            panic!(
                "Timeout: got {} completions, expected {}",
                completions.borrow().len(),
                num_ops
            );
        }
        std::hint::spin_loop();
    }

    // Verify all entries were received
    let comps = completions.borrow();
    assert_eq!(comps.len(), num_ops, "Should have all completions");
    for i in 0..num_ops {
        assert!(
            comps.contains(&(100 + i as u64)),
            "Missing entry {}",
            100 + i
        );
    }

    println!(
        "MonoCq multiple completions test passed! ({} completions)",
        num_ops
    );
}

/// High-load test for CQE compression.
///
/// This test sends many operations rapidly to trigger CQE compression.
/// CQE compression is expected to occur when multiple completions arrive
/// in a short time window.
#[test]
fn test_mono_cq_high_load() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    let send_completions: Rc<RefCell<Vec<u64>>> = Rc::new(RefCell::new(Vec::new()));
    let send_completions_clone = send_completions.clone();

    let send_callback = move |_cqe: Cqe, entry: u64| {
        send_completions_clone.borrow_mut().push(entry);
    };

    let recv_completions: Rc<RefCell<Vec<u64>>> = Rc::new(RefCell::new(Vec::new()));
    let recv_completions_clone = recv_completions.clone();

    let recv_callback = move |_cqe: Cqe, entry: u64| {
        recv_completions_clone.borrow_mut().push(entry);
    };

    // Use larger CQ to accommodate high load
    let send_cq = ctx
        .ctx
        .create_mono_cq::<RcQpForMonoCq<u64>, _>(512, send_callback)
        .expect("Failed to create send MonoCq");

    let recv_cq1 = ctx
        .ctx
        .create_mono_cq::<RcQpForMonoCq<u64>, _>(512, recv_callback)
        .expect("Failed to create recv MonoCq 1");

    // Dummy recv CQ for qp2
    let recv_cq2 = ctx
        .ctx
        .create_mono_cq::<RcQpForMonoCq<u64>, _>(512, |_cqe: Cqe, _entry: u64| {})
        .expect("Failed to create recv MonoCq 2");

    let config = RcQpConfig::default();

    let qp1 = ctx
        .ctx
        .create_rc_qp_for_mono_cq::<u64, _, _, _>(&ctx.pd, &send_cq, &recv_cq1, &config)
        .expect("Failed to create QP1");
    let qp2 = ctx
        .ctx
        .create_rc_qp_for_mono_cq::<u64, _, _, _>(&ctx.pd, &send_cq, &recv_cq2, &config)
        .expect("Failed to create QP2");

    send_cq.register(&qp1);
    send_cq.register(&qp2);
    recv_cq1.register(&qp1);
    recv_cq2.register(&qp2);

    let remote1 = RemoteQpInfo {
        qp_number: qp1.borrow().qpn(),
        packet_sequence_number: 0,
        local_identifier: ctx.port_attr.lid,
    };
    let remote2 = RemoteQpInfo {
        qp_number: qp2.borrow().qpn(),
        packet_sequence_number: 0,
        local_identifier: ctx.port_attr.lid,
    };

    let access = full_access().bits();
    qp1.borrow_mut()
        .connect(&remote2, ctx.port, 0, 4, 4, access)
        .expect("connect QP1");
    qp2.borrow_mut()
        .connect(&remote1, ctx.port, 0, 4, 4, access)
        .expect("connect QP2");

    // Allocate larger buffers for high-load test
    let mut local_buf = AlignedBuffer::new(64 * 1024);
    let remote_buf = AlignedBuffer::new(64 * 1024);

    let local_mr = unsafe {
        ctx.pd
            .register(local_buf.as_ptr(), local_buf.size(), full_access())
    }
    .expect("Failed to register local MR");
    let remote_mr = unsafe {
        ctx.pd
            .register(remote_buf.as_ptr(), remote_buf.size(), full_access())
    }
    .expect("Failed to register remote MR");

    local_buf.fill(0xAB);

    // Post many operations to trigger CQE compression
    let num_ops = 64;
    let chunk_size = 64u32; // Small writes to maximize CQE generation

    println!("Posting {} RDMA WRITEs...", num_ops);

    for i in 0..num_ops {
        let offset = (i as u64) * (chunk_size as u64);
        qp1.borrow_mut()
            .wqe_builder(i as u64)
            .expect("wqe_builder failed")
            .ctrl_rdma_write(WqeFlags::COMPLETION)
            .rdma(remote_buf.addr() + offset, remote_mr.rkey())
            .sge(local_buf.addr() + offset, chunk_size, local_mr.lkey())
            .finish_with_blueflame()
            .expect("finish_with_blueflame failed");
    }

    // Poll until all completions are received
    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(10);

    while send_completions.borrow().len() < num_ops {
        let count = send_cq.poll();
        if count > 0 {
            send_cq.flush();
        }
        if start.elapsed() > timeout {
            panic!(
                "Timeout: got {} completions, expected {}",
                send_completions.borrow().len(),
                num_ops
            );
        }
        std::hint::spin_loop();
    }

    // Verify all entries were received
    let comps = send_completions.borrow();
    assert_eq!(comps.len(), num_ops, "Should have all completions");

    println!(
        "MonoCq high-load test passed! ({} completions in {:?})",
        num_ops,
        start.elapsed()
    );
}

/// Test recv CQ with RDMA WRITE IMM (simulates benchmark pattern).
///
/// This tests the recv side CQE handling which is different from send side.
/// RDMA WRITE with IMM generates a CQE on the receiver's RQ.
#[test]
fn test_mono_cq_recv_rdma_write_imm() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    let send_completions: Rc<RefCell<Vec<u64>>> = Rc::new(RefCell::new(Vec::new()));
    let send_completions_clone = send_completions.clone();

    let send_callback = move |_cqe: Cqe, entry: u64| {
        send_completions_clone.borrow_mut().push(entry);
    };

    let recv_completions: Rc<RefCell<Vec<(u32, u64)>>> = Rc::new(RefCell::new(Vec::new()));
    let recv_completions_clone = recv_completions.clone();

    // Recv callback captures immediate data from CQE
    let recv_callback = move |cqe: Cqe, entry: u64| {
        recv_completions_clone.borrow_mut().push((cqe.imm, entry));
    };

    let send_cq = ctx
        .ctx
        .create_mono_cq::<RcQpForMonoCq<u64>, _>(256, send_callback)
        .expect("Failed to create send MonoCq");

    let recv_cq = ctx
        .ctx
        .create_mono_cq::<RcQpForMonoCq<u64>, _>(256, recv_callback)
        .expect("Failed to create recv MonoCq");

    // Dummy CQs for qp2
    let send_cq2 = ctx
        .ctx
        .create_mono_cq::<RcQpForMonoCq<u64>, _>(256, |_cqe: Cqe, _entry: u64| {})
        .expect("Failed to create send MonoCq 2");
    let recv_cq2 = ctx
        .ctx
        .create_mono_cq::<RcQpForMonoCq<u64>, _>(256, |_cqe: Cqe, _entry: u64| {})
        .expect("Failed to create recv MonoCq 2");

    let config = RcQpConfig::default();

    let qp1 = ctx
        .ctx
        .create_rc_qp_for_mono_cq::<u64, _, _, _>(&ctx.pd, &send_cq, &recv_cq, &config)
        .expect("Failed to create QP1");
    let qp2 = ctx
        .ctx
        .create_rc_qp_for_mono_cq::<u64, _, _, _>(&ctx.pd, &send_cq2, &recv_cq2, &config)
        .expect("Failed to create QP2");

    send_cq.register(&qp1);
    recv_cq.register(&qp1);
    send_cq2.register(&qp2);
    recv_cq2.register(&qp2);

    let remote1 = RemoteQpInfo {
        qp_number: qp1.borrow().qpn(),
        packet_sequence_number: 0,
        local_identifier: ctx.port_attr.lid,
    };
    let remote2 = RemoteQpInfo {
        qp_number: qp2.borrow().qpn(),
        packet_sequence_number: 0,
        local_identifier: ctx.port_attr.lid,
    };

    let access = full_access().bits();
    qp1.borrow_mut()
        .connect(&remote2, ctx.port, 0, 4, 4, access)
        .expect("connect QP1");
    qp2.borrow_mut()
        .connect(&remote1, ctx.port, 0, 4, 4, access)
        .expect("connect QP2");

    // Allocate buffers
    let mut send_buf = AlignedBuffer::new(4096);
    let mut recv_buf = AlignedBuffer::new(4096);

    let send_mr = unsafe {
        ctx.pd
            .register(send_buf.as_ptr(), send_buf.size(), full_access())
    }
    .expect("Failed to register send MR");
    let recv_mr = unsafe {
        ctx.pd
            .register(recv_buf.as_ptr(), recv_buf.size(), full_access())
    }
    .expect("Failed to register recv MR");

    send_buf.fill(0xCD);
    recv_buf.fill(0);

    // Post receive buffer on QP1
    let num_ops = 8;
    for i in 0..num_ops {
        let offset = (i * 64) as u64;
        qp1.borrow()
            .recv_builder(1000 + i as u64)
            .expect("recv_builder failed")
            .sge(recv_buf.addr() + offset, 64, recv_mr.lkey())
            .finish();
    }
    qp1.borrow().ring_rq_doorbell();

    // QP2 sends RDMA WRITE with IMM to QP1
    println!("Posting {} RDMA WRITE IMM operations...", num_ops);
    for i in 0..num_ops {
        let offset = (i * 64) as u64;
        let imm_data = (i + 100) as u32;
        qp2.borrow_mut()
            .wqe_builder(i as u64)
            .expect("wqe_builder failed")
            .ctrl_rdma_write_imm(WqeFlags::COMPLETION, imm_data)
            .rdma(recv_buf.addr() + offset, recv_mr.rkey())
            .sge(send_buf.addr() + offset, 64, send_mr.lkey())
            .finish_with_blueflame()
            .expect("finish_with_blueflame failed");
    }

    // Poll both CQs
    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(10);

    // We only care about recv completions for this test
    while recv_completions.borrow().len() < num_ops {
        let recv_count = recv_cq.poll();
        if recv_count > 0 {
            recv_cq.flush();
        }

        // Also drain send CQ to avoid overflow
        let send_count = send_cq2.poll();
        if send_count > 0 {
            send_cq2.flush();
        }

        if start.elapsed() > timeout {
            panic!(
                "Timeout: recv={}, expected={}",
                recv_completions.borrow().len(),
                num_ops
            );
        }
        std::hint::spin_loop();
    }

    // Verify recv completions have correct IMM data
    let recv_comps = recv_completions.borrow();
    assert_eq!(
        recv_comps.len(),
        num_ops,
        "Should have all recv completions"
    );

    for (imm, entry) in recv_comps.iter() {
        println!("Recv completion: entry={}, imm={}", entry, imm);
    }

    println!(
        "MonoCq recv RDMA WRITE IMM test passed! ({} recv completions)",
        num_ops
    );
}

/// Bidirectional pingpong test (simulates benchmark pattern).
///
/// Both sides send and receive, similar to the actual benchmark.
#[test]
fn test_mono_cq_bidirectional_pingpong() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    // QP1 completions
    let qp1_send_count = Rc::new(RefCell::new(0usize));
    let qp1_recv_count = Rc::new(RefCell::new(0usize));

    let qp1_send_clone = qp1_send_count.clone();
    let qp1_recv_clone = qp1_recv_count.clone();

    let qp1_send_cb = move |_cqe: Cqe, _entry: u64| {
        *qp1_send_clone.borrow_mut() += 1;
    };
    let qp1_recv_cb = move |_cqe: Cqe, _entry: u64| {
        *qp1_recv_clone.borrow_mut() += 1;
    };

    // QP2 completions
    let qp2_send_count = Rc::new(RefCell::new(0usize));
    let qp2_recv_count = Rc::new(RefCell::new(0usize));

    let qp2_send_clone = qp2_send_count.clone();
    let qp2_recv_clone = qp2_recv_count.clone();

    let qp2_send_cb = move |_cqe: Cqe, _entry: u64| {
        *qp2_send_clone.borrow_mut() += 1;
    };
    let qp2_recv_cb = move |_cqe: Cqe, _entry: u64| {
        *qp2_recv_clone.borrow_mut() += 1;
    };

    // Create CQs
    let qp1_send_cq = ctx
        .ctx
        .create_mono_cq::<RcQpForMonoCq<u64>, _>(256, qp1_send_cb)
        .expect("qp1_send_cq");
    let qp1_recv_cq = ctx
        .ctx
        .create_mono_cq::<RcQpForMonoCq<u64>, _>(256, qp1_recv_cb)
        .expect("qp1_recv_cq");
    let qp2_send_cq = ctx
        .ctx
        .create_mono_cq::<RcQpForMonoCq<u64>, _>(256, qp2_send_cb)
        .expect("qp2_send_cq");
    let qp2_recv_cq = ctx
        .ctx
        .create_mono_cq::<RcQpForMonoCq<u64>, _>(256, qp2_recv_cb)
        .expect("qp2_recv_cq");

    let config = RcQpConfig::default();

    let qp1 = ctx
        .ctx
        .create_rc_qp_for_mono_cq::<u64, _, _, _>(&ctx.pd, &qp1_send_cq, &qp1_recv_cq, &config)
        .expect("QP1");
    let qp2 = ctx
        .ctx
        .create_rc_qp_for_mono_cq::<u64, _, _, _>(&ctx.pd, &qp2_send_cq, &qp2_recv_cq, &config)
        .expect("QP2");

    qp1_send_cq.register(&qp1);
    qp1_recv_cq.register(&qp1);
    qp2_send_cq.register(&qp2);
    qp2_recv_cq.register(&qp2);

    let remote1 = RemoteQpInfo {
        qp_number: qp1.borrow().qpn(),
        packet_sequence_number: 0,
        local_identifier: ctx.port_attr.lid,
    };
    let remote2 = RemoteQpInfo {
        qp_number: qp2.borrow().qpn(),
        packet_sequence_number: 0,
        local_identifier: ctx.port_attr.lid,
    };

    let access = full_access().bits();
    qp1.borrow_mut()
        .connect(&remote2, ctx.port, 0, 4, 4, access)
        .expect("connect QP1");
    qp2.borrow_mut()
        .connect(&remote1, ctx.port, 0, 4, 4, access)
        .expect("connect QP2");

    // Buffers
    let mut buf1 = AlignedBuffer::new(16 * 1024);
    let mut buf2 = AlignedBuffer::new(16 * 1024);

    let mr1 = unsafe { ctx.pd.register(buf1.as_ptr(), buf1.size(), full_access()) }.expect("mr1");
    let mr2 = unsafe { ctx.pd.register(buf2.as_ptr(), buf2.size(), full_access()) }.expect("mr2");

    buf1.fill(0x11);
    buf2.fill(0x22);

    let num_iters = 32;
    let msg_size = 64u32;

    // Pre-post receives on both sides
    for i in 0..num_iters {
        let offset = (i * msg_size as usize) as u64;
        qp1.borrow()
            .recv_builder(i as u64)
            .expect("recv_builder")
            .sge(buf1.addr() + offset, msg_size, mr1.lkey())
            .finish();
        qp2.borrow()
            .recv_builder(i as u64)
            .expect("recv_builder")
            .sge(buf2.addr() + offset, msg_size, mr2.lkey())
            .finish();
    }
    qp1.borrow().ring_rq_doorbell();
    qp2.borrow().ring_rq_doorbell();

    println!("Starting {} pingpong iterations...", num_iters);

    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(10);

    // Pingpong: QP1 sends, QP2 receives, QP2 sends back, QP1 receives
    for i in 0..num_iters {
        let offset = (i * msg_size as usize) as u64;
        let imm = i as u32;

        // QP1 -> QP2
        qp1.borrow_mut()
            .wqe_builder(i as u64)
            .expect("wqe_builder")
            .ctrl_rdma_write_imm(WqeFlags::COMPLETION, imm)
            .rdma(buf2.addr() + offset, mr2.rkey())
            .sge(buf1.addr() + offset, msg_size, mr1.lkey())
            .finish_with_blueflame()
            .expect("finish");

        // Wait for QP2 to receive
        while *qp2_recv_count.borrow() <= i {
            qp2_recv_cq.poll();
            qp2_recv_cq.flush();

            if start.elapsed() > timeout {
                panic!(
                    "Timeout at iter {}: qp2_recv={}",
                    i,
                    *qp2_recv_count.borrow()
                );
            }
        }

        // QP2 -> QP1
        qp2.borrow_mut()
            .wqe_builder(i as u64)
            .expect("wqe_builder")
            .ctrl_rdma_write_imm(WqeFlags::COMPLETION, imm)
            .rdma(buf1.addr() + offset, mr1.rkey())
            .sge(buf2.addr() + offset, msg_size, mr2.lkey())
            .finish_with_blueflame()
            .expect("finish");

        // Wait for QP1 to receive
        while *qp1_recv_count.borrow() <= i {
            qp1_recv_cq.poll();
            qp1_recv_cq.flush();

            if start.elapsed() > timeout {
                panic!(
                    "Timeout at iter {}: qp1_recv={}",
                    i,
                    *qp1_recv_count.borrow()
                );
            }
        }

        // Drain send CQs occasionally
        if i % 8 == 7 {
            qp1_send_cq.poll();
            qp1_send_cq.flush();
            qp2_send_cq.poll();
            qp2_send_cq.flush();
        }
    }

    // Drain remaining send completions
    qp1_send_cq.poll();
    qp1_send_cq.flush();
    qp2_send_cq.poll();
    qp2_send_cq.flush();

    println!(
        "Bidirectional pingpong test passed! {} iters in {:?}",
        num_iters,
        start.elapsed()
    );
    println!(
        "  QP1: send={}, recv={}",
        *qp1_send_count.borrow(),
        *qp1_recv_count.borrow()
    );
    println!(
        "  QP2: send={}, recv={}",
        *qp2_send_count.borrow(),
        *qp2_recv_count.borrow()
    );
}

/// Test CQ wrap-around behavior with MonoCq.
///
/// This test verifies that the CQ works correctly when the consumer index
/// wraps around. The CQ size is 256, so we need to process 512+ completions
/// to wrap around twice.
#[test]
fn test_mono_cq_wraparound() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    let completion_count = Rc::new(Cell::new(0u64));
    let completion_count_clone = completion_count.clone();

    let callback = move |_cqe: Cqe, _entry: u64| {
        completion_count_clone.set(completion_count_clone.get() + 1);
    };

    let cq_size = 256; // Standard CQ size
    let num_operations = cq_size * 3; // Process 768 completions to wrap around ~3 times
    let batch_size = 32; // Submit in batches to avoid filling the CQ

    let send_cq = ctx
        .ctx
        .create_mono_cq::<RcQpForMonoCq<u64>, _>(cq_size as i32, callback)
        .expect("Failed to create send MonoCq");

    let recv_cq = ctx
        .ctx
        .create_mono_cq::<RcQpForMonoCq<u64>, _>(cq_size as i32, |_cqe: Cqe, _entry: u64| {})
        .expect("Failed to create recv MonoCq");

    let config = RcQpConfig::default();

    let qp1 = ctx
        .ctx
        .create_rc_qp_for_mono_cq::<u64, _, _, _>(&ctx.pd, &send_cq, &recv_cq, &config)
        .expect("Failed to create QP1");
    let qp2 = ctx
        .ctx
        .create_rc_qp_for_mono_cq::<u64, _, _, _>(&ctx.pd, &recv_cq, &send_cq, &config)
        .expect("Failed to create QP2");

    send_cq.register(&qp1);
    recv_cq.register(&qp2);

    let remote1 = RemoteQpInfo {
        qp_number: qp1.borrow().qpn(),
        packet_sequence_number: 0,
        local_identifier: ctx.port_attr.lid,
    };
    let remote2 = RemoteQpInfo {
        qp_number: qp2.borrow().qpn(),
        packet_sequence_number: 0,
        local_identifier: ctx.port_attr.lid,
    };

    let access = full_access().bits();
    qp1.borrow_mut()
        .connect(&remote2, ctx.port, 0, 4, 4, access)
        .expect("connect QP1");
    qp2.borrow_mut()
        .connect(&remote1, ctx.port, 0, 4, 4, access)
        .expect("connect QP2");

    let mut buf = AlignedBuffer::new(4096);
    let mr = unsafe { ctx.pd.register(buf.as_ptr(), buf.size(), full_access()) }
        .expect("Failed to register MR");

    buf.fill(0xAB);

    println!(
        "Testing CQ wrap-around: {} completions (CQ size = {})",
        num_operations, cq_size
    );

    let start = std::time::Instant::now();
    let mut submitted = 0u64;

    while submitted < num_operations as u64 {
        // Submit a batch of operations
        {
            let qp = qp1.borrow();
            for i in 0..batch_size.min((num_operations as u64 - submitted) as usize) {
                let offset = (i * 32) as u64;
                let _ = qp
                    .wqe_builder(submitted + i as u64)
                    .unwrap()
                    .ctrl_rdma_write(WqeFlags::COMPLETION)
                    .rdma(buf.addr() + offset, mr.lkey())
                    .sge(buf.addr() + offset, 32, mr.lkey())
                    .finish();
            }
            qp.ring_sq_doorbell();
        }
        submitted += batch_size as u64;

        // Wait for completions
        let expected = completion_count.get() + batch_size as u64;
        let timeout = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while completion_count.get() < expected {
            send_cq.poll();
            send_cq.flush();
            if std::time::Instant::now() > timeout {
                panic!(
                    "Timeout at batch {}: got {} completions, expected {}",
                    submitted / batch_size as u64,
                    completion_count.get(),
                    expected
                );
            }
        }
    }

    println!(
        "CQ wrap-around test passed! {} completions in {:?}",
        completion_count.get(),
        start.elapsed()
    );
}

/// Test RX CQ with CQE compression enabled.
///
/// CQE compression is only valid for RX (responder side) completions.
/// This test verifies that RX CQ with compression enabled works correctly.
/// Note: Actual compressed CQEs (format=3) may require Strided RQ to be generated by HW.
#[test]
fn test_mono_cq_rx_compression() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    let send_completions = Rc::new(Cell::new(0u64));
    let send_completions_clone = send_completions.clone();

    let send_callback = move |_cqe: Cqe, _entry: u64| {
        send_completions_clone.set(send_completions_clone.get() + 1);
    };

    let recv_completions = Rc::new(Cell::new(0u64));
    let recv_completions_clone = recv_completions.clone();

    let recv_callback = move |_cqe: Cqe, _entry: u64| {
        recv_completions_clone.set(recv_completions_clone.get() + 1);
    };

    // TX CQ without compression
    let send_cq = ctx
        .ctx
        .create_mono_cq::<RcQpForMonoCq<u64>, _>(256, send_callback)
        .expect("Failed to create send MonoCq");

    // RX CQ with compression enabled
    let rx_config = CqConfig {
        compression_format: Some(CqeCompressionFormat::Hash),
        ..Default::default()
    };
    let recv_cq = ctx
        .ctx
        .create_mono_cq_with_config::<RcQpForMonoCq<u64>, _>(256, recv_callback, &rx_config)
        .expect("Failed to create recv MonoCq with compression");

    // Dummy CQs for qp2 (sender side)
    let send_cq2 = ctx
        .ctx
        .create_mono_cq::<RcQpForMonoCq<u64>, _>(256, |_cqe: Cqe, _entry: u64| {})
        .expect("Failed to create send MonoCq 2");
    let recv_cq2 = ctx
        .ctx
        .create_mono_cq::<RcQpForMonoCq<u64>, _>(256, |_cqe: Cqe, _entry: u64| {})
        .expect("Failed to create recv MonoCq 2");

    let config = RcQpConfig::default();

    let qp1 = ctx
        .ctx
        .create_rc_qp_for_mono_cq::<u64, _, _, _>(&ctx.pd, &send_cq, &recv_cq, &config)
        .expect("Failed to create QP1");
    let qp2 = ctx
        .ctx
        .create_rc_qp_for_mono_cq::<u64, _, _, _>(&ctx.pd, &send_cq2, &recv_cq2, &config)
        .expect("Failed to create QP2");

    send_cq.register(&qp1);
    recv_cq.register(&qp1);
    send_cq2.register(&qp2);
    recv_cq2.register(&qp2);

    let remote1 = RemoteQpInfo {
        qp_number: qp1.borrow().qpn(),
        packet_sequence_number: 0,
        local_identifier: ctx.port_attr.lid,
    };
    let remote2 = RemoteQpInfo {
        qp_number: qp2.borrow().qpn(),
        packet_sequence_number: 0,
        local_identifier: ctx.port_attr.lid,
    };

    let access = full_access().bits();
    qp1.borrow_mut()
        .connect(&remote2, ctx.port, 0, 4, 4, access)
        .expect("connect QP1");
    qp2.borrow_mut()
        .connect(&remote1, ctx.port, 0, 4, 4, access)
        .expect("connect QP2");

    let mut send_buf = AlignedBuffer::new(4096);
    let mut recv_buf = AlignedBuffer::new(4096);

    let send_mr = unsafe {
        ctx.pd
            .register(send_buf.as_ptr(), send_buf.size(), full_access())
    }
    .expect("Failed to register send MR");
    let recv_mr = unsafe {
        ctx.pd
            .register(recv_buf.as_ptr(), recv_buf.size(), full_access())
    }
    .expect("Failed to register recv MR");

    send_buf.fill(0xCD);
    recv_buf.fill(0);

    // Post many receive buffers on QP1 to receive RDMA WRITE IMM from QP2
    let num_ops = 64;
    let chunk_size = 32u32;

    for i in 0..num_ops {
        let offset = (i * chunk_size) as u64;
        qp1.borrow_mut()
            .recv_builder(i as u64)
            .expect("recv_builder failed")
            .sge(recv_buf.addr() + offset, chunk_size, recv_mr.lkey())
            .finish();
    }
    qp1.borrow().ring_rq_doorbell();

    println!(
        "Testing RX CQ compression with {} RDMA WRITE IMM operations...",
        num_ops
    );

    // QP2 sends RDMA WRITE IMM to QP1 (this generates RX completions on QP1)
    for i in 0..num_ops {
        let offset = (i * chunk_size) as u64;
        let _ = qp2
            .borrow()
            .wqe_builder(i as u64)
            .unwrap()
            .ctrl_rdma_write_imm(WqeFlags::COMPLETION, i as u32)
            .rdma(recv_buf.addr() + offset, recv_mr.rkey())
            .sge(send_buf.addr() + offset, chunk_size, send_mr.lkey())
            .finish();
    }
    qp2.borrow().ring_sq_doorbell();

    // Wait for RX completions on QP1
    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(5);

    while recv_completions.get() < num_ops as u64 {
        recv_cq.poll();
        recv_cq.flush();

        // Also drain send completions from QP2
        send_cq2.poll();
        send_cq2.flush();

        if start.elapsed() > timeout {
            panic!(
                "Timeout: recv={}, expected={}",
                recv_completions.get(),
                num_ops
            );
        }
        std::hint::spin_loop();
    }

    println!(
        "RX CQ compression test passed! {} recv completions in {:?}",
        recv_completions.get(),
        start.elapsed()
    );
    println!(
        "  Compression enabled: {}, Compressed CQEs detected: {}",
        recv_cq.is_compression_enabled(),
        recv_cq.compressed_cqe_count()
    );
}

/// Benchmark comparison: RX CQ compression vs non-compression mode.
/// Run with: cargo test --package mlx5 --test mono_cq_tests test_rx_compression_benchmark -- --nocapture
#[test]
fn test_rx_compression_benchmark() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    const NUM_OPS: u32 = 1024;
    const CHUNK_SIZE: u32 = 32;
    const ITERATIONS: u32 = 10;

    println!("\n=== RX CQ Compression Benchmark ===");
    println!("Operations per iteration: {}", NUM_OPS);
    println!("Chunk size: {} bytes", CHUNK_SIZE);
    println!();

    // Benchmark non-compression mode
    let mut non_comp_times = Vec::new();
    for i in 0..ITERATIONS {
        let recv_completions = Rc::new(Cell::new(0u64));
        let recv_completions_clone = recv_completions.clone();
        let recv_callback = move |_cqe: Cqe, _entry: u64| {
            recv_completions_clone.set(recv_completions_clone.get() + 1);
        };

        let elapsed = {
            let recv_cq = ctx
                .ctx
                .create_mono_cq::<RcQpForMonoCq<u64>, _>(NUM_OPS as i32 * 2, recv_callback)
                .expect("Failed to create recv MonoCq");

            run_benchmark_inner(&ctx, recv_cq, &recv_completions, NUM_OPS, CHUNK_SIZE)
        };
        non_comp_times.push(elapsed);
        if i == 0 {
            println!("Non-compression warmup: {:?}", elapsed);
        }
    }

    // Benchmark compression mode
    let mut comp_times = Vec::new();
    for i in 0..ITERATIONS {
        let recv_completions = Rc::new(Cell::new(0u64));
        let recv_completions_clone = recv_completions.clone();
        let recv_callback = move |_cqe: Cqe, _entry: u64| {
            recv_completions_clone.set(recv_completions_clone.get() + 1);
        };

        let elapsed = {
            let rx_config = CqConfig {
                compression_format: Some(CqeCompressionFormat::Hash),
                ..Default::default()
            };
            let recv_cq = ctx
                .ctx
                .create_mono_cq_with_config::<RcQpForMonoCq<u64>, _>(
                    NUM_OPS as i32 * 2,
                    recv_callback,
                    &rx_config,
                )
                .expect("Failed to create recv MonoCq with compression");

            run_benchmark_inner(&ctx, recv_cq, &recv_completions, NUM_OPS, CHUNK_SIZE)
        };
        comp_times.push(elapsed);
        if i == 0 {
            println!("Compression warmup: {:?}", elapsed);
        }
    }

    // Skip first iteration (warmup) and calculate averages
    let non_comp_avg: std::time::Duration =
        non_comp_times[1..].iter().sum::<std::time::Duration>() / (ITERATIONS - 1);
    let comp_avg: std::time::Duration =
        comp_times[1..].iter().sum::<std::time::Duration>() / (ITERATIONS - 1);

    let non_comp_throughput = NUM_OPS as f64 / non_comp_avg.as_secs_f64() / 1_000_000.0;
    let comp_throughput = NUM_OPS as f64 / comp_avg.as_secs_f64() / 1_000_000.0;

    println!();
    println!(
        "Results (average of {} iterations, excluding warmup):",
        ITERATIONS - 1
    );
    println!(
        "  Non-compression: {:?} ({:.2} Mops/s)",
        non_comp_avg, non_comp_throughput
    );
    println!(
        "  Compression:     {:?} ({:.2} Mops/s)",
        comp_avg, comp_throughput
    );
    println!(
        "  Overhead:        {:.1}%",
        (comp_avg.as_nanos() as f64 / non_comp_avg.as_nanos() as f64 - 1.0) * 100.0
    );
}

fn run_benchmark_inner<F>(
    ctx: &TestContext,
    recv_cq: mlx5::mono_cq::MonoCq<RcQpForMonoCq<u64>, F>,
    recv_completions: &Rc<Cell<u64>>,
    num_ops: u32,
    chunk_size: u32,
) -> std::time::Duration
where
    F: Fn(Cqe, u64),
{
    let send_cq = ctx
        .ctx
        .create_mono_cq::<RcQpForMonoCq<u64>, _>(num_ops as i32 * 2, |_: Cqe, _: u64| {})
        .expect("Failed to create send MonoCq");

    let send_cq2 = ctx
        .ctx
        .create_mono_cq::<RcQpForMonoCq<u64>, _>(num_ops as i32 * 2, |_: Cqe, _: u64| {})
        .expect("Failed to create send MonoCq 2");
    let recv_cq2 = ctx
        .ctx
        .create_mono_cq::<RcQpForMonoCq<u64>, _>(num_ops as i32 * 2, |_: Cqe, _: u64| {})
        .expect("Failed to create recv MonoCq 2");

    let config = RcQpConfig {
        max_send_wr: num_ops * 2,
        max_recv_wr: num_ops * 2,
        ..RcQpConfig::default()
    };

    let qp1 = ctx
        .ctx
        .create_rc_qp_for_mono_cq::<u64, _, _, _>(&ctx.pd, &send_cq, &recv_cq, &config)
        .expect("Failed to create QP1");
    let qp2 = ctx
        .ctx
        .create_rc_qp_for_mono_cq::<u64, _, _, _>(&ctx.pd, &send_cq2, &recv_cq2, &config)
        .expect("Failed to create QP2");

    send_cq.register(&qp1);
    recv_cq.register(&qp1);
    send_cq2.register(&qp2);
    recv_cq2.register(&qp2);

    let remote1 = RemoteQpInfo {
        qp_number: qp1.borrow().qpn(),
        packet_sequence_number: 0,
        local_identifier: ctx.port_attr.lid,
    };
    let remote2 = RemoteQpInfo {
        qp_number: qp2.borrow().qpn(),
        packet_sequence_number: 0,
        local_identifier: ctx.port_attr.lid,
    };

    let access = full_access().bits();
    qp1.borrow_mut()
        .connect(&remote2, ctx.port, 0, 4, 4, access)
        .expect("connect QP1");
    qp2.borrow_mut()
        .connect(&remote1, ctx.port, 0, 4, 4, access)
        .expect("connect QP2");

    let mut send_buf = AlignedBuffer::new((num_ops * chunk_size) as usize);
    let mut recv_buf = AlignedBuffer::new((num_ops * chunk_size) as usize);

    let send_mr = unsafe {
        ctx.pd
            .register(send_buf.as_ptr(), send_buf.size(), full_access())
    }
    .expect("Failed to register send MR");
    let recv_mr = unsafe {
        ctx.pd
            .register(recv_buf.as_ptr(), recv_buf.size(), full_access())
    }
    .expect("Failed to register recv MR");

    send_buf.fill(0xCD);
    recv_buf.fill(0);

    // Reset completion counter
    recv_completions.set(0);

    // Post receive buffers
    for i in 0..num_ops {
        let offset = (i * chunk_size) as u64;
        qp1.borrow_mut()
            .recv_builder(i as u64)
            .expect("recv_builder failed")
            .sge(recv_buf.addr() + offset, chunk_size, recv_mr.lkey())
            .finish();
    }
    qp1.borrow().ring_rq_doorbell();

    // Send RDMA WRITE IMM from QP2 to QP1
    for i in 0..num_ops {
        let offset = (i * chunk_size) as u64;
        let _ = qp2
            .borrow()
            .wqe_builder(i as u64)
            .unwrap()
            .ctrl_rdma_write_imm(WqeFlags::COMPLETION, i)
            .rdma(recv_buf.addr() + offset, recv_mr.rkey())
            .sge(send_buf.addr() + offset, chunk_size, send_mr.lkey())
            .finish();
    }
    qp2.borrow().ring_sq_doorbell();

    // Measure poll time
    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(5);

    while recv_completions.get() < num_ops as u64 {
        recv_cq.poll();
        recv_cq.flush();
        send_cq2.poll();
        send_cq2.flush();

        if start.elapsed() > timeout {
            panic!(
                "Timeout: recv={}, expected={}",
                recv_completions.get(),
                num_ops
            );
        }
    }

    start.elapsed()
}
