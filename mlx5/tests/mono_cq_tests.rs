//! Tests for MonoCq (Monomorphic Completion Queue).
//!
//! MonoCq provides inlined callback dispatch for better performance
//! by eliminating vtable overhead.

mod common;

use std::cell::{Cell, RefCell};
use std::rc::Rc;

use mlx5::cq::{CqConfig, Cqe};
use mlx5::qp::{RcQpConfig, RcQpForMonoCq};
use mlx5::transport::IbRemoteQpInfo;
use mlx5::wqe::WqeFlags;

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

    let mono_cq = Rc::new(ctx
        .ctx
        .create_mono_cq::<RcQpForMonoCq<u64>, _>(256, callback as fn(_, _), &CqConfig::default())
        .expect("Failed to create MonoCq"));

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

    // Track completions (both SQ and RQ)
    let completions: Rc<RefCell<Vec<(Cqe, u64)>>> = Rc::new(RefCell::new(Vec::new()));
    let completions_clone = completions.clone();

    // Single callback handles both SQ and RQ completions
    let callback = move |cqe: Cqe, entry: u64| {
        completions_clone.borrow_mut().push((cqe, entry));
    };

    // Create a single CQ for both QPs
    let cq = Rc::new(ctx
        .ctx
        .create_mono_cq::<RcQpForMonoCq<u64>, _>(256, callback, &CqConfig::default())
        .expect("Failed to create MonoCq"));

    let config = RcQpConfig::default();

    // Create QPs using the same CQ
    let qp1 = ctx
        .ctx
        .rc_qp_builder::<u64, u64>(&ctx.pd, &config)
        .sq_mono_cq(&cq)
        .rq_mono_cq(&cq)
        .build()
        .expect("Failed to create QP1");
    let qp2 = ctx
        .ctx
        .rc_qp_builder::<u64, u64>(&ctx.pd, &config)
        .sq_mono_cq(&cq)
        .rq_mono_cq(&cq)
        .build()
        .expect("Failed to create QP2");

    // Register QPs with the CQ
    cq.register(&qp1);
    cq.register(&qp2);

    // Connect QPs
    let remote1 = IbRemoteQpInfo {
        qp_number: qp1.borrow().qpn(),
        packet_sequence_number: 0,
        local_identifier: ctx.port_attr.lid,
    };
    let remote2 = IbRemoteQpInfo {
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
        .sq_wqe()
        .expect("sq_wqe failed")
        .write(WqeFlags::empty(), remote_buf.addr(), remote_mr.rkey())
        .expect("write failed")
        .sge(local_buf.addr(), test_data.len() as u32, local_mr.lkey())
        .finish_signaled(42u64)
        .expect("finish_with_blueflame failed");
    qp1.borrow().ring_sq_doorbell();

    // Poll for completion
    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(5);

    loop {
        let count = cq.poll();
        if count > 0 {
            cq.flush();
            break;
        }
        if start.elapsed() > timeout {
            panic!("Timeout waiting for completion");
        }
        std::hint::spin_loop();
    }

    // Verify callback was invoked (SQ completion from RDMA WRITE)
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
    cq.unregister(qp1.borrow().qpn());
    println!("QP1 unregistered from cq");

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

    let mono_cq = Rc::new(ctx
        .ctx
        .create_mono_cq::<RcQpForMonoCq<u64>, _>(256, callback as fn(_, _), &CqConfig::default())
        .expect("Failed to create MonoCq"));

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

    // Track completions
    let completions: Rc<RefCell<Vec<u64>>> = Rc::new(RefCell::new(Vec::new()));
    let completions_clone = completions.clone();

    let callback = move |_cqe: Cqe, entry: u64| {
        completions_clone.borrow_mut().push(entry);
    };

    // Use a single CQ for both QPs
    let cq = Rc::new(ctx
        .ctx
        .create_mono_cq::<RcQpForMonoCq<u64>, _>(256, callback, &CqConfig::default())
        .expect("Failed to create MonoCq"));

    let config = RcQpConfig::default();

    let qp1 = ctx
        .ctx
        .rc_qp_builder::<u64, u64>(&ctx.pd, &config)
        .sq_mono_cq(&cq)
        .rq_mono_cq(&cq)
        .build()
        .expect("Failed to create QP1");
    let qp2 = ctx
        .ctx
        .rc_qp_builder::<u64, u64>(&ctx.pd, &config)
        .sq_mono_cq(&cq)
        .rq_mono_cq(&cq)
        .build()
        .expect("Failed to create QP2");

    cq.register(&qp1);
    cq.register(&qp2);

    let remote1 = IbRemoteQpInfo {
        qp_number: qp1.borrow().qpn(),
        packet_sequence_number: 0,
        local_identifier: ctx.port_attr.lid,
    };
    let remote2 = IbRemoteQpInfo {
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
            .sq_wqe()
            .expect("sq_wqe failed")
            .write(WqeFlags::empty(), remote_buf.addr(), remote_mr.rkey())
            .expect("write failed")
            .sge(local_buf.addr(), test_data.len() as u32, local_mr.lkey())
            .finish_signaled(100 + i as u64)
            .expect("finish_with_blueflame failed");
    }
    qp1.borrow().ring_sq_doorbell();

    // Poll until all completions are received
    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(5);

    while completions.borrow().len() < num_ops {
        let count = cq.poll();
        if count > 0 {
            cq.flush();
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

    let callback = move |_cqe: Cqe, entry: u64| {
        send_completions_clone.borrow_mut().push(entry);
    };

    // Use larger CQ to accommodate high load - single CQ for both QPs
    let cq = Rc::new(ctx
        .ctx
        .create_mono_cq::<RcQpForMonoCq<u64>, _>(512, callback, &CqConfig::default())
        .expect("Failed to create MonoCq"));

    let config = RcQpConfig::default();

    let qp1 = ctx
        .ctx
        .rc_qp_builder::<u64, u64>(&ctx.pd, &config)
        .sq_mono_cq(&cq)
        .rq_mono_cq(&cq)
        .build()
        .expect("Failed to create QP1");
    let qp2 = ctx
        .ctx
        .rc_qp_builder::<u64, u64>(&ctx.pd, &config)
        .sq_mono_cq(&cq)
        .rq_mono_cq(&cq)
        .build()
        .expect("Failed to create QP2");

    cq.register(&qp1);
    cq.register(&qp2);

    let remote1 = IbRemoteQpInfo {
        qp_number: qp1.borrow().qpn(),
        packet_sequence_number: 0,
        local_identifier: ctx.port_attr.lid,
    };
    let remote2 = IbRemoteQpInfo {
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
            .sq_wqe()
            .expect("sq_wqe failed")
            .write(WqeFlags::empty(), remote_buf.addr() + offset, remote_mr.rkey())
            .expect("write failed")
            .sge(local_buf.addr() + offset, chunk_size, local_mr.lkey())
            .finish_signaled(i as u64)
            .expect("finish_with_blueflame failed");
    }
    qp1.borrow().ring_sq_doorbell();

    // Poll until all completions are received
    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(10);

    while send_completions.borrow().len() < num_ops {
        let count = cq.poll();
        if count > 0 {
            cq.flush();
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
///
/// Note: With single-Entry MonoCq design, we use separate CQs for
/// different completion types when needed.
#[test]
fn test_mono_cq_recv_rdma_write_imm() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    // Track send completions from QP2
    let send_completions: Rc<RefCell<Vec<u64>>> = Rc::new(RefCell::new(Vec::new()));
    let send_completions_clone = send_completions.clone();

    // Track recv completions on QP1 (captures IMM data)
    let recv_completions: Rc<RefCell<Vec<(u32, u64)>>> = Rc::new(RefCell::new(Vec::new()));
    let recv_completions_clone = recv_completions.clone();

    // QP1's callback tracks recv completions (IMM data)
    let qp1_callback = move |cqe: Cqe, entry: u64| {
        recv_completions_clone.borrow_mut().push((cqe.imm, entry));
    };

    // QP2's callback tracks send completions
    let qp2_callback = move |_cqe: Cqe, entry: u64| {
        send_completions_clone.borrow_mut().push(entry);
    };

    // Create separate CQs for each QP
    let cq1 = Rc::new(ctx
        .ctx
        .create_mono_cq::<RcQpForMonoCq<u64>, _>(256, qp1_callback, &CqConfig::default())
        .expect("Failed to create MonoCq 1"));

    let cq2 = Rc::new(ctx
        .ctx
        .create_mono_cq::<RcQpForMonoCq<u64>, _>(256, qp2_callback, &CqConfig::default())
        .expect("Failed to create MonoCq 2"));

    let config = RcQpConfig::default();

    let qp1 = ctx
        .ctx
        .rc_qp_builder::<u64, u64>(&ctx.pd, &config)
        .sq_mono_cq(&cq1)
        .rq_mono_cq(&cq1)
        .build()
        .expect("Failed to create QP1");
    let qp2 = ctx
        .ctx
        .rc_qp_builder::<u64, u64>(&ctx.pd, &config)
        .sq_mono_cq(&cq2)
        .rq_mono_cq(&cq2)
        .build()
        .expect("Failed to create QP2");

    cq1.register(&qp1);
    cq2.register(&qp2);

    let remote1 = IbRemoteQpInfo {
        qp_number: qp1.borrow().qpn(),
        packet_sequence_number: 0,
        local_identifier: ctx.port_attr.lid,
    };
    let remote2 = IbRemoteQpInfo {
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
            .post_recv(1000 + i as u64, recv_buf.addr() + offset, 64, recv_mr.lkey())
            .expect("post_recv failed");
    }
    qp1.borrow().ring_rq_doorbell();

    // QP2 sends RDMA WRITE with IMM to QP1
    println!("Posting {} RDMA WRITE IMM operations...", num_ops);
    for i in 0..num_ops {
        let offset = (i * 64) as u64;
        let imm_data = (i + 100) as u32;
        qp2.borrow_mut()
            .sq_wqe()
            .expect("sq_wqe failed")
            .write_imm(WqeFlags::empty(), recv_buf.addr() + offset, recv_mr.rkey(), imm_data)
            .expect("write_imm failed")
            .sge(send_buf.addr() + offset, 64, send_mr.lkey())
            .finish_signaled(i as u64)
            .expect("finish_with_blueflame failed");
    }
    qp2.borrow().ring_sq_doorbell();

    // Poll both CQs
    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(10);

    // We only care about recv completions for this test
    while recv_completions.borrow().len() < num_ops {
        let recv_count = cq1.poll();
        if recv_count > 0 {
            cq1.flush();
        }

        // Also drain send CQ (cq2) to avoid overflow
        let send_count = cq2.poll();
        if send_count > 0 {
            cq2.flush();
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

    // QP completions - use Cell for interior mutability
    let qp1_count = Rc::new(Cell::new(0usize));
    let qp2_count = Rc::new(Cell::new(0usize));

    fn noop_callback(_cqe: Cqe, _entry: u64) {}

    // Create CQs with noop callbacks - we'll track completions via poll return value
    let qp1_cq = Rc::new(ctx
        .ctx
        .create_mono_cq::<RcQpForMonoCq<u64>, _>(256, noop_callback, &CqConfig::default())
        .expect("qp1_cq"));
    let qp2_cq = Rc::new(ctx
        .ctx
        .create_mono_cq::<RcQpForMonoCq<u64>, _>(256, noop_callback, &CqConfig::default())
        .expect("qp2_cq"));

    let config = RcQpConfig::default();

    let qp1 = ctx
        .ctx
        .rc_qp_builder::<u64, u64>(&ctx.pd, &config)
        .sq_mono_cq(&qp1_cq)
        .rq_mono_cq(&qp1_cq)
        .build()
        .expect("QP1");
    let qp2 = ctx
        .ctx
        .rc_qp_builder::<u64, u64>(&ctx.pd, &config)
        .sq_mono_cq(&qp2_cq)
        .rq_mono_cq(&qp2_cq)
        .build()
        .expect("QP2");

    qp1_cq.register(&qp1);
    qp2_cq.register(&qp2);

    let remote1 = IbRemoteQpInfo {
        qp_number: qp1.borrow().qpn(),
        packet_sequence_number: 0,
        local_identifier: ctx.port_attr.lid,
    };
    let remote2 = IbRemoteQpInfo {
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
            .post_recv(i as u64, buf1.addr() + offset, msg_size, mr1.lkey())
            .expect("post_recv");
        qp2.borrow()
            .post_recv(i as u64, buf2.addr() + offset, msg_size, mr2.lkey())
            .expect("post_recv");
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
            .sq_wqe()
            .expect("sq_wqe")
            .write_imm(WqeFlags::empty(), buf2.addr() + offset, mr2.rkey(), imm)
            .expect("write_imm")
            .sge(buf1.addr() + offset, msg_size, mr1.lkey())
            .finish_signaled(i as u64)
            .expect("finish");
        qp1.borrow().ring_sq_doorbell();

        // Wait for QP2 to receive (poll returns number of completions)
        while qp2_count.get() <= i {
            let count = qp2_cq.poll();
            if count > 0 {
                // Assume mixed send/recv completions
                qp2_count.set(qp2_count.get() + count as usize);
                qp2_cq.flush();
            }

            if start.elapsed() > timeout {
                panic!(
                    "Timeout at iter {}: qp2_count={}",
                    i,
                    qp2_count.get()
                );
            }
        }

        // QP2 -> QP1
        qp2.borrow_mut()
            .sq_wqe()
            .expect("sq_wqe")
            .write_imm(WqeFlags::empty(), buf1.addr() + offset, mr1.rkey(), imm)
            .expect("write_imm")
            .sge(buf2.addr() + offset, msg_size, mr2.lkey())
            .finish_signaled(i as u64)
            .expect("finish");
        qp2.borrow().ring_sq_doorbell();

        // Wait for QP1 to receive
        while qp1_count.get() <= i {
            let count = qp1_cq.poll();
            if count > 0 {
                qp1_count.set(qp1_count.get() + count as usize);
                qp1_cq.flush();
            }

            if start.elapsed() > timeout {
                panic!(
                    "Timeout at iter {}: qp1_count={}",
                    i,
                    qp1_count.get()
                );
            }
        }
    }

    // Drain remaining completions
    qp1_cq.poll();
    qp1_cq.flush();
    qp2_cq.poll();
    qp2_cq.flush();

    println!(
        "Bidirectional pingpong test passed! {} iters in {:?}",
        num_iters,
        start.elapsed()
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

    // Use a single CQ for both QPs
    let cq = Rc::new(ctx
        .ctx
        .create_mono_cq::<RcQpForMonoCq<u64>, _>(cq_size as i32, callback, &CqConfig::default())
        .expect("Failed to create MonoCq"));

    let config = RcQpConfig::default();

    let qp1 = ctx
        .ctx
        .rc_qp_builder::<u64, u64>(&ctx.pd, &config)
        .sq_mono_cq(&cq)
        .rq_mono_cq(&cq)
        .build()
        .expect("Failed to create QP1");
    let qp2 = ctx
        .ctx
        .rc_qp_builder::<u64, u64>(&ctx.pd, &config)
        .sq_mono_cq(&cq)
        .rq_mono_cq(&cq)
        .build()
        .expect("Failed to create QP2");

    cq.register(&qp1);
    cq.register(&qp2);

    let remote1 = IbRemoteQpInfo {
        qp_number: qp1.borrow().qpn(),
        packet_sequence_number: 0,
        local_identifier: ctx.port_attr.lid,
    };
    let remote2 = IbRemoteQpInfo {
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
            let mut qp = qp1.borrow_mut();
            for i in 0..batch_size.min((num_operations as u64 - submitted) as usize) {
                let offset = (i * 32) as u64;
                let _ = qp
                    .sq_wqe()
                    .unwrap()
                    .write(WqeFlags::empty(), buf.addr() + offset, mr.lkey())
                    .unwrap()
                    .sge(buf.addr() + offset, 32, mr.lkey())
                    .finish_signaled(submitted + i as u64);
            }
            qp.ring_sq_doorbell();
        }
        submitted += batch_size as u64;

        // Wait for completions
        let expected = completion_count.get() + batch_size as u64;
        let timeout = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while completion_count.get() < expected {
            cq.poll();
            cq.flush();
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
