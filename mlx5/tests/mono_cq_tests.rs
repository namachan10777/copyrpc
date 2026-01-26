//! Tests for MonoCq (Monomorphic Completion Queue).
//!
//! MonoCq provides inlined callback dispatch for better performance
//! by eliminating vtable overhead.

mod common;

use std::cell::{Cell, RefCell};
use std::rc::Rc;

use mlx5::cq::{CqConfig, Cqe};
use mlx5::emit_wqe;
use mlx5::qp::{RcQpConfig, RcQpForMonoCq, RcQpForMonoCqWithSrqAndSqCb};
use mlx5::srq::SrqConfig;
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
    let qp1_ref = qp1.borrow();
    let ctx = qp1_ref.emit_ctx().expect("emit_ctx failed");
    emit_wqe!(&ctx, write {
        flags: WqeFlags::empty(),
        remote_addr: remote_buf.addr(),
        rkey: remote_mr.rkey(),
        sge: { addr: local_buf.addr(), len: test_data.len() as u32, lkey: local_mr.lkey() },
        signaled: 42u64,
    }).expect("emit_wqe failed");
    qp1_ref.ring_sq_doorbell();
    drop(qp1_ref);

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
    {
        let qp1_ref = qp1.borrow();
        let ctx = qp1_ref.emit_ctx().expect("emit_ctx failed");
        for i in 0..num_ops {
            emit_wqe!(&ctx, write {
                flags: WqeFlags::empty(),
                remote_addr: remote_buf.addr(),
                rkey: remote_mr.rkey(),
                sge: { addr: local_buf.addr(), len: test_data.len() as u32, lkey: local_mr.lkey() },
                signaled: 100 + i as u64,
            }).expect("emit_wqe failed");
        }
        qp1_ref.ring_sq_doorbell();
    }

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

    {
        let qp1_ref = qp1.borrow();
        let ctx = qp1_ref.emit_ctx().expect("emit_ctx failed");
        for i in 0..num_ops {
            let offset = (i as u64) * (chunk_size as u64);
            emit_wqe!(&ctx, write {
                flags: WqeFlags::empty(),
                remote_addr: remote_buf.addr() + offset,
                rkey: remote_mr.rkey(),
                sge: { addr: local_buf.addr() + offset, len: chunk_size, lkey: local_mr.lkey() },
                signaled: i as u64,
            }).expect("emit_wqe failed");
        }
        qp1_ref.ring_sq_doorbell();
    }

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
    {
        let qp2_ref = qp2.borrow();
        let ctx = qp2_ref.emit_ctx().expect("emit_ctx failed");
        for i in 0..num_ops {
            let offset = (i * 64) as u64;
            let imm_data = (i + 100) as u32;
            emit_wqe!(&ctx, write_imm {
                flags: WqeFlags::empty(),
                remote_addr: recv_buf.addr() + offset,
                rkey: recv_mr.rkey(),
                imm: imm_data,
                sge: { addr: send_buf.addr() + offset, len: 64, lkey: send_mr.lkey() },
                signaled: i as u64,
            }).expect("emit_wqe failed");
        }
        qp2_ref.ring_sq_doorbell();
    }

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
        {
            let qp1_ref = qp1.borrow();
            let ctx = qp1_ref.emit_ctx().expect("emit_ctx");
            emit_wqe!(&ctx, write_imm {
                flags: WqeFlags::empty(),
                remote_addr: buf2.addr() + offset,
                rkey: mr2.rkey(),
                imm: imm,
                sge: { addr: buf1.addr() + offset, len: msg_size, lkey: mr1.lkey() },
                signaled: i as u64,
            }).expect("emit_wqe");
            qp1_ref.ring_sq_doorbell();
        }

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
        {
            let qp2_ref = qp2.borrow();
            let ctx = qp2_ref.emit_ctx().expect("emit_ctx");
            emit_wqe!(&ctx, write_imm {
                flags: WqeFlags::empty(),
                remote_addr: buf1.addr() + offset,
                rkey: mr1.rkey(),
                imm: imm,
                sge: { addr: buf2.addr() + offset, len: msg_size, lkey: mr2.lkey() },
                signaled: i as u64,
            }).expect("emit_wqe");
            qp2_ref.ring_sq_doorbell();
        }

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
        .create_mono_cq::<RcQpForMonoCq<u64>, _>(cq_size, callback, &CqConfig::default())
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
            let qp = qp1.borrow();
            let ctx = qp.emit_ctx().unwrap();
            for i in 0..batch_size.min((num_operations as u64 - submitted) as usize) {
                let offset = (i * 32) as u64;
                emit_wqe!(&ctx, write {
                    flags: WqeFlags::empty(),
                    remote_addr: buf.addr() + offset,
                    rkey: mr.lkey(),
                    sge: { addr: buf.addr() + offset, len: 32, lkey: mr.lkey() },
                    signaled: submitted + i as u64,
                }).unwrap();
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

/// Test MonoCq with SRQ and RDMA WRITE+IMM.
///
/// This test verifies that MonoCq works correctly with Shared Receive Queue,
/// which is the pattern used by copyrpc.
#[test]
fn test_mono_cq_with_srq() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    #[derive(Clone, Copy, Debug)]
    struct SrqEntry {
        qpn: u32,
    }

    // Track recv completions
    let recv_completions: Rc<RefCell<Vec<(u32, SrqEntry)>>> = Rc::new(RefCell::new(Vec::new()));
    let recv_completions_clone = recv_completions.clone();

    let recv_callback = move |cqe: Cqe, entry: SrqEntry| {
        eprintln!("recv_callback: qpn={}, imm={}", entry.qpn, cqe.imm);
        recv_completions_clone.borrow_mut().push((cqe.imm, entry));
    };

    // Create send CQ (normal CQ)
    let send_cq = Rc::new(ctx.ctx.create_cq(256, &CqConfig::default()).expect("send_cq"));

    // Create SRQ
    let srq: mlx5::srq::Srq<SrqEntry> = ctx.pd.create_srq(&SrqConfig { max_wr: 256, max_sge: 1 }).expect("SRQ");
    let srq = Rc::new(srq);

    let config = RcQpConfig::default();

    // Use function pointer type for SQ callback to avoid anonymous closure types
    type SqCallback = fn(Cqe, SrqEntry);
    fn empty_sq_callback(_cqe: Cqe, _entry: SrqEntry) {}

    // Create recv CQ (MonoCq for SRQ-based QP with function pointer callback type)
    let recv_cq: Rc<mlx5::mono_cq::MonoCq<RcQpForMonoCqWithSrqAndSqCb<SrqEntry, SqCallback>, _>> = Rc::new(
        ctx.ctx
            .create_mono_cq(256, recv_callback, &CqConfig::default())
            .expect("recv_cq"),
    );

    // Create QP1 with SRQ using MonoCq for recv
    let qp1 = ctx.ctx
        .rc_qp_builder::<SrqEntry, SrqEntry>(&ctx.pd, &config)
        .with_srq(srq.clone())
        .sq_cq(send_cq.clone(), empty_sq_callback as SqCallback)
        .rq_mono_cq(&recv_cq)
        .build()
        .expect("Failed to create QP1");

    let qpn1 = qp1.borrow().qpn();
    eprintln!("QP1 QPN: {}", qpn1);

    // Register QP1 with recv MonoCq
    recv_cq.register(&qp1);

    // Create QP2 (sender) - normal QP without SRQ
    let qp2 = ctx.ctx
        .rc_qp_builder::<u64, u64>(&ctx.pd, &config)
        .sq_cq(send_cq.clone(), |_cqe, _entry| {})
        .rq_cq(send_cq.clone(), |_cqe, _entry| {})
        .build()
        .expect("Failed to create QP2");

    let qpn2 = qp2.borrow().qpn();
    eprintln!("QP2 QPN: {}", qpn2);

    // Connect QPs
    let remote1 = IbRemoteQpInfo {
        qp_number: qpn1,
        packet_sequence_number: 0,
        local_identifier: ctx.port_attr.lid,
    };
    let remote2 = IbRemoteQpInfo {
        qp_number: qpn2,
        packet_sequence_number: 0,
        local_identifier: ctx.port_attr.lid,
    };

    let access = full_access().bits();
    qp1.borrow_mut().connect(&remote2, ctx.port, 0, 4, 4, access).expect("connect QP1");
    qp2.borrow_mut().connect(&remote1, ctx.port, 0, 4, 4, access).expect("connect QP2");

    // Allocate and register buffers
    let mut send_buf = AlignedBuffer::new(4096);
    let mut recv_buf = AlignedBuffer::new(4096);

    let send_mr = unsafe { ctx.pd.register(send_buf.as_ptr(), send_buf.size(), full_access()) }.expect("send_mr");
    let recv_mr = unsafe { ctx.pd.register(recv_buf.as_ptr(), recv_buf.size(), full_access()) }.expect("recv_mr");

    send_buf.fill(0xAB);
    recv_buf.fill(0);

    // Post recv to SRQ with QPN
    let num_ops = 8;
    for _ in 0..num_ops {
        srq.post_recv(SrqEntry { qpn: qpn1 }, 0, 0, 0).expect("post_recv");
    }
    srq.ring_doorbell();

    // QP2 sends RDMA WRITE+IMM to QP1
    eprintln!("Posting {} RDMA WRITE IMM operations...", num_ops);
    {
        let qp2_ref = qp2.borrow();
        let ctx = qp2_ref.emit_ctx().expect("emit_ctx");
        for i in 0..num_ops {
            let offset = (i * 64) as u64;
            let imm_data = (i + 100) as u32;
            emit_wqe!(&ctx, write_imm {
                flags: WqeFlags::empty(),
                remote_addr: recv_buf.addr() + offset,
                rkey: recv_mr.rkey(),
                imm: imm_data,
                sge: { addr: send_buf.addr() + offset, len: 64, lkey: send_mr.lkey() },
                signaled: i as u64,
            }).expect("emit_wqe");
        }
        qp2_ref.ring_sq_doorbell();
    }

    // Poll for completions
    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(10);

    while recv_completions.borrow().len() < num_ops {
        let recv_count = recv_cq.poll();
        if recv_count > 0 {
            eprintln!("recv_cq.poll() returned {}", recv_count);
            recv_cq.flush();
        }

        // Drain send CQ
        send_cq.poll();
        send_cq.flush();

        if start.elapsed() > timeout {
            panic!(
                "Timeout: recv={}, expected={}",
                recv_completions.borrow().len(),
                num_ops
            );
        }
        std::hint::spin_loop();
    }

    // Verify completions
    let comps = recv_completions.borrow();
    assert_eq!(comps.len(), num_ops, "Should have all recv completions");

    for (imm, entry) in comps.iter() {
        eprintln!("Recv: imm={}, qpn={}", imm, entry.qpn);
        assert_eq!(entry.qpn, qpn1, "QPN should match");
    }

    eprintln!("MonoCq with SRQ test passed!");
}

/// Test MonoCq with UD QP.
///
/// This test verifies that MonoCq works correctly with Unreliable Datagram QPs.
/// UD is connectionless and each message is independently addressed.
#[test]
fn test_mono_cq_with_ud_qp() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    use mlx5::ud::{UdQpConfig, UdQpForMonoCq};
    use mlx5::pd::RemoteUdQpInfo;
    use mlx5::wqe::emit::UdAvIb;
    use mlx5::emit_ud_wqe;
    use mlx5::wqe::WqeFlags;
    use std::cell::RefCell;

    const GRH_SIZE: usize = 40;

    // Track completions
    let completions: Rc<RefCell<Vec<(Cqe, u64)>>> = Rc::new(RefCell::new(Vec::new()));
    let completions_clone = completions.clone();

    let callback = move |cqe: Cqe, entry: u64| {
        eprintln!("UD callback: entry={}, opcode={:?}, syndrome={}", entry, cqe.opcode, cqe.syndrome);
        completions_clone.borrow_mut().push((cqe, entry));
    };

    // Create MonoCq for UD QPs
    let cq = Rc::new(ctx
        .ctx
        .create_mono_cq::<UdQpForMonoCq<u64>, _>(256, callback, &CqConfig::default())
        .expect("Failed to create MonoCq"));

    let qkey: u32 = 0x1BCDEF00;
    let config = UdQpConfig {
        qkey,
        ..Default::default()
    };

    // Create sender UD QP with MonoCq
    let sender = ctx
        .ctx
        .ud_qp_builder::<u64, u64>(&ctx.pd, &config)
        .sq_mono_cq(&cq)
        .rq_mono_cq(&cq)
        .build()
        .expect("Failed to create sender QP");

    // Create receiver UD QP with MonoCq
    let receiver = ctx
        .ctx
        .ud_qp_builder::<u64, u64>(&ctx.pd, &config)
        .sq_mono_cq(&cq)
        .rq_mono_cq(&cq)
        .build()
        .expect("Failed to create receiver QP");

    // Register QPs with MonoCq
    cq.register(&sender);
    cq.register(&receiver);

    // Activate QPs
    sender.borrow_mut().activate(ctx.port, 0).expect("activate sender");
    receiver.borrow_mut().activate(ctx.port, 0).expect("activate receiver");

    eprintln!("Sender QPN: 0x{:x}", sender.borrow().qpn());
    eprintln!("Receiver QPN: 0x{:x}", receiver.borrow().qpn());

    // Allocate buffers
    let mut send_buf = AlignedBuffer::new(4096);
    let recv_buf = AlignedBuffer::new(4096);

    let send_mr = unsafe { ctx.pd.register(send_buf.as_ptr(), send_buf.size(), full_access()) }
        .expect("register send MR");
    let recv_mr = unsafe { ctx.pd.register(recv_buf.as_ptr(), recv_buf.size(), full_access()) }
        .expect("register recv MR");

    // Prepare test data
    let test_data = b"UD MonoCq test data!";
    send_buf.fill_bytes(test_data);

    // Post receive (must include space for GRH)
    let recv_len = 256 + GRH_SIZE as u32;
    receiver
        .borrow()
        .post_recv(1000u64, recv_buf.addr(), recv_len, recv_mr.lkey())
        .expect("post_recv failed");
    receiver.borrow().ring_rq_doorbell();

    // Create Address Handle for receiver
    let remote_info = RemoteUdQpInfo {
        qpn: receiver.borrow().qpn(),
        qkey,
        lid: ctx.port_attr.lid,
    };
    let ah = ctx.pd.create_ah(ctx.port, &remote_info).expect("create AH");

    // Post UD SEND
    {
        let sender_ref = sender.borrow();
        let emit_ctx = sender_ref.emit_ctx().expect("emit_ctx");
        emit_ud_wqe!(&emit_ctx, send {
            av: UdAvIb::new(ah.qpn(), ah.qkey(), ah.dlid()),
            flags: WqeFlags::empty(),
            sge: { addr: send_buf.addr(), len: test_data.len() as u32, lkey: send_mr.lkey() },
            signaled: 42u64,
        }).expect("emit_ud_wqe");
        sender_ref.ring_sq_doorbell();
    }

    // Poll for completions (expecting 2: send and recv)
    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(5);

    while completions.borrow().len() < 2 {
        let count = cq.poll();
        if count > 0 {
            eprintln!("poll returned {}", count);
            cq.flush();
        }

        if start.elapsed() > timeout {
            panic!(
                "Timeout: got {} completions, expected 2",
                completions.borrow().len()
            );
        }
        std::hint::spin_loop();
    }

    // Verify completions
    let comps = completions.borrow();
    assert_eq!(comps.len(), 2, "Should have 2 completions (send + recv)");

    // Find send and recv completions
    let mut found_send = false;
    let mut found_recv = false;
    for (cqe, entry) in comps.iter() {
        if cqe.opcode.is_responder() {
            // Recv completion
            assert_eq!(*entry, 1000, "Recv entry should be 1000");
            found_recv = true;
        } else {
            // Send completion
            assert_eq!(*entry, 42, "Send entry should be 42");
            found_send = true;
        }
        assert_eq!(cqe.syndrome, 0, "CQE should have no error");
    }
    assert!(found_send && found_recv, "Should have both send and recv completions");

    // Verify received data
    let received = recv_buf.read_bytes(GRH_SIZE + test_data.len());
    assert_eq!(&received[GRH_SIZE..], test_data, "UD data mismatch");

    eprintln!("MonoCq with UD QP test passed!");
}
