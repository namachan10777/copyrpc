//! Tests for MonoCq (Monomorphic Completion Queue).
//!
//! MonoCq provides inlined callback dispatch for better performance
//! by eliminating vtable overhead.

mod common;

use std::cell::RefCell;
use std::rc::Rc;

use mlx5::cq::Cqe;
use mlx5::qp::{RcQpConfig, RcQpForMonoCq, RemoteQpInfo};
use mlx5::wqe::{WqeFlags, WqeOpcode};

use common::{full_access, AlignedBuffer, TestContext};

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
    assert!(!mono_cq.as_ptr().is_null(), "MonoCq as_ptr should not be null");

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

    let local_mr =
        unsafe { ctx.pd.register(local_buf.as_ptr(), local_buf.size(), full_access()) }
            .expect("Failed to register local MR");
    let remote_mr =
        unsafe { ctx.pd.register(remote_buf.as_ptr(), remote_buf.size(), full_access()) }
            .expect("Failed to register remote MR");

    let test_data = b"MonoCq test data";
    local_buf.fill_bytes(test_data);
    remote_buf.fill(0);

    // Post RDMA WRITE with entry = 42
    qp1.borrow_mut()
        .wqe_builder(42u64)
        .expect("wqe_builder failed")
        .ctrl(WqeOpcode::RdmaWrite, WqeFlags::COMPLETION, 0)
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

    let local_mr =
        unsafe { ctx.pd.register(local_buf.as_ptr(), local_buf.size(), full_access()) }
            .expect("Failed to register local MR");
    let remote_mr =
        unsafe { ctx.pd.register(remote_buf.as_ptr(), remote_buf.size(), full_access()) }
            .expect("Failed to register remote MR");

    let test_data = b"Multi completion test";
    local_buf.fill_bytes(test_data);

    // Post multiple WQEs
    let num_ops = 5;
    for i in 0..num_ops {
        qp1.borrow_mut()
            .wqe_builder(100 + i as u64)
            .expect("wqe_builder failed")
            .ctrl(WqeOpcode::RdmaWrite, WqeFlags::COMPLETION, 0)
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
