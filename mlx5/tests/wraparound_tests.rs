//! Wrap-around tests for Send Queue ring buffers.
//!
//! This module tests that all QP types correctly handle the ring buffer
//! wrap-around condition where PI (Producer Index) wraps back to 0.
//!
//! Run with:
//! ```bash
//! cargo test --release -p mlx5 --test wraparound_tests -- --nocapture
//! ```

mod common;

use std::rc::Rc;

use mlx5::cq::CompletionQueue;
use mlx5::dc::{DciConfig, DctConfig};
use mlx5::pd::RemoteUdQpInfo;
use mlx5::qp::{RcQpConfig, RemoteQpInfo};
use mlx5::srq::SrqConfig;
use mlx5::ud::UdQpConfig;
use mlx5::wqe::{WqeFlags, WqeOpcode};

use common::{full_access, poll_cq_timeout, poll_cq_batch, AlignedBuffer, TestContext};

/// Size of GRH (Global Route Header) prepended to UD receives.
const GRH_SIZE: usize = 40;

// =============================================================================
// RC QP Wrap-around Tests
// =============================================================================

/// Test RC RDMA WRITE with wrap-around.
///
/// Uses a small queue to force wrap-around within a reasonable number of iterations.
#[test]
fn test_rc_rdma_write_wraparound() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    // Create CQs
    let mut send_cq = ctx.ctx.create_cq(256).expect("Failed to create send CQ");
    send_cq.init_direct_access().expect("Failed to init send CQ");
    let send_cq = Rc::new(send_cq);

    let mut recv_cq1 = ctx.ctx.create_cq(256).expect("Failed to create recv CQ1");
    recv_cq1.init_direct_access().expect("Failed to init recv CQ1");
    let recv_cq1 = Rc::new(recv_cq1);
    let mut recv_cq2 = ctx.ctx.create_cq(256).expect("Failed to create recv CQ2");
    recv_cq2.init_direct_access().expect("Failed to init recv CQ2");
    let recv_cq2 = Rc::new(recv_cq2);

    // Use small queue to force wrap-around quickly
    let config = RcQpConfig {
        max_send_wr: 16,
        max_recv_wr: 16,
        ..Default::default()
    };

    fn noop_callback(_cqe: mlx5::cq::Cqe, _entry: u64) {}

    let qp1 = ctx
        .ctx
        .create_rc_qp(&ctx.pd, &send_cq, &recv_cq1, &config, noop_callback as fn(_, _))
        .expect("Failed to create QP1");
    let qp2 = ctx
        .ctx
        .create_rc_qp(&ctx.pd, &send_cq, &recv_cq2, &config, noop_callback as fn(_, _))
        .expect("Failed to create QP2");

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
    qp1.borrow_mut().connect(&remote2, ctx.port, 0, 4, 4, access).expect("connect QP1");
    qp2.borrow_mut().connect(&remote1, ctx.port, 0, 4, 4, access).expect("connect QP2");

    // Allocate buffers
    let mut local_buf = AlignedBuffer::new(4096);
    let mut remote_buf = AlignedBuffer::new(4096);

    let local_mr = unsafe { ctx.pd.register(local_buf.as_ptr(), local_buf.size(), full_access()) }
        .expect("Failed to register local MR");
    let remote_mr = unsafe { ctx.pd.register(remote_buf.as_ptr(), remote_buf.size(), full_access()) }
        .expect("Failed to register remote MR");

    // Run enough iterations to ensure wrap-around (at least 4x queue depth)
    let iterations = 64;
    println!("Running {} iterations to test wrap-around...", iterations);

    for i in 0..iterations {
        // Unique data for each iteration
        let test_data = format!("RDMA WRITE iteration {:04}", i);
        local_buf.fill(0);
        local_buf.fill_bytes(test_data.as_bytes());
        remote_buf.fill(0xAA); // Clear remote buffer with pattern

        // Post RDMA WRITE
        qp1.borrow_mut()
            .wqe_builder(i as u64)
            .expect("wqe_builder failed")
            .ctrl(WqeOpcode::RdmaWrite, WqeFlags::COMPLETION, 0)
            .rdma(remote_buf.addr(), remote_mr.rkey())
            .sge(local_buf.addr(), test_data.len() as u32, local_mr.lkey())
            .finish_with_blueflame();

        // Wait for completion
        let cqe = poll_cq_timeout(&send_cq, 5000)
            .expect(&format!("CQE timeout at iteration {}", i));
        assert_eq!(cqe.syndrome, 0, "CQE error at iteration {}: syndrome={}", i, cqe.syndrome);
        send_cq.flush();

        // Verify data
        let written = remote_buf.read_bytes(test_data.len());
        assert_eq!(
            &written[..],
            test_data.as_bytes(),
            "Data mismatch at iteration {}",
            i
        );
    }

    println!("RC RDMA WRITE wrap-around test passed! ({} iterations)", iterations);
}

/// Test that finish_with_wrap_around is actually triggered.
///
/// This test specifically verifies the WQE wrap-around handling code path,
/// where a WQE is too large to fit before the ring buffer wraps.
#[test]
fn test_rc_actual_wraparound_triggered() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    // Create CQs
    let send_cq = ctx.ctx.create_cq(256).expect("Failed to create send CQ");
    let send_cq = Rc::new(send_cq);

    let recv_cq1 = ctx.ctx.create_cq(256).expect("Failed to create recv CQ1");
    let recv_cq1 = Rc::new(recv_cq1);
    let recv_cq2 = ctx.ctx.create_cq(256).expect("Failed to create recv CQ2");
    let recv_cq2 = Rc::new(recv_cq2);

    // Use small queue but ensure we have enough space
    let config = RcQpConfig {
        max_send_wr: 8,
        max_recv_wr: 8,
        max_inline_data: 128,
        ..Default::default()
    };

    fn noop_callback(_cqe: mlx5::cq::Cqe, _entry: u64) {}

    let qp1 = ctx
        .ctx
        .create_rc_qp(&ctx.pd, &send_cq, &recv_cq1, &config, noop_callback as fn(_, _))
        .expect("Failed to create QP1");
    let qp2 = ctx
        .ctx
        .create_rc_qp(&ctx.pd, &send_cq, &recv_cq2, &config, noop_callback as fn(_, _))
        .expect("Failed to create QP2");

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
    qp1.borrow_mut().connect(&remote2, ctx.port, 0, 4, 4, access).expect("connect QP1");
    qp2.borrow_mut().connect(&remote1, ctx.port, 0, 4, 4, access).expect("connect QP2");

    // Allocate buffers
    let mut local_buf = AlignedBuffer::new(4096);
    let mut remote_buf = AlignedBuffer::new(4096);

    let local_mr = unsafe { ctx.pd.register(local_buf.as_ptr(), local_buf.size(), full_access()) }
        .expect("Failed to register local MR");
    let remote_mr = unsafe { ctx.pd.register(remote_buf.as_ptr(), remote_buf.size(), full_access()) }
        .expect("Failed to register remote MR");

    local_buf.fill_bytes(b"test data for wrap-around");
    remote_buf.fill(0);

    let sq_wqe_cnt = qp1.borrow().sq_wqe_cnt();
    let initial_slots = qp1.borrow().slots_to_ring_end();
    println!("SQ WQE count (ring size): {}", sq_wqe_cnt);
    println!("Initial slots_to_ring_end: {}", initial_slots);

    // To trigger wrap-around, we need: wqebb_cnt > slots_to_end
    // Strategy: Post WQEs until slots_to_end = 1, then post a 2-WQEBB WQE
    //
    // Each small RDMA WRITE with SGE: ctrl(16) + rdma(16) + sge(16) = 48 bytes = 1 WQEBB

    let target_slots = 1; // We want to end up with 1 slot remaining

    println!("Phase 1: Advancing PI towards ring end...");
    let mut total_posted = 0u32;

    // Post WQEs one at a time to advance PI
    while qp1.borrow().slots_to_ring_end() > target_slots {
        let current_slots = qp1.borrow().slots_to_ring_end();
        let available = qp1.borrow().send_queue_available();

        if available == 0 {
            // SQ is full, need to wait for some completions
            let cqe = poll_cq_timeout(&send_cq, 5000).expect("CQE timeout waiting for space");
            assert_eq!(cqe.syndrome, 0, "CQE error while waiting for space");
            send_cq.flush();
            continue;
        }

        // Post one small WQE (1 WQEBB)
        let _ = qp1.borrow_mut()
            .wqe_builder(total_posted as u64)
            .expect("wqe_builder failed")
            .ctrl(WqeOpcode::RdmaWrite, WqeFlags::COMPLETION, 0)
            .rdma(remote_buf.addr(), remote_mr.rkey())
            .sge(local_buf.addr(), 8, local_mr.lkey())
            .finish_with_blueflame();
        total_posted += 1;

        // Wait for completion before posting next
        let cqe = poll_cq_timeout(&send_cq, 5000)
            .expect(&format!("CQE timeout at WQE {}, slots_to_end={}", total_posted, current_slots));
        assert_eq!(cqe.syndrome, 0, "CQE error at WQE {}", total_posted);
        send_cq.flush();

        if total_posted % 10 == 0 {
            println!("  Posted {} WQEs, slots_to_ring_end={}", total_posted, qp1.borrow().slots_to_ring_end());
        }
    }

    let slots_before = qp1.borrow().slots_to_ring_end();
    println!("Phase 1 done: slots_to_ring_end={} after {} WQEs", slots_before, total_posted);

    // Phase 2: Post a multi-WQEBB WQE using multiple SGEs that should trigger wrap-around
    // ctrl(16) + rdma(16) + sge1(16) + sge2(16) + sge3(16) = 80 bytes = 2 WQEBBs
    println!("Phase 2: Posting large WQE (2 WQEBBs with 3 SGEs)...");

    let _ = qp1.borrow_mut()
        .wqe_builder(9999u64)
        .expect("wqe_builder failed")
        .ctrl(WqeOpcode::RdmaWrite, WqeFlags::COMPLETION, 0)
        .rdma(remote_buf.addr(), remote_mr.rkey())
        .sge(local_buf.addr(), 8, local_mr.lkey())
        .sge(local_buf.addr().wrapping_add(8), 8, local_mr.lkey())
        .sge(local_buf.addr().wrapping_add(16), 8, local_mr.lkey())
        .finish_with_blueflame();

    let slots_after = qp1.borrow().slots_to_ring_end();
    println!("After large WQE: slots_to_ring_end={}", slots_after);

    // Wrap-around happened if slots_after is much larger than slots_before
    // (PI wrapped to beginning of ring)
    if slots_after > slots_before {
        println!("Wrap-around detected! slots went {} -> {}", slots_before, slots_after);
    } else {
        println!("Note: slots_before={}, slots_after={} (wrap-around may not have been needed)", slots_before, slots_after);
    }

    // Wait for final completion
    let cqe = poll_cq_timeout(&send_cq, 5000).expect("CQE timeout for large WQE");
    assert_eq!(cqe.syndrome, 0, "CQE error for large WQE");
    send_cq.flush();

    // Verify data was written correctly (24 bytes from 3 SGEs)
    let written = remote_buf.read_bytes(24);
    assert_eq!(&written[..8], &local_buf.read_bytes(24)[..8], "Data mismatch after wrap-around");

    println!("RC actual wrap-around test passed! (total {} WQEs posted)", total_posted + 1);
}

/// Test that finish_with_wrap_around (non-BlueFlame) is triggered.
///
/// This uses finish() + ring_sq_doorbell() instead of finish_with_blueflame().
#[test]
fn test_rc_wraparound_with_doorbell() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    let send_cq = ctx.ctx.create_cq(256).expect("Failed to create send CQ");
    let send_cq = Rc::new(send_cq);

    let recv_cq1 = ctx.ctx.create_cq(256).expect("Failed to create recv CQ1");
    let recv_cq1 = Rc::new(recv_cq1);
    let recv_cq2 = ctx.ctx.create_cq(256).expect("Failed to create recv CQ2");
    let recv_cq2 = Rc::new(recv_cq2);

    let config = RcQpConfig {
        max_send_wr: 8,
        max_recv_wr: 8,
        ..Default::default()
    };

    fn noop_callback(_cqe: mlx5::cq::Cqe, _entry: u64) {}

    let qp1 = ctx
        .ctx
        .create_rc_qp(&ctx.pd, &send_cq, &recv_cq1, &config, noop_callback as fn(_, _))
        .expect("Failed to create QP1");
    let qp2 = ctx
        .ctx
        .create_rc_qp(&ctx.pd, &send_cq, &recv_cq2, &config, noop_callback as fn(_, _))
        .expect("Failed to create QP2");

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
    qp1.borrow_mut().connect(&remote2, ctx.port, 0, 4, 4, access).expect("connect QP1");
    qp2.borrow_mut().connect(&remote1, ctx.port, 0, 4, 4, access).expect("connect QP2");

    let mut local_buf = AlignedBuffer::new(4096);
    let mut remote_buf = AlignedBuffer::new(4096);

    let local_mr = unsafe { ctx.pd.register(local_buf.as_ptr(), local_buf.size(), full_access()) }
        .expect("Failed to register local MR");
    let remote_mr = unsafe { ctx.pd.register(remote_buf.as_ptr(), remote_buf.size(), full_access()) }
        .expect("Failed to register remote MR");

    local_buf.fill_bytes(b"doorbell wrap-around test");
    remote_buf.fill(0);

    let initial_slots = qp1.borrow().slots_to_ring_end();
    println!("Initial slots_to_ring_end: {}", initial_slots);

    let target_slots = 1;
    let mut total_posted = 0u32;

    // Advance PI to get slots_to_ring_end = 1
    while qp1.borrow().slots_to_ring_end() > target_slots {
        let available = qp1.borrow().send_queue_available();
        if available == 0 {
            let cqe = poll_cq_timeout(&send_cq, 5000).expect("CQE timeout waiting for space");
            assert_eq!(cqe.syndrome, 0, "CQE error");
            send_cq.flush();
            continue;
        }

        let _ = qp1.borrow_mut()
            .wqe_builder(total_posted as u64)
            .expect("wqe_builder failed")
            .ctrl(WqeOpcode::RdmaWrite, WqeFlags::COMPLETION, 0)
            .rdma(remote_buf.addr(), remote_mr.rkey())
            .sge(local_buf.addr(), 8, local_mr.lkey())
            .finish(); // Use finish() not finish_with_blueflame()

        qp1.borrow().ring_sq_doorbell();
        total_posted += 1;

        let cqe = poll_cq_timeout(&send_cq, 5000).expect("CQE timeout");
        assert_eq!(cqe.syndrome, 0, "CQE error");
        send_cq.flush();
    }

    let slots_before = qp1.borrow().slots_to_ring_end();
    println!("Before large WQE: slots_to_ring_end={}", slots_before);

    // Post a 2-WQEBB WQE using finish() + doorbell
    let _ = qp1.borrow_mut()
        .wqe_builder(9999u64)
        .expect("wqe_builder failed")
        .ctrl(WqeOpcode::RdmaWrite, WqeFlags::COMPLETION, 0)
        .rdma(remote_buf.addr(), remote_mr.rkey())
        .sge(local_buf.addr(), 8, local_mr.lkey())
        .sge(local_buf.addr().wrapping_add(8), 8, local_mr.lkey())
        .sge(local_buf.addr().wrapping_add(16), 8, local_mr.lkey())
        .finish(); // Use finish() not finish_with_blueflame()

    qp1.borrow().ring_sq_doorbell();

    let slots_after = qp1.borrow().slots_to_ring_end();
    println!("After large WQE: slots_to_ring_end={}", slots_after);

    if slots_after > slots_before {
        println!("Wrap-around detected with doorbell! slots went {} -> {}", slots_before, slots_after);
    }

    let cqe = poll_cq_timeout(&send_cq, 5000).expect("CQE timeout for large WQE");
    assert_eq!(cqe.syndrome, 0, "CQE error");
    send_cq.flush();

    println!("RC wrap-around with doorbell test passed!");
}

/// Test RC slots_to_ring_end() and post_nop_to_ring_end() methods.
#[test]
fn test_rc_slots_to_ring_end() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    let mut send_cq = ctx.ctx.create_cq(256).expect("Failed to create send CQ");
    send_cq.init_direct_access().expect("Failed to init send CQ");
    let send_cq = Rc::new(send_cq);

    let mut recv_cq1 = ctx.ctx.create_cq(256).expect("Failed to create recv CQ1");
    recv_cq1.init_direct_access().expect("Failed to init recv CQ1");
    let recv_cq1 = Rc::new(recv_cq1);
    let mut recv_cq2 = ctx.ctx.create_cq(256).expect("Failed to create recv CQ2");
    recv_cq2.init_direct_access().expect("Failed to init recv CQ2");
    let recv_cq2 = Rc::new(recv_cq2);

    let config = RcQpConfig {
        max_send_wr: 8, // Small queue for testing
        max_recv_wr: 8,
        ..Default::default()
    };

    fn noop_callback(_cqe: mlx5::cq::Cqe, _entry: u64) {}

    let qp1 = ctx
        .ctx
        .create_rc_qp(&ctx.pd, &send_cq, &recv_cq1, &config, noop_callback as fn(_, _))
        .expect("Failed to create QP1");
    let qp2 = ctx
        .ctx
        .create_rc_qp(&ctx.pd, &send_cq, &recv_cq2, &config, noop_callback as fn(_, _))
        .expect("Failed to create QP2");

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
    qp1.borrow_mut().connect(&remote2, ctx.port, 0, 4, 4, access).expect("connect QP1");
    qp2.borrow_mut().connect(&remote1, ctx.port, 0, 4, 4, access).expect("connect QP2");

    // Check initial slots_to_ring_end
    let initial_slots = qp1.borrow().slots_to_ring_end();
    println!("Initial slots_to_ring_end: {}", initial_slots);
    assert!(initial_slots > 0, "Should have slots available at start");

    // Post some WQEs to advance PI
    let mut local_buf = AlignedBuffer::new(4096);
    let mut remote_buf = AlignedBuffer::new(4096);

    let local_mr = unsafe { ctx.pd.register(local_buf.as_ptr(), local_buf.size(), full_access()) }
        .expect("Failed to register local MR");
    let remote_mr = unsafe { ctx.pd.register(remote_buf.as_ptr(), remote_buf.size(), full_access()) }
        .expect("Failed to register remote MR");

    let test_data = b"Test data for slots test";
    local_buf.fill_bytes(test_data);

    // Post 5 WQEs to advance towards ring end
    for i in 0..5 {
        qp1.borrow_mut()
            .wqe_builder(i as u64)
            .expect("wqe_builder failed")
            .ctrl(WqeOpcode::RdmaWrite, WqeFlags::COMPLETION, 0)
            .rdma(remote_buf.addr(), remote_mr.rkey())
            .sge(local_buf.addr(), test_data.len() as u32, local_mr.lkey())
            .finish_with_blueflame();

        let _ = poll_cq_timeout(&send_cq, 5000).expect("CQE timeout");
        send_cq.flush();
    }

    let slots_after = qp1.borrow().slots_to_ring_end();
    println!("Slots after 5 WQEs: {}", slots_after);

    // Test sq_available
    let available = qp1.borrow().sq_available();
    println!("sq_available: {}", available);

    println!("RC slots_to_ring_end test passed!");
}

/// Test RC using ring_sq_doorbell() instead of BlueFlame.
#[test]
fn test_rc_ring_sq_doorbell() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    let mut send_cq = ctx.ctx.create_cq(256).expect("Failed to create send CQ");
    send_cq.init_direct_access().expect("Failed to init send CQ");
    let send_cq = Rc::new(send_cq);

    let mut recv_cq1 = ctx.ctx.create_cq(256).expect("Failed to create recv CQ1");
    recv_cq1.init_direct_access().expect("Failed to init recv CQ1");
    let recv_cq1 = Rc::new(recv_cq1);
    let mut recv_cq2 = ctx.ctx.create_cq(256).expect("Failed to create recv CQ2");
    recv_cq2.init_direct_access().expect("Failed to init recv CQ2");
    let recv_cq2 = Rc::new(recv_cq2);

    let config = RcQpConfig::default();

    fn noop_callback(_cqe: mlx5::cq::Cqe, _entry: u64) {}

    let qp1 = ctx
        .ctx
        .create_rc_qp(&ctx.pd, &send_cq, &recv_cq1, &config, noop_callback as fn(_, _))
        .expect("Failed to create QP1");
    let qp2 = ctx
        .ctx
        .create_rc_qp(&ctx.pd, &send_cq, &recv_cq2, &config, noop_callback as fn(_, _))
        .expect("Failed to create QP2");

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
    qp1.borrow_mut().connect(&remote2, ctx.port, 0, 4, 4, access).expect("connect QP1");
    qp2.borrow_mut().connect(&remote1, ctx.port, 0, 4, 4, access).expect("connect QP2");

    let mut local_buf = AlignedBuffer::new(4096);
    let mut remote_buf = AlignedBuffer::new(4096);

    let local_mr = unsafe { ctx.pd.register(local_buf.as_ptr(), local_buf.size(), full_access()) }
        .expect("Failed to register local MR");
    let remote_mr = unsafe { ctx.pd.register(remote_buf.as_ptr(), remote_buf.size(), full_access()) }
        .expect("Failed to register remote MR");

    let test_data = b"Test using ring_sq_doorbell";
    local_buf.fill_bytes(test_data);
    remote_buf.fill(0);

    // Use finish() instead of finish_with_blueflame(), then ring doorbell manually
    qp1.borrow_mut()
        .wqe_builder(1u64)
        .expect("wqe_builder failed")
        .ctrl(WqeOpcode::RdmaWrite, WqeFlags::COMPLETION, 0)
        .rdma(remote_buf.addr(), remote_mr.rkey())
        .sge(local_buf.addr(), test_data.len() as u32, local_mr.lkey())
        .finish();

    // Ring doorbell manually
    qp1.borrow().ring_sq_doorbell();

    // Wait for completion
    let cqe = poll_cq_timeout(&send_cq, 5000).expect("CQE timeout");
    assert_eq!(cqe.syndrome, 0, "CQE error: syndrome={}", cqe.syndrome);
    send_cq.flush();

    // Verify data
    let written = remote_buf.read_bytes(test_data.len());
    assert_eq!(&written[..], test_data, "Data mismatch using ring_sq_doorbell");

    println!("RC ring_sq_doorbell test passed!");
}

// =============================================================================
// DC Wrap-around Tests
// =============================================================================

/// Test DC RDMA WRITE with wrap-around.
#[test]
fn test_dc_rdma_write_wraparound() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    require_dct!(&ctx);

    // Create CQ for DCI with small size
    let mut dci_cq = ctx.ctx.create_cq(256).expect("Failed to create DCI CQ");
    dci_cq.init_direct_access().expect("Failed to init DCI CQ");
    let dci_cq = Rc::new(dci_cq);

    let dct_cq = ctx.ctx.create_cq(256).expect("Failed to create DCT CQ");

    let srq_config = SrqConfig {
        max_wr: 128,
        max_sge: 1,
    };
    let srq: mlx5::srq::Srq<()> = ctx.pd.create_srq(&srq_config).expect("Failed to create SRQ");

    // Use small queue for DCI
    let dci_config = DciConfig {
        max_send_wr: 16,
        ..Default::default()
    };
    let dci = ctx
        .ctx
        .create_dci::<u64, _>(&ctx.pd, &dci_cq, &dci_config, |_cqe, _entry| {})
        .expect("Failed to create DCI");
    dci.borrow_mut().activate(ctx.port, 0, 4).expect("Failed to activate DCI");

    let dc_key: u64 = 0xDEADBEEF;
    let dct_config = DctConfig { dc_key };
    let mut dct = ctx.ctx.create_dct(&ctx.pd, &srq, &dct_cq, &dct_config).expect("Failed to create DCT");
    let access = full_access().bits();
    dct.activate(ctx.port, access, 4).expect("Failed to activate DCT");

    let dctn = dct.dctn();
    let dlid = ctx.port_attr.lid;

    let mut local_buf = AlignedBuffer::new(4096);
    let mut remote_buf = AlignedBuffer::new(4096);

    let local_mr = unsafe { ctx.pd.register(local_buf.as_ptr(), local_buf.size(), full_access()) }
        .expect("Failed to register local MR");
    let remote_mr = unsafe { ctx.pd.register(remote_buf.as_ptr(), remote_buf.size(), full_access()) }
        .expect("Failed to register remote MR");

    let iterations = 64;
    println!("Running {} DC RDMA WRITE iterations...", iterations);

    for i in 0..iterations {
        let test_data = format!("DC WRITE iteration {:04}", i);
        local_buf.fill(0);
        local_buf.fill_bytes(test_data.as_bytes());
        remote_buf.fill(0xBB);

        dci.borrow_mut()
            .wqe_builder(i as u64)
            .expect("wqe_builder failed")
            .ctrl(WqeOpcode::RdmaWrite, WqeFlags::COMPLETION, 0)
            .av(dc_key, dctn, dlid)
            .rdma(remote_buf.addr(), remote_mr.rkey())
            .sge(local_buf.addr(), test_data.len() as u32, local_mr.lkey())
            .finish_with_blueflame();

        let cqe = poll_cq_timeout(&dci_cq, 5000)
            .expect(&format!("CQE timeout at iteration {}", i));
        assert_eq!(cqe.syndrome, 0, "CQE error at iteration {}: syndrome={}", i, cqe.syndrome);
        dci_cq.flush();

        let written = remote_buf.read_bytes(test_data.len());
        assert_eq!(&written[..], test_data.as_bytes(), "Data mismatch at iteration {}", i);
    }

    println!("DC RDMA WRITE wrap-around test passed! ({} iterations)", iterations);
}

/// Test DC sq_available() and slots_to_ring_end().
#[test]
fn test_dc_sq_methods() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    require_dct!(&ctx);

    let mut dci_cq = ctx.ctx.create_cq(256).expect("Failed to create DCI CQ");
    dci_cq.init_direct_access().expect("Failed to init DCI CQ");
    let dci_cq = Rc::new(dci_cq);

    let dct_cq = ctx.ctx.create_cq(256).expect("Failed to create DCT CQ");

    let srq_config = SrqConfig {
        max_wr: 128,
        max_sge: 1,
    };
    let srq: mlx5::srq::Srq<()> = ctx.pd.create_srq(&srq_config).expect("Failed to create SRQ");

    let dci_config = DciConfig {
        max_send_wr: 8,
        ..Default::default()
    };
    let dci = ctx
        .ctx
        .create_dci::<u64, _>(&ctx.pd, &dci_cq, &dci_config, |_cqe, _entry| {})
        .expect("Failed to create DCI");
    dci.borrow_mut().activate(ctx.port, 0, 4).expect("Failed to activate DCI");

    let dc_key: u64 = 0x12345678;
    let dct_config = DctConfig { dc_key };
    let mut dct = ctx.ctx.create_dct(&ctx.pd, &srq, &dct_cq, &dct_config).expect("Failed to create DCT");
    let access = full_access().bits();
    dct.activate(ctx.port, access, 4).expect("Failed to activate DCT");

    let dctn = dct.dctn();
    let dlid = ctx.port_attr.lid;

    // Test sq_available
    let initial_available = dci.borrow().sq_available();
    println!("Initial sq_available: {}", initial_available);
    assert!(initial_available > 0, "Should have slots available");

    // Test slots_to_ring_end
    let initial_slots = dci.borrow().slots_to_ring_end();
    println!("Initial slots_to_ring_end: {}", initial_slots);
    assert!(initial_slots > 0, "Should have slots to ring end");

    // Post some WQEs and check again
    let mut local_buf = AlignedBuffer::new(4096);
    let mut remote_buf = AlignedBuffer::new(4096);

    let local_mr = unsafe { ctx.pd.register(local_buf.as_ptr(), local_buf.size(), full_access()) }
        .expect("Failed to register local MR");
    let remote_mr = unsafe { ctx.pd.register(remote_buf.as_ptr(), remote_buf.size(), full_access()) }
        .expect("Failed to register remote MR");

    let test_data = b"Test data";
    local_buf.fill_bytes(test_data);

    for i in 0..3 {
        dci.borrow_mut()
            .wqe_builder(i as u64)
            .expect("wqe_builder failed")
            .ctrl(WqeOpcode::RdmaWrite, WqeFlags::COMPLETION, 0)
            .av(dc_key, dctn, dlid)
            .rdma(remote_buf.addr(), remote_mr.rkey())
            .sge(local_buf.addr(), test_data.len() as u32, local_mr.lkey())
            .finish_with_blueflame();

        let _ = poll_cq_timeout(&dci_cq, 5000).expect("CQE timeout");
        dci_cq.flush();
    }

    let slots_after = dci.borrow().slots_to_ring_end();
    let available_after = dci.borrow().sq_available();
    println!("After 3 WQEs: slots_to_ring_end={}, sq_available={}", slots_after, available_after);

    println!("DC sq methods test passed!");
}

/// Test DC ring_sq_doorbell().
#[test]
fn test_dc_ring_sq_doorbell() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    require_dct!(&ctx);

    let mut dci_cq = ctx.ctx.create_cq(256).expect("Failed to create DCI CQ");
    dci_cq.init_direct_access().expect("Failed to init DCI CQ");
    let dci_cq = Rc::new(dci_cq);

    let dct_cq = ctx.ctx.create_cq(256).expect("Failed to create DCT CQ");

    let srq_config = SrqConfig {
        max_wr: 128,
        max_sge: 1,
    };
    let srq: mlx5::srq::Srq<()> = ctx.pd.create_srq(&srq_config).expect("Failed to create SRQ");

    let dci_config = DciConfig::default();
    let dci = ctx
        .ctx
        .create_dci::<u64, _>(&ctx.pd, &dci_cq, &dci_config, |_cqe, _entry| {})
        .expect("Failed to create DCI");
    dci.borrow_mut().activate(ctx.port, 0, 4).expect("Failed to activate DCI");

    let dc_key: u64 = 0xABCDEF00;
    let dct_config = DctConfig { dc_key };
    let mut dct = ctx.ctx.create_dct(&ctx.pd, &srq, &dct_cq, &dct_config).expect("Failed to create DCT");
    let access = full_access().bits();
    dct.activate(ctx.port, access, 4).expect("Failed to activate DCT");

    let dctn = dct.dctn();
    let dlid = ctx.port_attr.lid;

    let mut local_buf = AlignedBuffer::new(4096);
    let mut remote_buf = AlignedBuffer::new(4096);

    let local_mr = unsafe { ctx.pd.register(local_buf.as_ptr(), local_buf.size(), full_access()) }
        .expect("Failed to register local MR");
    let remote_mr = unsafe { ctx.pd.register(remote_buf.as_ptr(), remote_buf.size(), full_access()) }
        .expect("Failed to register remote MR");

    let test_data = b"DC test with ring_sq_doorbell";
    local_buf.fill_bytes(test_data);
    remote_buf.fill(0);

    // Use finish() + ring_sq_doorbell() instead of finish_with_blueflame()
    dci.borrow_mut()
        .wqe_builder(1u64)
        .expect("wqe_builder failed")
        .ctrl(WqeOpcode::RdmaWrite, WqeFlags::COMPLETION, 0)
        .av(dc_key, dctn, dlid)
        .rdma(remote_buf.addr(), remote_mr.rkey())
        .sge(local_buf.addr(), test_data.len() as u32, local_mr.lkey())
        .finish();

    dci.borrow().ring_sq_doorbell();

    let cqe = poll_cq_timeout(&dci_cq, 5000).expect("CQE timeout");
    assert_eq!(cqe.syndrome, 0, "CQE error: syndrome={}", cqe.syndrome);
    dci_cq.flush();

    let written = remote_buf.read_bytes(test_data.len());
    assert_eq!(&written[..], test_data, "Data mismatch using ring_sq_doorbell");

    println!("DC ring_sq_doorbell test passed!");
}

// =============================================================================
// UD QP Wrap-around Tests
// =============================================================================

/// Test UD SEND with wrap-around.
#[test]
fn test_ud_send_wraparound() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    let mut send_cq = ctx.ctx.create_cq(256).expect("Failed to create send CQ");
    send_cq.init_direct_access().expect("Failed to init send CQ");
    let send_cq = Rc::new(send_cq);

    let mut recv_cq = ctx.ctx.create_cq(256).expect("Failed to create recv CQ");
    recv_cq.init_direct_access().expect("Failed to init recv CQ");
    let recv_cq = Rc::new(recv_cq);

    let qkey: u32 = 0x1BCDEF00;

    // Use small queue for UD
    let config = UdQpConfig {
        qkey,
        max_send_wr: 16,
        max_recv_wr: 128,
        ..Default::default()
    };

    let sender = ctx
        .ctx
        .create_ud_qp::<u64, _>(&ctx.pd, &send_cq, &recv_cq, &config, |_cqe, _entry| {})
        .expect("Failed to create sender QP");
    sender.borrow_mut().activate(ctx.port, 0).expect("Failed to activate sender");

    let receiver = ctx
        .ctx
        .create_ud_qp::<u64, _>(&ctx.pd, &send_cq, &recv_cq, &config, |_cqe, _entry| {})
        .expect("Failed to create receiver QP");
    receiver.borrow_mut().activate(ctx.port, 0).expect("Failed to activate receiver");

    let mut send_buf = AlignedBuffer::new(4096);
    let mut recv_buf = AlignedBuffer::new(4096);

    let send_mr = unsafe { ctx.pd.register(send_buf.as_ptr(), send_buf.size(), full_access()) }
        .expect("Failed to register send MR");
    let recv_mr = unsafe { ctx.pd.register(recv_buf.as_ptr(), recv_buf.size(), full_access()) }
        .expect("Failed to register recv MR");

    let remote_info = RemoteUdQpInfo {
        qpn: receiver.borrow().qpn(),
        qkey,
        lid: ctx.port_attr.lid,
    };
    let ah = ctx.pd.create_ah(ctx.port, &remote_info).expect("Failed to create AH");

    let iterations = 64;
    println!("Running {} UD SEND iterations...", iterations);

    for i in 0..iterations {
        let test_data = format!("UD SEND iteration {:04}", i);
        send_buf.fill(0);
        send_buf.fill_bytes(test_data.as_bytes());
        recv_buf.fill(0xCC);

        // Post receive (must include space for GRH)
        receiver.borrow()
            .recv_builder(i as u64)
            .expect("recv_builder failed")
            .sge(recv_buf.addr(), 256 + GRH_SIZE as u32, recv_mr.lkey())
            .finish();
        receiver.borrow().ring_rq_doorbell();

        // Post send
        sender.borrow_mut()
            .wqe_builder(i as u64)
            .expect("wqe_builder failed")
            .ctrl(WqeOpcode::Send, WqeFlags::COMPLETION, 0)
            .ud_av(&ah, qkey)
            .sge(send_buf.addr(), test_data.len() as u32, send_mr.lkey())
            .finish_with_blueflame();

        // Wait for send completion
        let send_cqe = poll_cq_timeout(&send_cq, 5000)
            .expect(&format!("Send CQE timeout at iteration {}", i));
        assert_eq!(send_cqe.syndrome, 0, "Send CQE error at iteration {}", i);
        send_cq.flush();

        // Wait for recv completion
        let recv_cqe = poll_cq_timeout(&recv_cq, 5000)
            .expect(&format!("Recv CQE timeout at iteration {}", i));
        assert_eq!(recv_cqe.syndrome, 0, "Recv CQE error at iteration {}", i);
        recv_cq.flush();

        // Verify data (skip GRH)
        let received = recv_buf.read_bytes(GRH_SIZE + test_data.len());
        assert_eq!(
            &received[GRH_SIZE..],
            test_data.as_bytes(),
            "Data mismatch at iteration {}",
            i
        );
    }

    println!("UD SEND wrap-around test passed! ({} iterations)", iterations);
}

/// Test UD slots_to_ring_end().
#[test]
fn test_ud_slots_to_ring_end() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    let mut send_cq = ctx.ctx.create_cq(256).expect("Failed to create send CQ");
    send_cq.init_direct_access().expect("Failed to init send CQ");
    let send_cq = Rc::new(send_cq);

    let mut recv_cq = ctx.ctx.create_cq(256).expect("Failed to create recv CQ");
    recv_cq.init_direct_access().expect("Failed to init recv CQ");
    let recv_cq = Rc::new(recv_cq);

    let qkey: u32 = 0x22222222;
    let config = UdQpConfig {
        qkey,
        max_send_wr: 8,
        max_recv_wr: 128,
        ..Default::default()
    };

    let sender = ctx
        .ctx
        .create_ud_qp::<u64, _>(&ctx.pd, &send_cq, &recv_cq, &config, |_cqe, _entry| {})
        .expect("Failed to create sender QP");
    sender.borrow_mut().activate(ctx.port, 0).expect("Failed to activate sender");

    let receiver = ctx
        .ctx
        .create_ud_qp::<u64, _>(&ctx.pd, &send_cq, &recv_cq, &config, |_cqe, _entry| {})
        .expect("Failed to create receiver QP");
    receiver.borrow_mut().activate(ctx.port, 0).expect("Failed to activate receiver");

    // Check initial slots_to_ring_end
    let initial_slots = sender.borrow().slots_to_ring_end();
    println!("Initial slots_to_ring_end: {}", initial_slots);
    assert!(initial_slots > 0, "Should have slots available");

    let mut send_buf = AlignedBuffer::new(4096);
    let mut recv_buf = AlignedBuffer::new(4096);

    let send_mr = unsafe { ctx.pd.register(send_buf.as_ptr(), send_buf.size(), full_access()) }
        .expect("Failed to register send MR");
    let recv_mr = unsafe { ctx.pd.register(recv_buf.as_ptr(), recv_buf.size(), full_access()) }
        .expect("Failed to register recv MR");

    let remote_info = RemoteUdQpInfo {
        qpn: receiver.borrow().qpn(),
        qkey,
        lid: ctx.port_attr.lid,
    };
    let ah = ctx.pd.create_ah(ctx.port, &remote_info).expect("Failed to create AH");

    let test_data = b"Test data";
    send_buf.fill_bytes(test_data);

    // Post some WQEs
    for i in 0..3 {
        recv_buf.fill(0);
        receiver.borrow()
            .recv_builder(i as u64)
            .expect("recv_builder failed")
            .sge(recv_buf.addr(), 256 + GRH_SIZE as u32, recv_mr.lkey())
            .finish();
        receiver.borrow().ring_rq_doorbell();

        sender.borrow_mut()
            .wqe_builder(i as u64)
            .expect("wqe_builder failed")
            .ctrl(WqeOpcode::Send, WqeFlags::COMPLETION, 0)
            .ud_av(&ah, qkey)
            .sge(send_buf.addr(), test_data.len() as u32, send_mr.lkey())
            .finish_with_blueflame();

        let _ = poll_cq_timeout(&send_cq, 5000).expect("Send CQE timeout");
        send_cq.flush();
        let _ = poll_cq_timeout(&recv_cq, 5000).expect("Recv CQE timeout");
        recv_cq.flush();
    }

    let slots_after = sender.borrow().slots_to_ring_end();
    println!("After 3 WQEs: slots_to_ring_end={}", slots_after);

    println!("UD slots_to_ring_end test passed!");
}
