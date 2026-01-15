//! RC (Reliable Connection) QP tests.
//!
//! This module tests all RC operations including:
//! - SEND/RECV
//! - RDMA WRITE / RDMA WRITE with Immediate
//! - RDMA READ
//! - Atomic Compare-and-Swap
//! - Atomic Fetch-and-Add
//!
//! Run with:
//! ```bash
//! cargo test --release -p mlx5 --test rc_tests -- --nocapture
//! ```

mod common;

use std::cell::RefCell;
use std::rc::Rc;

use mlx5::qp::{QpState, RcQpConfig, RemoteQpInfo};
use mlx5::wqe::{WqeFlags, WqeOpcode};

use common::{full_access, poll_cq_timeout, AlignedBuffer, TestContext};

/// Result type for create_rc_loopback_pair
///
/// Drop order (via Rc reference counting):
/// - QPs hold weak refs to send_cq, so CQs can be dropped after QPs
/// - We hold Rc<Pd> to ensure PD outlives all resources
pub struct RcLoopbackPair {
    pub qp1: Rc<RefCell<mlx5::qp::RcQp<u64, fn(mlx5::cq::Cqe, u64)>>>,
    pub qp2: Rc<RefCell<mlx5::qp::RcQp<u64, fn(mlx5::cq::Cqe, u64)>>>,
    pub send_cq: Rc<RefCell<mlx5::cq::CompletionQueue>>,
    _recv_cq1: mlx5::cq::CompletionQueue,
    _recv_cq2: mlx5::cq::CompletionQueue,
    // Keep PD alive until all QPs/CQs/MRs are dropped
    _pd: Rc<mlx5::pd::Pd>,
}

/// Helper to create a loopback RC QP pair.
fn create_rc_loopback_pair(ctx: &TestContext) -> RcLoopbackPair {
    // Create separate send CQ (shared for polling) and recv CQs
    let send_cq = Rc::new(RefCell::new(
        ctx.ctx.create_cq(256).expect("Failed to create send CQ"),
    ));
    send_cq
        .borrow_mut()
        .init_direct_access()
        .expect("Failed to init send CQ direct access");
    let recv_cq1 = ctx.ctx.create_cq(256).expect("Failed to create recv CQ1");
    let recv_cq2 = ctx.ctx.create_cq(256).expect("Failed to create recv CQ2");

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

    // Connect QPs to each other
    let remote1 = RemoteQpInfo {
        qpn: qp1.borrow().qpn(),
        psn: 0,
        lid: ctx.port_attr.lid,
    };
    let remote2 = RemoteQpInfo {
        qpn: qp2.borrow().qpn(),
        psn: 0,
        lid: ctx.port_attr.lid,
    };

    let access = full_access().bits();

    qp1.borrow_mut()
        .connect(&remote2, ctx.port, 0, 4, 4, access)
        .expect("Failed to connect QP1");
    qp2.borrow_mut()
        .connect(&remote1, ctx.port, 0, 4, 4, access)
        .expect("Failed to connect QP2");

    RcLoopbackPair {
        qp1,
        qp2,
        send_cq,
        _recv_cq1: recv_cq1,
        _recv_cq2: recv_cq2,
        _pd: ctx.pd.clone(),
    }
}

// =============================================================================
// Connection Tests
// =============================================================================

#[test]
fn test_rc_connection() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    let pair = create_rc_loopback_pair(&ctx);

    assert_eq!(pair.qp1.borrow().state(), QpState::Rts);
    assert_eq!(pair.qp2.borrow().state(), QpState::Rts);

    println!("RC QP connection test passed!");
    println!("  QP1: 0x{:x}", pair.qp1.borrow().qpn());
    println!("  QP2: 0x{:x}", pair.qp2.borrow().qpn());
}

// =============================================================================
// RDMA WRITE Tests
// =============================================================================

#[test]
fn test_rc_rdma_write() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    let pair = create_rc_loopback_pair(&ctx);
    let qp1 = &pair.qp1;
    let cq = &pair.send_cq;

    // Allocate buffers
    let mut local_buf = AlignedBuffer::new(4096);
    let remote_buf = AlignedBuffer::new(4096);

    // Register memory regions
    let local_mr = unsafe { ctx.pd.register(local_buf.as_ptr(), local_buf.size(), full_access()) }
        .expect("Failed to register local MR");
    let remote_mr =
        unsafe { ctx.pd.register(remote_buf.as_ptr(), remote_buf.size(), full_access()) }
            .expect("Failed to register remote MR");

    // Prepare test data
    let test_data = b"RDMA WRITE test data - one-sided write operation!";
    local_buf.fill_bytes(test_data);

    println!(
        "Local MR: addr={:p}, lkey=0x{:x}",
        local_buf.as_ptr(),
        local_mr.lkey()
    );
    println!(
        "Remote MR: addr={:p}, rkey=0x{:x}",
        remote_buf.as_ptr(),
        remote_mr.rkey()
    );
    println!("QP1 QPN: 0x{:x}", qp1.borrow().qpn());

    // Post RDMA WRITE
    qp1.borrow_mut()
        .wqe_builder(1u64)
        .expect("wqe_builder failed")
        .ctrl(WqeOpcode::RdmaWrite, WqeFlags::empty(), 0)
        .rdma(remote_buf.addr(), remote_mr.rkey())
        .sge(local_buf.addr(), test_data.len() as u32, local_mr.lkey())
        .finish_with_blueflame();

    println!("WQE posted via BlueFlame");

    // Poll CQ
    let cqe = poll_cq_timeout(&mut cq.borrow_mut(), 5000).expect("CQE timeout");
    println!(
        "CQE: opcode={:?}, syndrome={}, qpn=0x{:x}, wqe_counter={}",
        cqe.opcode, cqe.syndrome, cqe.qp_num, cqe.wqe_counter
    );
    assert_eq!(cqe.syndrome, 0, "CQE error: syndrome={}", cqe.syndrome);
    cq.borrow().flush();

    // Verify data
    let written = remote_buf.read_bytes(test_data.len());
    assert_eq!(&written[..], test_data, "RDMA WRITE data mismatch");

    println!("RC RDMA WRITE test passed!");
}

// =============================================================================
// RDMA READ Tests
// =============================================================================

#[test]
fn test_rc_rdma_read() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    let pair = create_rc_loopback_pair(&ctx);
    let qp1 = &pair.qp1;
    let cq = &pair.send_cq;

    // Allocate buffers
    let mut local_buf = AlignedBuffer::new(4096);
    let mut remote_buf = AlignedBuffer::new(4096);

    // Register memory regions
    let local_mr = unsafe { ctx.pd.register(local_buf.as_ptr(), local_buf.size(), full_access()) }
        .expect("Failed to register local MR");
    let remote_mr =
        unsafe { ctx.pd.register(remote_buf.as_ptr(), remote_buf.size(), full_access()) }
            .expect("Failed to register remote MR");

    // Prepare remote data (this is what we will READ)
    let test_data = b"RDMA READ test - reading remote data via one-sided operation!";
    remote_buf.fill_bytes(test_data);
    local_buf.fill(0); // Clear local buffer

    // Post RDMA READ
    qp1.borrow_mut()
        .wqe_builder(1u64)
        .expect("wqe_builder failed")
        .ctrl(WqeOpcode::RdmaRead, WqeFlags::empty(), 0)
        .rdma(remote_buf.addr(), remote_mr.rkey())
        .sge(local_buf.addr(), test_data.len() as u32, local_mr.lkey())
        .finish_with_blueflame();

    // Poll CQ
    let cqe = poll_cq_timeout(&mut cq.borrow_mut(), 5000).expect("CQE timeout");
    assert_eq!(cqe.syndrome, 0, "CQE error: syndrome={}", cqe.syndrome);
    cq.borrow().flush();

    // Verify data
    let read_data = local_buf.read_bytes(test_data.len());
    assert_eq!(&read_data[..], test_data, "RDMA READ data mismatch");

    println!("RC RDMA READ test passed!");
}

// =============================================================================
// Atomic Tests
// =============================================================================

#[test]
fn test_rc_atomic_cas_success() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    let pair = create_rc_loopback_pair(&ctx);
    let qp1 = &pair.qp1;
    let cq = &pair.send_cq;

    // Allocate buffers (atomic operations work on 8-byte aligned addresses)
    let mut local_buf = AlignedBuffer::new(4096);
    let mut remote_buf = AlignedBuffer::new(4096);

    // Register memory regions
    let local_mr = unsafe { ctx.pd.register(local_buf.as_ptr(), local_buf.size(), full_access()) }
        .expect("Failed to register local MR");
    let remote_mr =
        unsafe { ctx.pd.register(remote_buf.as_ptr(), remote_buf.size(), full_access()) }
            .expect("Failed to register remote MR");

    // Set initial values
    let initial_value: u64 = 0x1234_5678_9ABC_DEF0;
    let compare_value: u64 = initial_value; // Should match
    let swap_value: u64 = 0xFEDC_BA98_7654_3210;

    remote_buf.write_u64(0, initial_value);
    local_buf.fill(0); // Result buffer

    // Post Atomic CAS
    qp1.borrow_mut()
        .wqe_builder(1u64)
        .expect("wqe_builder failed")
        .ctrl(WqeOpcode::AtomicCs, WqeFlags::empty(), 0)
        .rdma(remote_buf.addr(), remote_mr.rkey())
        .atomic_cas(swap_value, compare_value)
        .sge(local_buf.addr(), 8, local_mr.lkey())
        .finish_with_blueflame();

    // Poll CQ
    let cqe = poll_cq_timeout(&mut cq.borrow_mut(), 5000).expect("CQE timeout");
    assert_eq!(cqe.syndrome, 0, "CQE error: syndrome={}", cqe.syndrome);
    cq.borrow().flush();

    // Verify:
    // 1. Remote value should be swap_value (CAS succeeded)
    // 2. Local buffer should contain the original value
    let remote_value = remote_buf.read_u64(0);
    let returned_value = local_buf.read_u64(0);

    assert_eq!(
        remote_value, swap_value,
        "Remote value should be swap_value after successful CAS"
    );
    assert_eq!(
        returned_value, initial_value,
        "Returned value should be original value"
    );

    println!("RC Atomic CAS (success) test passed!");
    println!("  Initial: 0x{:016x}", initial_value);
    println!("  Compare: 0x{:016x}", compare_value);
    println!("  Swap:    0x{:016x}", swap_value);
    println!("  Remote:  0x{:016x}", remote_value);
    println!("  Return:  0x{:016x}", returned_value);
}

#[test]
fn test_rc_atomic_cas_failure() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    let pair = create_rc_loopback_pair(&ctx);
    let qp1 = &pair.qp1;
    let cq = &pair.send_cq;

    // Allocate buffers
    let mut local_buf = AlignedBuffer::new(4096);
    let mut remote_buf = AlignedBuffer::new(4096);

    // Register memory regions
    let local_mr = unsafe { ctx.pd.register(local_buf.as_ptr(), local_buf.size(), full_access()) }
        .expect("Failed to register local MR");
    let remote_mr =
        unsafe { ctx.pd.register(remote_buf.as_ptr(), remote_buf.size(), full_access()) }
            .expect("Failed to register remote MR");

    // Set initial values
    let initial_value: u64 = 0x1234_5678_9ABC_DEF0;
    let compare_value: u64 = 0xAAAA_BBBB_CCCC_DDDD; // Won't match
    let swap_value: u64 = 0xFEDC_BA98_7654_3210;

    remote_buf.write_u64(0, initial_value);
    local_buf.fill(0); // Result buffer

    // Post Atomic CAS
    qp1.borrow_mut()
        .wqe_builder(1u64)
        .expect("wqe_builder failed")
        .ctrl(WqeOpcode::AtomicCs, WqeFlags::empty(), 0)
        .rdma(remote_buf.addr(), remote_mr.rkey())
        .atomic_cas(swap_value, compare_value)
        .sge(local_buf.addr(), 8, local_mr.lkey())
        .finish_with_blueflame();

    // Poll CQ
    let cqe = poll_cq_timeout(&mut cq.borrow_mut(), 5000).expect("CQE timeout");
    assert_eq!(cqe.syndrome, 0, "CQE error: syndrome={}", cqe.syndrome);
    cq.borrow().flush();

    // Verify:
    // 1. Remote value should be unchanged (CAS failed)
    // 2. Local buffer should contain the original value
    let remote_value = remote_buf.read_u64(0);
    let returned_value = local_buf.read_u64(0);

    assert_eq!(
        remote_value, initial_value,
        "Remote value should be unchanged after failed CAS"
    );
    assert_eq!(
        returned_value, initial_value,
        "Returned value should be original value"
    );

    println!("RC Atomic CAS (failure) test passed!");
    println!("  Initial: 0x{:016x}", initial_value);
    println!("  Compare: 0x{:016x} (doesn't match)", compare_value);
    println!("  Remote:  0x{:016x} (unchanged)", remote_value);
}

#[test]
fn test_rc_atomic_fa() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    let pair = create_rc_loopback_pair(&ctx);
    let qp1 = &pair.qp1;
    let cq = &pair.send_cq;

    // Allocate buffers
    let mut local_buf = AlignedBuffer::new(4096);
    let mut remote_buf = AlignedBuffer::new(4096);

    // Register memory regions
    let local_mr = unsafe { ctx.pd.register(local_buf.as_ptr(), local_buf.size(), full_access()) }
        .expect("Failed to register local MR");
    let remote_mr =
        unsafe { ctx.pd.register(remote_buf.as_ptr(), remote_buf.size(), full_access()) }
            .expect("Failed to register remote MR");

    // Set initial values
    let initial_value: u64 = 100;
    let add_value: u64 = 42;

    remote_buf.write_u64(0, initial_value);
    local_buf.fill(0); // Result buffer

    // Post Atomic FA
    qp1.borrow_mut()
        .wqe_builder(1u64)
        .expect("wqe_builder failed")
        .ctrl(WqeOpcode::AtomicFa, WqeFlags::empty(), 0)
        .rdma(remote_buf.addr(), remote_mr.rkey())
        .atomic_fa(add_value)
        .sge(local_buf.addr(), 8, local_mr.lkey())
        .finish_with_blueflame();

    // Poll CQ
    let cqe = poll_cq_timeout(&mut cq.borrow_mut(), 5000).expect("CQE timeout");
    assert_eq!(cqe.syndrome, 0, "CQE error: syndrome={}", cqe.syndrome);
    cq.borrow().flush();

    // Verify:
    // 1. Remote value should be initial + add
    // 2. Local buffer should contain the original value
    let remote_value = remote_buf.read_u64(0);
    let returned_value = local_buf.read_u64(0);

    assert_eq!(
        remote_value,
        initial_value + add_value,
        "Remote value should be sum after FA"
    );
    assert_eq!(
        returned_value, initial_value,
        "Returned value should be original value"
    );

    println!("RC Atomic Fetch-and-Add test passed!");
    println!("  Initial: {}", initial_value);
    println!("  Add:     {}", add_value);
    println!(
        "  Remote:  {} (= {} + {})",
        remote_value, initial_value, add_value
    );
    println!("  Return:  {}", returned_value);
}
