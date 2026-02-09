//! RELAXED_ORDERING and FENCE flag tests.
//!
//! This module tests:
//! - FENCE flag behavior (ensures prior RDMA READ operations complete)
//! - RELAXED_ORDERING flag for WQE and MR
//!
//! Note: Observing actual reordering is very difficult due to hardware/driver
//! optimizations. The primary goal is to verify flags work correctly without errors.
//!
//! Run with:
//! ```bash
//! cargo test --release -p mlx5 --test relaxed_ordering_tests -- --nocapture
//! ```

mod common;

use std::rc::Rc;

use mlx5::cq::CqConfig;
use mlx5::emit_wqe;
use mlx5::pd::AccessFlags;
use mlx5::qp::{RcQpConfig, RcQpIb};
use mlx5::transport::IbRemoteQpInfo;
use mlx5::wqe::WqeFlags;

use common::{AlignedBuffer, TestContext, full_access, poll_cq_batch};

/// Callback type alias for tests
type TestCallback = fn(mlx5::cq::Cqe, u64);

/// Result type for create_rc_loopback_pair
pub struct RcLoopbackPair {
    pub qp1: Rc<std::cell::RefCell<RcQpIb<u64, u64, TestCallback, TestCallback>>>,
    pub qp2: Rc<std::cell::RefCell<RcQpIb<u64, u64, TestCallback, TestCallback>>>,
    pub send_cq: Rc<mlx5::cq::Cq>,
    _recv_cq1: Rc<mlx5::cq::Cq>,
    _recv_cq2: Rc<mlx5::cq::Cq>,
    _pd: mlx5::pd::Pd,
}

/// Helper to create a loopback RC QP pair.
fn create_rc_loopback_pair(ctx: &TestContext) -> RcLoopbackPair {
    let send_cq = ctx
        .ctx
        .create_cq(256, &CqConfig::default())
        .expect("Failed to create send CQ");
    let send_cq = Rc::new(send_cq);
    let recv_cq1 = ctx
        .ctx
        .create_cq(256, &CqConfig::default())
        .expect("Failed to create recv CQ1");
    let recv_cq1 = Rc::new(recv_cq1);
    let recv_cq2 = ctx
        .ctx
        .create_cq(256, &CqConfig::default())
        .expect("Failed to create recv CQ2");
    let recv_cq2 = Rc::new(recv_cq2);

    let config = RcQpConfig::default();

    fn noop_sq_callback(_cqe: mlx5::cq::Cqe, _entry: u64) {}
    fn noop_rq_callback(_cqe: mlx5::cq::Cqe, _entry: u64) {}

    let qp1 = ctx
        .ctx
        .rc_qp_builder::<u64, u64>(&ctx.pd, &config)
        .sq_cq(send_cq.clone(), noop_sq_callback as TestCallback)
        .rq_cq(recv_cq1.clone(), noop_rq_callback as TestCallback)
        .build()
        .expect("Failed to create QP1");

    let qp2 = ctx
        .ctx
        .rc_qp_builder::<u64, u64>(&ctx.pd, &config)
        .sq_cq(send_cq.clone(), noop_sq_callback as TestCallback)
        .rq_cq(recv_cq2.clone(), noop_rq_callback as TestCallback)
        .build()
        .expect("Failed to create QP2");

    // Connect QPs to each other
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
        _pd: ctx.ctx.alloc_pd().expect("Failed to allocate PD for pair"),
    }
}

/// Test FENCE flag with READ-WRITE pattern.
///
/// FENCE ensures that prior RDMA READ operations complete before subsequent
/// operations begin. Pattern: READ → WRITE(with FENCE)
///
/// This test performs:
/// 1. Multiple iterations of READ followed by WRITE with FENCE
/// 2. Verifies that values read are consistent (FENCE ensures ordering)
#[test]
fn test_fence_ordering() {
    let ctx = match TestContext::new() {
        Some(c) => c,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    println!("Creating loopback QP pair...");
    let pair = create_rc_loopback_pair(&ctx);

    // Create buffers
    let mut remote_buf = AlignedBuffer::new(4096); // Remote buffer (counter)
    let mut local_read_buf = AlignedBuffer::new(4096); // Local buffer for READ
    let mut local_write_buf = AlignedBuffer::new(4096); // Local buffer for WRITE source

    // Register MRs
    let access = full_access();
    let remote_mr = unsafe {
        ctx.pd
            .register(remote_buf.as_ptr(), remote_buf.size(), access)
            .expect("Failed to register remote MR")
    };
    let local_read_mr = unsafe {
        ctx.pd
            .register(local_read_buf.as_ptr(), local_read_buf.size(), access)
            .expect("Failed to register local read MR")
    };
    let local_write_mr = unsafe {
        ctx.pd
            .register(local_write_buf.as_ptr(), local_write_buf.size(), access)
            .expect("Failed to register local write MR")
    };

    // Initialize remote counter to 0
    remote_buf.write_u64(0, 0);

    const N: u64 = 100;
    let mut read_values = Vec::with_capacity(N as usize);

    println!("Running FENCE ordering test with {} iterations...", N);

    for i in 1..=N {
        // Prepare write value
        local_write_buf.write_u64(0, i);

        // Clear read buffer
        local_read_buf.write_u64(0, 0xDEADBEEF);

        {
            let qp_ref = pair.qp1.borrow();
            let ctx = qp_ref.emit_ctx().expect("emit_ctx failed");

            // First: RDMA READ (read current counter value)
            emit_wqe!(&ctx, read {
                flags: WqeFlags::empty(),
                remote_addr: remote_buf.addr(),
                rkey: remote_mr.rkey(),
                sge: { addr: local_read_buf.addr(), len: 8, lkey: local_read_mr.lkey() },
                signaled: i * 2 - 1,
            })
            .expect("emit_wqe failed");

            qp_ref.ring_sq_doorbell();
        }

        // Wait for READ completion
        poll_cq_batch(&pair.send_cq, 1, 5000).expect("READ timeout");

        // Record the value we read
        let read_val = local_read_buf.read_u64(0);
        read_values.push(read_val);

        {
            let qp_ref = pair.qp1.borrow();
            let ctx = qp_ref.emit_ctx().expect("emit_ctx failed");

            // Second: RDMA WRITE with FENCE (ensures READ completed before this WRITE starts)
            // FENCE waits for prior READ to complete
            emit_wqe!(&ctx, write {
                flags: WqeFlags::FENCE,
                remote_addr: remote_buf.addr(),
                rkey: remote_mr.rkey(),
                sge: { addr: local_write_buf.addr(), len: 8, lkey: local_write_mr.lkey() },
                signaled: i * 2,
            })
            .expect("emit_wqe failed");

            qp_ref.ring_sq_doorbell();
        }

        // Wait for WRITE completion
        poll_cq_batch(&pair.send_cq, 1, 5000).expect("WRITE timeout");
    }

    // Verify: read values should be monotonically increasing (0, 1, 2, ... N-1)
    println!(
        "Read values: {:?}",
        &read_values[..10.min(read_values.len())]
    );

    for (idx, &val) in read_values.iter().enumerate() {
        assert_eq!(
            val, idx as u64,
            "Expected {} at iteration {}, got {}",
            idx, idx, val
        );
    }

    println!("FENCE ordering test passed: all {} values correct", N);
}

/// Test RELAXED_ORDERING flag with various MR/WQE combinations.
///
/// Tests 4 variants:
/// - Variant 0: No relaxed ordering (both MR and WQE)
/// - Variant 1: MR RELAXED_ORDERING only
/// - Variant 2: WQE RELAXED_ORDERING only
/// - Variant 3: Both MR and WQE RELAXED_ORDERING
///
/// Note: Actual reordering is extremely difficult to observe. This test
/// primarily verifies that the flags work without errors.
#[test]
fn test_relaxed_ordering_variants() {
    let ctx = match TestContext::new() {
        Some(c) => c,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    println!("Creating loopback QP pair...");
    let pair = create_rc_loopback_pair(&ctx);

    let normal_access = full_access();
    let relaxed_access = full_access() | AccessFlags::RELAXED_ORDERING;

    // For each variant, create buffers and run tests
    for variant in 0..4 {
        let mr_relaxed = (variant & 1) != 0;
        let wqe_relaxed = (variant & 2) != 0;

        println!(
            "\nVariant {}: MR_RELAXED={}, WQE_RELAXED={}",
            variant, mr_relaxed, wqe_relaxed
        );

        let mut remote_buf = AlignedBuffer::new(4096);
        let mut local_read_buf = AlignedBuffer::new(4096);
        let mut local_write_buf = AlignedBuffer::new(4096);

        let mr_access = if mr_relaxed {
            relaxed_access
        } else {
            normal_access
        };

        let remote_mr = unsafe {
            ctx.pd
                .register(remote_buf.as_ptr(), remote_buf.size(), mr_access)
                .expect("Failed to register remote MR")
        };
        let local_read_mr = unsafe {
            ctx.pd
                .register(local_read_buf.as_ptr(), local_read_buf.size(), mr_access)
                .expect("Failed to register local read MR")
        };
        let local_write_mr = unsafe {
            ctx.pd
                .register(local_write_buf.as_ptr(), local_write_buf.size(), mr_access)
                .expect("Failed to register local write MR")
        };

        let wqe_flags = if wqe_relaxed {
            WqeFlags::RELAXED_ORDERING
        } else {
            WqeFlags::empty()
        };

        // Initialize
        remote_buf.write_u64(0, 0);

        const N: u64 = 50;

        for i in 1..=N {
            local_write_buf.write_u64(0, i);
            local_read_buf.write_u64(0, 0);

            {
                let qp_ref = pair.qp1.borrow();
                let ctx = qp_ref.emit_ctx().expect("emit_ctx failed");

                // RDMA WRITE
                emit_wqe!(&ctx, write {
                    flags: wqe_flags,
                    remote_addr: remote_buf.addr(),
                    rkey: remote_mr.rkey(),
                    sge: { addr: local_write_buf.addr(), len: 8, lkey: local_write_mr.lkey() },
                    signaled: i * 2 - 1,
                })
                .expect("emit_wqe failed");

                qp_ref.ring_sq_doorbell();
            }

            poll_cq_batch(&pair.send_cq, 1, 5000).expect("WRITE timeout");

            {
                let qp_ref = pair.qp1.borrow();
                let ctx = qp_ref.emit_ctx().expect("emit_ctx failed");

                // RDMA READ
                emit_wqe!(&ctx, read {
                    flags: wqe_flags,
                    remote_addr: remote_buf.addr(),
                    rkey: remote_mr.rkey(),
                    sge: { addr: local_read_buf.addr(), len: 8, lkey: local_read_mr.lkey() },
                    signaled: i * 2,
                })
                .expect("emit_wqe failed");

                qp_ref.ring_sq_doorbell();
            }

            poll_cq_batch(&pair.send_cq, 1, 5000).expect("READ timeout");

            let read_val = local_read_buf.read_u64(0);
            assert_eq!(
                read_val, i,
                "Variant {}: Expected {} but got {}",
                variant, i, read_val
            );
        }

        println!(
            "Variant {} completed successfully ({} iterations)",
            variant, N
        );
    }

    println!("\nAll RELAXED_ORDERING variants passed!");
}

/// Test basic RELAXED_ORDERING with FENCE.
///
/// Uses RELAXED_ORDERING MR combined with FENCE flag to ensure
/// correct ordering even with relaxed memory ordering.
#[test]
fn test_fence_with_relaxed_mr() {
    let ctx = match TestContext::new() {
        Some(c) => c,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    println!("Creating loopback QP pair...");
    let pair = create_rc_loopback_pair(&ctx);

    let relaxed_access = full_access() | AccessFlags::RELAXED_ORDERING;

    let mut remote_buf = AlignedBuffer::new(4096);
    let mut local_read_buf = AlignedBuffer::new(4096);
    let mut local_write_buf = AlignedBuffer::new(4096);

    let remote_mr = unsafe {
        ctx.pd
            .register(remote_buf.as_ptr(), remote_buf.size(), relaxed_access)
            .expect("Failed to register remote MR")
    };
    let local_read_mr = unsafe {
        ctx.pd
            .register(
                local_read_buf.as_ptr(),
                local_read_buf.size(),
                relaxed_access,
            )
            .expect("Failed to register local read MR")
    };
    let local_write_mr = unsafe {
        ctx.pd
            .register(
                local_write_buf.as_ptr(),
                local_write_buf.size(),
                relaxed_access,
            )
            .expect("Failed to register local write MR")
    };

    remote_buf.write_u64(0, 0);

    const N: u64 = 100;
    let mut read_values = Vec::with_capacity(N as usize);

    println!(
        "Running FENCE with RELAXED_ORDERING MR test ({} iterations)...",
        N
    );

    for i in 1..=N {
        local_write_buf.write_u64(0, i);
        local_read_buf.write_u64(0, 0xDEADBEEF);

        {
            let qp_ref = pair.qp1.borrow();
            let ctx = qp_ref.emit_ctx().expect("emit_ctx failed");

            // READ with RELAXED_ORDERING
            emit_wqe!(&ctx, read {
                flags: WqeFlags::RELAXED_ORDERING,
                remote_addr: remote_buf.addr(),
                rkey: remote_mr.rkey(),
                sge: { addr: local_read_buf.addr(), len: 8, lkey: local_read_mr.lkey() },
                signaled: i * 2 - 1,
            })
            .expect("emit_wqe failed");

            qp_ref.ring_sq_doorbell();
        }

        poll_cq_batch(&pair.send_cq, 1, 5000).expect("READ timeout");

        let read_val = local_read_buf.read_u64(0);
        read_values.push(read_val);

        {
            let qp_ref = pair.qp1.borrow();
            let ctx = qp_ref.emit_ctx().expect("emit_ctx failed");

            // WRITE with FENCE and RELAXED_ORDERING
            // FENCE ensures the prior READ completes before this WRITE
            emit_wqe!(&ctx, write {
                flags: WqeFlags::FENCE | WqeFlags::RELAXED_ORDERING,
                remote_addr: remote_buf.addr(),
                rkey: remote_mr.rkey(),
                sge: { addr: local_write_buf.addr(), len: 8, lkey: local_write_mr.lkey() },
                signaled: i * 2,
            })
            .expect("emit_wqe failed");

            qp_ref.ring_sq_doorbell();
        }

        poll_cq_batch(&pair.send_cq, 1, 5000).expect("WRITE timeout");
    }

    // Verify monotonic increase
    for (idx, &val) in read_values.iter().enumerate() {
        assert_eq!(
            val, idx as u64,
            "FENCE with RELAXED_ORDERING: Expected {} at idx {}, got {}",
            idx, idx, val
        );
    }

    println!(
        "FENCE with RELAXED_ORDERING MR test passed: all {} values correct",
        N
    );
}

/// Test batched READ-WRITE operations with FENCE.
///
/// Submits multiple READ-WRITE pairs in a batch, with FENCE on WRITE
/// to ensure each READ completes before its corresponding WRITE.
#[test]
fn test_fence_batched_read_write() {
    let ctx = match TestContext::new() {
        Some(c) => c,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    println!("Creating loopback QP pair...");
    let pair = create_rc_loopback_pair(&ctx);

    let access = full_access();

    let mut remote_buf = AlignedBuffer::new(4096);
    let mut local_read_bufs: Vec<AlignedBuffer> =
        (0..10).map(|_| AlignedBuffer::new(4096)).collect();
    let mut local_write_bufs: Vec<AlignedBuffer> =
        (0..10).map(|_| AlignedBuffer::new(4096)).collect();

    let remote_mr = unsafe {
        ctx.pd
            .register(remote_buf.as_ptr(), remote_buf.size(), access)
            .expect("Failed to register remote MR")
    };

    let local_read_mrs: Vec<_> = local_read_bufs
        .iter()
        .map(|buf| unsafe {
            ctx.pd
                .register(buf.as_ptr(), buf.size(), access)
                .expect("Failed to register local read MR")
        })
        .collect();

    let local_write_mrs: Vec<_> = local_write_bufs
        .iter()
        .map(|buf| unsafe {
            ctx.pd
                .register(buf.as_ptr(), buf.size(), access)
                .expect("Failed to register local write MR")
        })
        .collect();

    // Initialize
    remote_buf.write_u64(0, 0);
    for (i, buf) in local_write_bufs.iter_mut().enumerate() {
        buf.write_u64(0, (i + 1) as u64);
    }
    for buf in local_read_bufs.iter_mut() {
        buf.write_u64(0, 0xDEADBEEF);
    }

    const BATCH_SIZE: usize = 10;

    println!(
        "Running batched READ-WRITE test with batch size {}...",
        BATCH_SIZE
    );

    {
        let qp_ref = pair.qp1.borrow();
        let ctx = qp_ref.emit_ctx().expect("emit_ctx failed");

        // Submit batch: READ → WRITE(FENCE) pairs
        for i in 0..BATCH_SIZE {
            // READ current counter value
            emit_wqe!(&ctx, read {
                flags: WqeFlags::empty(),
                remote_addr: remote_buf.addr(),
                rkey: remote_mr.rkey(),
                sge: { addr: local_read_bufs[i].addr(), len: 8, lkey: local_read_mrs[i].lkey() },
                signaled: (i * 2) as u64,
            })
            .expect("emit_wqe failed");

            // WRITE new value with FENCE (ensures READ completed)
            emit_wqe!(&ctx, write {
                flags: WqeFlags::FENCE,
                remote_addr: remote_buf.addr(),
                rkey: remote_mr.rkey(),
                sge: { addr: local_write_bufs[i].addr(), len: 8, lkey: local_write_mrs[i].lkey() },
                signaled: (i * 2 + 1) as u64,
            })
            .expect("emit_wqe failed");
        }

        qp_ref.ring_sq_doorbell();
    }

    // Wait for all completions
    poll_cq_batch(&pair.send_cq, BATCH_SIZE * 2, 10000).expect("Batch timeout");

    // Verify read values are monotonically increasing (0, 1, 2, ...)
    let read_values: Vec<u64> = local_read_bufs.iter().map(|buf| buf.read_u64(0)).collect();
    println!("Batched read values: {:?}", read_values);

    for (idx, &val) in read_values.iter().enumerate() {
        assert_eq!(
            val, idx as u64,
            "Batched FENCE: Expected {} at idx {}, got {}",
            idx, idx, val
        );
    }

    // Verify final remote value
    let final_val = remote_buf.read_u64(0);
    assert_eq!(
        final_val, BATCH_SIZE as u64,
        "Expected final value {}, got {}",
        BATCH_SIZE, final_val
    );

    println!(
        "Batched READ-WRITE with FENCE test passed: batch of {} operations",
        BATCH_SIZE
    );
}

/// Basic test to verify RELAXED_ORDERING flags work without errors.
#[test]
fn test_relaxed_ordering_basic() {
    let ctx = match TestContext::new() {
        Some(c) => c,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    println!("Creating loopback QP pair...");
    let pair = create_rc_loopback_pair(&ctx);

    let relaxed_access = full_access() | AccessFlags::RELAXED_ORDERING;

    let remote_buf = AlignedBuffer::new(4096);
    let mut local_buf = AlignedBuffer::new(4096);

    let remote_mr = unsafe {
        ctx.pd
            .register(remote_buf.as_ptr(), remote_buf.size(), relaxed_access)
            .expect("Failed to register remote MR")
    };
    let local_mr = unsafe {
        ctx.pd
            .register(local_buf.as_ptr(), local_buf.size(), relaxed_access)
            .expect("Failed to register local MR")
    };

    // Test WRITE with RELAXED_ORDERING
    local_buf.write_u64(0, 42);
    {
        let qp_ref = pair.qp1.borrow();
        let ctx = qp_ref.emit_ctx().expect("emit_ctx failed");
        emit_wqe!(&ctx, write {
            flags: WqeFlags::RELAXED_ORDERING,
            remote_addr: remote_buf.addr(),
            rkey: remote_mr.rkey(),
            sge: { addr: local_buf.addr(), len: 8, lkey: local_mr.lkey() },
            signaled: 1u64,
        })
        .expect("emit_wqe failed");
        qp_ref.ring_sq_doorbell();
    }
    poll_cq_batch(&pair.send_cq, 1, 5000).expect("WRITE timeout");

    // Verify WRITE succeeded
    assert_eq!(
        remote_buf.read_u64(0),
        42,
        "WRITE with RELAXED_ORDERING failed"
    );

    // Test READ with RELAXED_ORDERING
    local_buf.write_u64(0, 0);
    {
        let qp_ref = pair.qp1.borrow();
        let ctx = qp_ref.emit_ctx().expect("emit_ctx failed");
        emit_wqe!(&ctx, read {
            flags: WqeFlags::RELAXED_ORDERING,
            remote_addr: remote_buf.addr(),
            rkey: remote_mr.rkey(),
            sge: { addr: local_buf.addr(), len: 8, lkey: local_mr.lkey() },
            signaled: 2u64,
        })
        .expect("emit_wqe failed");
        qp_ref.ring_sq_doorbell();
    }
    poll_cq_batch(&pair.send_cq, 1, 5000).expect("READ timeout");

    // Verify READ succeeded
    assert_eq!(
        local_buf.read_u64(0),
        42,
        "READ with RELAXED_ORDERING failed"
    );

    println!("Basic RELAXED_ORDERING test passed!");
}
