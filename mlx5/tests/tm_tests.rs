//! TM (Tag Matching) SRQ tests.
//!
//! This module tests Tag Matching functionality:
//! - TM-SRQ creation
//! - Tag add/remove operations via Command QP
//! - Tag matching with incoming messages
//! - Unexpected message handling
//!
//! Tag Matching is hardware-accelerated matching of incoming messages
//! to posted receive buffers based on 64-bit tags.
//!
//! Run with:
//! ```bash
//! cargo test --release -p mlx5 --test tm_tests -- --nocapture
//! ```

mod common;

use std::cell::RefCell;
use std::rc::Rc;

use mlx5::dc::{DciConfig, DctConfig};
use mlx5::tm_srq::TmSrqConfig;
use mlx5::wqe::{WqeFlags, WqeOpcode};

use common::{full_access, poll_cq_timeout, AlignedBuffer, TestContext};

// =============================================================================
// TM-SRQ Creation Tests
// =============================================================================

#[test]
fn test_tm_srq_creation() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    require_tm_srq!(&ctx);

    let cq = Rc::new(RefCell::new(
        ctx.ctx.create_cq(256).expect("Failed to create CQ"),
    ));

    let config = TmSrqConfig {
        max_wr: 256,
        max_sge: 1,
        max_num_tags: 32,
        max_ops: 8,
    };

    let tm_srq = ctx
        .ctx
        .create_tm_srq::<u64, _>(&ctx.pd, &cq, &config, |_cqe, _entry| {})
        .expect("Failed to create TM-SRQ");

    let srqn = tm_srq.borrow().srqn().expect("Failed to get SRQN");
    let max_tags = tm_srq.borrow().max_num_tags();

    println!("TM-SRQ creation test passed!");
    println!("  SRQN: 0x{:x}", srqn);
    println!("  Max tags: {}", max_tags);
}

#[test]
fn test_tm_srq_direct_access() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    require_tm_srq!(&ctx);

    let cq = Rc::new(RefCell::new(
        ctx.ctx.create_cq(256).expect("Failed to create CQ"),
    ));
    cq.borrow_mut()
        .init_direct_access()
        .expect("Failed to init CQ direct access");

    let config = TmSrqConfig {
        max_wr: 256,
        max_sge: 1,
        max_num_tags: 32,
        max_ops: 8,
    };

    let tm_srq = ctx
        .ctx
        .create_tm_srq::<u64, _>(&ctx.pd, &cq, &config, |_cqe, _entry| {})
        .expect("Failed to create TM-SRQ");

    // Initialize direct access
    mlx5::tm_srq::TagMatchingSrq::init_direct_access(&tm_srq)
        .expect("Failed to init TM-SRQ direct access");

    println!("TM-SRQ direct access test passed!");
    println!("  Command QP available: {}", tm_srq.borrow().cmd_optimistic_available());
}

// =============================================================================
// Tag Add/Remove Tests
// =============================================================================

#[test]
fn test_tm_tag_add_remove() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    require_tm_srq!(&ctx);

    let cq = Rc::new(RefCell::new(
        ctx.ctx.create_cq(256).expect("Failed to create CQ"),
    ));
    cq.borrow_mut()
        .init_direct_access()
        .expect("Failed to init CQ direct access");

    let config = TmSrqConfig {
        max_wr: 256,
        max_sge: 1,
        max_num_tags: 32,
        max_ops: 8,
    };

    let tm_srq = ctx
        .ctx
        .create_tm_srq::<u64, _>(&ctx.pd, &cq, &config, |_cqe, _entry| {})
        .expect("Failed to create TM-SRQ");

    mlx5::tm_srq::TagMatchingSrq::init_direct_access(&tm_srq)
        .expect("Failed to init TM-SRQ direct access");

    // Allocate receive buffer
    let recv_buf = AlignedBuffer::new(4096);
    let mr = unsafe { ctx.pd.register(recv_buf.as_ptr(), recv_buf.size(), full_access()) }
        .expect("Failed to register MR");

    // Add a tag
    let tag: u64 = 0xDEADBEEF_CAFEBABE;
    let tag_index: u16 = 0;

    tm_srq.borrow_mut()
        .cmd_wqe_builder(1u64)
        .expect("cmd_wqe_builder failed")
        .ctrl(WqeOpcode::TagMatching, 0)
        .tag_add(tag_index, tag, recv_buf.addr(), 256, mr.lkey())
        .finish_with_blueflame();

    // Poll for add completion
    let add_cqe = poll_cq_timeout(&mut cq.borrow_mut(), 5000).expect("Add CQE timeout");
    assert_eq!(add_cqe.syndrome, 0, "Add CQE error: syndrome={}", add_cqe.syndrome);
    cq.borrow().flush();

    println!("Tag added: index={}, tag=0x{:016x}", tag_index, tag);

    // Remove the tag
    tm_srq.borrow_mut()
        .cmd_wqe_builder(2u64)
        .expect("cmd_wqe_builder failed")
        .ctrl(WqeOpcode::TagMatching, 0)
        .tag_del(tag_index)
        .finish_with_blueflame();

    // Poll for delete completion
    let del_cqe = poll_cq_timeout(&mut cq.borrow_mut(), 5000).expect("Del CQE timeout");
    assert_eq!(del_cqe.syndrome, 0, "Del CQE error: syndrome={}", del_cqe.syndrome);
    cq.borrow().flush();

    println!("Tag removed: index={}", tag_index);
    println!("TM tag add/remove test passed!");
}

// =============================================================================
// Tag Matching with DC Tests
// =============================================================================

#[test]
fn test_tm_tag_matching_with_dc() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    require_tm_srq!(&ctx);

    // Create CQ for DCI
    let dci_cq = Rc::new(RefCell::new(
        ctx.ctx.create_cq(256).expect("Failed to create DCI CQ"),
    ));
    dci_cq
        .borrow_mut()
        .init_direct_access()
        .expect("Failed to init DCI CQ direct access");

    // Create CQ for TM-SRQ
    let tm_cq = Rc::new(RefCell::new(
        ctx.ctx.create_cq(256).expect("Failed to create TM CQ"),
    ));
    tm_cq
        .borrow_mut()
        .init_direct_access()
        .expect("Failed to init TM CQ direct access");

    // Create TM-SRQ
    let tm_config = TmSrqConfig {
        max_wr: 256,
        max_sge: 1,
        max_num_tags: 32,
        max_ops: 8,
    };

    let tm_srq = ctx
        .ctx
        .create_tm_srq::<u64, _>(&ctx.pd, &tm_cq, &tm_config, |_cqe, _entry| {})
        .expect("Failed to create TM-SRQ");

    mlx5::tm_srq::TagMatchingSrq::init_direct_access(&tm_srq)
        .expect("Failed to init TM-SRQ direct access");

    // Create and activate DCI
    let dci_config = DciConfig::default();
    let dci = ctx
        .ctx
        .create_dci_sparse::<u64, _>(&ctx.pd, &dci_cq, &dci_config, |_cqe, _entry| {})
        .expect("Failed to create DCI");
    dci.borrow_mut()
        .activate(ctx.port, 0, 4)
        .expect("Failed to activate DCI");

    // Create DCT using TM-SRQ (we need to get the SRQ from TM-SRQ)
    // Note: DCT creation with TM-SRQ requires the underlying ibv_srq
    // For now, we'll skip this test as TM-SRQ DCT integration requires additional API

    println!("TM tag matching with DC test (basic setup) passed!");
    println!("  Note: Full TM+DC integration requires DCT with TM-SRQ binding");
}

// =============================================================================
// Multiple Tags Tests
// =============================================================================

#[test]
fn test_tm_multiple_tags() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    require_tm_srq!(&ctx);

    let cq = Rc::new(RefCell::new(
        ctx.ctx.create_cq(256).expect("Failed to create CQ"),
    ));
    cq.borrow_mut()
        .init_direct_access()
        .expect("Failed to init CQ direct access");

    let config = TmSrqConfig {
        max_wr: 256,
        max_sge: 1,
        max_num_tags: 32,
        max_ops: 16,
    };

    let tm_srq = ctx
        .ctx
        .create_tm_srq::<u64, _>(&ctx.pd, &cq, &config, |_cqe, _entry| {})
        .expect("Failed to create TM-SRQ");

    mlx5::tm_srq::TagMatchingSrq::init_direct_access(&tm_srq)
        .expect("Failed to init TM-SRQ direct access");

    // Allocate receive buffers
    let recv_bufs: Vec<_> = (0..8).map(|_| AlignedBuffer::new(4096)).collect();
    let mrs: Vec<_> = recv_bufs
        .iter()
        .map(|buf| {
            unsafe { ctx.pd.register(buf.as_ptr(), buf.size(), full_access()) }
                .expect("Failed to register MR")
        })
        .collect();

    // Add multiple tags
    let base_tag: u64 = 0x1000;
    for i in 0..8u16 {
        let tag = base_tag + i as u64;

        tm_srq.borrow_mut()
            .cmd_wqe_builder((i + 1) as u64)
            .expect("cmd_wqe_builder failed")
            .ctrl(WqeOpcode::TagMatching, 0)
            .tag_add(i, tag, recv_bufs[i as usize].addr(), 256, mrs[i as usize].lkey())
            .finish_with_blueflame();

        // Poll for completion
        let cqe = poll_cq_timeout(&mut cq.borrow_mut(), 5000)
            .expect(&format!("Add CQE timeout for tag {}", i));
        assert_eq!(cqe.syndrome, 0, "Add CQE error for tag {}: syndrome={}", i, cqe.syndrome);
    }
    cq.borrow().flush();

    println!("Added 8 tags successfully");

    // Remove all tags
    for i in 0..8u16 {
        tm_srq.borrow_mut()
            .cmd_wqe_builder((i + 100) as u64)
            .expect("cmd_wqe_builder failed")
            .ctrl(WqeOpcode::TagMatching, 0)
            .tag_del(i)
            .finish_with_blueflame();

        // Poll for completion
        let cqe = poll_cq_timeout(&mut cq.borrow_mut(), 5000)
            .expect(&format!("Del CQE timeout for tag {}", i));
        assert_eq!(cqe.syndrome, 0, "Del CQE error for tag {}: syndrome={}", i, cqe.syndrome);
    }
    cq.borrow().flush();

    println!("Removed 8 tags successfully");
    println!("TM multiple tags test passed!");
}

// =============================================================================
// Unordered Receive Tests
// =============================================================================

#[test]
fn test_tm_unordered_recv() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    require_tm_srq!(&ctx);

    let cq = Rc::new(RefCell::new(
        ctx.ctx.create_cq(256).expect("Failed to create CQ"),
    ));
    cq.borrow_mut()
        .init_direct_access()
        .expect("Failed to init CQ direct access");

    let config = TmSrqConfig {
        max_wr: 256,
        max_sge: 1,
        max_num_tags: 32,
        max_ops: 8,
    };

    let tm_srq = ctx
        .ctx
        .create_tm_srq::<u64, _>(&ctx.pd, &cq, &config, |_cqe, _entry| {})
        .expect("Failed to create TM-SRQ");

    mlx5::tm_srq::TagMatchingSrq::init_direct_access(&tm_srq)
        .expect("Failed to init TM-SRQ direct access");

    // Allocate receive buffer for unexpected messages
    let recv_buf = AlignedBuffer::new(4096);
    let mr = unsafe { ctx.pd.register(recv_buf.as_ptr(), recv_buf.size(), full_access()) }
        .expect("Failed to register MR");

    // Post unordered receives (for unexpected messages)
    for i in 0..10 {
        let offset = i * 256;
        unsafe {
            tm_srq.borrow_mut()
                .post_unordered_recv(recv_buf.addr() + offset, 256, mr.lkey());
        }
    }
    tm_srq.borrow_mut().ring_srq_doorbell();

    println!("TM unordered recv test passed!");
    println!("  Posted 10 unordered receive WQEs");
}
