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

use mlx5::cq::CqConfig;
use mlx5::dc::{DciConfig, DctConfig};
use mlx5::pd::RemoteUdQpInfo;
use mlx5::qp::RcQpConfig;
use mlx5::srq::SrqConfig;
use mlx5::transport::IbRemoteQpInfo;
use mlx5::ud::UdQpConfig;
use mlx5::wqe::WqeFlags;

use common::{AlignedBuffer, TestContext, full_access, poll_cq_timeout};

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
    let send_cq = ctx.ctx.create_cq(256, &CqConfig::default()).expect("Failed to create send CQ");
    let send_cq = Rc::new(send_cq);

    let recv_cq1 = ctx.ctx.create_cq(256, &CqConfig::default()).expect("Failed to create recv CQ1");
    let recv_cq1 = Rc::new(recv_cq1);
    let recv_cq2 = ctx.ctx.create_cq(256, &CqConfig::default()).expect("Failed to create recv CQ2");
    let recv_cq2 = Rc::new(recv_cq2);

    // Use small queue to force wrap-around quickly
    let config = RcQpConfig {
        max_send_wr: 16,
        max_recv_wr: 16,
        ..Default::default()
    };

    fn noop_sq_callback(_cqe: mlx5::cq::Cqe, _entry: u64) {}
    fn noop_rq_callback(_cqe: mlx5::cq::Cqe, _entry: u64) {}

    let qp1 = ctx
        .ctx
        .rc_qp_builder::<u64, u64>(&ctx.pd, &config)
        .sq_cq(send_cq.clone(), noop_sq_callback as fn(_, _))
        .rq_cq(recv_cq1.clone(), noop_rq_callback as fn(_, _))
        .build()
        .expect("Failed to create QP1");
    let qp2 = ctx
        .ctx
        .rc_qp_builder::<u64, u64>(&ctx.pd, &config)
        .sq_cq(send_cq.clone(), noop_sq_callback as fn(_, _))
        .rq_cq(recv_cq2.clone(), noop_rq_callback as fn(_, _))
        .build()
        .expect("Failed to create QP2");

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
            .sq_wqe()
            .expect("sq_wqe failed")
            .write(WqeFlags::empty(), remote_buf.addr(), remote_mr.rkey())
            .expect("write failed")
            .sge(local_buf.addr(), test_data.len() as u32, local_mr.lkey())
            .finish_signaled(i as u64)
            .expect("finish failed");
        qp1.borrow().ring_sq_doorbell();

        // Wait for completion
        let cqe =
            poll_cq_timeout(&send_cq, 5000).unwrap_or_else(|| panic!("CQE timeout at iteration {}", i));
        assert_eq!(
            cqe.syndrome, 0,
            "CQE error at iteration {}: syndrome={}",
            i, cqe.syndrome
        );
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

    println!(
        "RC RDMA WRITE wrap-around test passed! ({} iterations)",
        iterations
    );
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

    let send_cq = ctx.ctx.create_cq(256, &CqConfig::default()).expect("Failed to create send CQ");
    let send_cq = Rc::new(send_cq);

    let recv_cq1 = ctx.ctx.create_cq(256, &CqConfig::default()).expect("Failed to create recv CQ1");
    let recv_cq1 = Rc::new(recv_cq1);
    let recv_cq2 = ctx.ctx.create_cq(256, &CqConfig::default()).expect("Failed to create recv CQ2");
    let recv_cq2 = Rc::new(recv_cq2);

    let config = RcQpConfig::default();

    fn noop_sq_callback(_cqe: mlx5::cq::Cqe, _entry: u64) {}
    fn noop_rq_callback(_cqe: mlx5::cq::Cqe, _entry: u64) {}

    let qp1 = ctx
        .ctx
        .rc_qp_builder::<u64, u64>(&ctx.pd, &config)
        .sq_cq(send_cq.clone(), noop_sq_callback as fn(_, _))
        .rq_cq(recv_cq1.clone(), noop_rq_callback as fn(_, _))
        .build()
        .expect("Failed to create QP1");
    let qp2 = ctx
        .ctx
        .rc_qp_builder::<u64, u64>(&ctx.pd, &config)
        .sq_cq(send_cq.clone(), noop_sq_callback as fn(_, _))
        .rq_cq(recv_cq2.clone(), noop_rq_callback as fn(_, _))
        .build()
        .expect("Failed to create QP2");

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

    let test_data = b"Test using ring_sq_doorbell";
    local_buf.fill_bytes(test_data);
    remote_buf.fill(0);

    // Use finish_signaled() instead of finish_signaled_with_blueflame(), then ring doorbell manually
    let _ = qp1
        .borrow_mut()
        .sq_wqe()
        .expect("sq_wqe failed")
        .write(WqeFlags::empty(), remote_buf.addr(), remote_mr.rkey())
        .expect("write failed")
        .sge(local_buf.addr(), test_data.len() as u32, local_mr.lkey())
        .finish_signaled(1u64);

    // Ring doorbell manually
    qp1.borrow().ring_sq_doorbell();

    // Wait for completion
    let cqe = poll_cq_timeout(&send_cq, 5000).expect("CQE timeout");
    assert_eq!(cqe.syndrome, 0, "CQE error: syndrome={}", cqe.syndrome);
    send_cq.flush();

    // Verify data
    let written = remote_buf.read_bytes(test_data.len());
    assert_eq!(
        &written[..],
        test_data,
        "Data mismatch using ring_sq_doorbell"
    );

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
    let dci_cq = ctx.ctx.create_cq(256, &CqConfig::default()).expect("Failed to create DCI CQ");
    let dci_cq = Rc::new(dci_cq);

    let dct_cq = ctx.ctx.create_cq(256, &CqConfig::default()).expect("Failed to create DCT CQ");

    let srq_config = SrqConfig {
        max_wr: 128,
        max_sge: 1,
    };
    let srq: mlx5::srq::Srq<()> = ctx
        .pd
        .create_srq(&srq_config)
        .expect("Failed to create SRQ");

    // Use small queue for DCI
    let dci_config = DciConfig {
        max_send_wr: 16,
        ..Default::default()
    };
    let dci = ctx
        .ctx
        .dci_builder::<u64>(&ctx.pd, &dci_config)
            .sq_cq(dci_cq.clone(), |_cqe, _entry| {})
            .build()
        .expect("Failed to create DCI");
    dci.borrow_mut()
        .activate(ctx.port, 0, 4)
        .expect("Failed to activate DCI");

    let dc_key: u64 = 0xDEADBEEF;
    let dct_config = DctConfig { dc_key };
    let mut dct = ctx
        .ctx
        .dct_builder(&ctx.pd, &srq, &dct_config)
            .recv_cq(&dct_cq)
            .build()
        .expect("Failed to create DCT");
    let access = full_access().bits();
    dct.activate(ctx.port, access, 4)
        .expect("Failed to activate DCT");

    let dctn = dct.dctn();
    let dlid = ctx.port_attr.lid;

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

    let iterations = 64;
    println!("Running {} DC RDMA WRITE iterations...", iterations);

    for i in 0..iterations {
        let test_data = format!("DC WRITE iteration {:04}", i);
        local_buf.fill(0);
        local_buf.fill_bytes(test_data.as_bytes());
        remote_buf.fill(0xBB);

        dci.borrow_mut()
            .sq_wqe(dc_key, dctn, dlid)
            .expect("sq_wqe failed")
            .write(WqeFlags::empty(), remote_buf.addr(), remote_mr.rkey())
            .expect("write failed")
            .sge(local_buf.addr(), test_data.len() as u32, local_mr.lkey())
            .finish_signaled(i as u64)
            .expect("finish failed");
        dci.borrow().ring_sq_doorbell();

        let cqe = poll_cq_timeout(&dci_cq, 5000).unwrap_or_else(|| panic!("CQE timeout at iteration {}", i));
        assert_eq!(
            cqe.syndrome, 0,
            "CQE error at iteration {}: syndrome={}",
            i, cqe.syndrome
        );
        dci_cq.flush();

        let written = remote_buf.read_bytes(test_data.len());
        assert_eq!(
            &written[..],
            test_data.as_bytes(),
            "Data mismatch at iteration {}",
            i
        );
    }

    println!(
        "DC RDMA WRITE wrap-around test passed! ({} iterations)",
        iterations
    );
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

    let dci_cq = ctx.ctx.create_cq(256, &CqConfig::default()).expect("Failed to create DCI CQ");
    let dci_cq = Rc::new(dci_cq);

    let dct_cq = ctx.ctx.create_cq(256, &CqConfig::default()).expect("Failed to create DCT CQ");

    let srq_config = SrqConfig {
        max_wr: 128,
        max_sge: 1,
    };
    let srq: mlx5::srq::Srq<()> = ctx
        .pd
        .create_srq(&srq_config)
        .expect("Failed to create SRQ");

    let dci_config = DciConfig::default();
    let dci = ctx
        .ctx
        .dci_builder::<u64>(&ctx.pd, &dci_config)
            .sq_cq(dci_cq.clone(), |_cqe, _entry| {})
            .build()
        .expect("Failed to create DCI");
    dci.borrow_mut()
        .activate(ctx.port, 0, 4)
        .expect("Failed to activate DCI");

    let dc_key: u64 = 0xABCDEF00;
    let dct_config = DctConfig { dc_key };
    let mut dct = ctx
        .ctx
        .dct_builder(&ctx.pd, &srq, &dct_config)
            .recv_cq(&dct_cq)
            .build()
        .expect("Failed to create DCT");
    let access = full_access().bits();
    dct.activate(ctx.port, access, 4)
        .expect("Failed to activate DCT");

    let dctn = dct.dctn();
    let dlid = ctx.port_attr.lid;

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

    let test_data = b"DC test with ring_sq_doorbell";
    local_buf.fill_bytes(test_data);
    remote_buf.fill(0);

    // Use finish_signaled() + ring_sq_doorbell() instead of finish_signaled_with_blueflame()
    let _ = dci
        .borrow_mut()
        .sq_wqe(dc_key, dctn, dlid)
        .expect("sq_wqe failed")
        .write(WqeFlags::empty(), remote_buf.addr(), remote_mr.rkey())
        .expect("write failed")
        .sge(local_buf.addr(), test_data.len() as u32, local_mr.lkey())
        .finish_signaled(1u64);

    dci.borrow().ring_sq_doorbell();

    let cqe = poll_cq_timeout(&dci_cq, 5000).expect("CQE timeout");
    assert_eq!(cqe.syndrome, 0, "CQE error: syndrome={}", cqe.syndrome);
    dci_cq.flush();

    let written = remote_buf.read_bytes(test_data.len());
    assert_eq!(
        &written[..],
        test_data,
        "Data mismatch using ring_sq_doorbell"
    );

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

    let send_cq = ctx.ctx.create_cq(256, &CqConfig::default()).expect("Failed to create send CQ");
    let send_cq = Rc::new(send_cq);

    let recv_cq = ctx.ctx.create_cq(256, &CqConfig::default()).expect("Failed to create recv CQ");
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
        .ud_qp_builder::<u64, u64>(&ctx.pd, &config)
        .sq_cq(send_cq.clone(), |_cqe: mlx5::cq::Cqe, _entry: u64| {})
        .rq_cq(recv_cq.clone(), |_cqe: mlx5::cq::Cqe, _entry: u64| {})
        .build()
        .expect("Failed to create sender QP");
    sender
        .borrow_mut()
        .activate(ctx.port, 0)
        .expect("Failed to activate sender");

    let receiver = ctx
        .ctx
        .ud_qp_builder::<u64, u64>(&ctx.pd, &config)
        .sq_cq(send_cq.clone(), |_cqe: mlx5::cq::Cqe, _entry: u64| {})
        .rq_cq(recv_cq.clone(), |_cqe: mlx5::cq::Cqe, _entry: u64| {})
        .build()
        .expect("Failed to create receiver QP");
    receiver
        .borrow_mut()
        .activate(ctx.port, 0)
        .expect("Failed to activate receiver");

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

    let remote_info = RemoteUdQpInfo {
        qpn: receiver.borrow().qpn(),
        qkey,
        lid: ctx.port_attr.lid,
    };
    let ah = ctx
        .pd
        .create_ah(ctx.port, &remote_info)
        .expect("Failed to create AH");

    let iterations = 64;
    println!("Running {} UD SEND iterations...", iterations);

    for i in 0..iterations {
        let test_data = format!("UD SEND iteration {:04}", i);
        send_buf.fill(0);
        send_buf.fill_bytes(test_data.as_bytes());
        recv_buf.fill(0xCC);

        // Post receive (must include space for GRH)
        receiver
            .borrow()
            .post_recv(i as u64, recv_buf.addr(), 256 + GRH_SIZE as u32, recv_mr.lkey())
            .expect("post_recv failed");
        receiver.borrow().ring_rq_doorbell();

        // Post send
        sender
            .borrow_mut()
            .sq_wqe(&ah)
            .expect("sq_wqe failed")
            .send(WqeFlags::empty())
            .expect("send failed")
            .sge(send_buf.addr(), test_data.len() as u32, send_mr.lkey())
            .finish_signaled(i as u64)
            .expect("finish failed");
        sender.borrow().ring_sq_doorbell();

        // Wait for send completion
        let send_cqe =
            poll_cq_timeout(&send_cq, 5000).unwrap_or_else(|| panic!("Send CQE timeout at iteration {}", i));
        assert_eq!(send_cqe.syndrome, 0, "Send CQE error at iteration {}", i);
        send_cq.flush();

        // Wait for recv completion
        let recv_cqe =
            poll_cq_timeout(&recv_cq, 5000).unwrap_or_else(|| panic!("Recv CQE timeout at iteration {}", i));
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

    println!(
        "UD SEND wrap-around test passed! ({} iterations)",
        iterations
    );
}

// =============================================================================
// Inline Data Wrap-around Tests
// =============================================================================

/// Test RC SEND with inline data wrap-around.
///
/// Uses variable-size inline data to trigger wrap-around scenarios where
/// the WQE spans multiple WQEBBs and doesn't fit at the end of the ring.
#[test]
fn test_rc_inline_wraparound() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    // Create CQs
    let send_cq = ctx.ctx.create_cq(256, &CqConfig::default()).expect("Failed to create send CQ");
    let send_cq = Rc::new(send_cq);

    let recv_cq1 = ctx.ctx.create_cq(256, &CqConfig::default()).expect("Failed to create recv CQ1");
    let recv_cq1 = Rc::new(recv_cq1);
    let recv_cq2 = ctx.ctx.create_cq(256, &CqConfig::default()).expect("Failed to create recv CQ2");
    let recv_cq2 = Rc::new(recv_cq2);

    // Use small queue (16 WQEs) with max inline to force wrap-around
    // Each inline WQE with 128 bytes of data = ~3 WQEBBs (64 bytes each)
    let config = RcQpConfig {
        max_send_wr: 16,
        max_recv_wr: 64,
        max_send_sge: 1,
        max_recv_sge: 1,
        max_inline_data: 128,
        enable_scatter_to_cqe: false,
    };

    fn noop_sq_callback(_cqe: mlx5::cq::Cqe, _entry: u64) {}
    fn noop_rq_callback(_cqe: mlx5::cq::Cqe, _entry: u64) {}

    let qp1 = ctx
        .ctx
        .rc_qp_builder::<u64, u64>(&ctx.pd, &config)
        .sq_cq(send_cq.clone(), noop_sq_callback as fn(_, _))
        .rq_cq(recv_cq1.clone(), noop_rq_callback as fn(_, _))
        .build()
        .expect("Failed to create QP1");
    let qp2 = ctx
        .ctx
        .rc_qp_builder::<u64, u64>(&ctx.pd, &config)
        .sq_cq(send_cq.clone(), noop_sq_callback as fn(_, _))
        .rq_cq(recv_cq2.clone(), noop_rq_callback as fn(_, _))
        .build()
        .expect("Failed to create QP2");

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
        .expect("Failed to connect QP1");
    qp2.borrow_mut()
        .connect(&remote1, ctx.port, 0, 4, 4, access)
        .expect("Failed to connect QP2");

    // Allocate receive buffer
    let mut recv_buf = AlignedBuffer::new(4096);
    let recv_mr = unsafe {
        ctx.pd
            .register(recv_buf.as_ptr(), recv_buf.size(), full_access())
    }
    .expect("Failed to register recv MR");

    // Prepare inline test data (128 bytes to use ~3 WQEBBs per WQE)
    let test_data: Vec<u8> = (0..128).map(|i| i as u8).collect();

    println!("Testing RC inline data wrap-around...");
    println!("Queue size: 16 WQEs, inline data size: {} bytes", test_data.len());

    // Number of iterations to ensure multiple wrap-arounds
    // With 16 WQEs and ~3 WQEBBs per WQE, we should wrap around frequently
    let iterations = 32;
    let mut prev_wqe_idx: Option<u16> = None;
    let mut wrap_around_count = 0;

    for i in 0..iterations {
        // Post receive
        recv_buf.fill(0);
        qp2.borrow()
            .post_recv(i as u64, recv_buf.addr(), 256, recv_mr.lkey())
            .expect("post_recv failed");
        qp2.borrow().ring_rq_doorbell();

        // Post SEND with inline data
        let handle = qp1
            .borrow_mut()
            .sq_wqe()
            .expect("sq_wqe failed")
            .send(WqeFlags::empty())
            .expect("send failed")
            .inline(&test_data)
            .finish_signaled(i as u64)
            .expect("finish failed");
        qp1.borrow().ring_sq_doorbell();

        // Detect wrap-around: wqe_idx decreased or jumped significantly
        if let Some(prev) = prev_wqe_idx
            && (handle.wqe_idx < prev || (handle.wqe_idx == 0 && prev > 0)) {
                wrap_around_count += 1;
                println!(
                    "  Iteration {}: WRAP-AROUND detected (wqe_idx {} -> {})",
                    i, prev, handle.wqe_idx
                );
            }
        prev_wqe_idx = Some(handle.wqe_idx);

        // Poll send CQ
        let send_cqe = poll_cq_timeout(&send_cq, 5000).expect("Send CQE timeout");
        assert_eq!(send_cqe.syndrome, 0, "Send CQE error: syndrome={}", send_cqe.syndrome);
        send_cq.flush();

        // Poll recv CQ (qp2's recv CQ is recv_cq2)
        let recv_cqe = poll_cq_timeout(&recv_cq2, 5000).expect("Recv CQE timeout");
        assert_eq!(recv_cqe.syndrome, 0, "Recv CQE error: syndrome={}", recv_cqe.syndrome);
        recv_cq2.flush();

        // Verify data
        let received = recv_buf.read_bytes(test_data.len());
        assert_eq!(
            &received[..], &test_data[..],
            "Iteration {}: inline data mismatch", i
        );
    }

    println!(
        "RC inline wrap-around test passed! {} wrap-arounds in {} iterations",
        wrap_around_count, iterations
    );
    // Note: wrap-around may not be detected via wqe_idx if NOP fills the gap,
    // but successful data transfer across 32 iterations proves correctness
}

/// Test RC SEND with variable-size inline data to stress wrap-around boundary conditions.
#[test]
fn test_rc_inline_variable_size_wraparound() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    // Create CQs
    let send_cq = ctx.ctx.create_cq(256, &CqConfig::default()).expect("Failed to create send CQ");
    let send_cq = Rc::new(send_cq);

    let recv_cq1 = ctx.ctx.create_cq(256, &CqConfig::default()).expect("Failed to create recv CQ1");
    let recv_cq1 = Rc::new(recv_cq1);
    let recv_cq2 = ctx.ctx.create_cq(256, &CqConfig::default()).expect("Failed to create recv CQ2");
    let recv_cq2 = Rc::new(recv_cq2);

    // Use small queue with max inline
    let config = RcQpConfig {
        max_send_wr: 16,
        max_recv_wr: 128,
        max_send_sge: 1,
        max_recv_sge: 1,
        max_inline_data: 128,
        enable_scatter_to_cqe: false,
    };

    fn noop_sq_callback(_cqe: mlx5::cq::Cqe, _entry: u64) {}
    fn noop_rq_callback(_cqe: mlx5::cq::Cqe, _entry: u64) {}

    let qp1 = ctx
        .ctx
        .rc_qp_builder::<u64, u64>(&ctx.pd, &config)
        .sq_cq(send_cq.clone(), noop_sq_callback as fn(_, _))
        .rq_cq(recv_cq1.clone(), noop_rq_callback as fn(_, _))
        .build()
        .expect("Failed to create QP1");
    let qp2 = ctx
        .ctx
        .rc_qp_builder::<u64, u64>(&ctx.pd, &config)
        .sq_cq(send_cq.clone(), noop_sq_callback as fn(_, _))
        .rq_cq(recv_cq2.clone(), noop_rq_callback as fn(_, _))
        .build()
        .expect("Failed to create QP2");

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
        .expect("Failed to connect QP1");
    qp2.borrow_mut()
        .connect(&remote1, ctx.port, 0, 4, 4, access)
        .expect("Failed to connect QP2");

    // Allocate receive buffer
    let mut recv_buf = AlignedBuffer::new(4096);
    let recv_mr = unsafe {
        ctx.pd
            .register(recv_buf.as_ptr(), recv_buf.size(), full_access())
    }
    .expect("Failed to register recv MR");

    println!("Testing RC variable-size inline data wrap-around...");

    // Test with various inline data sizes to stress different boundary conditions
    // Sizes that map to different WQEBB counts:
    // - 1-12 bytes: 1 WQEBB (ctrl seg + inline header + data)
    // - 13-60 bytes: 2 WQEBBs
    // - 61-124 bytes: 3 WQEBBs
    let sizes = [8, 16, 32, 48, 64, 96, 128, 64, 32, 128, 48, 96, 16, 128, 8, 128];

    for (i, &size) in sizes.iter().enumerate() {
        let test_data: Vec<u8> = (0..size).map(|j| ((i + j) & 0xff) as u8).collect();

        // Post receive
        recv_buf.fill(0);
        qp2.borrow()
            .post_recv(i as u64, recv_buf.addr(), 256, recv_mr.lkey())
            .expect("post_recv failed");
        qp2.borrow().ring_rq_doorbell();

        // Post SEND with inline data
        qp1.borrow_mut()
            .sq_wqe()
            .expect("sq_wqe failed")
            .send(WqeFlags::empty())
            .expect("send failed")
            .inline(&test_data)
            .finish_signaled(i as u64)
            .expect("finish failed");
        qp1.borrow().ring_sq_doorbell();

        // Poll CQs
        let send_cqe = poll_cq_timeout(&send_cq, 5000).expect("Send CQE timeout");
        assert_eq!(send_cqe.syndrome, 0, "Send CQE error at size {}", size);
        send_cq.flush();

        let recv_cqe = poll_cq_timeout(&recv_cq2, 5000).expect("Recv CQE timeout");
        assert_eq!(recv_cqe.syndrome, 0, "Recv CQE error at size {}", size);
        recv_cq2.flush();

        // Verify data
        let received = recv_buf.read_bytes(size);
        assert_eq!(
            &received[..], &test_data[..],
            "Data mismatch at size {}", size
        );
    }

    println!("RC variable-size inline wrap-around test passed!");
}
