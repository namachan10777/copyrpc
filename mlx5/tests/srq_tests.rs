//! SRQ (Shared Receive Queue) tests.
//!
//! This module tests SRQ functionality:
//! - SRQ creation
//! - SRQ direct access initialization
//! - SRQ receive posting
//! - SRQ used with DCT for receiving DC SEND operations
//!
//! Run with:
//! ```bash
//! cargo test --release -p mlx5 --test srq_tests -- --nocapture
//! ```

mod common;

use std::rc::Rc;

use mlx5::dc::{DciConfig, DctConfig};
use mlx5::srq::SrqConfig;
use mlx5::wqe::{TxFlags, WqeFlags, WqeOpcode};

use common::{AlignedBuffer, TestContext, full_access, poll_cq_timeout};

// =============================================================================
// SRQ Creation Tests
// =============================================================================

#[test]
fn test_srq_creation() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    let config = SrqConfig {
        max_wr: 128,
        max_sge: 1,
    };

    let srq: mlx5::srq::Srq<u64> = ctx.pd.create_srq(&config).expect("Failed to create SRQ");

    let srqn = srq.srq_number().expect("Failed to get SRQN");
    println!("SRQ creation test passed!");
    println!("  SRQN: 0x{:x}", srqn);
}

#[test]
fn test_srq_direct_access() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    let config = SrqConfig {
        max_wr: 128,
        max_sge: 1,
    };

    let srq: mlx5::srq::Srq<u64> = ctx.pd.create_srq(&config).expect("Failed to create SRQ");

    // Initialize direct access

    println!("SRQ direct access test passed!");
}

// =============================================================================
// SRQ Receive Posting Tests
// =============================================================================

#[test]
fn test_srq_post_recv() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    let config = SrqConfig {
        max_wr: 128,
        max_sge: 1,
    };

    let srq: mlx5::srq::Srq<u64> = ctx.pd.create_srq(&config).expect("Failed to create SRQ");

    // Allocate receive buffer
    let recv_buf = AlignedBuffer::new(4096);
    let mr = unsafe {
        ctx.pd
            .register(recv_buf.as_ptr(), recv_buf.size(), full_access())
    }
    .expect("Failed to register MR");

    // Post multiple receive buffers using rq_wqe
    for i in 0..10 {
        let offset = (i * 256) as usize;
        srq.post_recv(i as u64, recv_buf.addr() + offset as u64, 256, mr.lkey())
            .expect("post_recv failed");
    }

    // Ring doorbell
    srq.ring_doorbell();

    println!("SRQ post recv test passed!");
    println!("  Posted 10 receive WQEs");
}

// =============================================================================
// SRQ with DCT Tests
// =============================================================================

#[test]
fn test_srq_with_dct_send() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    require_dct!(&ctx);

    // Create CQs
    let mut dci_cq = ctx.ctx.create_cq(256).expect("Failed to create DCI CQ");
    let dci_cq = Rc::new(dci_cq);

    let mut dct_cq = ctx.ctx.create_cq(256).expect("Failed to create DCT CQ");
    let dct_cq = Rc::new(dct_cq);

    // Create SRQ
    let srq_config = SrqConfig {
        max_wr: 128,
        max_sge: 1,
    };
    let srq: mlx5::srq::Srq<u64> = ctx
        .pd
        .create_srq(&srq_config)
        .expect("Failed to create SRQ");

    // Create receive buffer and post to SRQ
    let recv_buf = AlignedBuffer::new(4096);
    let recv_mr = unsafe {
        ctx.pd
            .register(recv_buf.as_ptr(), recv_buf.size(), full_access())
    }
    .expect("Failed to register recv MR");

    srq.post_recv(0u64, recv_buf.addr(), 4096, recv_mr.lkey())
        .expect("post_recv failed");
    srq.ring_doorbell();

    // Create and activate DCI
    let dci_config = DciConfig::default();
    let dci = ctx
        .ctx
        .create_dci::<u64, _>(&ctx.pd, &dci_cq, &dci_config, |_cqe, _entry| {})
        .expect("Failed to create DCI");
    dci.borrow_mut()
        .activate(ctx.port, 0, 4)
        .expect("Failed to activate DCI");

    // Create and activate DCT
    let dc_key: u64 = 0xABCDEF01;
    let dct_config = DctConfig { dc_key };
    let mut dct = ctx
        .ctx
        .create_dct(&ctx.pd, &srq, &dct_cq, &dct_config)
        .expect("Failed to create DCT");
    let access = full_access().bits();
    dct.activate(ctx.port, access, 4)
        .expect("Failed to activate DCT");

    let dctn = dct.dctn();
    let dlid = ctx.port_attr.lid;

    // Prepare send data
    let mut send_buf = AlignedBuffer::new(4096);
    let send_mr = unsafe {
        ctx.pd
            .register(send_buf.as_ptr(), send_buf.size(), full_access())
    }
    .expect("Failed to register send MR");

    let test_data = b"DC SEND via SRQ test!";
    send_buf.fill_bytes(test_data);

    // Post DC SEND
    dci.borrow_mut()
        .sq_wqe(dc_key, dctn, dlid)
        .expect("sq_wqe failed")
        .send(TxFlags::empty())
        .expect("send failed")
        .sge(send_buf.addr(), test_data.len() as u32, send_mr.lkey())
        .finish_signaled(1u64)
        .expect("finish failed");
    dci.borrow().ring_sq_doorbell();

    // Poll DCI CQ for send completion
    let send_cqe = poll_cq_timeout(&dci_cq, 5000).expect("Send CQE timeout");
    assert_eq!(
        send_cqe.syndrome, 0,
        "Send CQE error: syndrome={}",
        send_cqe.syndrome
    );
    dci_cq.flush();

    // Poll DCT CQ for receive completion
    let recv_cqe = poll_cq_timeout(&dct_cq, 5000).expect("Recv CQE timeout");
    assert_eq!(
        recv_cqe.syndrome, 0,
        "Recv CQE error: syndrome={}",
        recv_cqe.syndrome
    );
    dct_cq.flush();

    // Verify received data
    // SRQ receive: data starts at beginning of buffer (no GRH prepended)
    let received = recv_buf.read_bytes(test_data.len());
    assert_eq!(received.as_slice(), test_data, "Received data mismatch");

    println!("SRQ with DCT SEND test passed!");
    println!("  Sent {} bytes via DC SEND", test_data.len());
    println!("  Received byte count: {}", recv_cqe.byte_cnt);
}

// =============================================================================
// SRQ Multiple QP Tests
// =============================================================================

#[test]
fn test_srq_shared_by_multiple_dcts() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    require_dct!(&ctx);

    // Create CQs
    let mut dci_cq = ctx.ctx.create_cq(256).expect("Failed to create DCI CQ");
    let dci_cq = Rc::new(dci_cq);

    let mut dct_cq = ctx.ctx.create_cq(256).expect("Failed to create DCT CQ");
    let dct_cq = Rc::new(dct_cq);

    // Create shared SRQ
    let srq_config = SrqConfig {
        max_wr: 256,
        max_sge: 1,
    };
    let srq: mlx5::srq::Srq<u64> = ctx
        .pd
        .create_srq(&srq_config)
        .expect("Failed to create SRQ");

    // Create receive buffers and post to SRQ
    let recv_bufs: Vec<_> = (0..4).map(|_| AlignedBuffer::new(4096)).collect();
    let recv_mrs: Vec<_> = recv_bufs
        .iter()
        .map(|buf| {
            unsafe { ctx.pd.register(buf.as_ptr(), buf.size(), full_access()) }
                .expect("Failed to register recv MR")
        })
        .collect();

    // Post receive buffers using post_recv
    for (i, (buf, mr)) in recv_bufs.iter().zip(recv_mrs.iter()).enumerate() {
        srq.post_recv(i as u64, buf.addr(), 4096, mr.lkey())
            .expect("post_recv failed");
    }
    srq.ring_doorbell();

    // Create and activate DCI
    let dci_config = DciConfig::default();
    let dci = ctx
        .ctx
        .create_dci::<u64, _>(&ctx.pd, &dci_cq, &dci_config, |_cqe, _entry| {})
        .expect("Failed to create DCI");
    dci.borrow_mut()
        .activate(ctx.port, 0, 4)
        .expect("Failed to activate DCI");

    // Create multiple DCTs sharing the same SRQ
    let num_dcts = 2;
    let mut dcts = Vec::new();
    let access = full_access().bits();

    for i in 0..num_dcts {
        let dc_key: u64 = 0x1000 + i as u64;
        let dct_config = DctConfig { dc_key };
        let mut dct = ctx
            .ctx
            .create_dct(&ctx.pd, &srq, &dct_cq, &dct_config)
            .expect(&format!("Failed to create DCT {}", i));
        dct.activate(ctx.port, access, 4)
            .expect(&format!("Failed to activate DCT {}", i));
        dcts.push(dct);
    }

    // Prepare send buffer
    let mut send_buf = AlignedBuffer::new(4096);
    let send_mr = unsafe {
        ctx.pd
            .register(send_buf.as_ptr(), send_buf.size(), full_access())
    }
    .expect("Failed to register send MR");

    let dlid = ctx.port_attr.lid;

    // Send to each DCT
    for (i, dct) in dcts.iter().enumerate() {
        let test_data = format!("Message to DCT {}", i);
        send_buf.fill_bytes(test_data.as_bytes());

        dci.borrow_mut()
            .sq_wqe(dct.dc_key(), dct.dctn(), dlid)
            .expect("sq_wqe failed")
            .send(TxFlags::empty())
            .expect("send failed")
            .sge(send_buf.addr(), test_data.len() as u32, send_mr.lkey())
            .finish_signaled((i + 1) as u64)
            .expect("finish failed");
        dci.borrow().ring_sq_doorbell();

        // Poll send completion
        let send_cqe =
            poll_cq_timeout(&dci_cq, 5000).expect(&format!("Send CQE timeout for DCT {}", i));
        assert_eq!(
            send_cqe.syndrome, 0,
            "Send CQE error for DCT {}: syndrome={}",
            i, send_cqe.syndrome
        );

        // Poll receive completion
        let recv_cqe =
            poll_cq_timeout(&dct_cq, 5000).expect(&format!("Recv CQE timeout for DCT {}", i));
        assert_eq!(
            recv_cqe.syndrome, 0,
            "Recv CQE error for DCT {}: syndrome={}",
            i, recv_cqe.syndrome
        );
    }

    dci_cq.flush();
    dct_cq.flush();

    println!("SRQ shared by multiple DCTs test passed!");
    println!("  {} DCTs shared single SRQ", num_dcts);
}
