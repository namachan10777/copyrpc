//! UD (Unreliable Datagram) QP tests.
//!
//! This module tests UD functionality:
//! - UD QP creation
//! - UD SEND/RECV operations
//! - Address Handle usage
//!
//! UD is connectionless - each message is independently addressed.
//! Receives include a 40-byte GRH (Global Route Header).
//!
//! Run with:
//! ```bash
//! cargo test --release -p mlx5 --test ud_tests -- --nocapture
//! ```

mod common;

use std::rc::Rc;

use mlx5::pd::RemoteUdQpInfo;
use mlx5::ud::{UdQpConfig, UdQpState};
use mlx5::wqe::{WqeFlags, WqeOpcode};

use common::{full_access, poll_cq_timeout, AlignedBuffer, TestContext};

/// Size of GRH (Global Route Header) prepended to UD receives.
const GRH_SIZE: usize = 40;

// =============================================================================
// UD QP Creation Tests
// =============================================================================

#[test]
fn test_ud_creation() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    let send_cq = Rc::new(
        ctx.ctx.create_cq(256).expect("Failed to create send CQ"),
    );
    let recv_cq = Rc::new(ctx.ctx.create_cq(256).expect("Failed to create recv CQ"));

    let config = UdQpConfig {
        qkey: 0x12345678,
        ..Default::default()
    };

    let qp = ctx
        .ctx
        .create_ud_qp_sparse::<u64, _>(&ctx.pd, &send_cq, &recv_cq, &config, |_cqe, _entry| {})
        .expect("Failed to create UD QP");

    println!("UD QP creation test passed!");
    println!("  QPN: 0x{:x}", qp.borrow().qpn());
    println!("  Q_Key: 0x{:x}", qp.borrow().qkey());
}

#[test]
fn test_ud_activate() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    let send_cq = Rc::new(
        ctx.ctx.create_cq(256).expect("Failed to create send CQ"),
    );
    let recv_cq = Rc::new(ctx.ctx.create_cq(256).expect("Failed to create recv CQ"));

    let config = UdQpConfig {
        qkey: 0x12345678,
        ..Default::default()
    };

    let qp = ctx
        .ctx
        .create_ud_qp_sparse::<u64, _>(&ctx.pd, &send_cq, &recv_cq, &config, |_cqe, _entry| {})
        .expect("Failed to create UD QP");

    // Activate QP
    qp.borrow_mut()
        .activate(ctx.port, 0)
        .expect("Failed to activate UD QP");

    assert_eq!(qp.borrow().state(), UdQpState::Rts);

    println!("UD QP activate test passed!");
    println!("  State: {:?}", qp.borrow().state());
}

// =============================================================================
// UD SEND/RECV Tests
// =============================================================================

#[test]
fn test_ud_send_recv() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    // Create CQs
    let mut send_cq = ctx.ctx.create_cq(256).expect("Failed to create send CQ");
    send_cq
        .init_direct_access()
        .expect("Failed to init send CQ direct access");
    let send_cq = Rc::new(send_cq);

    let mut recv_cq = ctx.ctx.create_cq(256).expect("Failed to create recv CQ");
    recv_cq
        .init_direct_access()
        .expect("Failed to init recv CQ direct access");
    let recv_cq = Rc::new(recv_cq);

    // Q_Key must have MSB=0 for non-privileged users (bit 31 indicates privileged Q_Key)
    let qkey: u32 = 0x1BCDEF00;

    // Create sender QP
    let send_config = UdQpConfig {
        qkey,
        ..Default::default()
    };
    let sender = ctx
        .ctx
        .create_ud_qp_sparse::<u64, _>(&ctx.pd, &send_cq, &recv_cq, &send_config, |_cqe, _entry| {
        })
        .expect("Failed to create sender QP");
    sender
        .borrow_mut()
        .activate(ctx.port, 0)
        .expect("Failed to activate sender");

    // Create receiver QP
    let recv_config = UdQpConfig {
        qkey,
        ..Default::default()
    };
    let receiver = ctx
        .ctx
        .create_ud_qp_sparse::<u64, _>(&ctx.pd, &send_cq, &recv_cq, &recv_config, |_cqe, _entry| {
        })
        .expect("Failed to create receiver QP");
    receiver
        .borrow_mut()
        .activate(ctx.port, 0)
        .expect("Failed to activate receiver");

    // Allocate buffers
    let mut send_buf = AlignedBuffer::new(4096);
    let mut recv_buf = AlignedBuffer::new(4096);

    // Register memory regions
    let send_mr = unsafe { ctx.pd.register(send_buf.as_ptr(), send_buf.size(), full_access()) }
        .expect("Failed to register send MR");
    let recv_mr = unsafe { ctx.pd.register(recv_buf.as_ptr(), recv_buf.size(), full_access()) }
        .expect("Failed to register recv MR");

    // Post receive (must include space for GRH)
    let recv_len = 256 + GRH_SIZE as u32;
    receiver
        .borrow()
        .recv_builder(0u64)
        .expect("Failed to create recv builder")
        .sge(recv_buf.addr(), recv_len, recv_mr.lkey())
        .finish();
    receiver.borrow().ring_rq_doorbell();

    // Prepare test data
    let test_data = b"UD SEND test - connectionless datagram!";
    send_buf.fill_bytes(test_data);

    // Create Address Handle for receiver
    let remote_info = RemoteUdQpInfo {
        qpn: receiver.borrow().qpn(),
        qkey,
        lid: ctx.port_attr.lid,
    };
    let ah = ctx
        .pd
        .create_ah(ctx.port, &remote_info)
        .expect("Failed to create AH");

    // Post UD SEND
    sender
        .borrow_mut()
        .wqe_builder(1u64)
        .expect("wqe_builder failed")
        .ctrl(WqeOpcode::Send, WqeFlags::empty(), 0)
        .ud_av(&ah, qkey)
        .sge(send_buf.addr(), test_data.len() as u32, send_mr.lkey())
        .finish_with_blueflame();

    // Poll send CQ
    let send_cqe = poll_cq_timeout(&send_cq, 5000).expect("Send CQE timeout");
    assert_eq!(
        send_cqe.syndrome, 0,
        "Send CQE error: syndrome={}",
        send_cqe.syndrome
    );
    send_cq.flush();

    // Poll recv CQ
    let recv_cqe = poll_cq_timeout(&recv_cq, 5000).expect("Recv CQE timeout");
    assert_eq!(
        recv_cqe.syndrome, 0,
        "Recv CQE error: syndrome={}",
        recv_cqe.syndrome
    );
    recv_cq.flush();

    // Verify received data (skip GRH)
    let received = recv_buf.read_bytes(GRH_SIZE + test_data.len());
    assert_eq!(
        &received[GRH_SIZE..],
        test_data,
        "UD SEND data mismatch"
    );

    println!("UD SEND/RECV test passed!");
    println!("  Sent {} bytes", test_data.len());
    println!("  Received {} bytes (including {} byte GRH)", recv_cqe.byte_cnt, GRH_SIZE);
}

// =============================================================================
// UD with raw AV Tests
// =============================================================================

#[test]
fn test_ud_send_raw_av() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    // Create CQs
    let mut send_cq = ctx.ctx.create_cq(256).expect("Failed to create send CQ");
    send_cq
        .init_direct_access()
        .expect("Failed to init send CQ direct access");
    let send_cq = Rc::new(send_cq);

    let mut recv_cq = ctx.ctx.create_cq(256).expect("Failed to create recv CQ");
    recv_cq
        .init_direct_access()
        .expect("Failed to init recv CQ direct access");
    let recv_cq = Rc::new(recv_cq);

    let qkey: u32 = 0x11111111;

    // Create sender QP
    let send_config = UdQpConfig {
        qkey,
        ..Default::default()
    };
    let sender = ctx
        .ctx
        .create_ud_qp_sparse::<u64, _>(&ctx.pd, &send_cq, &recv_cq, &send_config, |_cqe, _entry| {
        })
        .expect("Failed to create sender QP");
    sender
        .borrow_mut()
        .activate(ctx.port, 0)
        .expect("Failed to activate sender");

    // Create receiver QP
    let recv_config = UdQpConfig {
        qkey,
        ..Default::default()
    };
    let receiver = ctx
        .ctx
        .create_ud_qp_sparse::<u64, _>(&ctx.pd, &send_cq, &recv_cq, &recv_config, |_cqe, _entry| {
        })
        .expect("Failed to create receiver QP");
    receiver
        .borrow_mut()
        .activate(ctx.port, 0)
        .expect("Failed to activate receiver");

    // Allocate buffers
    let mut send_buf = AlignedBuffer::new(4096);
    let mut recv_buf = AlignedBuffer::new(4096);

    // Register memory regions
    let send_mr = unsafe { ctx.pd.register(send_buf.as_ptr(), send_buf.size(), full_access()) }
        .expect("Failed to register send MR");
    let recv_mr = unsafe { ctx.pd.register(recv_buf.as_ptr(), recv_buf.size(), full_access()) }
        .expect("Failed to register recv MR");

    // Post receive
    let recv_len = 256 + GRH_SIZE as u32;
    receiver
        .borrow()
        .recv_builder(0u64)
        .expect("Failed to create recv builder")
        .sge(recv_buf.addr(), recv_len, recv_mr.lkey())
        .finish();
    receiver.borrow().ring_rq_doorbell();

    // Prepare test data
    let test_data = b"UD SEND with raw AV - direct addressing!";
    send_buf.fill_bytes(test_data);

    // Use raw AV (without creating ibv_ah)
    let remote_qpn = receiver.borrow().qpn();
    let dlid = ctx.port_attr.lid;

    // Post UD SEND with raw AV
    sender
        .borrow_mut()
        .wqe_builder(1u64)
        .expect("wqe_builder failed")
        .ctrl(WqeOpcode::Send, WqeFlags::empty(), 0)
        .ud_av_raw(remote_qpn, qkey, dlid)
        .sge(send_buf.addr(), test_data.len() as u32, send_mr.lkey())
        .finish_with_blueflame();

    // Poll send CQ
    let send_cqe = poll_cq_timeout(&send_cq, 5000).expect("Send CQE timeout");
    assert_eq!(
        send_cqe.syndrome, 0,
        "Send CQE error: syndrome={}",
        send_cqe.syndrome
    );
    send_cq.flush();

    // Poll recv CQ
    let recv_cqe = poll_cq_timeout(&recv_cq, 5000).expect("Recv CQE timeout");
    assert_eq!(
        recv_cqe.syndrome, 0,
        "Recv CQE error: syndrome={}",
        recv_cqe.syndrome
    );
    recv_cq.flush();

    // Verify received data
    let received = recv_buf.read_bytes(GRH_SIZE + test_data.len());
    assert_eq!(
        &received[GRH_SIZE..],
        test_data,
        "UD SEND (raw AV) data mismatch"
    );

    println!("UD SEND with raw AV test passed!");
}

// =============================================================================
// Multiple UD Destinations Tests
// =============================================================================

#[test]
fn test_ud_multiple_destinations() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    // Create CQs
    let mut cq = ctx.ctx.create_cq(256).expect("Failed to create CQ");
    cq.init_direct_access()
        .expect("Failed to init CQ direct access");
    let cq = Rc::new(cq);

    let mut recv_cq = ctx.ctx.create_cq(256).expect("Failed to create recv CQ");
    recv_cq
        .init_direct_access()
        .expect("Failed to init recv CQ direct access");
    let recv_cq = Rc::new(recv_cq);

    let qkey: u32 = 0x22222222;

    // Create sender QP
    let config = UdQpConfig {
        qkey,
        ..Default::default()
    };
    let sender = ctx
        .ctx
        .create_ud_qp_sparse::<u64, _>(&ctx.pd, &cq, &recv_cq, &config, |_cqe, _entry| {})
        .expect("Failed to create sender QP");
    sender
        .borrow_mut()
        .activate(ctx.port, 0)
        .expect("Failed to activate sender");

    // Create multiple receiver QPs
    let num_receivers = 3;
    let mut receivers = Vec::new();
    let mut recv_bufs = Vec::new();
    let mut recv_mrs = Vec::new();

    for i in 0..num_receivers {
        let receiver = ctx
            .ctx
            .create_ud_qp_sparse::<u64, _>(&ctx.pd, &cq, &recv_cq, &config, |_cqe, _entry| {})
            .expect(&format!("Failed to create receiver {}", i));
        receiver
            .borrow_mut()
            .activate(ctx.port, 0)
            .expect(&format!("Failed to activate receiver {}", i));

        let recv_buf = AlignedBuffer::new(4096);
        let recv_mr =
            unsafe { ctx.pd.register(recv_buf.as_ptr(), recv_buf.size(), full_access()) }
                .expect("Failed to register recv MR");

        // Post receive
        receiver
            .borrow()
            .recv_builder(i as u64)
            .expect("Failed to create recv builder")
            .sge(recv_buf.addr(), 256 + GRH_SIZE as u32, recv_mr.lkey())
            .finish();
        receiver.borrow().ring_rq_doorbell();

        receivers.push(receiver);
        recv_bufs.push(recv_buf);
        recv_mrs.push(recv_mr);
    }

    // Send buffer
    let mut send_buf = AlignedBuffer::new(4096);
    let send_mr = unsafe { ctx.pd.register(send_buf.as_ptr(), send_buf.size(), full_access()) }
        .expect("Failed to register send MR");

    // Send to each receiver
    for (i, receiver) in receivers.iter().enumerate() {
        let test_data = format!("Message to receiver {}", i);
        send_buf.fill_bytes(test_data.as_bytes());

        let remote_info = RemoteUdQpInfo {
            qpn: receiver.borrow().qpn(),
            qkey,
            lid: ctx.port_attr.lid,
        };
        let ah = ctx
            .pd
            .create_ah(ctx.port, &remote_info)
            .expect("Failed to create AH");

        sender
            .borrow_mut()
            .wqe_builder((i + 1) as u64)
            .expect("wqe_builder failed")
            .ctrl(WqeOpcode::Send, WqeFlags::empty(), 0)
            .ud_av(&ah, qkey)
            .sge(send_buf.addr(), test_data.len() as u32, send_mr.lkey())
            .finish_with_blueflame();

        // Poll send CQ
        let send_cqe = poll_cq_timeout(&cq, 5000)
            .expect(&format!("Send CQE timeout for receiver {}", i));
        assert_eq!(
            send_cqe.syndrome, 0,
            "Send CQE error for receiver {}: syndrome={}",
            i, send_cqe.syndrome
        );

        // Poll recv CQ
        let recv_cqe = poll_cq_timeout(&recv_cq, 5000)
            .expect(&format!("Recv CQE timeout for receiver {}", i));
        assert_eq!(
            recv_cqe.syndrome, 0,
            "Recv CQE error for receiver {}: syndrome={}",
            i, recv_cqe.syndrome
        );

        // Verify data
        let received = recv_bufs[i].read_bytes(GRH_SIZE + test_data.len());
        assert_eq!(
            &received[GRH_SIZE..],
            test_data.as_bytes(),
            "Data mismatch for receiver {}",
            i
        );
    }

    cq.flush();
    recv_cq.flush();

    println!("UD multiple destinations test passed!");
    println!("  Single sender sent to {} receivers", num_receivers);
}
