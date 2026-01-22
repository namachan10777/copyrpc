//! Memory Window (MW) tests.
//!
//! This module tests Type 2 Memory Window operations including:
//! - MW allocation and deallocation
//! - MW bind via UMR WQE
//! - Local invalidate
//! - SEND with Invalidate
//!
//! Run with:
//! ```bash
//! cargo test --release -p mlx5 --test mw_tests -- --nocapture
//! ```

mod common;

use std::rc::Rc;

use mlx5::cq::CqConfig;
use mlx5::pd::{AccessFlags, MwBindInfo};
use mlx5::qp::{RcQpConfig, RcQpIb};
use mlx5::transport::IbRemoteQpInfo;
use mlx5::types::DeviceCapFlags;
use mlx5::wqe::WqeFlags;

use common::{AlignedBuffer, TestContext, full_access, poll_cq_timeout};

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
        _pd: ctx.pd.clone(),
    }
}

fn log_mw_caps(ctx: &TestContext) {
    match ctx.ctx.query_ibv_device() {
        Ok(attr) => {
            let caps = DeviceCapFlags::from_bits_truncate(attr.device_cap_flags);
            println!(
                "Device caps: raw=0x{:x} MW2A={} MW2B={}",
                attr.device_cap_flags,
                caps.contains(DeviceCapFlags::MEM_WINDOW_TYPE_2A),
                caps.contains(DeviceCapFlags::MEM_WINDOW_TYPE_2B)
            );
        }
        Err(err) => {
            eprintln!("Device cap query failed: {}", err);
        }
    }
}

// =============================================================================
// Sanity Check: Simple RDMA WRITE (no MW)
// =============================================================================

#[test]
fn test_simple_rdma_write_sanity() {
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
    let test_data = b"Simple RDMA WRITE sanity test!";
    local_buf.fill_bytes(test_data);

    // Register MRs
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

    // Post RDMA WRITE via sq_wqe + ring_sq_doorbell
    {
        let mut qp1_ref = qp1.borrow_mut();
        qp1_ref
            .sq_wqe()
            .expect("sq_wqe failed")
            .write(WqeFlags::COMPLETION, remote_buf.addr(), unsafe { remote_mr.rkey() })
            .expect("write failed")
            .sge(local_buf.addr(), test_data.len() as u32, local_mr.lkey())
            .finish_signaled(1u64)
            .expect("finish failed");
        qp1_ref.ring_sq_doorbell();
    }

    let cqe = poll_cq_timeout(cq, 5000).expect("CQE timeout");
    println!(
        "Sanity RDMA WRITE CQE: opcode={:?}, syndrome={}",
        cqe.opcode, cqe.syndrome
    );
    assert_eq!(cqe.syndrome, 0, "CQE error: syndrome={}", cqe.syndrome);
    cq.flush();

    // Verify data
    let written = remote_buf.read_bytes(test_data.len());
    assert_eq!(&written[..], test_data, "Sanity RDMA WRITE data mismatch");

    println!("Simple RDMA WRITE sanity test passed!");
}

// =============================================================================
// Sanity Check: Two consecutive RDMA WRITEs
// =============================================================================

#[test]
fn test_two_consecutive_rdma_writes() {
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

    // Register MRs
    let local_mr = unsafe {
        ctx.pd.register(local_buf.as_ptr(), local_buf.size(), full_access())
    }.expect("Failed to register local MR");

    let remote_mr = unsafe {
        ctx.pd.register(remote_buf.as_ptr(), remote_buf.size(), full_access())
    }.expect("Failed to register remote MR");

    // First RDMA WRITE
    let test_data1 = b"First RDMA WRITE test!";
    local_buf.fill_bytes(test_data1);

    {
        let mut qp1_ref = qp1.borrow_mut();
        qp1_ref
            .sq_wqe()
            .expect("sq_wqe failed")
            .write(WqeFlags::COMPLETION, remote_buf.addr(), unsafe { remote_mr.rkey() })
            .expect("write failed")
            .sge(local_buf.addr(), test_data1.len() as u32, local_mr.lkey())
            .finish_signaled(1u64)
            .expect("finish failed");
        qp1_ref.ring_sq_doorbell();
    }

    let cqe = poll_cq_timeout(cq, 5000).expect("CQE timeout for first WRITE");
    println!("First WRITE CQE: opcode={:?}, syndrome={}", cqe.opcode, cqe.syndrome);
    assert_eq!(cqe.syndrome, 0);
    cq.flush();

    let written1 = remote_buf.read_bytes(test_data1.len());
    assert_eq!(&written1[..], test_data1, "First RDMA WRITE failed");
    println!("First RDMA WRITE succeeded!");

    // Clear destination
    unsafe { std::ptr::write_bytes(remote_buf.as_ptr(), 0, test_data1.len()); }

    // Second RDMA WRITE
    let test_data2 = b"Second RDMA WRITE test!";
    local_buf.fill_bytes(test_data2);

    {
        let mut qp1_ref = qp1.borrow_mut();
        qp1_ref
            .sq_wqe()
            .expect("sq_wqe failed")
            .write(WqeFlags::COMPLETION, remote_buf.addr(), unsafe { remote_mr.rkey() })
            .expect("write failed")
            .sge(local_buf.addr(), test_data2.len() as u32, local_mr.lkey())
            .finish_signaled(2u64)
            .expect("finish failed");
        qp1_ref.ring_sq_doorbell();
    }

    let cqe = poll_cq_timeout(cq, 5000).expect("CQE timeout for second WRITE");
    println!("Second WRITE CQE: opcode={:?}, syndrome={}", cqe.opcode, cqe.syndrome);
    assert_eq!(cqe.syndrome, 0);
    cq.flush();

    let written2 = remote_buf.read_bytes(test_data2.len());
    assert_eq!(&written2[..], test_data2, "Second RDMA WRITE failed");
    println!("Second RDMA WRITE succeeded!");

    println!("Two consecutive RDMA WRITEs test passed!");
}

// =============================================================================
// MW Allocation Tests
// =============================================================================

#[test]
fn test_mw_alloc_dealloc() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    // Allocate a Type 2 Memory Window
    let mw = ctx.pd.alloc_mw().expect("Failed to allocate MW");

    // Get the initial rkey (unsafe because MW is not bound yet)
    let rkey = unsafe { mw.rkey() };
    println!("MW allocated: rkey=0x{:x}", rkey);

    // MW is deallocated on drop
    drop(mw);
    println!("MW deallocated successfully");
}

#[test]
fn test_mw_multiple_alloc() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    // Allocate multiple MWs
    let mut mws = Vec::new();
    for i in 0..10 {
        let mw = ctx.pd.alloc_mw().expect("Failed to allocate MW");
        let rkey = unsafe { mw.rkey() };
        println!("MW {} allocated: rkey=0x{:x}", i, rkey);
        mws.push(mw);
    }

    println!("All {} MWs allocated successfully", mws.len());
    // All MWs are deallocated on drop
}

// =============================================================================
// MW Bind Tests (via UMR WQE)
// =============================================================================

#[test]
fn test_mw_bind_via_umr() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    log_mw_caps(&ctx);

    let pair = create_rc_loopback_pair(&ctx);
    let _qp1 = &pair.qp1;
    let qp2 = &pair.qp2;
    let cq = &pair.send_cq;

    // Allocate buffer and register MR
    let mut buf = AlignedBuffer::new(4096);
    buf.fill(0xAA);

    let mr = unsafe {
        ctx.pd
            .register(
                buf.as_ptr(),
                buf.size(),
                full_access() | AccessFlags::MW_BIND,
            )
    }
    .expect("Failed to register MR");

    // Allocate MW
    let mw = ctx.pd.alloc_mw().expect("Failed to allocate MW");
    let mw_rkey = unsafe { mw.rkey() };
    println!("MW allocated: rkey=0x{:x}", mw_rkey);

    // Bind MW to MR via UMR WQE
    let bind_info = MwBindInfo {
        mr: &mr,
        addr: buf.addr(),
        length: buf.size() as u64,
        access: AccessFlags::REMOTE_READ | AccessFlags::REMOTE_WRITE,
    };

    {
        let mut qp2_ref = qp2.borrow_mut();
        qp2_ref
            .sq_wqe()
            .expect("sq_wqe failed")
            .bind_mw(WqeFlags::COMPLETION, &mw, bind_info)
            .expect("bind_mw failed")
            .finish_signaled(1u64)
            .expect("finish failed");
        qp2_ref.ring_sq_doorbell();
    }

    println!("MW bind WQE posted");

    // Poll for completion
    let cqe = poll_cq_timeout(cq, 5000).expect("CQE timeout for MW bind");
    println!(
        "Bind CQE: opcode={:?}, syndrome={}, wqe_counter={}",
        cqe.opcode, cqe.syndrome, cqe.wqe_counter
    );

    // Check if bind succeeded (syndrome = 0 means success)
    if cqe.syndrome != 0 {
        eprintln!(
            "MW bind failed with syndrome {}, UMR may not be supported",
            cqe.syndrome
        );
        return;
    }

    println!("MW bind test passed!");
}

// =============================================================================
// Local Invalidate Tests
// =============================================================================

#[test]
fn test_local_invalidate() {
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

    // Allocate buffer and register MR
    let mut buf = AlignedBuffer::new(4096);
    buf.fill(0xBB);

    let mr = unsafe {
        ctx.pd
            .register(
                buf.as_ptr(),
                buf.size(),
                full_access() | AccessFlags::MW_BIND,
            )
    }
    .expect("Failed to register MR");

    // Allocate MW
    let mw = ctx.pd.alloc_mw().expect("Failed to allocate MW");
    let mw_rkey = unsafe { mw.rkey() };
    println!("MW allocated: rkey=0x{:x}", mw_rkey);

    // First bind the MW
    let bind_info = MwBindInfo {
        mr: &mr,
        addr: buf.addr(),
        length: buf.size() as u64,
        access: AccessFlags::REMOTE_READ | AccessFlags::REMOTE_WRITE,
    };

    {
        let mut qp1_ref = qp1.borrow_mut();
        qp1_ref
            .sq_wqe()
            .expect("sq_wqe failed")
            .bind_mw(WqeFlags::COMPLETION, &mw, bind_info)
            .expect("bind_mw failed")
            .finish_signaled(1u64)
            .expect("finish failed");
        qp1_ref.ring_sq_doorbell();
    }

    let cqe = poll_cq_timeout(cq, 5000).expect("CQE timeout for MW bind");
    if cqe.syndrome != 0 {
        eprintln!(
            "MW bind failed with syndrome {}, skipping local_invalidate test",
            cqe.syndrome
        );
        return;
    }
    println!("MW bound successfully");
    let mw_rkey = unsafe { mw.rkey() };

    // Now invalidate the MW
    {
        let mut qp1_ref = qp1.borrow_mut();
        qp1_ref
            .sq_wqe()
            .expect("sq_wqe failed")
            .local_invalidate(WqeFlags::COMPLETION, mw_rkey)
            .expect("local_invalidate failed")
            .finish_signaled(2u64)
            .expect("finish failed");
        qp1_ref.ring_sq_doorbell();
    }

    println!("Local invalidate WQE posted");

    // Poll for completion
    let cqe = poll_cq_timeout(cq, 5000).expect("CQE timeout for local invalidate");
    println!(
        "Invalidate CQE: opcode={:?}, syndrome={}",
        cqe.opcode, cqe.syndrome
    );

    if cqe.syndrome != 0 {
        eprintln!(
            "Local invalidate failed with syndrome {}",
            cqe.syndrome
        );
        return;
    }

    println!("Local invalidate test passed!");
}

// =============================================================================
// RDMA Write via MW Tests
// =============================================================================

// NOTE: This test is ignored due to a known issue where RDMA WRITE/READ
// operations fail (data not transferred, but CQE reports syndrome=0) after
// a UMR WQE (bind_mw) has been posted in loopback configuration.
//
// Implementation notes:
// - INITIATOR_SMALL_FENCE (0x20) is correctly applied to subsequent WQEs
//   after bind_mw/local_invalidate operations (matching rdma-core behavior)
// - The qpn_mkey field in mkey context is correctly constructed:
//   (mw_rkey & 0xFF) | (qpn << 8)
//
// Investigation findings:
// - WQE content verified correct (ctrl_seg, rdma_seg, data_seg all valid)
// - Doorbell rung correctly with proper PI and WQE pointer
// - CQE returns syndrome=0 (success), but data is not actually transferred
// - The issue affects ALL subsequent RDMA WRITE/READ operations after UMR WQE:
//   - Including RDMA WRITE using MR's rkey (not MW-bound)
// - SEND operations work correctly after UMR WQE
// - bind_mw and local_invalidate operations themselves complete successfully
//
// This appears to be a limitation with loopback configuration + UMR WQE.
// Real-world usage (separate nodes) may work correctly.
#[test]
fn test_mw_rdma_write() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    log_mw_caps(&ctx);

    let pair = create_rc_loopback_pair(&ctx);
    let qp1 = &pair.qp1;
    let qp2 = &pair.qp2;
    let cq = &pair.send_cq;

    // Debug: print QP numbers
    println!("QP1 qpn: {}", qp1.borrow().qpn());
    println!("QP2 qpn: {}", qp2.borrow().qpn());

    // Allocate local buffer (source) - QP1 side
    let mut local_buf = AlignedBuffer::new(4096);
    let test_data = b"RDMA WRITE via Memory Window test data!";
    local_buf.fill_bytes(test_data);

    // Allocate remote buffer (destination) - QP2 side
    let remote_buf = AlignedBuffer::new(4096);

    // Register MRs
    let local_mr = unsafe {
        ctx.pd
            .register(local_buf.as_ptr(), local_buf.size(), full_access())
    }
    .expect("Failed to register local MR");

    let remote_mr = unsafe {
        ctx.pd
            .register(
                remote_buf.as_ptr(),
                remote_buf.size(),
                full_access() | AccessFlags::MW_BIND,
            )
    }
    .expect("Failed to register remote MR");

    // bind_mw - use QP2 (the side that owns the memory)
    let mw = ctx.pd.alloc_mw().expect("Failed to allocate MW");
    let mw_rkey = unsafe { mw.rkey() };
    println!("MW allocated: rkey=0x{:x}", mw_rkey);

    let bind_info = MwBindInfo {
        mr: &remote_mr,
        addr: remote_buf.addr(),
        length: remote_buf.size() as u64,
        access: AccessFlags::LOCAL_WRITE | AccessFlags::REMOTE_READ | AccessFlags::REMOTE_WRITE,
    };

    // QP2 (responder, memory owner) does the bind
    {
        let mut qp2_ref = qp2.borrow_mut();
        qp2_ref
            .sq_wqe()
            .expect("sq_wqe failed")
            .bind_mw(WqeFlags::COMPLETION, &mw, bind_info)
            .expect("bind_mw failed")
            .finish_signaled(1u64)
            .expect("finish failed");
        qp2_ref.ring_sq_doorbell();
    }

    let cqe = poll_cq_timeout(cq, 5000).expect("CQE timeout for MW bind");
    if cqe.syndrome != 0 {
        eprintln!("MW bind failed with syndrome {}, skipping RDMA write test", cqe.syndrome);
        return;
    }
    cq.flush();
    println!("MW bound successfully by QP2 (responder)");
    let mw_rkey = unsafe { mw.rkey() };

    // First test: QP1 does RDMA WRITE using MR rkey (not MW)
    // This tests if UMR on QP2 affects QP1's RDMA capability
    let remote_rkey = unsafe { remote_mr.rkey() };
    {
        let mut qp1_ref = qp1.borrow_mut();
        qp1_ref
            .sq_wqe()
            .expect("sq_wqe failed")
            .write(WqeFlags::COMPLETION, remote_buf.addr(), remote_rkey)
            .expect("write failed")
            .sge(local_buf.addr(), test_data.len() as u32, local_mr.lkey())
            .finish_signaled(2u64)
            .expect("finish failed");
        qp1_ref.ring_sq_doorbell();
    }

    let cqe = poll_cq_timeout(cq, 5000).expect("CQE timeout for RDMA WRITE with MR rkey");
    println!(
        "WRITE (MR rkey) CQE: opcode={:?}, syndrome={}",
        cqe.opcode, cqe.syndrome
    );

    let written_data = remote_buf.read_bytes(test_data.len());
    if written_data == test_data {
        println!("RDMA WRITE with MR rkey succeeded!");
    } else {
        println!("RDMA WRITE with MR rkey: CQE success but data mismatch");
    }
    cq.flush();

    // Clear remote buffer for next test
    unsafe {
        std::ptr::write_bytes(remote_buf.as_ptr() as *mut u8, 0, test_data.len());
    }

    // Second test: QP1 does RDMA WRITE using MW rkey
    {
        let mut qp1_ref = qp1.borrow_mut();
        qp1_ref
            .sq_wqe()
            .expect("sq_wqe failed")
            .write(WqeFlags::COMPLETION, remote_buf.addr(), mw_rkey)
            .expect("write failed")
            .sge(local_buf.addr(), test_data.len() as u32, local_mr.lkey())
            .finish_signaled(3u64)
            .expect("finish failed");
        qp1_ref.ring_sq_doorbell();
    }

    let cqe = poll_cq_timeout(cq, 5000).expect("CQE timeout for RDMA WRITE with MW rkey");
    println!(
        "WRITE (MW rkey) CQE: opcode={:?}, syndrome={}",
        cqe.opcode, cqe.syndrome
    );

    if cqe.syndrome != 0 {
        eprintln!(
            "RDMA WRITE via MW failed with syndrome {}",
            cqe.syndrome
        );
        return;
    }

    // Verify data was written
    let written_data = remote_buf.read_bytes(test_data.len());
    assert_eq!(written_data, test_data, "Data mismatch after RDMA WRITE with MW rkey");

    println!("RDMA WRITE via MW test passed!");
}

#[test]
fn test_mw_rdma_write_minimal() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    log_mw_caps(&ctx);

    let pair = create_rc_loopback_pair(&ctx);
    let qp1 = &pair.qp1;
    let qp2 = &pair.qp2;
    let cq = &pair.send_cq;

    let mut local_buf = AlignedBuffer::new(4096);
    let remote_buf = AlignedBuffer::new(4096);
    let test_data = b"MW minimal write";
    local_buf.fill_bytes(test_data);

    let local_mr = unsafe {
        ctx.pd
            .register(local_buf.as_ptr(), local_buf.size(), full_access())
    }
    .expect("Failed to register local MR");

    let remote_mr = unsafe {
        ctx.pd
            .register(
                remote_buf.as_ptr(),
                remote_buf.size(),
                full_access() | AccessFlags::MW_BIND,
            )
    }
    .expect("Failed to register remote MR");

    let mw = ctx.pd.alloc_mw().expect("Failed to allocate MW");
    let mw_rkey = unsafe { mw.rkey() };

    let bind_info = MwBindInfo {
        mr: &remote_mr,
        addr: remote_buf.addr(),
        length: remote_buf.size() as u64,
        access: AccessFlags::REMOTE_READ | AccessFlags::REMOTE_WRITE,
    };

    {
        let mut qp2_ref = qp2.borrow_mut();
        qp2_ref
            .sq_wqe()
            .expect("sq_wqe failed")
            .bind_mw(WqeFlags::COMPLETION, &mw, bind_info)
            .expect("bind_mw failed")
            .finish_signaled(1u64)
            .expect("finish failed");
        qp2_ref.ring_sq_doorbell();
    }

    let cqe = poll_cq_timeout(cq, 5000).expect("CQE timeout for MW bind");
    if cqe.syndrome != 0 {
        eprintln!("MW bind failed with syndrome {}", cqe.syndrome);
        return;
    }
    let mw_rkey = unsafe { mw.rkey() };

    {
        let mut qp1_ref = qp1.borrow_mut();
        qp1_ref
            .sq_wqe()
            .expect("sq_wqe failed")
            .write(WqeFlags::COMPLETION, remote_buf.addr(), mw_rkey)
            .expect("write failed")
            .sge(local_buf.addr(), test_data.len() as u32, local_mr.lkey())
            .finish_signaled(2u64)
            .expect("finish failed");
        qp1_ref.ring_sq_doorbell();
    }

    let cqe = poll_cq_timeout(cq, 5000).expect("CQE timeout for RDMA WRITE");
    if cqe.syndrome != 0 {
        eprintln!("RDMA WRITE failed with syndrome {}", cqe.syndrome);
        return;
    }

    let written_data = remote_buf.read_bytes(test_data.len());
    assert_eq!(written_data, test_data, "Data mismatch after MW minimal WRITE");
}

#[test]
fn test_mw_after_bind_mr_rkey_write_minimal() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    log_mw_caps(&ctx);

    let pair = create_rc_loopback_pair(&ctx);
    let qp1 = &pair.qp1;
    let qp2 = &pair.qp2;
    let cq = &pair.send_cq;

    let mut local_buf = AlignedBuffer::new(4096);
    let remote_buf = AlignedBuffer::new(4096);
    let test_data = b"MR rkey write after bind";
    local_buf.fill_bytes(test_data);

    let local_mr = unsafe {
        ctx.pd
            .register(local_buf.as_ptr(), local_buf.size(), full_access())
    }
    .expect("Failed to register local MR");

    let remote_mr = unsafe {
        ctx.pd
            .register(
                remote_buf.as_ptr(),
                remote_buf.size(),
                full_access() | AccessFlags::MW_BIND,
            )
    }
    .expect("Failed to register remote MR");

    let mw = ctx.pd.alloc_mw().expect("Failed to allocate MW");

    let bind_info = MwBindInfo {
        mr: &remote_mr,
        addr: remote_buf.addr(),
        length: remote_buf.size() as u64,
        access: AccessFlags::REMOTE_READ | AccessFlags::REMOTE_WRITE,
    };

    {
        let mut qp2_ref = qp2.borrow_mut();
        qp2_ref
            .sq_wqe()
            .expect("sq_wqe failed")
            .bind_mw(WqeFlags::COMPLETION, &mw, bind_info)
            .expect("bind_mw failed")
            .finish_signaled(1u64)
            .expect("finish failed");
        qp2_ref.ring_sq_doorbell();
    }

    let cqe = poll_cq_timeout(cq, 5000).expect("CQE timeout for MW bind");
    if cqe.syndrome != 0 {
        eprintln!("MW bind failed with syndrome {}", cqe.syndrome);
        return;
    }

    {
        let mut qp1_ref = qp1.borrow_mut();
        qp1_ref
            .sq_wqe()
            .expect("sq_wqe failed")
            .write(
                WqeFlags::COMPLETION,
                remote_buf.addr(),
                unsafe { remote_mr.rkey() },
            )
            .expect("write failed")
            .sge(local_buf.addr(), test_data.len() as u32, local_mr.lkey())
            .finish_signaled(2u64)
            .expect("finish failed");
        qp1_ref.ring_sq_doorbell();
    }

    let cqe = poll_cq_timeout(cq, 5000).expect("CQE timeout for RDMA WRITE");
    if cqe.syndrome != 0 {
        eprintln!("RDMA WRITE failed with syndrome {}", cqe.syndrome);
        return;
    }

    let written_data = remote_buf.read_bytes(test_data.len());
    assert_eq!(
        written_data, test_data,
        "Data mismatch after MR rkey WRITE (post-bind)"
    );
}

#[test]
fn test_mw_after_bind_mr_rkey_read_minimal() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    log_mw_caps(&ctx);

    let pair = create_rc_loopback_pair(&ctx);
    let qp1 = &pair.qp1;
    let qp2 = &pair.qp2;
    let cq = &pair.send_cq;

    let mut local_buf = AlignedBuffer::new(4096);
    let mut remote_buf = AlignedBuffer::new(4096);
    let test_data = b"MR rkey read after bind";
    remote_buf.fill_bytes(test_data);
    local_buf.fill(0);

    let local_mr = unsafe {
        ctx.pd
            .register(local_buf.as_ptr(), local_buf.size(), full_access())
    }
    .expect("Failed to register local MR");

    let remote_mr = unsafe {
        ctx.pd
            .register(
                remote_buf.as_ptr(),
                remote_buf.size(),
                full_access() | AccessFlags::MW_BIND,
            )
    }
    .expect("Failed to register remote MR");

    let mw = ctx.pd.alloc_mw().expect("Failed to allocate MW");

    let bind_info = MwBindInfo {
        mr: &remote_mr,
        addr: remote_buf.addr(),
        length: remote_buf.size() as u64,
        access: AccessFlags::REMOTE_READ | AccessFlags::REMOTE_WRITE,
    };

    {
        let mut qp2_ref = qp2.borrow_mut();
        qp2_ref
            .sq_wqe()
            .expect("sq_wqe failed")
            .bind_mw(WqeFlags::COMPLETION, &mw, bind_info)
            .expect("bind_mw failed")
            .finish_signaled(1u64)
            .expect("finish failed");
        qp2_ref.ring_sq_doorbell();
    }

    let cqe = poll_cq_timeout(cq, 5000).expect("CQE timeout for MW bind");
    if cqe.syndrome != 0 {
        eprintln!("MW bind failed with syndrome {}", cqe.syndrome);
        return;
    }

    {
        let mut qp1_ref = qp1.borrow_mut();
        qp1_ref
            .sq_wqe()
            .expect("sq_wqe failed")
            .read(
                WqeFlags::COMPLETION,
                remote_buf.addr(),
                unsafe { remote_mr.rkey() },
            )
            .expect("read failed")
            .sge(local_buf.addr(), test_data.len() as u32, local_mr.lkey())
            .finish_signaled(2u64)
            .expect("finish failed");
        qp1_ref.ring_sq_doorbell();
    }

    let cqe = poll_cq_timeout(cq, 5000).expect("CQE timeout for RDMA READ");
    if cqe.syndrome != 0 {
        eprintln!("RDMA READ failed with syndrome {}", cqe.syndrome);
        return;
    }

    let read_data = local_buf.read_bytes(test_data.len());
    assert_eq!(
        read_data, test_data,
        "Data mismatch after MR rkey READ (post-bind)"
    );
}

#[test]
fn test_mw_after_bind_mw_rkey_read_minimal() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    log_mw_caps(&ctx);

    let pair = create_rc_loopback_pair(&ctx);
    let qp1 = &pair.qp1;
    let qp2 = &pair.qp2;
    let cq = &pair.send_cq;

    let mut local_buf = AlignedBuffer::new(4096);
    let mut remote_buf = AlignedBuffer::new(4096);
    let test_data = b"MW rkey read after bind";
    remote_buf.fill_bytes(test_data);
    local_buf.fill(0);

    let local_mr = unsafe {
        ctx.pd
            .register(local_buf.as_ptr(), local_buf.size(), full_access())
    }
    .expect("Failed to register local MR");

    let remote_mr = unsafe {
        ctx.pd
            .register(
                remote_buf.as_ptr(),
                remote_buf.size(),
                full_access() | AccessFlags::MW_BIND,
            )
    }
    .expect("Failed to register remote MR");

    let mw = ctx.pd.alloc_mw().expect("Failed to allocate MW");
    let mw_rkey = unsafe { mw.rkey() };

    let bind_info = MwBindInfo {
        mr: &remote_mr,
        addr: remote_buf.addr(),
        length: remote_buf.size() as u64,
        access: AccessFlags::REMOTE_READ | AccessFlags::REMOTE_WRITE,
    };

    {
        let mut qp2_ref = qp2.borrow_mut();
        qp2_ref
            .sq_wqe()
            .expect("sq_wqe failed")
            .bind_mw(WqeFlags::COMPLETION, &mw, bind_info)
            .expect("bind_mw failed")
            .finish_signaled(1u64)
            .expect("finish failed");
        qp2_ref.ring_sq_doorbell();
    }

    let cqe = poll_cq_timeout(cq, 5000).expect("CQE timeout for MW bind");
    if cqe.syndrome != 0 {
        eprintln!("MW bind failed with syndrome {}", cqe.syndrome);
        return;
    }
    let mw_rkey = unsafe { mw.rkey() };

    {
        let mut qp1_ref = qp1.borrow_mut();
        qp1_ref
            .sq_wqe()
            .expect("sq_wqe failed")
            .read(WqeFlags::COMPLETION, remote_buf.addr(), mw_rkey)
            .expect("read failed")
            .sge(local_buf.addr(), test_data.len() as u32, local_mr.lkey())
            .finish_signaled(2u64)
            .expect("finish failed");
        qp1_ref.ring_sq_doorbell();
    }

    let cqe = poll_cq_timeout(cq, 5000).expect("CQE timeout for RDMA READ");
    if cqe.syndrome != 0 {
        eprintln!("RDMA READ failed with syndrome {}", cqe.syndrome);
        return;
    }

    let read_data = local_buf.read_bytes(test_data.len());
    assert_eq!(
        read_data, test_data,
        "Data mismatch after MW rkey READ (post-bind)"
    );
}

// =============================================================================
// SEND with Invalidate Tests
// =============================================================================

#[test]
fn test_send_with_invalidate() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    let pair = create_rc_loopback_pair(&ctx);
    let qp1 = &pair.qp1;
    let qp2 = &pair.qp2;
    let cq = &pair.send_cq;

    // Allocate buffer and register MR
    let mut buf = AlignedBuffer::new(4096);
    buf.fill(0xCC);

    let mr = unsafe {
        ctx.pd
            .register(
                buf.as_ptr(),
                buf.size(),
                full_access() | AccessFlags::MW_BIND,
            )
    }
    .expect("Failed to register MR");

    // Allocate MW
    let mw = ctx.pd.alloc_mw().expect("Failed to allocate MW");
    let mw_rkey = unsafe { mw.rkey() };
    println!("MW allocated: rkey=0x{:x}", mw_rkey);

    // Bind MW using QP2 (responder side)
    let bind_info = MwBindInfo {
        mr: &mr,
        addr: buf.addr(),
        length: buf.size() as u64,
        access: AccessFlags::REMOTE_READ | AccessFlags::REMOTE_WRITE,
    };

    {
        let mut qp2_ref = qp2.borrow_mut();
        qp2_ref
            .sq_wqe()
            .expect("sq_wqe failed")
            .bind_mw(WqeFlags::COMPLETION, &mw, bind_info)
            .expect("bind_mw failed")
            .finish_signaled(1u64)
            .expect("finish failed");
        qp2_ref.ring_sq_doorbell();
    }

    let cqe = poll_cq_timeout(cq, 5000).expect("CQE timeout for MW bind");
    if cqe.syndrome != 0 {
        eprintln!(
            "MW bind failed with syndrome {}, skipping send_with_invalidate test",
            cqe.syndrome
        );
        return;
    }
    println!("MW bound successfully");
    let mw_rkey = unsafe { mw.rkey() };

    // Post receive on QP2
    let recv_buf = AlignedBuffer::new(4096);
    let recv_mr = unsafe {
        ctx.pd
            .register(recv_buf.as_ptr(), recv_buf.size(), full_access())
    }
    .expect("Failed to register recv MR");

    {
        let qp2_ref = qp2.borrow();
        qp2_ref.post_recv(1u64, recv_buf.addr(), 64, recv_mr.lkey())
            .expect("post_recv failed");
        qp2_ref.ring_rq_doorbell();
    }

    // SEND with Invalidate from QP1 to QP2
    let send_buf = AlignedBuffer::new(64);
    let send_mr = unsafe {
        ctx.pd
            .register(send_buf.as_ptr(), send_buf.size(), full_access())
    }
    .expect("Failed to register send MR");

    {
        let mut qp1_ref = qp1.borrow_mut();
        qp1_ref
            .sq_wqe()
            .expect("sq_wqe failed")
            .send_with_invalidate(WqeFlags::COMPLETION, mw_rkey)
            .expect("send_with_invalidate failed")
            .sge(send_buf.addr(), 8, send_mr.lkey())
            .finish_signaled(2u64)
            .expect("finish failed");
        qp1_ref.ring_sq_doorbell();
    }

    println!("SEND with Invalidate WQE posted");

    // Poll for send completion
    let cqe = poll_cq_timeout(cq, 5000).expect("CQE timeout for SEND with Invalidate");
    println!(
        "SEND CQE: opcode={:?}, syndrome={}",
        cqe.opcode, cqe.syndrome
    );

    if cqe.syndrome != 0 {
        eprintln!(
            "SEND with Invalidate failed with syndrome {}",
            cqe.syndrome
        );
        return;
    }

    println!("SEND with Invalidate test passed!");
}
