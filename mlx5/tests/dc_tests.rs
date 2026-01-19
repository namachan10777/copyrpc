//! DC (Dynamically Connected) transport tests.
//!
//! This module tests DC operations:
//! - DCI/DCT creation
//! - DCI → DCT SEND with RDMA WRITE
//! - DCI → DCT RDMA READ
//!
//! DC is a scalable connectionless transport where:
//! - DCI (DC Initiator) sends RDMA operations to DCTs
//! - DCT (DC Target) receives via an SRQ
//!
//! Run with:
//! ```bash
//! cargo test --release -p mlx5 --test dc_tests -- --nocapture
//! ```

mod common;

use std::rc::Rc;

use mlx5::dc::{DciConfig, DctConfig};
use mlx5::srq::SrqConfig;
use mlx5::wqe::{WqeFlags, WqeOpcode};

use common::{full_access, poll_cq_timeout, AlignedBuffer, TestContext};

// =============================================================================
// DC Creation Tests
// =============================================================================

#[test]
fn test_dc_creation() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    require_dct!(&ctx);

    // Create CQ for DCI
    let dci_cq = Rc::new(
        ctx.ctx.create_cq(256).expect("Failed to create DCI CQ"),
    );

    // Create CQ for DCT (receives go to SRQ completion)
    let dct_cq = ctx.ctx.create_cq(256).expect("Failed to create DCT CQ");

    // Create SRQ for DCT
    let srq_config = SrqConfig {
        max_wr: 128,
        max_sge: 1,
    };
    let srq: mlx5::srq::Srq<()> = ctx
        .pd
        .create_srq(&srq_config)
        .expect("Failed to create SRQ");

    // Create DCI
    let dci_config = DciConfig::default();
    let dci = ctx
        .ctx
        .create_dci::<u64, _>(&ctx.pd, &dci_cq, &dci_config, |_cqe, _entry| {})
        .expect("Failed to create DCI");

    // Activate DCI
    dci.borrow_mut()
        .activate(ctx.port, 0, 4)
        .expect("Failed to activate DCI");

    // Create DCT
    let dct_config = DctConfig { dc_key: 0x12345 };
    let mut dct = ctx
        .ctx
        .create_dct(&ctx.pd, &srq, &dct_cq, &dct_config)
        .expect("Failed to create DCT");

    // Activate DCT
    let access = full_access().bits();
    dct.activate(ctx.port, access, 4)
        .expect("Failed to activate DCT");

    println!("DC creation test passed!");
    println!("  DCI QPN: 0x{:x}", dci.borrow().qpn());
    println!("  DCT DCTN: 0x{:x}", dct.dctn());
    println!("  DCT DC_KEY: 0x{:x}", dct.dc_key());
}

// =============================================================================
// DC RDMA WRITE Tests
// =============================================================================

#[test]
fn test_dc_rdma_write() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    require_dct!(&ctx);

    // Create CQ for DCI
    let mut dci_cq = ctx.ctx.create_cq(256).expect("Failed to create DCI CQ");
    dci_cq
        .init_direct_access()
        .expect("Failed to init DCI CQ direct access");
    let dci_cq = Rc::new(dci_cq);

    // Create CQ for DCT
    let dct_cq = ctx.ctx.create_cq(256).expect("Failed to create DCT CQ");

    // Create SRQ for DCT
    let srq_config = SrqConfig {
        max_wr: 128,
        max_sge: 1,
    };
    let srq: mlx5::srq::Srq<()> = ctx
        .pd
        .create_srq(&srq_config)
        .expect("Failed to create SRQ");

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
    let dc_key: u64 = 0xDEADBEEF;
    let dct_config = DctConfig { dc_key };
    let mut dct = ctx
        .ctx
        .create_dct(&ctx.pd, &srq, &dct_cq, &dct_config)
        .expect("Failed to create DCT");
    let access = full_access().bits();
    dct.activate(ctx.port, access, 4)
        .expect("Failed to activate DCT");

    // Get DCT info
    let dctn = dct.dctn();
    let dlid = ctx.port_attr.lid;

    // Allocate buffers
    let mut local_buf = AlignedBuffer::new(4096);
    let mut remote_buf = AlignedBuffer::new(4096);

    // Register memory regions
    let local_mr = unsafe { ctx.pd.register(local_buf.as_ptr(), local_buf.size(), full_access()) }
        .expect("Failed to register local MR");
    let remote_mr =
        unsafe { ctx.pd.register(remote_buf.as_ptr(), remote_buf.size(), full_access()) }
            .expect("Failed to register remote MR");

    // Prepare test data
    let test_data = b"DC RDMA WRITE test - connectionless one-sided write!";
    local_buf.fill_bytes(test_data);
    remote_buf.fill(0);

    // Post RDMA WRITE via DCI
    dci.borrow_mut()
        .wqe_builder(1u64)
        .expect("wqe_builder failed")
        .ctrl(WqeOpcode::RdmaWrite, WqeFlags::empty(), 0)
        .av(dc_key, dctn, dlid)
        .rdma(remote_buf.addr(), remote_mr.rkey())
        .sge(local_buf.addr(), test_data.len() as u32, local_mr.lkey())
        .finish_with_blueflame();

    // Poll CQ
    let cqe = poll_cq_timeout(&dci_cq, 5000).expect("CQE timeout");
    assert_eq!(cqe.syndrome, 0, "CQE error: syndrome={}", cqe.syndrome);
    dci_cq.flush();

    // Verify data
    let written = remote_buf.read_bytes(test_data.len());
    assert_eq!(&written[..], test_data, "DC RDMA WRITE data mismatch");

    println!("DC RDMA WRITE test passed!");
}

// =============================================================================
// DC RDMA READ Tests
// =============================================================================

#[test]
fn test_dc_rdma_read() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    require_dct!(&ctx);

    // Create CQ for DCI
    let mut dci_cq = ctx.ctx.create_cq(256).expect("Failed to create DCI CQ");
    dci_cq
        .init_direct_access()
        .expect("Failed to init DCI CQ direct access");
    let dci_cq = Rc::new(dci_cq);

    // Create CQ for DCT
    let dct_cq = ctx.ctx.create_cq(256).expect("Failed to create DCT CQ");

    // Create SRQ for DCT
    let srq_config = SrqConfig {
        max_wr: 128,
        max_sge: 1,
    };
    let srq: mlx5::srq::Srq<()> = ctx
        .pd
        .create_srq(&srq_config)
        .expect("Failed to create SRQ");

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
    let dc_key: u64 = 0xCAFEBABE;
    let dct_config = DctConfig { dc_key };
    let mut dct = ctx
        .ctx
        .create_dct(&ctx.pd, &srq, &dct_cq, &dct_config)
        .expect("Failed to create DCT");
    let access = full_access().bits();
    dct.activate(ctx.port, access, 4)
        .expect("Failed to activate DCT");

    // Get DCT info
    let dctn = dct.dctn();
    let dlid = ctx.port_attr.lid;

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
    let test_data = b"DC RDMA READ test - connectionless one-sided read!";
    remote_buf.fill_bytes(test_data);
    local_buf.fill(0);

    // Post RDMA READ via DCI
    dci.borrow_mut()
        .wqe_builder(1u64)
        .expect("wqe_builder failed")
        .ctrl(WqeOpcode::RdmaRead, WqeFlags::empty(), 0)
        .av(dc_key, dctn, dlid)
        .rdma(remote_buf.addr(), remote_mr.rkey())
        .sge(local_buf.addr(), test_data.len() as u32, local_mr.lkey())
        .finish_with_blueflame();

    // Poll CQ
    let cqe = poll_cq_timeout(&dci_cq, 5000).expect("CQE timeout");
    assert_eq!(cqe.syndrome, 0, "CQE error: syndrome={}", cqe.syndrome);
    dci_cq.flush();

    // Verify data
    let read_data = local_buf.read_bytes(test_data.len());
    assert_eq!(&read_data[..], test_data, "DC RDMA READ data mismatch");

    println!("DC RDMA READ test passed!");
}

// =============================================================================
// Multiple DCI to Single DCT Tests
// =============================================================================

#[test]
fn test_dc_multiple_dci() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    require_dct!(&ctx);

    // Create shared CQ for all DCIs
    let mut dci_cq = ctx.ctx.create_cq(256).expect("Failed to create DCI CQ");
    dci_cq
        .init_direct_access()
        .expect("Failed to init DCI CQ direct access");
    let dci_cq = Rc::new(dci_cq);

    // Create CQ for DCT
    let dct_cq = ctx.ctx.create_cq(256).expect("Failed to create DCT CQ");

    // Create SRQ for DCT
    let srq_config = SrqConfig {
        max_wr: 128,
        max_sge: 1,
    };
    let srq: mlx5::srq::Srq<()> = ctx
        .pd
        .create_srq(&srq_config)
        .expect("Failed to create SRQ");

    // Create and activate multiple DCIs
    let dci_config = DciConfig::default();
    let num_dcis = 3;
    let mut dcis = Vec::new();

    for i in 0..num_dcis {
        let dci = ctx
            .ctx
            .create_dci::<u64, _>(&ctx.pd, &dci_cq, &dci_config, |_cqe, _entry| {})
            .expect(&format!("Failed to create DCI {}", i));
        dci.borrow_mut()
            .activate(ctx.port, 0, 4)
            .expect(&format!("Failed to activate DCI {}", i));
        dcis.push(dci);
    }

    // Create and activate single DCT
    let dc_key: u64 = 0x12345678;
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

    // Allocate buffers - one local per DCI, one shared remote
    let mut local_bufs: Vec<_> = (0..num_dcis).map(|_| AlignedBuffer::new(4096)).collect();
    let mut remote_buf = AlignedBuffer::new(4096);

    // Register memory regions
    let local_mrs: Vec<_> = local_bufs
        .iter()
        .map(|buf| unsafe { ctx.pd.register(buf.as_ptr(), buf.size(), full_access()) }.unwrap())
        .collect();
    let remote_mr =
        unsafe { ctx.pd.register(remote_buf.as_ptr(), remote_buf.size(), full_access()) }
            .expect("Failed to register remote MR");

    // Each DCI writes to a different offset in the remote buffer
    for (i, dci) in dcis.iter().enumerate() {
        let test_data = format!("Data from DCI {}", i);
        let offset = i * 64;
        local_bufs[i].fill_bytes(test_data.as_bytes());

        dci.borrow_mut()
            .wqe_builder((i + 1) as u64)
            .expect("wqe_builder failed")
            .ctrl(WqeOpcode::RdmaWrite, WqeFlags::empty(), 0)
            .av(dc_key, dctn, dlid)
            .rdma(remote_buf.addr() + offset as u64, remote_mr.rkey())
            .sge(
                local_bufs[i].addr(),
                test_data.len() as u32,
                local_mrs[i].lkey(),
            )
            .finish_with_blueflame();

        // Poll for this DCI's completion
        let cqe = poll_cq_timeout(&dci_cq, 5000)
            .expect(&format!("CQE timeout for DCI {}", i));
        assert_eq!(
            cqe.syndrome, 0,
            "CQE error for DCI {}: syndrome={}",
            i, cqe.syndrome
        );
    }
    dci_cq.flush();

    // Verify all data
    for i in 0..num_dcis {
        let expected = format!("Data from DCI {}", i);
        let offset = i * 64;
        let written_ptr = unsafe { remote_buf.as_ptr().add(offset) };
        let written =
            unsafe { std::slice::from_raw_parts(written_ptr, expected.len()) };
        assert_eq!(
            written,
            expected.as_bytes(),
            "Data mismatch for DCI {}",
            i
        );
    }

    println!("DC multiple DCI test passed!");
    println!("  {} DCIs wrote to single DCT", num_dcis);
}
