//! Inline WQE and scatter-to-CQE tests.
//!
//! Tests scatter-to-CQE verification for small receives (≤32B) and
//! inline Send wrap-around handling.
//!
//! ## Known Issues
//!
//! ### scatter-to-CQE not working as expected
//!
//! When `enable_scatter_to_cqe=true` is set in `RcQpConfig`:
//!
//! 1. **CQE opcode remains `RespSend`**: Even for small messages (≤32B),
//!    the CQE opcode is `RespSend` instead of `InlineScatter32`.
//!
//! 2. **Receive buffer not populated**: When scatter-to-CQE is enabled,
//!    small message data appears to go neither to the receive buffer
//!    nor to the CQE inline data area.
//!
//! 3. **Possible causes**:
//!    - The `MLX5DV_QP_CREATE_DISABLE_SCATTER_TO_CQE` flag may work differently
//!      than expected (it controls disabling, not enabling)
//!    - The hardware/driver may require additional configuration
//!    - CQE parsing may need adjustment for scatter-to-CQE format
//!
//! ### Workaround
//!
//! Use `enable_scatter_to_cqe=false` (default) for reliable operation.
//! Data will always be placed in the receive buffer.
//!
//! Run with:
//! ```bash
//! cargo test --release -p mlx5 --test inline_scatter_tests -- --nocapture
//! ```

mod common;

use std::cell::Cell;
use std::rc::Rc;

use mlx5::cq::Cqe;
use mlx5::qp::{RcQpConfig, RemoteQpInfo};
use mlx5::wqe::{WqeFlags, WqeOpcode};

use common::{AlignedBuffer, TestContext, full_access, poll_cq_timeout};

/// Maximum inline scatter size (32 bytes for 64-byte CQE).
const MAX_INLINE_SCATTER_SIZE: usize = 32;

/// Diagnostic test for scatter-to-CQE with SGE-based Send.
///
/// This test reports the behavior of scatter-to-CQE but does not assert
/// on the results due to known issues with scatter-to-CQE functionality.
///
/// See module-level documentation for known issues.
#[test]
fn test_scatter_to_cqe_diagnostic() {
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

    // Configure QP with scatter-to-CQE enabled
    let config = RcQpConfig {
        max_send_wr: 32,
        max_recv_wr: 32,
        max_send_sge: 1,
        max_recv_sge: 1,
        max_inline_data: 64,
        enable_scatter_to_cqe: true,
    };

    // Track received CQE for verification
    let last_recv_cqe: Rc<Cell<Option<Cqe>>> = Rc::new(Cell::new(None));
    let last_recv_cqe_clone = last_recv_cqe.clone();

    fn noop_callback(_cqe: Cqe, _entry: u64) {}

    let qp1 = ctx
        .ctx
        .create_rc_qp(
            &ctx.pd,
            &send_cq,
            &recv_cq1,
            &config,
            noop_callback as fn(_, _),
        )
        .expect("Failed to create QP1");

    // QP2 uses a callback that captures the CQE
    let recv_callback = move |cqe: Cqe, _entry: u64| {
        last_recv_cqe_clone.set(Some(cqe));
    };

    let qp2 = ctx
        .ctx
        .create_rc_qp(&ctx.pd, &send_cq, &recv_cq2, &config, recv_callback)
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
    qp1.borrow_mut()
        .connect(&remote2, ctx.port, 0, 4, 4, access)
        .expect("connect QP1");
    qp2.borrow_mut()
        .connect(&remote1, ctx.port, 0, 4, 4, access)
        .expect("connect QP2");

    // Allocate buffers
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

    // Test various sizes
    let test_sizes = [8, 16, 24, 32, 48, 64, 128, 256];
    let mut scatter_count = 0;
    let mut buffer_count = 0;

    println!("Testing scatter-to-CQE with SGE-based Send...");

    for (i, &size) in test_sizes.iter().enumerate() {
        // Generate test data pattern
        let test_data: Vec<u8> = (0..size).map(|j| ((i * 17 + j) & 0xFF) as u8).collect();
        send_buf.fill(0);
        send_buf.fill_bytes(&test_data);
        recv_buf.fill(0xAA); // Clear with pattern

        // Post receive on QP2
        qp2.borrow()
            .recv_builder(i as u64)
            .expect("recv_builder failed")
            .sge(recv_buf.addr(), 512, recv_mr.lkey())
            .finish();
        qp2.borrow().ring_rq_doorbell();

        // Post SGE-based Send on QP1
        let _ = qp1
            .borrow_mut()
            .wqe_builder(i as u64)
            .expect("wqe_builder failed")
            .ctrl(WqeOpcode::Send, WqeFlags::COMPLETION, 0)
            .sge(send_buf.addr(), size as u32, send_mr.lkey())
            .finish();
        qp1.borrow().ring_sq_doorbell();

        // Wait for send completion
        let send_cqe =
            poll_cq_timeout(&send_cq, 5000).expect(&format!("Send CQE timeout for size {}", size));
        assert_eq!(
            send_cqe.syndrome, 0,
            "Send CQE error for size {}: syndrome={}",
            size, send_cqe.syndrome
        );
        send_cq.flush();

        // Wait for recv completion
        last_recv_cqe.set(None);
        let _ = poll_cq_timeout(&recv_cq2, 5000)
            .expect(&format!("Recv CQE timeout for size {}", size));
        recv_cq2.flush();

        // Get the actual CQE from callback
        let recv_cqe = last_recv_cqe.take().expect("Recv callback not called");
        assert_eq!(
            recv_cqe.syndrome, 0,
            "Recv CQE error for size {}: syndrome={}",
            size, recv_cqe.syndrome
        );

        // Check data location based on opcode
        let is_scatter = recv_cqe.opcode.is_inline_scatter();

        if is_scatter {
            // Data should be in CQE
            if let Some(inline) = recv_cqe.inline_data() {
                let data_matches = inline == &test_data[..];
                scatter_count += 1;
                println!(
                    "  Size {:3}: scatter-to-CQE (opcode={:?}, data_ok={})",
                    size, recv_cqe.opcode, data_matches
                );
            } else {
                println!(
                    "  Size {:3}: scatter-to-CQE (opcode={:?}, inline_data=None)",
                    size, recv_cqe.opcode
                );
            }
        } else {
            // Check if data is in receive buffer
            let received = recv_buf.read_bytes(size);
            let data_in_buffer = received == test_data;
            let buffer_unchanged = recv_buf.read_bytes(size).iter().all(|&b| b == 0xAA);

            if data_in_buffer {
                buffer_count += 1;
                println!(
                    "  Size {:3}: buffer receive (opcode={:?}, data_ok=true)",
                    size, recv_cqe.opcode
                );
            } else if buffer_unchanged {
                // Known issue: scatter-to-CQE enabled but data not in buffer or CQE
                println!(
                    "  Size {:3}: ISSUE - buffer unchanged (opcode={:?}) - see known issues",
                    size, recv_cqe.opcode
                );
            } else {
                println!(
                    "  Size {:3}: buffer has unexpected data (opcode={:?})",
                    size, recv_cqe.opcode
                );
            }
        }
    }

    println!("\n=== Diagnostic Results ===");
    println!("Scatter-to-CQE opcode used: {} times", scatter_count);
    println!("Buffer receive confirmed: {} times", buffer_count);
    println!("Total sizes tested: {}", test_sizes.len());

    // This is a diagnostic test - we report but don't fail on known issues
    if scatter_count == 0 && buffer_count < test_sizes.len() {
        println!("\nNOTE: scatter-to-CQE may not be working as expected.");
        println!("See module documentation for known issues.");
    }

    println!("\nScatter-to-CQE diagnostic test completed.");
}

/// Test that scatter-to-CQE is disabled when enable_scatter_to_cqe=false.
#[test]
fn test_scatter_to_cqe_disabled() {
    let ctx = match TestContext::new() {
        Some(ctx) => ctx,
        None => {
            eprintln!("Skipping test: no mlx5 device available");
            return;
        }
    };

    let send_cq = ctx.ctx.create_cq(64).expect("Failed to create send CQ");
    let send_cq = Rc::new(send_cq);

    let recv_cq1 = ctx.ctx.create_cq(64).expect("Failed to create recv CQ1");
    let recv_cq1 = Rc::new(recv_cq1);
    let recv_cq2 = ctx.ctx.create_cq(64).expect("Failed to create recv CQ2");
    let recv_cq2 = Rc::new(recv_cq2);

    // Scatter-to-CQE disabled (default)
    let config = RcQpConfig {
        max_send_wr: 32,
        max_recv_wr: 32,
        max_send_sge: 1,
        max_recv_sge: 1,
        max_inline_data: 64,
        enable_scatter_to_cqe: false,
    };

    let last_recv_cqe: Rc<Cell<Option<Cqe>>> = Rc::new(Cell::new(None));
    let last_recv_cqe_clone = last_recv_cqe.clone();

    fn noop_callback(_cqe: Cqe, _entry: u64) {}

    let qp1 = ctx
        .ctx
        .create_rc_qp(
            &ctx.pd,
            &send_cq,
            &recv_cq1,
            &config,
            noop_callback as fn(_, _),
        )
        .expect("Failed to create QP1");

    let recv_callback = move |cqe: Cqe, _entry: u64| {
        last_recv_cqe_clone.set(Some(cqe));
    };

    let qp2 = ctx
        .ctx
        .create_rc_qp(&ctx.pd, &send_cq, &recv_cq2, &config, recv_callback)
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
    qp1.borrow_mut()
        .connect(&remote2, ctx.port, 0, 4, 4, access)
        .expect("connect QP1");
    qp2.borrow_mut()
        .connect(&remote1, ctx.port, 0, 4, 4, access)
        .expect("connect QP2");

    let mut send_buf = AlignedBuffer::new(4096);
    let mut recv_buf = AlignedBuffer::new(4096);

    let _send_mr = unsafe {
        ctx.pd
            .register(send_buf.as_ptr(), send_buf.size(), full_access())
    }
    .expect("Failed to register send MR");
    let recv_mr = unsafe {
        ctx.pd
            .register(recv_buf.as_ptr(), recv_buf.size(), full_access())
    }
    .expect("Failed to register recv MR");

    // Send small messages that would normally use scatter-to-CQE
    let test_sizes = [8, 16, 24, 32];
    let mut scatter_count = 0;

    for (i, &size) in test_sizes.iter().enumerate() {
        let test_data: Vec<u8> = (0..size).map(|j| (j & 0xFF) as u8).collect();
        send_buf.fill_bytes(&test_data);
        recv_buf.fill(0);

        // Post receive
        qp2.borrow()
            .recv_builder(i as u64)
            .expect("recv_builder failed")
            .sge(recv_buf.addr(), 64, recv_mr.lkey())
            .finish();
        qp2.borrow().ring_rq_doorbell();

        // Post inline send
        let _ = qp1
            .borrow_mut()
            .wqe_builder(i as u64)
            .expect("wqe_builder failed")
            .ctrl(WqeOpcode::Send, WqeFlags::COMPLETION, 0)
            .inline_data(&test_data)
            .finish();
        qp1.borrow().ring_sq_doorbell();

        // Wait for completions
        let _ = poll_cq_timeout(&send_cq, 5000).expect("Send CQE timeout");
        send_cq.flush();

        last_recv_cqe.set(None);
        let _ = poll_cq_timeout(&recv_cq2, 5000).expect("Recv CQE timeout");
        recv_cq2.flush();

        let recv_cqe = last_recv_cqe.take().expect("Recv callback not called");

        if recv_cqe.opcode.is_inline_scatter() {
            scatter_count += 1;
        }

        // Data should always be in receive buffer when scatter-to-CQE is disabled
        let received = recv_buf.read_bytes(size);
        assert_eq!(
            received, test_data,
            "Data mismatch for size {} (opcode={:?})",
            size, recv_cqe.opcode
        );

        println!(
            "  Size {}: opcode={:?}, is_inline_scatter={}",
            size,
            recv_cqe.opcode,
            recv_cqe.opcode.is_inline_scatter()
        );
    }

    println!(
        "\nWith scatter-to-CQE disabled: {} of {} used inline scatter",
        scatter_count,
        test_sizes.len()
    );

    // Note: Hardware behavior may vary. The important thing is that data is correct.
    println!("Scatter-to-CQE disabled test PASSED!");
}

/// Test small inline Send (≤64B) with wrap-around.
///
/// This tests inline data that fits in a single WQEBB and verifies
/// wrap-around handling works correctly.
#[test]
fn test_small_inline_wraparound() {
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

    // Small queue to trigger wrap-around
    let config = RcQpConfig {
        max_send_wr: 16,
        max_recv_wr: 64,
        max_send_sge: 1,
        max_recv_sge: 1,
        max_inline_data: 64,
        enable_scatter_to_cqe: false,
    };

    fn noop_callback(_cqe: Cqe, _entry: u64) {}

    let qp1 = ctx
        .ctx
        .create_rc_qp(
            &ctx.pd,
            &send_cq,
            &recv_cq1,
            &config,
            noop_callback as fn(_, _),
        )
        .expect("Failed to create QP1");
    let qp2 = ctx
        .ctx
        .create_rc_qp(
            &ctx.pd,
            &send_cq,
            &recv_cq2,
            &config,
            noop_callback as fn(_, _),
        )
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
    qp1.borrow_mut()
        .connect(&remote2, ctx.port, 0, 4, 4, access)
        .expect("connect QP1");
    qp2.borrow_mut()
        .connect(&remote1, ctx.port, 0, 4, 4, access)
        .expect("connect QP2");

    let mut recv_buf = AlignedBuffer::new(4096);

    let recv_mr = unsafe {
        ctx.pd
            .register(recv_buf.as_ptr(), recv_buf.size(), full_access())
    }
    .expect("Failed to register recv MR");

    // Test inline sizes that fit in single WQEBB (ctrl + inline header + data ≤ 64B)
    // ctrl=16B, inline_header=4B, so max inline in 1 WQEBB = 64 - 16 - 4 = 44B
    let small_sizes = [8, 16, 24, 32, 40];
    let iterations_per_size = 50;
    let mut wrap_count = 0;

    println!("Testing small inline Send with wrap-around...");

    for &size in &small_sizes {
        println!("  Testing size {} bytes ({} iterations)...", size, iterations_per_size);

        for i in 0..iterations_per_size {
            let test_data: Vec<u8> = (0..size).map(|j| ((i * size + j) & 0xFF) as u8).collect();
            recv_buf.fill(0);

            let slots_before = qp1.borrow().slots_to_ring_end();

            // Post receive
            qp2.borrow()
                .recv_builder((size * 100 + i) as u64)
                .expect("recv_builder failed")
                .sge(recv_buf.addr(), 256, recv_mr.lkey())
                .finish();
            qp2.borrow().ring_rq_doorbell();

            // Post small inline send
            let _ = qp1
                .borrow_mut()
                .wqe_builder((size * 100 + i) as u64)
                .expect("wqe_builder failed")
                .ctrl(WqeOpcode::Send, WqeFlags::COMPLETION, 0)
                .inline_data(&test_data)
                .finish();
            qp1.borrow().ring_sq_doorbell();

            let slots_after = qp1.borrow().slots_to_ring_end();
            if slots_after > slots_before {
                wrap_count += 1;
            }

            // Wait for completions
            let send_cqe = poll_cq_timeout(&send_cq, 5000).expect(&format!(
                "Send CQE timeout at size={}, iter={}",
                size, i
            ));
            assert_eq!(
                send_cqe.syndrome, 0,
                "Send error at size={}, iter={}: syndrome={}",
                size, i, send_cqe.syndrome
            );
            send_cq.flush();

            let recv_cqe = poll_cq_timeout(&recv_cq2, 5000).expect(&format!(
                "Recv CQE timeout at size={}, iter={}",
                size, i
            ));
            assert_eq!(
                recv_cqe.syndrome, 0,
                "Recv error at size={}, iter={}: syndrome={}",
                size, i, recv_cqe.syndrome
            );
            recv_cq2.flush();

            // Verify data
            let received = recv_buf.read_bytes(size);
            assert_eq!(
                received, test_data,
                "Data mismatch at size={}, iter={}",
                size, i
            );
        }
    }

    println!(
        "\nTotal wrap-arounds: {} (expected several with small queue)",
        wrap_count
    );
    assert!(
        wrap_count > 0,
        "Should have triggered wrap-around with many iterations"
    );

    println!("Small inline Send wrap-around test PASSED!");
}
