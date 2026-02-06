//! Common test utilities for eRPC integration tests.

#![allow(dead_code)]

pub use mlx5::test_utils::AlignedBuffer;

use mlx5::device::DeviceList;
use mlx5::pd::Pd;
use mlx5::types::PortAttr;

/// Test context with device, PD, and port info.
///
/// Unlike the mlx5 TestContext, this scans for an active port (port 1 or 2)
/// since eRPC requires an active IB port.
pub struct TestContext {
    pub ctx: mlx5::device::Context,
    pub pd: Pd,
    pub port: u8,
    pub port_attr: PortAttr,
}

impl TestContext {
    /// Create a new test context.
    pub fn new() -> Option<Self> {
        let device_list = DeviceList::list().ok()?;

        for device in device_list.iter() {
            if let Ok(ctx) = device.open() {
                // Find an active port
                for port in 1..=2u8 {
                    if let Ok(port_attr) = ctx.query_port(port)
                        && port_attr.state == mlx5::types::PortState::Active {
                            let pd = ctx.alloc_pd().ok()?;
                            return Some(Self {
                                ctx,
                                pd,
                                port,
                                port_attr,
                            });
                        }
                }
            }
        }

        None
    }
}

/// Poll completion queue with timeout. Returns number of completions.
pub fn poll_cq_timeout(cq: &mlx5::cq::Cq, timeout_ms: u64) -> usize {
    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_millis(timeout_ms);

    loop {
        let count = cq.poll();
        if count > 0 {
            cq.flush();
            return count;
        }

        if start.elapsed() > timeout {
            return 0;
        }

        std::hint::spin_loop();
    }
}
