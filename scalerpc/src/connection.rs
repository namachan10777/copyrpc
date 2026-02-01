//! Connection wrapper for RC QP.
//!
//! This module wraps mlx5 RC QP for ScaleRPC use.

use std::cell::RefCell;
use std::rc::Rc;

use mlx5::cq::{Cq, CqConfig, Cqe};
use mlx5::device::Context;
use mlx5::pd::{AccessFlags, Pd};
use mlx5::qp::{RcQpConfig, RcQpIb};
use mlx5::transport::IbRemoteQpInfo;
use mlx5::types::PortAttr;

use crate::error::Result;

/// Connection ID type.
pub type ConnectionId = usize;

/// Remote endpoint information for establishing a connection.
#[derive(Debug, Clone, Default)]
pub struct RemoteEndpoint {
    /// Remote QP number.
    pub qpn: u32,
    /// Remote PSN.
    pub psn: u32,
    /// Remote LID.
    pub lid: u16,
    /// Remote slot base address for RDMA WRITE.
    pub slot_addr: u64,
    /// Remote slot rkey.
    pub slot_rkey: u32,
    /// Remote event buffer address for context switch notification.
    pub event_buffer_addr: u64,
    /// Remote event buffer rkey.
    pub event_buffer_rkey: u32,
    /// Remote warmup buffer address for RDMA READ.
    pub warmup_buffer_addr: u64,
    /// Remote warmup buffer rkey.
    pub warmup_buffer_rkey: u32,
    /// Number of slots in warmup buffer.
    pub warmup_buffer_slots: u32,
    /// Server's endpoint entry address (client writes to this).
    pub endpoint_entry_addr: u64,
    /// Server's endpoint entry rkey.
    pub endpoint_entry_rkey: u32,
    /// Server-assigned connection ID (used as sender_conn_id in requests).
    /// This allows the server to route responses back through the correct QP.
    pub server_conn_id: u32,
    /// Number of slots in server's processing pool.
    /// Used by call_direct to cycle through server slots correctly.
    pub pool_num_slots: u32,
}

/// Callback type for SQ completions.
pub type SqCallback = fn(Cqe, u64);

/// Callback type for RQ completions.
pub type RqCallback = fn(Cqe, u64);

/// RC QP connection wrapper.
///
/// Wraps an mlx5 RC QP with associated CQs and provides
/// a simplified interface for ScaleRPC operations.
pub struct Connection {
    /// Connection identifier.
    conn_id: ConnectionId,
    /// The underlying RC QP.
    qp: Rc<RefCell<RcQpIb<u64, u64, SqCallback, RqCallback>>>,
    /// Local port number.
    port: u8,
    /// Port attributes.
    port_attr: PortAttr,
    /// Remote endpoint information.
    remote: Option<RemoteEndpoint>,
}

/// Create shared CQs for multiple connections.
///
/// Returns (send_cq, recv_cq) that can be shared across all connections.
pub fn create_shared_cqs(ctx: &Context, cq_size: usize) -> Result<(Rc<Cq>, Rc<Cq>)> {
    let cq_config = CqConfig::default();
    let send_cq = Rc::new(ctx.create_cq(cq_size as i32, &cq_config)?);
    let recv_cq = Rc::new(ctx.create_cq(cq_size as i32, &cq_config)?);
    Ok((send_cq, recv_cq))
}

impl Connection {
    /// Create a new connection with shared CQs.
    ///
    /// # Arguments
    /// * `ctx` - Device context
    /// * `pd` - Protection domain
    /// * `conn_id` - Unique connection identifier
    /// * `port` - Local port number
    /// * `send_cq` - Shared send completion queue
    /// * `recv_cq` - Shared receive completion queue
    /// * `sq_callback` - Callback for send completions
    /// * `rq_callback` - Callback for receive completions
    pub fn new(
        ctx: &Context,
        pd: &Pd,
        conn_id: ConnectionId,
        port: u8,
        send_cq: Rc<Cq>,
        recv_cq: Rc<Cq>,
        sq_callback: SqCallback,
        rq_callback: RqCallback,
    ) -> Result<Self> {
        let port_attr = ctx.query_port(port)?;

        // Create QP with shared CQs
        let qp_config = RcQpConfig {
            max_send_wr: 256,
            max_recv_wr: 256,
            max_send_sge: 4,
            max_recv_sge: 4,
            max_inline_data: 64,
            enable_scatter_to_cqe: false,
        };

        let qp = ctx
            .rc_qp_builder::<u64, u64>(pd, &qp_config)
            .sq_cq(send_cq, sq_callback)
            .rq_cq(recv_cq, rq_callback)
            .build()?;

        Ok(Self {
            conn_id,
            qp,
            port,
            port_attr,
            remote: None,
        })
    }

    /// Get the connection ID.
    pub fn conn_id(&self) -> ConnectionId {
        self.conn_id
    }

    /// Get the local QP number.
    pub fn qpn(&self) -> u32 {
        self.qp.borrow().qpn()
    }

    /// Get the local LID.
    pub fn lid(&self) -> u16 {
        self.port_attr.lid
    }

    /// Get local endpoint information for exchange with remote.
    pub fn local_endpoint(&self) -> RemoteEndpoint {
        RemoteEndpoint {
            qpn: self.qpn(),
            psn: 0,
            lid: self.lid(),
            slot_addr: 0,              // To be filled by caller
            slot_rkey: 0,              // To be filled by caller
            event_buffer_addr: 0,      // To be filled by caller (client only)
            event_buffer_rkey: 0,      // To be filled by caller (client only)
            warmup_buffer_addr: 0,     // To be filled by caller (client only)
            warmup_buffer_rkey: 0,     // To be filled by caller (client only)
            warmup_buffer_slots: 0,    // To be filled by caller (client only)
            endpoint_entry_addr: 0,    // To be filled by caller (server only)
            endpoint_entry_rkey: 0,    // To be filled by caller (server only)
            server_conn_id: self.conn_id as u32, // Filled by connection
            pool_num_slots: 0,         // To be filled by caller (server only)
        }
    }

    /// Connect to a remote endpoint.
    pub fn connect(&mut self, remote: RemoteEndpoint) -> Result<()> {
        let remote_qp_info = IbRemoteQpInfo {
            qp_number: remote.qpn,
            packet_sequence_number: remote.psn,
            local_identifier: remote.lid,
        };

        let access = (AccessFlags::LOCAL_WRITE
            | AccessFlags::REMOTE_WRITE
            | AccessFlags::REMOTE_READ
            | AccessFlags::REMOTE_ATOMIC)
            .bits();

        self.qp
            .borrow_mut()
            .connect(&remote_qp_info, self.port, 0, 4, 4, access)?;

        self.remote = Some(remote);
        Ok(())
    }

    /// Get the remote endpoint information.
    pub fn remote(&self) -> Option<&RemoteEndpoint> {
        self.remote.as_ref()
    }

    /// Get a reference to the QP for WQE emission.
    pub fn qp(&self) -> &Rc<RefCell<RcQpIb<u64, u64, SqCallback, RqCallback>>> {
        &self.qp
    }

    /// Ring the SQ doorbell.
    pub fn ring_sq_doorbell(&self) {
        self.qp.borrow().ring_sq_doorbell();
    }

    /// Ring the RQ doorbell.
    pub fn ring_rq_doorbell(&self) {
        self.qp.borrow().ring_rq_doorbell();
    }

    /// Post a receive buffer.
    pub fn post_recv(&self, entry: u64, addr: u64, len: u32, lkey: u32) -> Result<()> {
        self.qp.borrow().post_recv(entry, addr, len, lkey)?;
        Ok(())
    }
}
