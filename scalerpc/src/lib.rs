#![allow(unsafe_op_in_unsafe_fn)]

//! # ScaleRPC - High-performance RDMA-based RPC
//!
//! ScaleRPC is an implementation based on the EuroSys 2019 paper,
//! providing high-performance RPC over RDMA with:
//!
//! - **Connection Grouping**: Limits active connections to prevent NIC cache thrashing
//! - **Virtualized Mapping**: Shared message pools for CPU cache efficiency
//! - **One-sided RDMA**: WRITE for request, WRITE back for response
//!
//! ## Architecture
//!
//! ScaleRPC uses a shared message pool model where:
//! 1. Clients allocate slots from a local pool for requests
//! 2. Requests are sent via RDMA WRITE to server slots
//! 3. Server processes requests and writes responses back via RDMA WRITE
//! 4. Client polls its slot for response completion
//!
//! ## Usage
//!
//! ```ignore
//! // Create a client
//! let pool = MessagePool::new(&pd, &PoolConfig::default())?;
//! let client = RpcClient::new(pool);
//!
//! // Make an RPC call
//! let slot = client.call(server_conn, rpc_type, &request_data)?;
//! let response = slot.wait_response()?;
//! ```

pub mod config;
pub mod error;
pub mod mapping;
pub mod pool;
pub mod protocol;
pub mod connection;
pub mod client;
pub mod server;

pub use config::{ClientConfig, GroupConfig, PoolConfig, ServerConfig};
pub use error::{Error, Result, SlotState};
pub use mapping::VirtualMapping;
pub use pool::{MessagePool, MessageSlot, SlotHandle};
pub use protocol::{
    ContextSwitchEvent, EndpointEntry, MessageTrailer, RequestHeader, ResponseHeader,
    MESSAGE_BLOCK_SIZE, MESSAGE_MAX_DATA_SIZE, MESSAGE_TRAILER_SIZE,
};
pub use connection::{Connection, RemoteEndpoint, ConnectionId};
pub use client::{ClientState, PendingRpc, RpcClient, RpcResponse};
pub use server::{GroupScheduler, IncomingRequest, RpcServer, SchedulerState};
