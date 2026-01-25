//! # eRPC - Efficient RPC for RDMA
//!
//! This crate implements eRPC (NSDI 2019) in Rust, providing a high-performance
//! RPC system over Unreliable Datagram (UD) transport.
//!
//! ## Features
//!
//! - **UD-based transport**: Uses mlx5 UD QP for connectionless, low-latency communication
//! - **Configurable request multiplexing**: Set the request window size per session
//! - **Credit-based flow control**: Prevents receiver buffer overflow
//! - **Go-Back-N reliability**: Handles packet loss with retransmission
//! - **Timely congestion control**: Optional RTT-based congestion control
//!
//! ## Usage
//!
//! ```ignore
//! use erpc::{Rpc, RpcConfig};
//! use mlx5::device::Context;
//!
//! // Create RDMA context
//! let ctx = Context::open(None)?;
//!
//! // Configure RPC
//! let config = RpcConfig::default()
//!     .with_req_window(8)
//!     .with_session_credits(32);
//!
//! // Create RPC instance
//! let rpc = Rpc::new(&ctx, 1, config)?;
//!
//! // Set request handler
//! rpc.set_req_handler(|ctx, resp| {
//!     // Process request and send response
//!     resp.respond(&[1, 2, 3]).unwrap();
//! });
//!
//! // Create session
//! let session = rpc.create_session(&remote_info)?;
//!
//! // Send request
//! rpc.enqueue_request(session, 0, &request_data, |_, response| {
//!     println!("Got response: {:?}", response);
//! })?;
//!
//! // Run event loop
//! loop {
//!     rpc.run_event_loop_once();
//! }
//! ```
//!
//! ## Architecture
//!
//! The crate is organized as follows:
//!
//! - [`config`]: Configuration types (`RpcConfig`, `SessionConfig`)
//! - [`packet`]: Packet header format (`PktHdr`)
//! - [`buffer`]: Message buffer management (`MsgBuffer`)
//! - [`transport`]: UD transport wrapper (`UdTransport`)
//! - [`session`]: Session and slot management (`Session`, `SSlot`)
//! - [`reliability`]: Go-Back-N protocol implementation
//! - [`flow_control`]: Credit and congestion control
//! - [`timing`]: Timing wheel for retransmission timeouts
//! - [`rpc`]: Main RPC API (`Rpc`)
//!
//! ## References
//!
//! - [eRPC: General-Purpose RPCs for the Datacenter (NSDI 2019)](https://www.usenix.org/conference/nsdi19/presentation/kalia)

pub mod buffer;
pub mod config;
pub mod error;
pub mod flow_control;
pub mod packet;
pub mod reliability;
pub mod rpc;
pub mod session;
pub mod timing;
pub mod transport;

// Re-export main types
pub use buffer::{MsgBuffer, ZeroCopyPool};
pub use config::{RpcConfig, SessionConfig};
pub use error::{Error, Result};
pub use packet::{PktHdr, PktType, PKT_HDR_SIZE};
pub use rpc::{Continuation, ReqContext, ReqHandler, RespHandle, Rpc};
pub use session::{Session, SessionHandle, SessionState, SSlot, SSlotState};
pub use transport::{LocalInfo, RecvCompletion, RemoteInfo, SendCompletion, UdTransport};
