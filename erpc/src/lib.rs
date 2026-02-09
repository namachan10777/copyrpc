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
//! // Create client RPC instance with on_response callback
//! let client: Rpc<u64> = Rpc::new(&ctx, 1, config.clone(), |user_data, response| {
//!     println!("Got response for request {}: {:?}", user_data, response);
//! })?;
//!
//! // Create server RPC instance (no response callback needed for server)
//! let server: Rpc<()> = Rpc::new(&ctx, 1, config, |_, _| {})?;
//!
//! // Create session
//! let session = client.create_session(&remote_info)?;
//!
//! // Client: Send request with user_data
//! let req_num = client.call(session, 0, &request_data, 42u64)?;
//!
//! // Server event loop: poll for incoming requests and reply
//! loop {
//!     server.run_event_loop_once();
//!     while let Some(req) = server.recv() {
//!         server.reply(&req, &response_data)?;
//!     }
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
pub use packet::{PKT_HDR_SIZE, PktHdr, PktType};
pub use rpc::{IncomingRequest, OnResponse, Rpc};
pub use session::{SSlot, SSlotState, Session, SessionHandle, SessionState};
pub use transport::{LocalInfo, RecvCompletion, RemoteInfo, SendCompletion, UdTransport};
