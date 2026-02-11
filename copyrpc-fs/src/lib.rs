//! copyrpc-fs: RDMA distributed filesystem based on CHFS/FINCHFS design.
//!
//! # Architecture
//!
//! ```text
//! Client (library)  ──ipc shm──▶  Daemon (ipc::Server)
//!                                    │
//!                                    ├── local: PmemStore read/write
//!                                    └── remote: copyrpc → remote daemon
//!                                                  │
//!                                                  └── RDMA READ/WRITE to client shm
//! ```
//!
//! - No metadata server: consistent hashing distributes all data/metadata
//! - Daemon relay: client → ipc → daemon → copyrpc → remote daemon
//! - Zero-copy DMA: client shm buffers are MR-registered by daemon

pub mod client;
pub mod message;
pub mod routing;
pub mod store;

pub use client::FsClient;
pub use message::{
    FsRequest, FsResponse, InodeHeader, RemoteReadReq, RemoteResponse, RemoteWriteReq,
};
pub use routing::{global_to_local, koyama_hash, route};
pub use store::PmemStore;
