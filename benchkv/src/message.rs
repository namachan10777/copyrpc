/// Message types for benchkv inter-layer communication.

// === ipc layer: Client ↔ Daemon ===

#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub enum Request {
    MetaPut { rank: u32, key: u64, value: u64 },
    MetaGet { rank: u32, key: u64 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(C)]
#[allow(clippy::enum_variant_names)]
pub enum Response {
    MetaPutOk,
    MetaGetOk { value: u64 },
    MetaGetNotFound,
}

unsafe impl ipc::Serial for Request {}
unsafe impl ipc::Serial for Response {}

impl Request {
    #[inline]
    pub fn rank(&self) -> u32 {
        match self {
            Request::MetaPut { rank, .. } | Request::MetaGet { rank, .. } => *rank,
        }
    }

    #[inline]
    pub fn key(&self) -> u64 {
        match self {
            Request::MetaPut { key, .. } | Request::MetaGet { key, .. } => *key,
        }
    }
}

// === Flux layer: Daemon ↔ Daemon (intra-node delegation) ===

/// Payload exchanged over Flux between daemon threads.
/// Used for both the call value and the reply value.
#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub enum DelegatePayload {
    /// A request forwarded to the key-owning daemon (or Daemon #0 for remote).
    Req(Request),
    /// The response from the key-owning daemon.
    Resp(Response),
}

unsafe impl inproc::Serial for DelegatePayload {}

// === copyrpc layer: inter-node communication ===

/// Serialized copyrpc request payload.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct RemoteRequest {
    pub request: Request,
}

/// Serialized copyrpc response payload.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct RemoteResponse {
    pub response: Response,
}

impl RemoteRequest {
    pub fn as_bytes(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(self as *const Self as *const u8, std::mem::size_of::<Self>())
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        assert!(bytes.len() >= std::mem::size_of::<Self>());
        unsafe { std::ptr::read_unaligned(bytes.as_ptr() as *const Self) }
    }
}

impl RemoteResponse {
    pub fn as_bytes(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(self as *const Self as *const u8, std::mem::size_of::<Self>())
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        assert!(bytes.len() >= std::mem::size_of::<Self>());
        unsafe { std::ptr::read_unaligned(bytes.as_ptr() as *const Self) }
    }
}

// === Thread-local response queues ===

use std::cell::RefCell;
use ipc::RequestToken;

/// Response routed back from the Flux on_response callback.
#[derive(Debug)]
pub struct FluxResponseEntry {
    pub token: RequestToken,
    pub response: Response,
}

/// Response routed back from the copyrpc on_response callback.
#[derive(Debug)]
pub struct CopyrpcResponseEntry {
    pub token: RequestToken,
    pub response: Response,
}

thread_local! {
    pub static FLUX_RESPONSES: RefCell<Vec<FluxResponseEntry>> = const { RefCell::new(Vec::new()) };
    pub static COPYRPC_RESPONSES: RefCell<Vec<CopyrpcResponseEntry>> = const { RefCell::new(Vec::new()) };
}
