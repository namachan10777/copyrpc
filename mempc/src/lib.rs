//! MPSC-level RPC primitives for intra-process and inter-process communication.
//!
//! Provides unified SPSC→MPSC conversion with multiple transport backends:
//! - `onesided`: Producer-only write with generation counters
//! - `fastforward`: FastForward algorithm with validity flags
//! - `lamport`: Batched index synchronization
//! - `fetch_add`: Native lock-free MPSC via fetch_add

pub mod bbq;
pub mod common;
pub mod fastforward;
pub mod fetch_add;
pub mod hazard;
pub mod lamport;
pub mod lcrq;
pub mod lprq;
pub mod onesided;
pub mod serial;
pub mod user_data;

pub use common::Response;
pub use onesided::{RawReceiver, RawSender, Slot};
pub use serial::Serial;
pub use user_data::CallerWithUserData;

/// Error returned when a call fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallError<T> {
    /// The channel is full.
    Full(T),
    /// Inflight limit exceeded.
    InflightExceeded(T),
    /// The peer has disconnected.
    Disconnected(T),
}

impl<T> std::fmt::Display for CallError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CallError::Full(_) => write!(f, "channel is full"),
            CallError::InflightExceeded(_) => write!(f, "inflight limit exceeded"),
            CallError::Disconnected(_) => write!(f, "peer has disconnected"),
        }
    }
}

impl<T: std::fmt::Debug> std::error::Error for CallError<T> {}

/// Opaque token for deferred reply.
pub struct ReplyToken {
    pub(crate) caller_id: usize,
    pub(crate) slot_token: u64,
}

impl ReplyToken {
    /// Returns the caller ID that sent the original request.
    #[inline]
    pub fn caller_id(&self) -> usize {
        self.caller_id
    }
}

/// Per-client caller handle.
pub trait MpscCaller<Req: Serial, Resp: Serial>: Send {
    /// Send a request. Returns a token for response matching.
    fn call(&mut self, req: Req) -> Result<u64, CallError<Req>>;

    /// Synchronize state (required for Lamport, no-op for others).
    fn sync(&mut self);

    /// Try to receive a response. Returns (token, resp).
    fn try_recv_response(&mut self) -> Option<(u64, Resp)>;

    /// Number of inflight requests.
    fn pending_count(&self) -> usize;
}

/// Methods available on every `MpscServer::RecvRef`.
pub trait MpscRecvRef<Req: Serial> {
    /// Returns the caller lane that sent this request.
    fn caller_id(&self) -> usize;

    /// Returns a copy of the request data.
    fn data(&self) -> Req;

    /// Consumes the handle and returns a deferred-reply token.
    fn into_token(self) -> ReplyToken;
}

/// Server that handles N callers with zero-copy recv via GAT.
pub trait MpscServer<Req: Serial, Resp: Serial> {
    /// Zero-copy receive handle type.
    type RecvRef<'a>: MpscRecvRef<Req>
    where
        Self: 'a;

    /// Scan all caller rings. Returns number of available requests.
    fn poll(&mut self) -> u32;

    /// Zero-copy recv: returns a handle borrowing the ring slot.
    fn try_recv(&mut self) -> Option<Self::RecvRef<'_>>;

    /// Deferred reply using a token obtained from RecvRef::into_token().
    fn reply(&mut self, token: ReplyToken, resp: Resp);
}

/// Factory trait for creating MPSC channel sets.
#[allow(clippy::type_complexity)]
pub trait MpscChannel: 'static + Send + Sync {
    type Caller<Req: Serial + Send, Resp: Serial + Send>: MpscCaller<Req, Resp>;
    type Server<Req: Serial + Send, Resp: Serial + Send>: MpscServer<Req, Resp> + Send;

    fn create<Req: Serial + Send, Resp: Serial + Send>(
        max_callers: usize,
        ring_depth: usize,
        max_inflight: usize,
    ) -> (Vec<Self::Caller<Req, Resp>>, Self::Server<Req, Resp>);
}

pub use bbq::BbqMpsc;
pub use fastforward::FastForwardMpsc;
pub use fetch_add::FetchAddMpsc;
pub use lamport::LamportMpsc;
pub use lcrq::LcrqMpsc;
pub use lprq::LprqMpsc;
pub use onesided::OnesidedMpsc;
