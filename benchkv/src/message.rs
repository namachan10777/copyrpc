#![allow(unsafe_op_in_unsafe_fn)]

/// Message types for benchkv inter-layer communication.
use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicU32, Ordering};

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
            std::slice::from_raw_parts(
                self as *const Self as *const u8,
                std::mem::size_of::<Self>(),
            )
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
            std::slice::from_raw_parts(
                self as *const Self as *const u8,
                std::mem::size_of::<Self>(),
            )
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        assert!(bytes.len() >= std::mem::size_of::<Self>());
        unsafe { std::ptr::read_unaligned(bytes.as_ptr() as *const Self) }
    }
}

// === client slot layer: Client ↔ Daemon fixed slot on ipc extra_buffer ===

#[repr(u32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReqSlotState {
    Empty = 0,
    Ready = 1,
    Inflight = 2,
}

#[repr(u32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RespSlotState {
    Empty = 0,
    Done = 3,
}

#[repr(C, align(64))]
struct RequestLane {
    state: AtomicU32,
    seq: AtomicU32,
    _pad0: [u8; 56],
    request: UnsafeCell<Request>,
}

#[repr(C, align(64))]
struct ResponseLane {
    state: AtomicU32,
    seq: AtomicU32,
    _pad0: [u8; 56],
    response: UnsafeCell<Response>,
}

#[repr(C, align(64))]
pub struct ClientSlot {
    req: RequestLane,
    resp: ResponseLane,
}

unsafe impl Send for ClientSlot {}
unsafe impl Sync for ClientSlot {}

pub const CLIENT_SLOT_SIZE: usize = std::mem::size_of::<ClientSlot>();

const _: () = {
    // Keep request/response payloads off the state cache line to avoid false sharing.
    assert!(std::mem::offset_of!(RequestLane, request) >= 64);
    assert!(std::mem::offset_of!(ResponseLane, response) >= 64);
    assert!(std::mem::offset_of!(ClientSlot, resp) >= 64);
    assert!(std::mem::size_of::<RequestLane>() % 64 == 0);
    assert!(std::mem::size_of::<ResponseLane>() % 64 == 0);
};

impl ClientSlot {
    pub fn init(&self) {
        self.req
            .state
            .store(ReqSlotState::Empty as u32, Ordering::Release);
        self.req.seq.store(0, Ordering::Relaxed);
        self.resp
            .state
            .store(RespSlotState::Empty as u32, Ordering::Release);
        self.resp.seq.store(0, Ordering::Relaxed);
    }

    #[inline]
    fn req_state(&self) -> ReqSlotState {
        match self.req.state.load(Ordering::Acquire) {
            0 => ReqSlotState::Empty,
            1 => ReqSlotState::Ready,
            2 => ReqSlotState::Inflight,
            _ => ReqSlotState::Empty,
        }
    }

    #[inline]
    fn resp_state(&self) -> RespSlotState {
        match self.resp.state.load(Ordering::Acquire) {
            3 => RespSlotState::Done,
            _ => RespSlotState::Empty,
        }
    }
}

#[inline]
pub unsafe fn slot_from_extra(ptr: *mut u8) -> *mut ClientSlot {
    ptr as *mut ClientSlot
}

#[inline]
pub unsafe fn slot_init(ptr: *mut ClientSlot) {
    (*ptr).init();
}

#[inline]
pub unsafe fn slot_submit(ptr: *mut ClientSlot, req: Request, seq: u32) -> bool {
    let slot = &*ptr;
    if slot.req_state() != ReqSlotState::Empty {
        return false;
    }
    if slot.resp_state() != RespSlotState::Empty {
        return false;
    }
    *slot.req.request.get() = req;
    slot.req.seq.store(seq, Ordering::Relaxed);
    slot.req
        .state
        .store(ReqSlotState::Ready as u32, Ordering::Release);
    true
}

#[inline]
pub unsafe fn slot_try_take_ready(ptr: *mut ClientSlot) -> Option<Request> {
    let slot = &*ptr;
    // Single daemon consumer per slot: avoid locked RMW and use TSO-friendly load/store.
    if slot.req.state.load(Ordering::Acquire) != ReqSlotState::Ready as u32 {
        return None;
    }
    let req = *slot.req.request.get();
    slot.req
        .state
        .store(ReqSlotState::Inflight as u32, Ordering::Release);
    Some(req)
}

#[inline]
pub unsafe fn slot_restore_ready(ptr: *mut ClientSlot) {
    let slot = &*ptr;
    slot.req
        .state
        .store(ReqSlotState::Ready as u32, Ordering::Release);
}

#[inline]
pub unsafe fn slot_complete(ptr: *mut ClientSlot, resp: Response) {
    let slot = &*ptr;
    *slot.resp.response.get() = resp;
    let seq = slot.req.seq.load(Ordering::Acquire);
    slot.resp.seq.store(seq, Ordering::Relaxed);
    slot.resp
        .state
        .store(RespSlotState::Done as u32, Ordering::Release);
}

#[inline]
pub unsafe fn slot_try_read_done(ptr: *mut ClientSlot, expected_seq: u32) -> Option<Response> {
    let slot = &*ptr;
    if slot.resp_state() != RespSlotState::Done {
        return None;
    }
    if slot.resp.seq.load(Ordering::Acquire) != expected_seq {
        return None;
    }
    Some(*slot.resp.response.get())
}

#[inline]
pub unsafe fn slot_mark_empty(ptr: *mut ClientSlot) {
    let slot = &*ptr;
    slot.resp
        .state
        .store(RespSlotState::Empty as u32, Ordering::Release);
    slot.req
        .state
        .store(ReqSlotState::Empty as u32, Ordering::Release);
}
