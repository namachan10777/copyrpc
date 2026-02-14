#![allow(unsafe_op_in_unsafe_fn)]

/// Message types for benchkv inter-layer communication.

// === Client ↔ Daemon (via /dev/shm slot) ===

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

    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                self as *const Self as *const u8,
                std::mem::size_of::<Self>(),
            )
        }
    }

    #[inline]
    pub fn from_bytes(bytes: &[u8]) -> Self {
        assert!(bytes.len() >= std::mem::size_of::<Self>());
        unsafe { std::ptr::read_unaligned(bytes.as_ptr() as *const Self) }
    }
}

impl Response {
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                self as *const Self as *const u8,
                std::mem::size_of::<Self>(),
            )
        }
    }

}

// === Flux layer: Daemon ↔ Daemon (intra-node forwarding) ===

#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub enum DelegatePayload {
    Req(Request),
    Resp(Response),
}

unsafe impl inproc::Serial for DelegatePayload {}

// === copyrpc layer: inter-node communication ===

#[derive(Clone, Copy)]
#[repr(C)]
pub struct RemoteRequest {
    pub request: Request,
}

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
