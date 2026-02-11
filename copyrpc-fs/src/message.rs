//! Message types for copyrpc-fs IPC and RPC protocols.

use ipc::Serial;

/// Inode metadata header, stored inline at the beginning of chunk 0.
#[repr(C)]
#[derive(Copy, Clone, Debug, Default)]
pub struct InodeHeader {
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub _pad0: u32,
    pub size: u64,
    pub chunk_size: u32,
    pub _pad1: u32,
    pub mtime_sec: i64,
    pub mtime_nsec: i64,
    pub ctime_sec: i64,
    pub ctime_nsec: i64,
}

unsafe impl Serial for InodeHeader {}

/// Maximum path length that fits in the extra buffer path area.
pub const PATH_AREA_SIZE: usize = 4096;

/// Offset of the data area in the extra buffer.
pub const DATA_AREA_OFFSET: usize = PATH_AREA_SIZE;

/// IPC request (Client → Daemon).
#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub enum FsRequest {
    Create {
        path_hash: u64,
        path_len: u16,
        mode: u32,
        chunk_size: u32,
    },
    Stat {
        path_hash: u64,
        path_len: u16,
    },
    Read {
        path_hash: u64,
        chunk_index: u32,
        offset: u32,
        len: u32,
    },
    Write {
        path_hash: u64,
        chunk_index: u32,
        offset: u32,
        len: u32,
    },
    Unlink {
        path_hash: u64,
        path_len: u16,
    },
    Mkdir {
        path_hash: u64,
        path_len: u16,
        mode: u32,
    },
    Readdir {
        path_hash: u64,
        path_len: u16,
    },
}

unsafe impl Serial for FsRequest {}

/// IPC response (Daemon → Client).
#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub enum FsResponse {
    Ok,
    StatOk { header: InodeHeader },
    ReadOk { len: u32 },
    ReaddirOk { count: u32 },
    NotFound,
    Error { code: i32 },
}

unsafe impl Serial for FsResponse {}

/// copyrpc payload: Remote WRITE request (Daemon → Remote Daemon).
///
/// The remote daemon will RDMA READ from the client's shm buffer
/// and write the data to its local PmemStore.
#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct RemoteWriteReq {
    pub path_hash: u64,
    pub chunk_index: u32,
    pub offset: u32,
    pub len: u32,
    pub client_rkey: u32,
    pub client_addr: u64,
}

/// copyrpc payload: Remote READ request (Daemon → Remote Daemon).
///
/// The remote daemon will RDMA WRITE from its local PmemStore
/// to the client's shm buffer.
#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct RemoteReadReq {
    pub path_hash: u64,
    pub chunk_index: u32,
    pub offset: u32,
    pub len: u32,
    pub client_rkey: u32,
    pub client_addr: u64,
}

/// copyrpc payload: Remote CREATE request.
#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct RemoteCreateReq {
    pub path_hash: u64,
    pub mode: u32,
    pub chunk_size: u32,
}

/// copyrpc payload: Remote STAT request.
#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct RemoteStatReq {
    pub path_hash: u64,
}

/// copyrpc payload: Remote UNLINK request.
#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct RemoteUnlinkReq {
    pub path_hash: u64,
}

/// Wrapper enum for all copyrpc request types.
#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub enum RemoteRequest {
    Write(RemoteWriteReq),
    Read(RemoteReadReq),
    Create(RemoteCreateReq),
    Stat(RemoteStatReq),
    Unlink(RemoteUnlinkReq),
}

/// copyrpc response (Remote Daemon → Daemon).
#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct RemoteResponse {
    pub status: i32,
    pub len: u32,
}

/// copyrpc stat response (includes inode header).
#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct RemoteStatResponse {
    pub status: i32,
    pub header: InodeHeader,
}
