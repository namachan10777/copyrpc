//! FsClient: POSIX-like filesystem client over ipc shared memory.

use crate::InodeHeader;
use crate::message::{DATA_AREA_OFFSET, FsRequest, FsResponse};
use crate::routing::koyama_hash;

/// File descriptor handle.
pub type Fd = u32;

/// Filesystem error.
#[derive(Debug)]
pub enum FsError {
    Ipc(ipc::CallError),
    NotFound,
    NoSuchFd,
    ServerError(i32),
}

impl std::fmt::Display for FsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FsError::Ipc(e) => write!(f, "ipc error: {}", e),
            FsError::NotFound => write!(f, "not found"),
            FsError::NoSuchFd => write!(f, "invalid file descriptor"),
            FsError::ServerError(code) => write!(f, "server error: {}", code),
        }
    }
}

impl std::error::Error for FsError {}

impl From<ipc::CallError> for FsError {
    fn from(e: ipc::CallError) -> Self {
        FsError::Ipc(e)
    }
}

struct FdEntry {
    path_hash: u64,
}

type SyncClient = ipc::Client<FsRequest, FsResponse, (), fn((), FsResponse)>;

/// Filesystem client that communicates with aggfsd daemons via shared memory.
pub struct FsClient {
    client: SyncClient,
    chunk_size: usize,
    fd_table: Vec<Option<FdEntry>>,
}

impl FsClient {
    /// Connect to a aggfsd daemon via shared memory.
    ///
    /// # Safety
    /// The shared memory path must correspond to a live aggfsd server.
    pub unsafe fn connect(shm_path: &str, chunk_size: usize) -> Result<Self, ipc::ConnectError> {
        let client: SyncClient = unsafe { ipc::Client::connect_sync(shm_path)? };
        Ok(Self {
            client,
            chunk_size,
            fd_table: Vec::new(),
        })
    }

    /// Data area capacity within the extra buffer.
    fn data_capacity(&self) -> usize {
        self.client.extra_buffer_size() as usize - DATA_AREA_OFFSET
    }

    /// Get a pointer to the data area of the extra buffer.
    fn data_ptr(&self) -> *mut u8 {
        unsafe { self.client.extra_buffer().add(DATA_AREA_OFFSET) }
    }

    /// Create a file.
    pub fn create(&mut self, path: &str, mode: u32) -> Result<(), FsError> {
        let path_hash = koyama_hash(path.as_bytes());
        let resp = self.client.call_blocking(FsRequest::Create {
            path_hash,
            path_len: path.len().min(u16::MAX as usize) as u16,
            mode,
            chunk_size: self.chunk_size as u32,
        })?;
        match resp {
            FsResponse::Ok => Ok(()),
            FsResponse::Error { code } => Err(FsError::ServerError(code)),
            _ => Err(FsError::ServerError(-1)),
        }
    }

    /// Open a file and return a file descriptor.
    pub fn open(&mut self, path: &str) -> Result<Fd, FsError> {
        let path_hash = koyama_hash(path.as_bytes());

        // Find a free slot or append
        let fd = if let Some(pos) = self.fd_table.iter().position(|e| e.is_none()) {
            self.fd_table[pos] = Some(FdEntry { path_hash });
            pos as Fd
        } else {
            let fd = self.fd_table.len() as Fd;
            self.fd_table.push(Some(FdEntry { path_hash }));
            fd
        };

        Ok(fd)
    }

    /// Close a file descriptor.
    pub fn close(&mut self, fd: Fd) -> Result<(), FsError> {
        let idx = fd as usize;
        if idx >= self.fd_table.len() || self.fd_table[idx].is_none() {
            return Err(FsError::NoSuchFd);
        }
        self.fd_table[idx] = None;
        Ok(())
    }

    /// Write data at a given offset (like pwrite).
    pub fn pwrite(&mut self, fd: Fd, buf: &[u8], offset: u64) -> Result<usize, FsError> {
        let path_hash = self.fd_path_hash(fd)?;
        let data_cap = self.data_capacity();
        let chunk_size = self.chunk_size;
        let mut written = 0usize;
        let mut file_offset = offset as usize;

        while written < buf.len() {
            let chunk_index = (file_offset / chunk_size) as u32;
            let chunk_offset = file_offset % chunk_size;
            let max_in_chunk = chunk_size - chunk_offset;
            let remaining = buf.len() - written;
            let this_len = remaining.min(max_in_chunk).min(data_cap);

            // Copy data to extra buffer data area
            unsafe {
                std::ptr::copy_nonoverlapping(buf[written..].as_ptr(), self.data_ptr(), this_len);
            }

            let resp = self.client.call_blocking(FsRequest::Write {
                path_hash,
                chunk_index,
                offset: chunk_offset as u32,
                len: this_len as u32,
            })?;

            match resp {
                FsResponse::Ok => {}
                FsResponse::Error { code } => return Err(FsError::ServerError(code)),
                _ => return Err(FsError::ServerError(-1)),
            }

            written += this_len;
            file_offset += this_len;
        }

        Ok(written)
    }

    /// Read data at a given offset (like pread).
    pub fn pread(&mut self, fd: Fd, buf: &mut [u8], offset: u64) -> Result<usize, FsError> {
        let path_hash = self.fd_path_hash(fd)?;
        let data_cap = self.data_capacity();
        let chunk_size = self.chunk_size;
        let mut total_read = 0usize;
        let mut file_offset = offset as usize;

        while total_read < buf.len() {
            let chunk_index = (file_offset / chunk_size) as u32;
            let chunk_offset = file_offset % chunk_size;
            let max_in_chunk = chunk_size - chunk_offset;
            let remaining = buf.len() - total_read;
            let this_len = remaining.min(max_in_chunk).min(data_cap);

            let resp = self.client.call_blocking(FsRequest::Read {
                path_hash,
                chunk_index,
                offset: chunk_offset as u32,
                len: this_len as u32,
            })?;

            match resp {
                FsResponse::ReadOk { len } => {
                    let actual = len as usize;
                    if actual > 0 {
                        unsafe {
                            std::ptr::copy_nonoverlapping(
                                self.data_ptr(),
                                buf[total_read..].as_mut_ptr(),
                                actual,
                            );
                        }
                    }
                    total_read += actual;
                    file_offset += actual;
                    // Short read means EOF within this chunk
                    if actual < this_len {
                        break;
                    }
                }
                FsResponse::Error { code } => return Err(FsError::ServerError(code)),
                _ => return Err(FsError::ServerError(-1)),
            }
        }

        Ok(total_read)
    }

    /// Get file metadata.
    pub fn stat(&mut self, path: &str) -> Result<InodeHeader, FsError> {
        let path_hash = koyama_hash(path.as_bytes());
        let resp = self.client.call_blocking(FsRequest::Stat {
            path_hash,
            path_len: path.len().min(u16::MAX as usize) as u16,
        })?;
        match resp {
            FsResponse::StatOk { header } => Ok(header),
            FsResponse::NotFound => Err(FsError::NotFound),
            FsResponse::Error { code } => Err(FsError::ServerError(code)),
            _ => Err(FsError::ServerError(-1)),
        }
    }

    /// Remove a file.
    pub fn unlink(&mut self, path: &str) -> Result<(), FsError> {
        let path_hash = koyama_hash(path.as_bytes());
        let resp = self.client.call_blocking(FsRequest::Unlink {
            path_hash,
            path_len: path.len().min(u16::MAX as usize) as u16,
        })?;
        match resp {
            FsResponse::Ok => Ok(()),
            FsResponse::Error { code } => Err(FsError::ServerError(code)),
            _ => Err(FsError::ServerError(-1)),
        }
    }

    /// Create a directory.
    pub fn mkdir(&mut self, path: &str, mode: u32) -> Result<(), FsError> {
        let path_hash = koyama_hash(path.as_bytes());
        let resp = self.client.call_blocking(FsRequest::Mkdir {
            path_hash,
            path_len: path.len().min(u16::MAX as usize) as u16,
            mode,
        })?;
        match resp {
            FsResponse::Ok => Ok(()),
            FsResponse::Error { code } => Err(FsError::ServerError(code)),
            _ => Err(FsError::ServerError(-1)),
        }
    }

    fn fd_path_hash(&self, fd: Fd) -> Result<u64, FsError> {
        let idx = fd as usize;
        match self.fd_table.get(idx).and_then(|e| e.as_ref()) {
            Some(entry) => Ok(entry.path_hash),
            None => Err(FsError::NoSuchFd),
        }
    }
}
