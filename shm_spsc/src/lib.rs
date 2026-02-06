//! RPC slot-based communication over shared memory.
//!
//! Each client gets a dedicated request/response slot pair.
//! The server polls all slots and responds to each client.
//!
//! Uses a toggle-bit protocol (inspired by ffwd, SOSP 2017):
//! - `client_seq` incremented by client when posting a request
//! - `server_seq` incremented by server when posting a response
//! - `client_seq != server_seq` → request pending
//! - `client_seq == server_seq` → idle / response ready

pub mod shm;

use shm::SharedMemory;
use std::io;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU8, Ordering};

/// Marker trait for types that can be safely transmitted through the channel.
///
/// # Safety
/// Types implementing this trait must be `Copy` and have a stable memory layout
/// suitable for inter-process communication.
pub unsafe trait Serial: Copy {}

unsafe impl Serial for u8 {}
unsafe impl Serial for u16 {}
unsafe impl Serial for u32 {}
unsafe impl Serial for u64 {}
unsafe impl Serial for u128 {}
unsafe impl Serial for usize {}
unsafe impl Serial for i8 {}
unsafe impl Serial for i16 {}
unsafe impl Serial for i32 {}
unsafe impl Serial for i64 {}
unsafe impl Serial for i128 {}
unsafe impl Serial for isize {}
unsafe impl Serial for f32 {}
unsafe impl Serial for f64 {}
unsafe impl Serial for bool {}
unsafe impl<T: Copy, const N: usize> Serial for [T; N] {}

/// Identifies a client slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ClientId(pub u32);

/// Error returned when `Client::call()` fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallError {
    ServerDisconnected,
}

impl std::fmt::Display for CallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CallError::ServerDisconnected => write!(f, "server disconnected"),
        }
    }
}

impl std::error::Error for CallError {}

/// Error returned when `Server::respond()` fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RespondError {
    InvalidClient,
    NoRequest,
    ClientDisconnected,
}

impl std::fmt::Display for RespondError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RespondError::InvalidClient => write!(f, "invalid client id"),
            RespondError::NoRequest => write!(f, "no pending request from this client"),
            RespondError::ClientDisconnected => write!(f, "client disconnected"),
        }
    }
}

impl std::error::Error for RespondError {}

/// Error returned when `Client::connect()` fails.
#[derive(Debug)]
pub enum ConnectError {
    ServerNotAlive,
    ServerFull,
    LayoutMismatch,
    Io(io::Error),
}

impl std::fmt::Display for ConnectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectError::ServerNotAlive => write!(f, "server not alive"),
            ConnectError::ServerFull => write!(f, "server is full"),
            ConnectError::LayoutMismatch => write!(f, "layout mismatch"),
            ConnectError::Io(e) => write!(f, "io error: {}", e),
        }
    }
}

impl std::error::Error for ConnectError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ConnectError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for ConnectError {
    fn from(e: io::Error) -> Self {
        ConnectError::Io(e)
    }
}

const MAGIC: u64 = 0x5250_4353_4C4F_5421; // "RPCSLOT!"
const VERSION: u32 = 4;
const HEADER_SIZE: usize = 64;
const CACHE_LINE_SIZE: usize = 64;

/// Header stored at the beginning of shared memory.
/// Fixed 64 bytes.
#[repr(C)]
struct Header {
    magic: u64,           // 8
    version: u32,         // 4
    max_clients: u32,     // 4
    req_size: u32,        // 4
    resp_size: u32,       // 4
    req_align: u32,       // 4
    resp_align: u32,      // 4
    server_alive: AtomicBool, // 1
    _pad: [u8; 3],       // 3
    next_client_id: AtomicU32, // 4
    _reserved: [u8; 24],  // 24
    // total = 64
}

const _: () = assert!(std::mem::size_of::<Header>() == HEADER_SIZE);

/// Layout of a single slot in shared memory.
struct SlotLayout {
    slot_size: usize,
    client_seq_offset: usize,  // 0
    server_seq_offset: usize,  // 1
    alive_offset: usize,       // 2
    req_offset: usize,
    resp_offset: usize,
}

fn calc_slot_layout<Req, Resp>() -> SlotLayout {
    let align = std::mem::align_of::<Req>()
        .max(std::mem::align_of::<Resp>())
        .max(8);

    // [client_seq: AtomicU8(1)] [server_seq: AtomicU8(1)] [alive: AtomicBool(1)] [padding to align] [req] [padding to align] [resp] [padding to cache line]
    let client_seq_offset = 0;
    let server_seq_offset = 1;
    let alive_offset = 2;
    let req_offset = align_up(3, align);
    let resp_offset = align_up(req_offset + std::mem::size_of::<Req>(), align);
    let slot_size = align_up(resp_offset + std::mem::size_of::<Resp>(), CACHE_LINE_SIZE);

    SlotLayout {
        slot_size,
        client_seq_offset,
        server_seq_offset,
        alive_offset,
        req_offset,
        resp_offset,
    }
}

fn calc_shm_size<Req, Resp>(max_clients: u32) -> usize {
    let layout = calc_slot_layout::<Req, Resp>();
    HEADER_SIZE + layout.slot_size * max_clients as usize
}

fn align_up(val: usize, align: usize) -> usize {
    (val + align - 1) & !(align - 1)
}

/// Server side of the RPC slot channel.
pub struct Server<Req: Serial, Resp: Serial> {
    _shm: SharedMemory,
    header: *mut Header,
    base: *mut u8,
    layout: SlotLayout,
    max_clients: u32,
    poll_cursor: u32,
    server_seqs: Vec<u8>,
    in_processing: u64,
    _marker: PhantomData<(Req, Resp)>,
}

unsafe impl<Req: Serial + Send, Resp: Serial + Send> Send for Server<Req, Resp> {}

impl<Req: Serial, Resp: Serial> Server<Req, Resp> {
    /// Creates a new server and shared memory region.
    ///
    /// # Safety
    /// The caller must ensure that `path` is a valid shared memory path
    /// and that no other process is using the same path.
    pub unsafe fn create<P: AsRef<Path>>(path: P, max_clients: u32) -> io::Result<Self> {
        assert!(max_clients > 0, "max_clients must be > 0");
        assert!(max_clients <= 64, "max_clients must be <= 64");

        let size = calc_shm_size::<Req, Resp>(max_clients);
        let shm = unsafe { SharedMemory::create(path, size)? };
        let base = shm.as_ptr();

        let header = base as *mut Header;
        unsafe {
            std::ptr::write(
                header,
                Header {
                    magic: MAGIC,
                    version: VERSION,
                    max_clients,
                    req_size: std::mem::size_of::<Req>() as u32,
                    resp_size: std::mem::size_of::<Resp>() as u32,
                    req_align: std::mem::align_of::<Req>() as u32,
                    resp_align: std::mem::align_of::<Resp>() as u32,
                    server_alive: AtomicBool::new(true),
                    _pad: [0; 3],
                    next_client_id: AtomicU32::new(0),
                    _reserved: [0; 24],
                },
            );
        }

        let layout = calc_slot_layout::<Req, Resp>();

        // Initialize all slots
        for i in 0..max_clients {
            let slot_base = unsafe { base.add(HEADER_SIZE + layout.slot_size * i as usize) };
            unsafe {
                let client_seq =
                    &*(slot_base.add(layout.client_seq_offset) as *const AtomicU8);
                client_seq.store(0, Ordering::Relaxed);
                let server_seq =
                    &*(slot_base.add(layout.server_seq_offset) as *const AtomicU8);
                server_seq.store(0, Ordering::Relaxed);
                let alive = &*(slot_base.add(layout.alive_offset) as *const AtomicBool);
                alive.store(false, Ordering::Relaxed);
            }
        }

        Ok(Self {
            _shm: shm,
            header,
            base,
            layout,
            max_clients,
            poll_cursor: 0,
            server_seqs: vec![0u8; max_clients as usize],
            in_processing: 0,
            _marker: PhantomData,
        })
    }

    fn slot_base(&self, client_id: u32) -> *mut u8 {
        unsafe {
            self.base
                .add(HEADER_SIZE + self.layout.slot_size * client_id as usize)
        }
    }

    fn slot_client_seq(&self, client_id: u32) -> &AtomicU8 {
        unsafe {
            &*(self.slot_base(client_id).add(self.layout.client_seq_offset) as *const AtomicU8)
        }
    }

    fn slot_server_seq(&self, client_id: u32) -> &AtomicU8 {
        unsafe {
            &*(self.slot_base(client_id).add(self.layout.server_seq_offset) as *const AtomicU8)
        }
    }

    fn slot_alive(&self, client_id: u32) -> &AtomicBool {
        unsafe {
            &*(self.slot_base(client_id).add(self.layout.alive_offset) as *const AtomicBool)
        }
    }

    fn slot_req_ptr(&self, client_id: u32) -> *const Req {
        unsafe { self.slot_base(client_id).add(self.layout.req_offset) as *const Req }
    }

    fn slot_resp_ptr(&self, client_id: u32) -> *mut Resp {
        unsafe { self.slot_base(client_id).add(self.layout.resp_offset) as *mut Resp }
    }

    /// Polls for a pending request using round-robin across all allocated slots.
    pub fn try_poll(&mut self) -> Option<(ClientId, Req)> {
        let allocated = unsafe { (*self.header).next_client_id.load(Ordering::Relaxed) };
        if allocated == 0 {
            return None;
        }

        for _ in 0..allocated {
            let id = self.poll_cursor;
            self.poll_cursor = (self.poll_cursor + 1) % allocated;

            // Skip in-processing clients
            if self.in_processing & (1 << id) != 0 {
                continue;
            }

            // Skip dead clients
            if !self.slot_alive(id).load(Ordering::Acquire) {
                continue;
            }

            let client_seq = self.slot_client_seq(id).load(Ordering::Acquire);
            if client_seq != self.server_seqs[id as usize] {
                // New request
                let req = unsafe { std::ptr::read_volatile(self.slot_req_ptr(id)) };
                self.in_processing |= 1 << id;
                return Some((ClientId(id), req));
            }
        }

        None
    }

    /// Sends a response to a client.
    pub fn respond(&mut self, client_id: ClientId, resp: Resp) -> Result<(), RespondError> {
        let id = client_id.0;
        if id >= self.max_clients {
            return Err(RespondError::InvalidClient);
        }

        let allocated = unsafe { (*self.header).next_client_id.load(Ordering::Relaxed) };
        if id >= allocated {
            return Err(RespondError::InvalidClient);
        }

        if !self.slot_alive(id).load(Ordering::Acquire) {
            return Err(RespondError::ClientDisconnected);
        }

        if self.in_processing & (1 << id) == 0 {
            return Err(RespondError::NoRequest);
        }

        // Write response
        unsafe { std::ptr::write_volatile(self.slot_resp_ptr(id), resp) };
        // Flip server_seq to match client_seq
        self.server_seqs[id as usize] = self.server_seqs[id as usize].wrapping_add(1);
        self.slot_server_seq(id)
            .store(self.server_seqs[id as usize], Ordering::Release);
        self.in_processing &= !(1 << id);

        Ok(())
    }

    /// Returns true if a specific client is connected.
    pub fn is_client_connected(&self, client_id: ClientId) -> bool {
        let id = client_id.0;
        let allocated = unsafe { (*self.header).next_client_id.load(Ordering::Relaxed) };
        if id >= allocated {
            return false;
        }
        self.slot_alive(id).load(Ordering::Acquire)
    }

    /// Returns the number of currently connected clients.
    pub fn connected_clients(&self) -> u32 {
        let allocated = unsafe { (*self.header).next_client_id.load(Ordering::Relaxed) };
        let mut count = 0;
        for i in 0..allocated {
            if self.slot_alive(i).load(Ordering::Acquire) {
                count += 1;
            }
        }
        count
    }
}

impl<Req: Serial, Resp: Serial> Drop for Server<Req, Resp> {
    fn drop(&mut self) {
        unsafe {
            (*self.header).server_alive.store(false, Ordering::Release);
        }
    }
}

/// Client side of the RPC slot channel.
pub struct Client<Req: Serial, Resp: Serial> {
    _shm: SharedMemory,
    header: *mut Header,
    slot_base: *mut u8,
    layout: SlotLayout,
    client_id: u32,
    client_seq: u8,
    _marker: PhantomData<(Req, Resp)>,
}

unsafe impl<Req: Serial + Send, Resp: Serial + Send> Send for Client<Req, Resp> {}

impl<Req: Serial, Resp: Serial> Client<Req, Resp> {
    /// Connects to an existing shared memory region.
    ///
    /// # Safety
    /// The caller must ensure that a server has created the shared memory
    /// with the same types `Req` and `Resp`.
    pub unsafe fn connect<P: AsRef<Path>>(path: P) -> Result<Self, ConnectError> {
        // First open with just the header to validate
        let header_shm = unsafe { SharedMemory::open(&path, HEADER_SIZE)? };
        let header = header_shm.as_ptr() as *const Header;

        let (max_clients, shm_size) = unsafe {
            if (*header).magic != MAGIC {
                return Err(ConnectError::LayoutMismatch);
            }
            if (*header).version != VERSION {
                return Err(ConnectError::LayoutMismatch);
            }
            if (*header).req_size != std::mem::size_of::<Req>() as u32
                || (*header).resp_size != std::mem::size_of::<Resp>() as u32
                || (*header).req_align != std::mem::align_of::<Req>() as u32
                || (*header).resp_align != std::mem::align_of::<Resp>() as u32
            {
                return Err(ConnectError::LayoutMismatch);
            }
            if !(*header).server_alive.load(Ordering::Acquire) {
                return Err(ConnectError::ServerNotAlive);
            }
            let mc = (*header).max_clients;
            (mc, calc_shm_size::<Req, Resp>(mc))
        };

        drop(header_shm);

        // Re-open with full size
        let shm = unsafe { SharedMemory::open(path, shm_size)? };
        let base = shm.as_ptr();
        let header = base as *mut Header;

        // Allocate a slot
        let client_id = unsafe { (*header).next_client_id.fetch_add(1, Ordering::Relaxed) };
        if client_id >= max_clients {
            // Undo the allocation (best-effort; slot won't be reused anyway)
            return Err(ConnectError::ServerFull);
        }

        let layout = calc_slot_layout::<Req, Resp>();
        let slot_base = unsafe { base.add(HEADER_SIZE + layout.slot_size * client_id as usize) };

        // Mark as alive
        unsafe {
            let alive = &*(slot_base.add(layout.alive_offset) as *const AtomicBool);
            alive.store(true, Ordering::Release);
        }

        Ok(Self {
            _shm: shm,
            header,
            slot_base,
            layout,
            client_id,
            client_seq: 0,
            _marker: PhantomData,
        })
    }

    fn slot_client_seq(&self) -> &AtomicU8 {
        unsafe {
            &*(self.slot_base.add(self.layout.client_seq_offset) as *const AtomicU8)
        }
    }

    fn slot_server_seq(&self) -> &AtomicU8 {
        unsafe {
            &*(self.slot_base.add(self.layout.server_seq_offset) as *const AtomicU8)
        }
    }

    fn req_ptr(&self) -> *mut Req {
        unsafe { self.slot_base.add(self.layout.req_offset) as *mut Req }
    }

    fn resp_ptr(&self) -> *const Resp {
        unsafe { self.slot_base.add(self.layout.resp_offset) as *const Resp }
    }

    /// Synchronous blocking RPC call.
    pub fn call(&mut self, req: Req) -> Result<Resp, CallError> {
        // Write request
        unsafe { std::ptr::write_volatile(self.req_ptr(), req) };
        // Flip client_seq
        self.client_seq = self.client_seq.wrapping_add(1);
        self.slot_client_seq()
            .store(self.client_seq, Ordering::Release);

        // Spin until server_seq matches client_seq
        loop {
            if self.slot_server_seq().load(Ordering::Acquire) == self.client_seq {
                break;
            }
            // Check server liveness
            if !self.is_server_alive() {
                return Err(CallError::ServerDisconnected);
            }
            std::hint::spin_loop();
        }

        // Read response
        let resp = unsafe { std::ptr::read_volatile(self.resp_ptr()) };
        Ok(resp)
    }

    /// Returns the client's slot ID.
    pub fn id(&self) -> ClientId {
        ClientId(self.client_id)
    }

    /// Returns true if the server is still alive.
    pub fn is_server_alive(&self) -> bool {
        unsafe { (*self.header).server_alive.load(Ordering::Acquire) }
    }
}

impl<Req: Serial, Resp: Serial> Drop for Client<Req, Resp> {
    fn drop(&mut self) {
        unsafe {
            let alive = &*(self.slot_base.add(self.layout.alive_offset) as *const AtomicBool);
            alive.store(false, Ordering::Release);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_rpc() {
        let name = format!("/shm_rpc_basic_{}", std::process::id());

        unsafe {
            let mut server = Server::<u64, u64>::create(&name, 4).unwrap();

            let name_clone = name.clone();
            let client_thread = std::thread::spawn(move || {
                std::thread::sleep(std::time::Duration::from_millis(10));
                let mut client = Client::<u64, u64>::connect(&name_clone).unwrap();
                let resp = client.call(42).unwrap();
                assert_eq!(resp, 43);
            });

            // Poll for request
            loop {
                if let Some((cid, req)) = server.try_poll() {
                    assert_eq!(req, 42);
                    server.respond(cid, req + 1).unwrap();
                    break;
                }
                std::hint::spin_loop();
            }

            client_thread.join().unwrap();
        }
    }

    #[test]
    fn test_disconnect_detection() {
        let name = format!("/shm_rpc_disc_{}", std::process::id());

        unsafe {
            let server = Server::<u64, u64>::create(&name, 4).unwrap();

            let name_clone = name.clone();
            {
                let client = Client::<u64, u64>::connect(&name_clone).unwrap();
                assert!(server.is_client_connected(client.id()));
                let cid = client.id();
                drop(client);
                assert!(!server.is_client_connected(cid));
            }
        }
    }

    #[test]
    fn test_server_disconnect() {
        let name = format!("/shm_rpc_sdisc_{}", std::process::id());

        unsafe {
            let server = Server::<u64, u64>::create(&name, 4).unwrap();
            let name_clone = name.clone();
            let client = Client::<u64, u64>::connect(&name_clone).unwrap();
            assert!(client.is_server_alive());
            drop(server);
            assert!(!client.is_server_alive());
        }
    }

    #[test]
    fn test_multiple_calls() {
        let name = format!("/shm_rpc_multi_{}", std::process::id());

        unsafe {
            let mut server = Server::<u64, u64>::create(&name, 4).unwrap();

            let name_clone = name.clone();
            let client_thread = std::thread::spawn(move || {
                std::thread::sleep(std::time::Duration::from_millis(10));
                let mut client = Client::<u64, u64>::connect(&name_clone).unwrap();
                for i in 0..100u64 {
                    let resp = client.call(i).unwrap();
                    assert_eq!(resp, i * 2);
                }
            });

            let mut served = 0;
            while served < 100 {
                if let Some((cid, req)) = server.try_poll() {
                    server.respond(cid, req * 2).unwrap();
                    served += 1;
                } else {
                    std::hint::spin_loop();
                }
            }

            client_thread.join().unwrap();
        }
    }

    #[test]
    fn test_different_req_resp_types() {
        let name = format!("/shm_rpc_types_{}", std::process::id());

        unsafe {
            let mut server = Server::<u32, u64>::create(&name, 4).unwrap();

            let name_clone = name.clone();
            let client_thread = std::thread::spawn(move || {
                std::thread::sleep(std::time::Duration::from_millis(10));
                let mut client = Client::<u32, u64>::connect(&name_clone).unwrap();
                let resp = client.call(10u32).unwrap();
                assert_eq!(resp, 100u64);
            });

            loop {
                if let Some((cid, req)) = server.try_poll() {
                    assert_eq!(req, 10u32);
                    server.respond(cid, req as u64 * 10).unwrap();
                    break;
                }
                std::hint::spin_loop();
            }

            client_thread.join().unwrap();
        }
    }
}
