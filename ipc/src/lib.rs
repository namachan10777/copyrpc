//! RPC over shared memory using generation counter SPSC protocol.
//!
//! Each client gets a dedicated SPSC request ring. The server scans all client
//! request rings (flat combining) and replies via per-client SPSC response rings.
//! Supports out-of-order responses via slot_idx matching on the client side.
//!
//! ## Client API
//!
//! - `call(req, user_data)` — non-blocking send; polls internally if ring full
//! - `poll()` — scans response ring, fires `on_response(user_data, resp)`
//! - `call_blocking(req)` — synchronous RPC (`SyncClient` only)
//!
//! ## Server API
//!
//! - `poll()` — scans all client request rings for new requests
//! - `recv()` — returns next request via `RecvHandle` (data is copied)
//! - `reply(token, resp)` — writes response (immediate or deferred)

pub mod shm;

pub use mempc::Serial;
use mempc::common::Response;
use mempc::onesided::{RawReceiver, RawSender, Slot};

use shm::SharedMemory;
use std::io;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

// === Public types ===

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ClientId(pub u32);

/// Opaque token for a pending request. Move-only to prevent double-reply.
#[derive(Debug)]
pub struct RequestToken {
    client_id: ClientId,
    slot_idx: u64,
}

impl RequestToken {
    pub fn client_id(&self) -> ClientId {
        self.client_id
    }
}

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

// === Constants ===

const MAGIC: u64 = 0x5250_4353_4646_5721; // "RPCSFFWD"
const VERSION: u32 = 3;
const HEADER_SIZE: usize = 64;
const CACHE_LINE_SIZE: usize = 64;

fn align_up(val: usize, align: usize) -> usize {
    (val + align - 1) & !(align - 1)
}

// === Header ===

#[repr(C)]
struct Header {
    magic: u64,                // 8
    version: u32,              // 4
    max_clients: u32,          // 4
    ring_depth: u32,           // 4
    req_slot_size: u32,        // 4
    resp_slot_size: u32,       // 4
    extra_buffer_size: u32,    // 4
    server_alive: AtomicBool,  // 1
    _pad: [u8; 3],             // 3
    next_client_id: AtomicU32, // 4
    _reserved: [u8; 20],       // 20 → total = 64
}

const _: () = assert!(std::mem::size_of::<Header>() == HEADER_SIZE);

// === Layout functions ===

fn per_client_block_size(
    req_slot_size: usize,
    resp_slot_size: usize,
    ring_depth: u32,
    extra_buffer_size: u32,
) -> usize {
    let control_size = CACHE_LINE_SIZE;
    let req_ring_size = req_slot_size * ring_depth as usize;
    let resp_ring_size = resp_slot_size * ring_depth as usize;
    align_up(
        control_size + req_ring_size + resp_ring_size + extra_buffer_size as usize,
        CACHE_LINE_SIZE,
    )
}

fn total_shm_size(
    req_slot_size: usize,
    resp_slot_size: usize,
    max_clients: u32,
    ring_depth: u32,
    extra_buffer_size: u32,
) -> usize {
    HEADER_SIZE
        + per_client_block_size(req_slot_size, resp_slot_size, ring_depth, extra_buffer_size)
            * max_clients as usize
}

// === RecvHandle ===

/// Receive handle for accessing request data (copied from SHM).
pub struct RecvHandle<'a, Req: Serial, Resp: Serial> {
    server: &'a mut Server<Req, Resp>,
    client_idx: usize,
    slot_idx: usize,
    data: Req,
}

impl<Req: Serial, Resp: Serial> RecvHandle<'_, Req, Resp> {
    /// Access to the request data.
    #[inline]
    pub fn data(&self) -> &Req {
        &self.data
    }

    /// Consumes the handle and replies immediately.
    #[inline]
    pub fn reply(self, resp: Resp) {
        let response = Response {
            token: self.slot_idx as u64,
            data: resp,
        };
        self.server.resp_txs[self.client_idx].try_send(response);
        self.server.resp_txs[self.client_idx].publish();
    }

    /// Extracts a move-only token for deferred reply.
    #[inline]
    pub fn into_token(self) -> RequestToken {
        RequestToken {
            client_id: ClientId(self.client_idx as u32),
            slot_idx: self.slot_idx as u64,
        }
    }

    #[inline]
    pub fn client_id(&self) -> ClientId {
        ClientId(self.client_idx as u32)
    }
}

// =============================================================================
// Server
// =============================================================================

pub struct Server<Req: Serial, Resp: Serial> {
    _shm: SharedMemory,
    header: *mut Header,
    max_clients: u32,
    ring_depth: u32,
    // Per-client raw SPSC handles
    req_rxs: Vec<RawReceiver<Req>>,
    resp_txs: Vec<RawSender<Response<Resp>>>,
    control_ptrs: Vec<*const AtomicBool>,
    extra_ptrs: Vec<*mut u8>,
    extra_buffer_size: u32,
    // Scan state
    scan_pos: u32,
    recv_scan_pos: u32,
    req_avail: Vec<u32>,
    recv_start: u32,
    _phantom: PhantomData<(Req, Resp)>,
}

unsafe impl<Req: Serial + Send, Resp: Serial + Send> Send for Server<Req, Resp> {}

impl<Req: Serial, Resp: Serial> Server<Req, Resp> {
    /// Creates a new shared memory RPC server.
    ///
    /// # Safety
    /// - `ring_depth` must be a power of 2
    /// - Caller must ensure no other process is using this path
    pub unsafe fn create<P: AsRef<Path>>(
        path: P,
        max_clients: u32,
        ring_depth: u32,
        extra_buffer_size: u32,
    ) -> io::Result<Self> {
        assert!(
            ring_depth.is_power_of_two(),
            "ring_depth must be power of 2"
        );
        assert!(max_clients > 0, "max_clients must be > 0");

        let req_slot_size = std::mem::size_of::<Slot<Req>>();
        let resp_slot_size = std::mem::size_of::<Slot<Response<Resp>>>();

        let size = total_shm_size(
            req_slot_size,
            resp_slot_size,
            max_clients,
            ring_depth,
            extra_buffer_size,
        );
        let shm = unsafe { SharedMemory::create(path, size)? };

        let header = shm.as_ptr() as *mut Header;
        unsafe {
            std::ptr::write(
                header,
                Header {
                    magic: MAGIC,
                    version: VERSION,
                    max_clients,
                    ring_depth,
                    req_slot_size: req_slot_size as u32,
                    resp_slot_size: resp_slot_size as u32,
                    extra_buffer_size,
                    server_alive: AtomicBool::new(true),
                    _pad: [0; 3],
                    next_client_id: AtomicU32::new(0),
                    _reserved: [0; 20],
                },
            );
        }

        let block_size =
            per_client_block_size(req_slot_size, resp_slot_size, ring_depth, extra_buffer_size);
        let extra_offset = CACHE_LINE_SIZE
            + req_slot_size * ring_depth as usize
            + resp_slot_size * ring_depth as usize;

        let mut req_rxs = Vec::with_capacity(max_clients as usize);
        let mut resp_txs = Vec::with_capacity(max_clients as usize);
        let mut control_ptrs = Vec::with_capacity(max_clients as usize);
        let mut extra_ptrs = Vec::with_capacity(max_clients as usize);

        for i in 0..max_clients as usize {
            let block_base = unsafe { shm.as_ptr().add(HEADER_SIZE + i * block_size) };
            let alive = unsafe { &mut *(block_base as *mut AtomicBool) };
            *alive = AtomicBool::new(false);

            control_ptrs.push(block_base as *const AtomicBool);

            let req_ring_base = unsafe { block_base.add(CACHE_LINE_SIZE) };
            let resp_ring_base =
                unsafe { block_base.add(CACHE_LINE_SIZE + ring_depth as usize * req_slot_size) };
            extra_ptrs.push(unsafe { block_base.add(extra_offset) });

            // Initialize all request ring slots
            for slot_idx in 0..ring_depth as usize {
                let slot_ptr =
                    unsafe { req_ring_base.add(slot_idx * req_slot_size) as *mut Slot<Req> };
                unsafe { Slot::<Req>::init(slot_ptr) };
            }

            // Initialize all response ring slots
            for slot_idx in 0..ring_depth as usize {
                let slot_ptr = unsafe {
                    resp_ring_base.add(slot_idx * resp_slot_size) as *mut Slot<Response<Resp>>
                };
                unsafe { Slot::<Response<Resp>>::init(slot_ptr) };
            }

            // Construct RawReceiver and RawSender
            let req_rx = unsafe {
                RawReceiver::<Req>::new(req_ring_base as *const Slot<Req>, ring_depth as usize)
            };
            let resp_tx = unsafe {
                RawSender::<Response<Resp>>::new(
                    resp_ring_base as *mut Slot<Response<Resp>>,
                    ring_depth as usize,
                )
            };

            req_rxs.push(req_rx);
            resp_txs.push(resp_tx);
        }

        Ok(Self {
            _shm: shm,
            header,
            max_clients,
            ring_depth,
            req_rxs,
            resp_txs,
            control_ptrs,
            extra_ptrs,
            extra_buffer_size,
            scan_pos: 0,
            recv_scan_pos: 0,
            req_avail: vec![0; max_clients as usize],
            recv_start: 0,
            _phantom: PhantomData,
        })
    }

    /// Scan all client request rings. Returns total available request count.
    pub fn poll(&mut self) -> u32 {
        let mut count = 0u32;
        let mut scan = self.scan_pos;

        for _ in 0..self.max_clients {
            let idx = scan as usize;
            scan = (scan + 1) % self.max_clients;

            let alive = unsafe { &*self.control_ptrs[idx] };
            if !alive.load(Ordering::Relaxed) {
                self.req_avail[idx] = 0;
                continue;
            }

            // Multi-drain: count consecutive available slots
            let avail = self.req_rxs[idx].count_available(self.ring_depth);
            self.req_avail[idx] = avail;
            count += avail;
        }

        self.scan_pos = scan;
        self.recv_start = self.scan_pos;
        self.recv_scan_pos = 0;
        count
    }

    /// Receive a request. Lane-drains per client.
    pub fn recv(&mut self) -> Option<RecvHandle<'_, Req, Resp>> {
        while (self.recv_scan_pos as usize) < self.max_clients as usize {
            let client_idx = ((self.recv_start + self.recv_scan_pos) % self.max_clients) as usize;

            if self.req_avail[client_idx] > 0
                && let Some((slot_idx, data)) = self.req_rxs[client_idx].try_recv()
            {
                self.req_avail[client_idx] -= 1;
                return Some(RecvHandle {
                    server: self,
                    client_idx,
                    slot_idx,
                    data,
                });
            }

            self.recv_scan_pos += 1;
        }
        None
    }

    /// Reply to a previously received request using its token.
    pub fn reply(&mut self, token: RequestToken, resp: Resp) {
        let idx = token.client_id.0 as usize;
        let response = Response {
            token: token.slot_idx,
            data: resp,
        };
        self.resp_txs[idx].try_send(response);
        self.resp_txs[idx].publish();
    }

    pub fn is_client_connected(&self, client_id: ClientId) -> bool {
        let client_idx = client_id.0 as usize;
        if client_idx >= self.max_clients as usize {
            return false;
        }
        let alive = unsafe { &*self.control_ptrs[client_idx] };
        alive.load(Ordering::Relaxed)
    }

    pub fn connected_clients(&self) -> u32 {
        (0..self.max_clients)
            .filter(|&i| self.is_client_connected(ClientId(i)))
            .count() as u32
    }

    /// Returns a pointer to the client's extra buffer region, or `None` if
    /// extra_buffer_size is 0 or the client_id is invalid.
    pub fn client_extra_buffer(&self, client_id: ClientId) -> Option<*mut u8> {
        let idx = client_id.0 as usize;
        if self.extra_buffer_size == 0 || idx >= self.max_clients as usize {
            return None;
        }
        Some(self.extra_ptrs[idx])
    }

    pub fn extra_buffer_size(&self) -> u32 {
        self.extra_buffer_size
    }
}

impl<Req: Serial, Resp: Serial> Drop for Server<Req, Resp> {
    fn drop(&mut self) {
        unsafe {
            (*self.header).server_alive.store(false, Ordering::Release);
        }
    }
}

// =============================================================================
// Client
// =============================================================================

pub struct Client<Req: Serial, Resp: Serial, U, F: FnMut(U, Resp)> {
    _shm: SharedMemory,
    header: *const Header,
    client_id: ClientId,
    ring_depth: u32,
    // Raw SPSC handles
    req_tx: RawSender<Req>,
    resp_rx: RawReceiver<Response<Resp>>,
    control_ptr: *const AtomicBool,
    extra_ptr: *mut u8,
    extra_buffer_size: u32,
    // Ring state
    in_flight: u32,
    on_response: F,
    user_data: Vec<Option<U>>,
    _phantom: PhantomData<(Req, Resp)>,
}

unsafe impl<Req: Serial + Send, Resp: Serial + Send, U: Send, F: FnMut(U, Resp) + Send> Send
    for Client<Req, Resp, U, F>
{
}

impl<Req: Serial, Resp: Serial, U, F: FnMut(U, Resp)> Client<Req, Resp, U, F> {
    /// Connect to an existing shared memory RPC server.
    ///
    /// # Safety
    /// Server must have been created with matching `Req`/`Resp` types.
    pub unsafe fn connect<P: AsRef<Path>>(path: P, on_response: F) -> Result<Self, ConnectError> {
        let header_shm =
            unsafe { SharedMemory::open(&path, HEADER_SIZE).map_err(ConnectError::Io)? };
        let header_ptr = header_shm.as_ptr() as *const Header;

        let (max_clients, ring_depth, extra_buf_size, req_slot_size, resp_slot_size) = unsafe {
            if (*header_ptr).magic != MAGIC {
                return Err(ConnectError::LayoutMismatch);
            }
            if (*header_ptr).version != VERSION {
                return Err(ConnectError::LayoutMismatch);
            }
            if !(*header_ptr).server_alive.load(Ordering::Acquire) {
                return Err(ConnectError::ServerNotAlive);
            }
            if (*header_ptr).req_slot_size != std::mem::size_of::<Slot<Req>>() as u32
                || (*header_ptr).resp_slot_size
                    != std::mem::size_of::<Slot<Response<Resp>>>() as u32
            {
                return Err(ConnectError::LayoutMismatch);
            }
            (
                (*header_ptr).max_clients,
                (*header_ptr).ring_depth,
                (*header_ptr).extra_buffer_size,
                (*header_ptr).req_slot_size as usize,
                (*header_ptr).resp_slot_size as usize,
            )
        };

        drop(header_shm);

        let size = total_shm_size(
            req_slot_size,
            resp_slot_size,
            max_clients,
            ring_depth,
            extra_buf_size,
        );
        let shm = unsafe { SharedMemory::open(path, size).map_err(ConnectError::Io)? };
        let header = shm.as_ptr() as *const Header;

        unsafe {
            let client_idx = (*header).next_client_id.fetch_add(1, Ordering::Relaxed);
            if client_idx >= max_clients {
                return Err(ConnectError::ServerFull);
            }

            let block_size =
                per_client_block_size(req_slot_size, resp_slot_size, ring_depth, extra_buf_size);
            let block_base = shm
                .as_ptr()
                .add(HEADER_SIZE + client_idx as usize * block_size);
            let control_ptr = block_base as *const AtomicBool;
            (&*control_ptr).store(true, Ordering::Release);

            let req_ring_base = block_base.add(CACHE_LINE_SIZE);
            let resp_ring_base =
                block_base.add(CACHE_LINE_SIZE + ring_depth as usize * req_slot_size);
            let extra_offset = CACHE_LINE_SIZE
                + req_slot_size * ring_depth as usize
                + resp_slot_size * ring_depth as usize;
            let extra_ptr = block_base.add(extra_offset);

            // Construct RawSender and RawReceiver
            // Note: server has already initialized all slots at creation time
            let req_tx =
                RawSender::<Req>::new(req_ring_base as *mut Slot<Req>, ring_depth as usize);
            let resp_rx = RawReceiver::<Response<Resp>>::new(
                resp_ring_base as *const Slot<Response<Resp>>,
                ring_depth as usize,
            );

            Ok(Self {
                _shm: shm,
                header,
                client_id: ClientId(client_idx),
                ring_depth,
                req_tx,
                resp_rx,
                control_ptr,
                extra_ptr,
                extra_buffer_size: extra_buf_size,
                in_flight: 0,
                on_response,
                user_data: (0..ring_depth as usize).map(|_| None).collect(),
                _phantom: PhantomData,
            })
        }
    }

    /// Send a request with associated user data.
    pub fn call(&mut self, req: Req, user_data: U) -> Result<(), CallError> {
        if !self.is_server_alive() {
            return Err(CallError::ServerDisconnected);
        }

        // Wait until the next ring slot's previous response has been consumed.
        // Using in_flight alone is insufficient: out-of-order responses can free
        // a *different* slot, allowing head to advance into a slot whose response
        // is still pending, overwriting its user_data and causing a panic on the
        // duplicate token.
        let mask = self.ring_depth as usize - 1;
        loop {
            let next_slot = self.req_tx.head() & mask;
            if self.user_data[next_slot].is_none() {
                break;
            }
            if self.poll()? == 0 {
                std::hint::spin_loop();
            }
        }

        let slot_idx = self
            .req_tx
            .try_send(req)
            .expect("inflight management prevents full");
        self.req_tx.publish();
        self.user_data[slot_idx] = Some(user_data);
        self.in_flight += 1;

        Ok(())
    }

    /// Poll for responses and invoke callbacks. Returns count processed.
    pub fn poll(&mut self) -> Result<u32, CallError> {
        let mut count = 0;

        while self.in_flight > 0 {
            match self.resp_rx.try_recv() {
                Some((_, response)) => {
                    self.in_flight -= 1;
                    let ud_idx = response.token as usize;
                    let user_data = self.user_data[ud_idx].take().expect("user_data must exist");
                    (self.on_response)(user_data, response.data);
                    count += 1;
                }
                None => break,
            }
        }

        if count == 0 && self.in_flight > 0 && !self.is_server_alive() {
            return Err(CallError::ServerDisconnected);
        }

        Ok(count)
    }

    pub fn pending_count(&self) -> u32 {
        self.in_flight
    }

    pub fn id(&self) -> ClientId {
        self.client_id
    }

    pub fn is_server_alive(&self) -> bool {
        unsafe { (*self.header).server_alive.load(Ordering::Acquire) }
    }

    pub fn extra_buffer(&self) -> *mut u8 {
        self.extra_ptr
    }

    pub fn extra_buffer_size(&self) -> u32 {
        self.extra_buffer_size
    }
}

impl<Req: Serial, Resp: Serial, U, F: FnMut(U, Resp)> Drop for Client<Req, Resp, U, F> {
    fn drop(&mut self) {
        let alive = unsafe { &*self.control_ptr };
        alive.store(false, Ordering::Release);
    }
}

// === SyncClient ===

pub type SyncClient<Req, Resp> = Client<Req, Resp, (), fn((), Resp)>;

impl<Req: Serial, Resp: Serial> SyncClient<Req, Resp> {
    /// Connect for synchronous-only usage.
    ///
    /// # Safety
    /// Same as [`Client::connect`].
    pub unsafe fn connect_sync<P: AsRef<Path>>(path: P) -> Result<Self, ConnectError> {
        unsafe { Self::connect(path, |_, _| {}) }
    }

    /// Blocking RPC call. Must not be mixed with async `call()` on the same client.
    pub fn call_blocking(&mut self, req: Req) -> Result<Resp, CallError> {
        if !self.is_server_alive() {
            return Err(CallError::ServerDisconnected);
        }

        while self.in_flight >= self.ring_depth {
            if self.poll()? == 0 {
                std::hint::spin_loop();
            }
        }

        self.req_tx
            .try_send(req)
            .expect("inflight management prevents full");
        self.req_tx.publish();
        self.in_flight += 1;

        loop {
            if let Some((_, response)) = self.resp_rx.try_recv() {
                self.in_flight -= 1;
                return Ok(response.data);
            }
            if !self.is_server_alive() {
                return Err(CallError::ServerDisconnected);
            }
            std::hint::spin_loop();
        }
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::ptr::{read_volatile, write_volatile};
    use std::sync::atomic::AtomicU64;

    static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn test_path() -> String {
        format!(
            "/shm_ipc_test_{}_{}",
            std::process::id(),
            TEST_COUNTER.fetch_add(1, Ordering::Relaxed)
        )
    }

    #[test]
    fn test_basic_rpc() {
        let path = test_path();
        unsafe {
            let mut server = Server::<u64, u64>::create(&path, 4, 8, 0).unwrap();
            let mut client = SyncClient::<u64, u64>::connect_sync(&path).unwrap();

            client.call(42, ()).unwrap();
            assert_eq!(server.poll(), 1);
            let handle = server.recv().unwrap();
            assert_eq!(*handle.data(), 42);
            handle.reply(84);

            while client.pending_count() > 0 {
                client.poll().unwrap();
            }
        }
    }

    #[test]
    fn test_blocking_rpc() {
        let path = test_path();
        unsafe {
            let mut server = Server::<u64, u64>::create(&path, 4, 8, 0).unwrap();
            let path2 = path.clone();
            let t = std::thread::spawn(move || {
                std::thread::sleep(std::time::Duration::from_millis(10));
                let mut client = SyncClient::<u64, u64>::connect_sync(&path2).unwrap();
                let resp = client.call_blocking(42).unwrap();
                assert_eq!(resp, 43);
            });

            loop {
                server.poll();
                if let Some(handle) = server.recv() {
                    let req = *handle.data();
                    handle.reply(req + 1);
                    break;
                }
                std::hint::spin_loop();
            }
            t.join().unwrap();
        }
    }

    #[test]
    fn test_disconnect_detection() {
        let path = test_path();
        unsafe {
            let server = Server::<u64, u64>::create(&path, 4, 8, 0).unwrap();
            let client = SyncClient::<u64, u64>::connect_sync(&path).unwrap();

            assert!(client.is_server_alive());
            assert_eq!(server.connected_clients(), 1);

            let cid = client.id();
            assert!(server.is_client_connected(cid));
            drop(client);
            assert!(!server.is_client_connected(cid));
        }
    }

    #[test]
    fn test_server_disconnect() {
        let path = test_path();
        unsafe {
            let server = Server::<u64, u64>::create(&path, 4, 8, 0).unwrap();
            let client = SyncClient::<u64, u64>::connect_sync(&path).unwrap();
            assert!(client.is_server_alive());
            drop(server);
            assert!(!client.is_server_alive());
        }
    }

    #[test]
    fn test_multiple_calls() {
        let path = test_path();
        unsafe {
            let mut server = Server::<u64, u64>::create(&path, 4, 16, 0).unwrap();
            let mut client = SyncClient::<u64, u64>::connect_sync(&path).unwrap();

            for i in 0..100 {
                client.call(i, ()).unwrap();
                server.poll();
                if let Some(handle) = server.recv() {
                    let req = *handle.data();
                    handle.reply(req * 2);
                }
                while client.pending_count() > 0 {
                    client.poll().unwrap();
                }
            }
        }
    }

    #[test]
    fn test_deferred_response() {
        let path = test_path();
        unsafe {
            let mut server = Server::<u64, u64>::create(&path, 4, 8, 0).unwrap();
            let path2 = path.clone();
            let t = std::thread::spawn(move || {
                std::thread::sleep(std::time::Duration::from_millis(10));
                let mut client = SyncClient::<u64, u64>::connect_sync(&path2).unwrap();
                let mut results = Vec::new();
                for i in 0..4u64 {
                    results.push(client.call_blocking(i).unwrap());
                }
                results
            });

            let mut deferred: Vec<(RequestToken, u64)> = Vec::new();
            let mut served = 0u32;

            while served < 4 {
                server.poll();
                while let Some(handle) = server.recv() {
                    served += 1;
                    let req = *handle.data();
                    if req % 2 == 0 {
                        let token = handle.into_token();
                        deferred.push((token, req));
                    } else {
                        handle.reply(req * 10);
                    }
                }
                for (token, req) in deferred.drain(..) {
                    server.reply(token, req * 100);
                }
                std::hint::spin_loop();
            }

            let results = t.join().unwrap();
            assert_eq!(results, vec![0, 10, 200, 30]);
        }
    }

    #[test]
    fn test_async_pipeline() {
        use std::cell::RefCell;
        use std::rc::Rc;

        let path = test_path();
        unsafe {
            let mut server = Server::<u64, u64>::create(&path, 4, 16, 0).unwrap();

            let responses = Rc::new(RefCell::new(Vec::new()));
            let responses_clone = responses.clone();

            let mut client: Client<u64, u64, u64, _> =
                Client::connect(&path, move |id: u64, resp| {
                    responses_clone.borrow_mut().push((id, resp));
                })
                .unwrap();

            for i in 0..10 {
                client.call(i, i).unwrap();
            }

            while responses.borrow().len() < 10 {
                server.poll();
                while let Some(handle) = server.recv() {
                    let req = *handle.data();
                    handle.reply(req * 2);
                }
                client.poll().unwrap();
            }

            let mut r = responses.borrow().clone();
            r.sort_by_key(|(id, _)| *id);
            for (i, (id, resp)) in r.iter().enumerate() {
                assert_eq!(*id, i as u64);
                assert_eq!(*resp, (i as u64) * 2);
            }
        }
    }

    #[test]
    fn test_slab_ooo_response() {
        use std::cell::RefCell;
        use std::rc::Rc;

        let path = test_path();
        unsafe {
            let mut server = Server::<u64, u64>::create(&path, 4, 8, 0).unwrap();

            let responses = Rc::new(RefCell::new(Vec::new()));
            let responses_clone = responses.clone();

            let mut client: Client<u64, u64, u64, _> =
                Client::connect(&path, move |ud: u64, resp| {
                    responses_clone.borrow_mut().push((ud, resp));
                })
                .unwrap();

            // Send 4 requests with user_data 100, 200, 300, 400
            client.call(10, 100).unwrap();
            client.call(20, 200).unwrap();
            client.call(30, 300).unwrap();
            client.call(40, 400).unwrap();

            // Server receives all 4 and defers them
            server.poll();
            let mut tokens = Vec::new();
            let mut reqs = Vec::new();
            while let Some(handle) = server.recv() {
                let req = *handle.data();
                reqs.push(req);
                tokens.push(handle.into_token());
            }
            assert_eq!(reqs, vec![10, 20, 30, 40]);

            // Reply in REVERSE order
            for (token, req) in tokens.into_iter().zip(reqs.iter()).rev() {
                server.reply(token, *req * 2);
            }

            while client.pending_count() > 0 {
                client.poll().unwrap();
            }

            let r = responses.borrow();
            assert_eq!(r.len(), 4);
            // Verify correct user_data ↔ response pairing
            for &(ud, resp) in r.iter() {
                match ud {
                    100 => assert_eq!(resp, 20),
                    200 => assert_eq!(resp, 40),
                    300 => assert_eq!(resp, 60),
                    400 => assert_eq!(resp, 80),
                    _ => panic!("unexpected user_data {}", ud),
                }
            }
        }
    }

    #[test]
    fn test_multi_client() {
        let path = test_path();
        unsafe {
            let mut server = Server::<u64, u64>::create(&path, 4, 16, 0).unwrap();

            let mut clients: Vec<SyncClient<u64, u64>> = vec![
                SyncClient::connect_sync(&path).unwrap(),
                SyncClient::connect_sync(&path).unwrap(),
                SyncClient::connect_sync(&path).unwrap(),
            ];

            assert_eq!(server.connected_clients(), 3);

            for (i, client) in clients.iter_mut().enumerate() {
                client.call(i as u64 * 100, ()).unwrap();
            }

            let mut count = 0;
            while count < 3 {
                count += server.poll();
                while let Some(handle) = server.recv() {
                    let req = *handle.data();
                    handle.reply(req + 1);
                }
            }

            for client in clients.iter_mut() {
                while client.pending_count() > 0 {
                    client.poll().unwrap();
                }
            }
        }
    }

    #[test]
    fn test_extra_buffer() {
        let path = test_path();
        unsafe {
            let server = Server::<u64, u64>::create(&path, 4, 8, 128).unwrap();
            assert_eq!(server.extra_buffer_size(), 128);

            let client = SyncClient::<u64, u64>::connect_sync(&path).unwrap();
            assert_eq!(client.extra_buffer_size(), 128);

            // Client writes to extra buffer
            let client_buf = client.extra_buffer();
            write_volatile(client_buf as *mut u64, 0xDEAD_BEEF);

            // Server reads same region
            let server_buf = server.client_extra_buffer(client.id()).unwrap();
            let val = read_volatile(server_buf as *const u64);
            assert_eq!(val, 0xDEAD_BEEF);
        }
    }

    #[test]
    fn test_no_extra_buffer() {
        let path = test_path();
        unsafe {
            let server = Server::<u64, u64>::create(&path, 4, 8, 0).unwrap();
            assert_eq!(server.extra_buffer_size(), 0);
            assert!(server.client_extra_buffer(ClientId(0)).is_none());
        }
    }
}
