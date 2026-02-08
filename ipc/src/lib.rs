//! RPC over shared memory using ffwd delegation pattern.
//!
//! Each client gets a dedicated SPSC request ring. The server scans all client
//! request rings (flat combining) and replies via per-client SPSC response rings.
//! Supports out-of-order responses via slab-keyed user data on the client side.
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
//! - `recv()` — returns next request via zero-copy `RecvHandle`
//! - `reply(token, resp)` — writes response (immediate or deferred)

pub mod shm;

use shm::SharedMemory;
use std::cell::Cell;
use std::cell::UnsafeCell;
use std::io;
use std::marker::PhantomData;
use std::path::Path;
use std::ptr::{read_volatile, write_volatile};
use std::sync::atomic::{compiler_fence, AtomicBool, AtomicU32, Ordering};

// === Serial trait ===

/// Marker trait for types safely transmittable through the channel.
///
/// # Safety
/// Types must be `Copy` with a stable memory layout suitable for IPC.
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

// === Public types ===

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ClientId(pub u32);

/// Opaque token for a pending request. Move-only to prevent double-reply.
#[derive(Debug)]
pub struct RequestToken {
    client_id: ClientId,
    slab_key: u32,
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
const VERSION: u32 = 2;
const HEADER_SIZE: usize = 64;
const CACHE_LINE_SIZE: usize = 64;
const SLAB_KEY_OFFSET: usize = 4;

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
    req_size: u32,             // 4
    resp_size: u32,            // 4
    req_align: u32,            // 4
    resp_align: u32,           // 4
    extra_buffer_size: u32,    // 4
    server_alive: AtomicBool,  // 1
    _pad: [u8; 3],            // 3
    next_client_id: AtomicU32, // 4
    _reserved: [u8; 16],      // 16 → total = 64
}

const _: () = assert!(std::mem::size_of::<Header>() == HEADER_SIZE);

// === Slot layout ===
//
// Each slot: [valid: u8][pad: 3B][slab_key: u32][data at offset 8+]
// Slot size is cache-line aligned.

fn calc_req_slot_size<Req: Serial>() -> usize {
    let data_offset = align_up(8, std::mem::align_of::<Req>().max(8));
    align_up(data_offset + std::mem::size_of::<Req>(), CACHE_LINE_SIZE)
}

fn calc_resp_slot_size<Resp: Serial>() -> usize {
    let data_offset = align_up(8, std::mem::align_of::<Resp>().max(8));
    align_up(data_offset + std::mem::size_of::<Resp>(), CACHE_LINE_SIZE)
}

fn per_client_block_size<Req: Serial, Resp: Serial>(
    ring_depth: u32,
    extra_buffer_size: u32,
) -> usize {
    let control_size = CACHE_LINE_SIZE;
    let req_ring_size = calc_req_slot_size::<Req>() * ring_depth as usize;
    let resp_ring_size = calc_resp_slot_size::<Resp>() * ring_depth as usize;
    align_up(
        control_size + req_ring_size + resp_ring_size + extra_buffer_size as usize,
        CACHE_LINE_SIZE,
    )
}

fn total_shm_size<Req: Serial, Resp: Serial>(
    max_clients: u32,
    ring_depth: u32,
    extra_buffer_size: u32,
) -> usize {
    HEADER_SIZE
        + per_client_block_size::<Req, Resp>(ring_depth, extra_buffer_size) * max_clients as usize
}

// === RecvHandle ===

/// Zero-copy receive handle for accessing request data in shared memory.
pub struct RecvHandle<'a, Req: Serial, Resp: Serial> {
    server: &'a Server<Req, Resp>,
    client_idx: usize,
    tail: u64,
    slab_key: u32,
    _marker: PhantomData<(Req, Resp)>,
}

impl<Req: Serial, Resp: Serial> RecvHandle<'_, Req, Resp> {
    /// Zero-copy access to the request data.
    #[inline]
    pub fn data(&self) -> &Req {
        let slot_idx = (self.tail & self.server.ring_mask) as usize;
        let slot_ptr = unsafe {
            self.server.req_ring_bases[self.client_idx]
                .add(slot_idx * self.server.req_slot_size)
        };
        unsafe { &*(slot_ptr.add(self.server.req_data_offset) as *const Req) }
    }

    /// Consumes the handle and replies immediately.
    #[inline]
    pub fn reply(self, resp: Resp) {
        let slot_idx = (self.tail & self.server.ring_mask) as usize;
        let slot_ptr = unsafe {
            self.server.req_ring_bases[self.client_idx]
                .add(slot_idx * self.server.req_slot_size)
        };
        compiler_fence(Ordering::Release);
        unsafe { write_volatile(slot_ptr, 0) };

        let req_tails = unsafe { &mut *self.server.req_tails.get() };
        req_tails[self.client_idx] = self.tail + 1;
        let req_avail = unsafe { &mut *self.server.req_avail.get() };
        req_avail[self.client_idx] -= 1;

        self.server
            .reply_to_client(ClientId(self.client_idx as u32), self.slab_key, resp);
    }

    /// Extracts a move-only token for deferred reply.
    #[inline]
    pub fn into_token(self) -> RequestToken {
        let slot_idx = (self.tail & self.server.ring_mask) as usize;
        let slot_ptr = unsafe {
            self.server.req_ring_bases[self.client_idx]
                .add(slot_idx * self.server.req_slot_size)
        };
        compiler_fence(Ordering::Release);
        unsafe { write_volatile(slot_ptr, 0) };

        let req_tails = unsafe { &mut *self.server.req_tails.get() };
        req_tails[self.client_idx] = self.tail + 1;
        let req_avail = unsafe { &mut *self.server.req_avail.get() };
        req_avail[self.client_idx] -= 1;

        RequestToken {
            client_id: ClientId(self.client_idx as u32),
            slab_key: self.slab_key,
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
    ring_mask: u64,
    req_slot_size: usize,
    resp_slot_size: usize,
    // Cached per-client SHM pointers
    req_ring_bases: Vec<*mut u8>,
    resp_ring_bases: Vec<*mut u8>,
    control_ptrs: Vec<*const AtomicBool>,
    extra_ptrs: Vec<*mut u8>,
    // Precomputed data offsets
    req_data_offset: usize,
    resp_data_offset: usize,
    extra_buffer_size: u32,
    // Per-client ring state (interior mutability)
    req_tails: UnsafeCell<Vec<u64>>,
    resp_heads: UnsafeCell<Vec<u64>>,
    scan_pos: Cell<u32>,
    // Zero-copy recv: per-client available count + lane-drain state
    req_avail: UnsafeCell<Vec<u32>>,
    recv_start: Cell<u32>,
    recv_scan_pos: Cell<u32>,
    // Deferred responses: (client_id, slab_key, resp)
    deferred_responses: UnsafeCell<Vec<(ClientId, u32, Resp)>>,
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

        let size = total_shm_size::<Req, Resp>(max_clients, ring_depth, extra_buffer_size);
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
                    req_size: std::mem::size_of::<Req>() as u32,
                    resp_size: std::mem::size_of::<Resp>() as u32,
                    req_align: std::mem::align_of::<Req>() as u32,
                    resp_align: std::mem::align_of::<Resp>() as u32,
                    extra_buffer_size,
                    server_alive: AtomicBool::new(true),
                    _pad: [0; 3],
                    next_client_id: AtomicU32::new(0),
                    _reserved: [0; 16],
                },
            );
        }

        let req_slot_size = calc_req_slot_size::<Req>();
        let resp_slot_size = calc_resp_slot_size::<Resp>();
        let block_size = per_client_block_size::<Req, Resp>(ring_depth, extra_buffer_size);
        let extra_offset = CACHE_LINE_SIZE
            + req_slot_size * ring_depth as usize
            + resp_slot_size * ring_depth as usize;

        let mut req_ring_bases = Vec::with_capacity(max_clients as usize);
        let mut resp_ring_bases = Vec::with_capacity(max_clients as usize);
        let mut control_ptrs = Vec::with_capacity(max_clients as usize);
        let mut extra_ptrs = Vec::with_capacity(max_clients as usize);

        for i in 0..max_clients as usize {
            let block_base = unsafe { shm.as_ptr().add(HEADER_SIZE + i * block_size) };
            let alive = unsafe { &mut *(block_base as *mut AtomicBool) };
            *alive = AtomicBool::new(false);

            control_ptrs.push(block_base as *const AtomicBool);
            req_ring_bases.push(unsafe { block_base.add(CACHE_LINE_SIZE) });
            resp_ring_bases.push(unsafe {
                block_base.add(CACHE_LINE_SIZE + ring_depth as usize * req_slot_size)
            });
            extra_ptrs.push(unsafe { block_base.add(extra_offset) });
        }

        let req_data_offset = align_up(8, std::mem::align_of::<Req>().max(8));
        let resp_data_offset = align_up(8, std::mem::align_of::<Resp>().max(8));

        Ok(Self {
            _shm: shm,
            header,
            max_clients,
            ring_mask: (ring_depth - 1) as u64,
            req_slot_size,
            resp_slot_size,
            req_ring_bases,
            resp_ring_bases,
            control_ptrs,
            extra_ptrs,
            req_data_offset,
            resp_data_offset,
            extra_buffer_size,
            req_tails: UnsafeCell::new(vec![0; max_clients as usize]),
            resp_heads: UnsafeCell::new(vec![0; max_clients as usize]),
            scan_pos: Cell::new(0),
            req_avail: UnsafeCell::new(vec![0; max_clients as usize]),
            recv_start: Cell::new(0),
            recv_scan_pos: Cell::new(0),
            deferred_responses: UnsafeCell::new(Vec::new()),
            _phantom: PhantomData,
        })
    }

    fn flush_deferred(&self) {
        let deferred = unsafe { &mut *self.deferred_responses.get() };
        let resp_heads = unsafe { &mut *self.resp_heads.get() };
        let mut i = 0;
        while i < deferred.len() {
            let (client_id, _, _) = &deferred[i];
            let client_idx = client_id.0 as usize;
            let head = resp_heads[client_idx];
            let slot_idx = (head & self.ring_mask) as usize;
            let slot_ptr = unsafe {
                self.resp_ring_bases[client_idx].add(slot_idx * self.resp_slot_size)
            };
            let valid = unsafe { read_volatile(slot_ptr as *const u8) };
            if valid != 0 {
                i += 1;
                continue;
            }

            let (_, slab_key, resp) = deferred.swap_remove(i);

            unsafe { write_volatile(slot_ptr.add(SLAB_KEY_OFFSET) as *mut u32, slab_key) };
            let resp_ptr = unsafe { slot_ptr.add(self.resp_data_offset) as *mut Resp };
            unsafe { write_volatile(resp_ptr, resp) };
            compiler_fence(Ordering::Release);
            unsafe { write_volatile(slot_ptr, 1) };

            resp_heads[client_idx] = head + 1;
        }
    }

    /// Scan all client request rings. Returns total available request count.
    pub fn poll(&self) -> u32 {
        let deferred = unsafe { &*self.deferred_responses.get() };
        if !deferred.is_empty() {
            self.flush_deferred();
        }

        let recv_start = self.scan_pos.get();
        let mut count = 0u32;
        let mut scan = self.scan_pos.get();

        let req_tails = unsafe { &*self.req_tails.get() };
        let req_avail = unsafe { &mut *self.req_avail.get() };

        for _ in 0..self.max_clients {
            let client_idx = scan as usize;
            scan = (scan + 1) % self.max_clients;

            let alive = unsafe { &*self.control_ptrs[client_idx] };
            if !alive.load(Ordering::Relaxed) {
                req_avail[client_idx] = 0;
                continue;
            }

            let req_ring_base = self.req_ring_bases[client_idx];
            let ring_depth = (self.ring_mask + 1) as u32;
            let mut tail = req_tails[client_idx];
            let mut avail = 0u32;
            while avail < ring_depth {
                let slot_idx = (tail & self.ring_mask) as usize;
                let slot_ptr = unsafe { req_ring_base.add(slot_idx * self.req_slot_size) };
                let valid = unsafe { read_volatile(slot_ptr as *const u8) };
                if valid == 0 {
                    break;
                }
                avail += 1;
                tail += 1;
            }
            req_avail[client_idx] = avail;
            count += avail;
        }

        self.scan_pos.set(scan);
        self.recv_start.set(recv_start);
        self.recv_scan_pos.set(0);
        count
    }

    /// Receive a request directly from SHM. Lane-drains per client.
    pub fn recv(&self) -> Option<RecvHandle<'_, Req, Resp>> {
        let req_avail = unsafe { &*self.req_avail.get() };
        let req_tails = unsafe { &*self.req_tails.get() };

        let mut scan = self.recv_scan_pos.get();
        while (scan as usize) < self.max_clients as usize {
            let client_idx =
                ((self.recv_start.get() + scan) % self.max_clients) as usize;

            if req_avail[client_idx] > 0 {
                let tail = req_tails[client_idx];
                let slot_idx = (tail & self.ring_mask) as usize;
                let slot_ptr = unsafe {
                    self.req_ring_bases[client_idx].add(slot_idx * self.req_slot_size)
                };
                let slab_key =
                    unsafe { read_volatile(slot_ptr.add(SLAB_KEY_OFFSET) as *const u32) };

                return Some(RecvHandle {
                    server: self,
                    client_idx,
                    tail,
                    slab_key,
                    _marker: PhantomData,
                });
            }

            scan += 1;
            self.recv_scan_pos.set(scan);
        }
        None
    }

    fn reply_to_client(&self, client_id: ClientId, slab_key: u32, resp: Resp) {
        let client_idx = client_id.0 as usize;
        let resp_heads = unsafe { &mut *self.resp_heads.get() };
        let head = resp_heads[client_idx];
        let slot_idx = (head & self.ring_mask) as usize;
        let slot_ptr =
            unsafe { self.resp_ring_bases[client_idx].add(slot_idx * self.resp_slot_size) };

        let valid = unsafe { read_volatile(slot_ptr as *const u8) };
        if valid != 0 {
            let deferred = unsafe { &mut *self.deferred_responses.get() };
            deferred.push((client_id, slab_key, resp));
            return;
        }

        unsafe { write_volatile(slot_ptr.add(SLAB_KEY_OFFSET) as *mut u32, slab_key) };
        let resp_ptr = unsafe { slot_ptr.add(self.resp_data_offset) as *mut Resp };
        unsafe { write_volatile(resp_ptr, resp) };
        compiler_fence(Ordering::Release);
        unsafe { write_volatile(slot_ptr, 1) };

        resp_heads[client_idx] = head + 1;
    }

    /// Reply to a previously received request using its token.
    pub fn reply(&self, token: RequestToken, resp: Resp) {
        self.reply_to_client(token.client_id, token.slab_key, resp);
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
    ring_mask: u64,
    req_slot_size: usize,
    resp_slot_size: usize,
    // Cached pointers
    req_ring_base: *mut u8,
    resp_ring_base: *mut u8,
    control_ptr: *const AtomicBool,
    extra_ptr: *mut u8,
    extra_buffer_size: u32,
    req_data_offset: usize,
    resp_data_offset: usize,
    // Ring state
    req_head: u64,
    resp_tail: u64,
    in_flight: u32,
    on_response: F,
    user_data_slab: slab::Slab<U>,
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
    pub unsafe fn connect<P: AsRef<Path>>(
        path: P,
        on_response: F,
    ) -> Result<Self, ConnectError> {
        let header_shm =
            unsafe { SharedMemory::open(&path, HEADER_SIZE).map_err(ConnectError::Io)? };
        let header_ptr = header_shm.as_ptr() as *const Header;

        let (max_clients, ring_depth, extra_buf_size) = unsafe {
            if (*header_ptr).magic != MAGIC {
                return Err(ConnectError::LayoutMismatch);
            }
            if (*header_ptr).version != VERSION {
                return Err(ConnectError::LayoutMismatch);
            }
            if !(*header_ptr).server_alive.load(Ordering::Acquire) {
                return Err(ConnectError::ServerNotAlive);
            }
            if (*header_ptr).req_size != std::mem::size_of::<Req>() as u32
                || (*header_ptr).resp_size != std::mem::size_of::<Resp>() as u32
                || (*header_ptr).req_align != std::mem::align_of::<Req>() as u32
                || (*header_ptr).resp_align != std::mem::align_of::<Resp>() as u32
            {
                return Err(ConnectError::LayoutMismatch);
            }
            (
                (*header_ptr).max_clients,
                (*header_ptr).ring_depth,
                (*header_ptr).extra_buffer_size,
            )
        };

        drop(header_shm);

        let size = total_shm_size::<Req, Resp>(max_clients, ring_depth, extra_buf_size);
        let shm = unsafe { SharedMemory::open(path, size).map_err(ConnectError::Io)? };
        let header = shm.as_ptr() as *const Header;

        unsafe {
            let client_idx = (*header).next_client_id.fetch_add(1, Ordering::Relaxed);
            if client_idx >= max_clients {
                return Err(ConnectError::ServerFull);
            }

            let req_slot_size = calc_req_slot_size::<Req>();
            let resp_slot_size = calc_resp_slot_size::<Resp>();
            let block_size = per_client_block_size::<Req, Resp>(ring_depth, extra_buf_size);
            let block_base = shm.as_ptr().add(HEADER_SIZE + client_idx as usize * block_size);
            let control_ptr = block_base as *const AtomicBool;
            (&*control_ptr).store(true, Ordering::Release);

            let req_ring_base = block_base.add(CACHE_LINE_SIZE);
            let resp_ring_base =
                block_base.add(CACHE_LINE_SIZE + ring_depth as usize * req_slot_size);
            let extra_offset = CACHE_LINE_SIZE
                + req_slot_size * ring_depth as usize
                + resp_slot_size * ring_depth as usize;
            let extra_ptr = block_base.add(extra_offset);

            let req_data_offset = align_up(8, std::mem::align_of::<Req>().max(8));
            let resp_data_offset = align_up(8, std::mem::align_of::<Resp>().max(8));

            Ok(Self {
                _shm: shm,
                header,
                client_id: ClientId(client_idx),
                ring_depth,
                ring_mask: (ring_depth - 1) as u64,
                req_slot_size,
                resp_slot_size,
                req_ring_base,
                resp_ring_base,
                control_ptr,
                extra_ptr,
                extra_buffer_size: extra_buf_size,
                req_data_offset,
                resp_data_offset,
                req_head: 0,
                resp_tail: 0,
                in_flight: 0,
                on_response,
                user_data_slab: slab::Slab::with_capacity(ring_depth as usize),
                _phantom: PhantomData,
            })
        }
    }

    /// Send a request with associated user data.
    pub fn call(&mut self, req: Req, user_data: U) -> Result<(), CallError> {
        if !self.is_server_alive() {
            return Err(CallError::ServerDisconnected);
        }

        while self.in_flight >= self.ring_depth {
            if self.poll()? == 0 {
                std::hint::spin_loop();
            }
        }

        let slot_idx = (self.req_head & self.ring_mask) as usize;
        let slot_ptr = unsafe { self.req_ring_base.add(slot_idx * self.req_slot_size) };

        loop {
            let valid = unsafe { read_volatile(slot_ptr as *const u8) };
            if valid == 0 {
                break;
            }
            std::hint::spin_loop();
        }

        let key = self.user_data_slab.insert(user_data);
        unsafe { write_volatile(slot_ptr.add(SLAB_KEY_OFFSET) as *mut u32, key as u32) };

        let req_ptr = unsafe { slot_ptr.add(self.req_data_offset) as *mut Req };
        unsafe { write_volatile(req_ptr, req) };

        compiler_fence(Ordering::Release);
        unsafe { write_volatile(slot_ptr, 1) };

        self.req_head += 1;
        self.in_flight += 1;

        Ok(())
    }

    /// Poll for responses and invoke callbacks. Returns count processed.
    pub fn poll(&mut self) -> Result<u32, CallError> {
        let mut count = 0;

        while self.in_flight > 0 {
            let slot_idx = (self.resp_tail & self.ring_mask) as usize;
            let slot_ptr = unsafe { self.resp_ring_base.add(slot_idx * self.resp_slot_size) };

            let valid = unsafe { read_volatile(slot_ptr as *const u8) };
            if valid == 0 {
                break;
            }

            compiler_fence(Ordering::Acquire);

            let slab_key =
                unsafe { read_volatile(slot_ptr.add(SLAB_KEY_OFFSET) as *const u32) };
            let resp_ptr = unsafe { slot_ptr.add(self.resp_data_offset) as *const Resp };
            let resp = unsafe { read_volatile(resp_ptr) };

            compiler_fence(Ordering::Release);
            unsafe { write_volatile(slot_ptr, 0) };

            self.resp_tail += 1;
            self.in_flight -= 1;

            let user_data = self.user_data_slab.remove(slab_key as usize);
            (self.on_response)(user_data, resp);

            count += 1;
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

        let slot_idx = (self.req_head & self.ring_mask) as usize;
        let slot_ptr = unsafe { self.req_ring_base.add(slot_idx * self.req_slot_size) };

        loop {
            let valid = unsafe { read_volatile(slot_ptr as *const u8) };
            if valid == 0 {
                break;
            }
            std::hint::spin_loop();
        }

        // slab_key = 0 for blocking calls (not tracked in slab)
        unsafe { write_volatile(slot_ptr.add(SLAB_KEY_OFFSET) as *mut u32, 0) };

        let req_ptr = unsafe { slot_ptr.add(self.req_data_offset) as *mut Req };
        unsafe { write_volatile(req_ptr, req) };

        compiler_fence(Ordering::Release);
        unsafe { write_volatile(slot_ptr, 1) };

        self.req_head += 1;
        self.in_flight += 1;

        loop {
            let resp_slot_idx = (self.resp_tail & self.ring_mask) as usize;
            let resp_slot_ptr =
                unsafe { self.resp_ring_base.add(resp_slot_idx * self.resp_slot_size) };

            let valid = unsafe { read_volatile(resp_slot_ptr as *const u8) };
            if valid != 0 {
                compiler_fence(Ordering::Acquire);

                let resp_ptr =
                    unsafe { resp_slot_ptr.add(self.resp_data_offset) as *const Resp };
                let resp = unsafe { read_volatile(resp_ptr) };

                compiler_fence(Ordering::Release);
                unsafe { write_volatile(resp_slot_ptr, 0) };

                self.resp_tail += 1;
                self.in_flight -= 1;

                return Ok(resp);
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
            let server = Server::<u64, u64>::create(&path, 4, 8, 0).unwrap();
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
            let server = Server::<u64, u64>::create(&path, 4, 8, 0).unwrap();
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
            let server = Server::<u64, u64>::create(&path, 4, 16, 0).unwrap();
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
            let server = Server::<u64, u64>::create(&path, 4, 8, 0).unwrap();
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
            let server = Server::<u64, u64>::create(&path, 4, 16, 0).unwrap();

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
            let server = Server::<u64, u64>::create(&path, 4, 8, 0).unwrap();

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
            let server = Server::<u64, u64>::create(&path, 4, 16, 0).unwrap();

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
