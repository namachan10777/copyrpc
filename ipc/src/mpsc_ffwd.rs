//! Shared-memory RPC using ffwd delegation pattern.
//!
//! Architecture:
//! - Each client has a dedicated SPSC request ring
//! - Server scans all client request rings (flat combining)
//! - Responses use per-client SPSC rings
//!
//! This differs from `mpsc_shared` by eliminating the shared MPSC request ring,
//! giving each client its own request ring that only the server reads.

use crate::shm::SharedMemory;
use crate::{align_up, CallError, ClientId, ConnectError, Serial, CACHE_LINE_SIZE};
use std::cell::{Cell, UnsafeCell};
use std::collections::VecDeque;
use std::io;
use std::marker::PhantomData;
use std::path::Path;
use std::ptr::{read_volatile, write_volatile};
use std::sync::atomic::{compiler_fence, AtomicBool, AtomicU32, Ordering};

const HEADER_SIZE: usize = 64;
const MAGIC: u64 = 0x5250_4353_4646_5721; // "RPCSFFWD"
const VERSION: u32 = 1;

/// Shared memory header structure
#[repr(C)]
struct Header {
    magic: u64,
    version: u32,
    max_clients: u32,
    ring_depth: u32,
    req_size: u32,
    resp_size: u32,
    req_align: u32,
    resp_align: u32,
    server_alive: AtomicBool,
    next_client_id: AtomicU32,
}

fn calc_req_slot_size<Req: Serial>() -> usize {
    let align = std::mem::align_of::<Req>().max(8);
    let req_offset = align_up(1, align); // valid (1 byte) + padding
    align_up(req_offset + std::mem::size_of::<Req>(), CACHE_LINE_SIZE)
}

fn calc_resp_slot_size<Resp: Serial>() -> usize {
    let align = std::mem::align_of::<Resp>().max(8);
    let resp_offset = align_up(1, align);
    align_up(resp_offset + std::mem::size_of::<Resp>(), CACHE_LINE_SIZE)
}

fn per_client_block_size<Req: Serial, Resp: Serial>(ring_depth: u32) -> usize {
    let control_size = CACHE_LINE_SIZE; // 64B: alive flag + padding
    let req_ring_size = calc_req_slot_size::<Req>() * ring_depth as usize;
    let resp_ring_size = calc_resp_slot_size::<Resp>() * ring_depth as usize;
    control_size + req_ring_size + resp_ring_size
}

fn total_shm_size<Req: Serial, Resp: Serial>(max_clients: u32, ring_depth: u32) -> usize {
    HEADER_SIZE + per_client_block_size::<Req, Resp>(ring_depth) * max_clients as usize
}

/// Token representing a pending request from a specific client
pub struct RequestToken {
    client_id: ClientId,
}

/// Zero-copy receive handle for accessing request data in shared memory.
pub struct RecvHandle<'a, Req: Serial, Resp: Serial> {
    server: &'a Server<Req, Resp>,
    client_idx: usize,
    tail: u64,
    _marker: PhantomData<(Req, Resp)>,
}

impl<Req: Serial, Resp: Serial> RecvHandle<'_, Req, Resp> {
    /// Zero-copy access to the request data.
    #[inline]
    pub fn data(&self) -> &Req {
        let slot_idx = (self.tail & self.server.ring_mask) as usize;
        let slot_ptr = unsafe {
            self.server.req_ring_bases[self.client_idx].add(slot_idx * self.server.req_slot_size)
        };
        unsafe { &*(slot_ptr.add(self.server.req_data_offset) as *const Req) }
    }

    /// Consumes the handle and replies immediately.
    #[inline]
    pub fn reply(self, resp: Resp) {
        // Clear valid flag
        let slot_idx = (self.tail & self.server.ring_mask) as usize;
        let slot_ptr = unsafe {
            self.server.req_ring_bases[self.client_idx].add(slot_idx * self.server.req_slot_size)
        };
        compiler_fence(Ordering::Release);
        unsafe { write_volatile(slot_ptr, 0) };

        let req_tails = unsafe { &mut *self.server.req_tails.get() };
        req_tails[self.client_idx] = self.tail + 1;
        let req_avail = unsafe { &mut *self.server.req_avail.get() };
        req_avail[self.client_idx] -= 1;

        self.server.reply_to_client(ClientId(self.client_idx as u32), resp);
    }

    /// Extracts a non-Copy token for deferred reply.
    #[inline]
    pub fn into_token(self) -> RequestToken {
        // Clear valid flag
        let slot_idx = (self.tail & self.server.ring_mask) as usize;
        let slot_ptr = unsafe {
            self.server.req_ring_bases[self.client_idx].add(slot_idx * self.server.req_slot_size)
        };
        compiler_fence(Ordering::Release);
        unsafe { write_volatile(slot_ptr, 0) };

        let req_tails = unsafe { &mut *self.server.req_tails.get() };
        req_tails[self.client_idx] = self.tail + 1;
        let req_avail = unsafe { &mut *self.server.req_avail.get() };
        req_avail[self.client_idx] -= 1;

        RequestToken {
            client_id: ClientId(self.client_idx as u32),
        }
    }

    /// Returns the client that sent this request.
    #[inline]
    pub fn client_id(&self) -> ClientId {
        ClientId(self.client_idx as u32)
    }
}

/// Server-side RPC handler
pub struct Server<Req: Serial, Resp: Serial> {
    shm: SharedMemory,
    header: *mut Header,
    max_clients: u32,
    ring_mask: u64,
    req_slot_size: usize,
    resp_slot_size: usize,
    // Cached per-client SHM pointers
    req_ring_bases: Vec<*mut u8>,
    resp_ring_bases: Vec<*mut u8>,
    control_ptrs: Vec<*const AtomicBool>,
    // Precomputed data offsets within slots
    req_data_offset: usize,
    resp_data_offset: usize,
    // Per-client ring state (interior mutability)
    req_tails: UnsafeCell<Vec<u64>>,
    resp_heads: UnsafeCell<Vec<u64>>,
    scan_pos: Cell<u32>,
    // Zero-copy recv: per-client available count + lane-drain state
    req_avail: UnsafeCell<Vec<u32>>,
    recv_start: Cell<u32>,
    recv_scan_pos: Cell<u32>,
    // Deferred responses (Vec + swap_remove)
    deferred_responses: UnsafeCell<Vec<(ClientId, Resp)>>,
    _phantom: PhantomData<(Req, Resp)>,
}

impl<Req: Serial, Resp: Serial> Server<Req, Resp> {
    /// Create a new shared memory RPC server.
    ///
    /// # Safety
    /// - `ring_depth` must be a power of 2
    /// - Caller must ensure no other process is using this path
    pub unsafe fn create<P: AsRef<Path>>(
        path: P,
        max_clients: u32,
        ring_depth: u32,
    ) -> io::Result<Self> {
        assert!(ring_depth.is_power_of_two(), "ring_depth must be power of 2");
        assert!(max_clients > 0, "max_clients must be > 0");

        let size = total_shm_size::<Req, Resp>(max_clients, ring_depth);
        let shm = unsafe { SharedMemory::create(path, size)? };

        let header = shm.as_ptr() as *mut Header;
        unsafe {
            (*header).magic = MAGIC;
            (*header).version = VERSION;
            (*header).max_clients = max_clients;
            (*header).ring_depth = ring_depth;
            (*header).req_size = std::mem::size_of::<Req>() as u32;
            (*header).resp_size = std::mem::size_of::<Resp>() as u32;
            (*header).req_align = std::mem::align_of::<Req>() as u32;
            (*header).resp_align = std::mem::align_of::<Resp>() as u32;
            (*header).server_alive = AtomicBool::new(true);
            (*header).next_client_id = AtomicU32::new(0);
        }

        let req_slot_size = calc_req_slot_size::<Req>();
        let resp_slot_size = calc_resp_slot_size::<Resp>();
        let block_size = per_client_block_size::<Req, Resp>(ring_depth);

        // Precompute per-client pointers and initialize control blocks
        let mut req_ring_bases = Vec::with_capacity(max_clients as usize);
        let mut resp_ring_bases = Vec::with_capacity(max_clients as usize);
        let mut control_ptrs = Vec::with_capacity(max_clients as usize);

        for i in 0..max_clients as usize {
            let block_base = unsafe { shm.as_ptr().add(HEADER_SIZE + i * block_size) };

            // Initialize control block
            let alive = unsafe { &mut *(block_base as *mut AtomicBool) };
            *alive = AtomicBool::new(false);

            control_ptrs.push(block_base as *const AtomicBool);
            req_ring_bases.push(unsafe { block_base.add(CACHE_LINE_SIZE) });
            resp_ring_bases.push(unsafe {
                block_base.add(CACHE_LINE_SIZE + ring_depth as usize * req_slot_size)
            });
        }

        // Precompute data offsets
        let req_align = std::mem::align_of::<Req>().max(8);
        let req_data_offset = align_up(1, req_align);
        let resp_align = std::mem::align_of::<Resp>().max(8);
        let resp_data_offset = align_up(1, resp_align);

        Ok(Self {
            shm,
            header,
            max_clients,
            ring_mask: (ring_depth - 1) as u64,
            req_slot_size,
            resp_slot_size,
            req_ring_bases,
            resp_ring_bases,
            control_ptrs,
            req_data_offset,
            resp_data_offset,
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

    /// Try to flush deferred responses to their respective response rings.
    fn flush_deferred(&self) {
        let deferred = unsafe { &mut *self.deferred_responses.get() };
        let resp_heads = unsafe { &mut *self.resp_heads.get() };
        let mut i = 0;
        while i < deferred.len() {
            let (client_id, _) = &deferred[i];
            let client_idx = client_id.0 as usize;
            let head = resp_heads[client_idx];
            let slot_idx = (head & self.ring_mask) as usize;

            let slot_ptr =
                unsafe { self.resp_ring_bases[client_idx].add(slot_idx * self.resp_slot_size) };
            let valid = unsafe { read_volatile(slot_ptr as *const u8) };

            if valid != 0 {
                i += 1;
                continue;
            }

            let (_, resp) = deferred.swap_remove(i);

            let resp_ptr = unsafe { slot_ptr.add(self.resp_data_offset) as *mut Resp };
            unsafe { write_volatile(resp_ptr, resp) };

            compiler_fence(Ordering::Release);
            unsafe { write_volatile(slot_ptr, 1) };

            resp_heads[client_idx] = head + 1;
        }
    }

    /// Scan all client request rings and count available requests.
    /// Returns the total number of requests available for recv().
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

    /// Receive a request directly from SHM.
    /// Drains all requests from the same client before moving to the next.
    pub fn recv(&self) -> Option<RecvHandle<'_, Req, Resp>> {
        let req_avail = unsafe { &*self.req_avail.get() };
        let req_tails = unsafe { &*self.req_tails.get() };

        let mut scan = self.recv_scan_pos.get();
        while (scan as usize) < self.max_clients as usize {
            let client_idx =
                ((self.recv_start.get() + scan) % self.max_clients) as usize;

            if req_avail[client_idx] > 0 {
                let tail = req_tails[client_idx];

                // Read data in RecvHandle::data() instead
                // Don't clear valid or advance tail here - do it in RecvHandle::reply/into_token

                return Some(RecvHandle {
                    server: self,
                    client_idx,
                    tail,
                    _marker: PhantomData,
                });
            }

            scan += 1;
            self.recv_scan_pos.set(scan);
        }
        None
    }

    /// Internal helper to write response to a client's response ring.
    fn reply_to_client(&self, client_id: ClientId, resp: Resp) {
        let client_idx = client_id.0 as usize;
        let resp_heads = unsafe { &mut *self.resp_heads.get() };
        let head = resp_heads[client_idx];
        let slot_idx = (head & self.ring_mask) as usize;

        let slot_ptr =
            unsafe { self.resp_ring_bases[client_idx].add(slot_idx * self.resp_slot_size) };

        let valid = unsafe { read_volatile(slot_ptr as *const u8) };
        if valid != 0 {
            let deferred = unsafe { &mut *self.deferred_responses.get() };
            deferred.push((client_id, resp));
            return;
        }

        let resp_ptr = unsafe { slot_ptr.add(self.resp_data_offset) as *mut Resp };
        unsafe { write_volatile(resp_ptr, resp) };

        compiler_fence(Ordering::Release);
        unsafe { write_volatile(slot_ptr, 1) };

        resp_heads[client_idx] = head + 1;
    }

    /// Send a response to a client.
    pub fn reply(&self, token: RequestToken, resp: Resp) {
        self.reply_to_client(token.client_id, resp);
    }

    /// Check if a client is connected.
    pub fn is_client_connected(&self, client_id: ClientId) -> bool {
        let client_idx = client_id.0 as usize;
        if client_idx >= self.max_clients as usize {
            return false;
        }
        let alive = unsafe { &*self.control_ptrs[client_idx] };
        alive.load(Ordering::Relaxed)
    }

    /// Count connected clients.
    pub fn connected_clients(&self) -> u32 {
        (0..self.max_clients)
            .filter(|&i| self.is_client_connected(ClientId(i)))
            .count() as u32
    }
}

impl<Req: Serial, Resp: Serial> Drop for Server<Req, Resp> {
    fn drop(&mut self) {
        unsafe {
            (*self.header).server_alive.store(false, Ordering::Release);
        }
    }
}

unsafe impl<Req: Serial, Resp: Serial> Send for Server<Req, Resp> {}

/// Client-side RPC caller with async callback interface.
pub struct Client<Req: Serial, Resp: Serial, U, F: FnMut(U, Resp)> {
    shm: SharedMemory,
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
    req_data_offset: usize,
    resp_data_offset: usize,
    // Ring state
    req_head: u64,
    resp_tail: u64,
    in_flight: u32,
    on_response: F,
    user_data_queue: VecDeque<U>,
    _phantom: PhantomData<(Req, Resp)>,
}

impl<Req: Serial, Resp: Serial, U, F: FnMut(U, Resp)> Client<Req, Resp, U, F> {
    /// Connect to an existing shared memory RPC server.
    ///
    /// # Safety
    /// - Server must have been created with matching types
    pub unsafe fn connect<P: AsRef<Path>>(
        path: P,
        on_response: F,
    ) -> Result<Self, ConnectError> {
        // First, open just the header to read configuration
        let header_shm =
            unsafe { SharedMemory::open(&path, HEADER_SIZE).map_err(ConnectError::Io)? };
        let header_ptr = header_shm.as_ptr() as *const Header;

        let (max_clients, ring_depth) = unsafe {
            // Validate basic header fields
            if (*header_ptr).magic != MAGIC {
                return Err(ConnectError::LayoutMismatch);
            }
            if (*header_ptr).version != VERSION {
                return Err(ConnectError::LayoutMismatch);
            }
            if !(*header_ptr).server_alive.load(Ordering::Acquire) {
                return Err(ConnectError::ServerNotAlive);
            }
            if (*header_ptr).req_size != std::mem::size_of::<Req>() as u32 {
                return Err(ConnectError::LayoutMismatch);
            }
            if (*header_ptr).resp_size != std::mem::size_of::<Resp>() as u32 {
                return Err(ConnectError::LayoutMismatch);
            }
            if (*header_ptr).req_align != std::mem::align_of::<Req>() as u32 {
                return Err(ConnectError::LayoutMismatch);
            }
            if (*header_ptr).resp_align != std::mem::align_of::<Resp>() as u32 {
                return Err(ConnectError::LayoutMismatch);
            }

            ((*header_ptr).max_clients, (*header_ptr).ring_depth)
        };

        // Drop the header-only mapping
        drop(header_shm);

        // Now open with full size
        let size = total_shm_size::<Req, Resp>(max_clients, ring_depth);
        let shm = unsafe { SharedMemory::open(path, size).map_err(ConnectError::Io)? };

        let header = shm.as_ptr() as *const Header;

        // Allocate a client ID and mark alive
        unsafe {
            let client_idx = (*header).next_client_id.fetch_add(1, Ordering::Relaxed);
            if client_idx >= max_clients {
                return Err(ConnectError::ServerFull);
            }

            let req_slot_size = calc_req_slot_size::<Req>();
            let resp_slot_size = calc_resp_slot_size::<Resp>();
            let block_size = per_client_block_size::<Req, Resp>(ring_depth);

            let block_base = shm.as_ptr().add(HEADER_SIZE + client_idx as usize * block_size);
            let control_ptr = block_base as *const AtomicBool;

            // Mark this client as alive
            let alive = &*(control_ptr);
            alive.store(true, Ordering::Release);

            let req_ring_base = block_base.add(CACHE_LINE_SIZE);
            let resp_ring_base =
                block_base.add(CACHE_LINE_SIZE + ring_depth as usize * req_slot_size);

            let req_align = std::mem::align_of::<Req>().max(8);
            let req_data_offset = align_up(1, req_align);
            let resp_align = std::mem::align_of::<Resp>().max(8);
            let resp_data_offset = align_up(1, resp_align);

            Ok(Self {
                shm,
                header,
                client_id: ClientId(client_idx),
                ring_depth,
                ring_mask: (ring_depth - 1) as u64,
                req_slot_size,
                resp_slot_size,
                req_ring_base,
                resp_ring_base,
                control_ptr,
                req_data_offset,
                resp_data_offset,
                req_head: 0,
                resp_tail: 0,
                in_flight: 0,
                on_response,
                user_data_queue: VecDeque::with_capacity(ring_depth as usize),
                _phantom: PhantomData,
            })
        }
    }

    /// Send a request with associated user data.
    pub fn call(&mut self, req: Req, user_data: U) -> Result<(), CallError> {
        if !self.is_server_alive() {
            return Err(CallError::ServerDisconnected);
        }

        // If ring is full, poll responses first
        while self.in_flight >= self.ring_depth {
            if self.poll()? == 0 {
                std::hint::spin_loop();
            }
        }

        let slot_idx = (self.req_head & self.ring_mask) as usize;
        let slot_ptr = unsafe { self.req_ring_base.add(slot_idx * self.req_slot_size) };

        // Spin until slot is free
        loop {
            let valid = unsafe { read_volatile(slot_ptr as *const u8) };
            if valid == 0 {
                break;
            }
            std::hint::spin_loop();
        }

        let req_ptr = unsafe { slot_ptr.add(self.req_data_offset) as *mut Req };
        unsafe { write_volatile(req_ptr, req) };

        compiler_fence(Ordering::Release);
        unsafe { write_volatile(slot_ptr, 1) };

        self.req_head += 1;
        self.in_flight += 1;
        self.user_data_queue.push_back(user_data);

        Ok(())
    }

    /// Poll for responses and invoke callbacks.
    /// Returns the number of responses processed.
    pub fn poll(&mut self) -> Result<u32, CallError> {
        if !self.is_server_alive() {
            return Err(CallError::ServerDisconnected);
        }

        let mut count = 0;

        while self.in_flight > 0 {
            let slot_idx = (self.resp_tail & self.ring_mask) as usize;
            let slot_ptr = unsafe { self.resp_ring_base.add(slot_idx * self.resp_slot_size) };

            let valid = unsafe { read_volatile(slot_ptr as *const u8) };
            if valid == 0 {
                break;
            }

            compiler_fence(Ordering::Acquire);

            let resp_ptr = unsafe { slot_ptr.add(self.resp_data_offset) as *const Resp };
            let resp = unsafe { read_volatile(resp_ptr) };

            compiler_fence(Ordering::Release);
            unsafe { write_volatile(slot_ptr, 0) };

            self.resp_tail += 1;
            self.in_flight -= 1;

            let user_data = self.user_data_queue.pop_front().unwrap();
            (self.on_response)(user_data, resp);

            count += 1;
        }

        Ok(count)
    }

    /// Get the number of in-flight requests.
    pub fn pending_count(&self) -> u32 {
        self.in_flight
    }

    /// Get this client's ID.
    pub fn id(&self) -> ClientId {
        self.client_id
    }

    /// Check if the server is still alive.
    pub fn is_server_alive(&self) -> bool {
        unsafe { (*self.header).server_alive.load(Ordering::Acquire) }
    }
}

impl<Req: Serial, Resp: Serial, U, F: FnMut(U, Resp)> Drop for Client<Req, Resp, U, F> {
    fn drop(&mut self) {
        let alive = unsafe { &*(self.control_ptr) };
        alive.store(false, Ordering::Release);
    }
}

unsafe impl<Req: Serial, Resp: Serial, U, F: FnMut(U, Resp)> Send for Client<Req, Resp, U, F> {}

/// Synchronous client (no user data, blocking API).
pub type SyncClient<Req, Resp> = Client<Req, Resp, (), fn((), Resp)>;

impl<Req: Serial, Resp: Serial> SyncClient<Req, Resp> {
    /// Connect to server with synchronous API.
    ///
    /// # Safety
    /// - Server must have been created with matching types
    pub unsafe fn connect_sync<P: AsRef<Path>>(path: P) -> Result<Self, ConnectError> {
        unsafe { Self::connect(path, |_, _| {}) }
    }

    /// Blocking RPC call that waits for the response.
    pub fn call_blocking(&mut self, req: Req) -> Result<Resp, CallError> {
        if !self.is_server_alive() {
            return Err(CallError::ServerDisconnected);
        }

        // If ring is full, poll responses first
        while self.in_flight >= self.ring_depth {
            if self.poll()? == 0 {
                std::hint::spin_loop();
            }
        }

        let slot_idx = (self.req_head & self.ring_mask) as usize;
        let slot_ptr = unsafe { self.req_ring_base.add(slot_idx * self.req_slot_size) };

        // Spin until slot is free
        loop {
            let valid = unsafe { read_volatile(slot_ptr as *const u8) };
            if valid == 0 {
                break;
            }
            std::hint::spin_loop();
        }

        let req_ptr = unsafe { slot_ptr.add(self.req_data_offset) as *mut Req };
        unsafe { write_volatile(req_ptr, req) };

        compiler_fence(Ordering::Release);
        unsafe { write_volatile(slot_ptr, 1) };

        self.req_head += 1;
        self.in_flight += 1;

        // Wait for response
        let resp = loop {
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

                break resp;
            }

            std::hint::spin_loop();
        };

        Ok(resp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU64;

    static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn test_basic_rpc() {
        let path = format!(
            "/shm_ffwd_test_{}_{}",
            std::process::id(),
            TEST_COUNTER.fetch_add(1, Ordering::Relaxed)
        );

        unsafe {
            let server = Server::<u64, u64>::create(&path, 4, 8).unwrap();
            let mut client: SyncClient<u64, u64> = SyncClient::connect_sync(&path).unwrap();

            // Send request
            client.call(42, ()).unwrap();

            // Server processes
            assert_eq!(server.poll(), 1);
            let handle = server.recv().unwrap();
            let req = *handle.data();
            assert_eq!(req, 42);
            handle.reply(req * 2);

            // Client receives response
            while client.pending_count() > 0 {
                client.poll().unwrap();
            }
        }
    }

    #[test]
    fn test_disconnect_detection() {
        let path = format!(
            "/shm_ffwd_test_{}_{}",
            std::process::id(),
            TEST_COUNTER.fetch_add(1, Ordering::Relaxed)
        );

        unsafe {
            let server = Server::<u64, u64>::create(&path, 4, 8).unwrap();
            let client: SyncClient<u64, u64> = SyncClient::connect_sync(&path).unwrap();

            assert!(client.is_server_alive());
            assert_eq!(server.connected_clients(), 1);

            drop(server);
            assert!(!client.is_server_alive());
        }
    }

    #[test]
    fn test_multiple_calls() {
        let path = format!(
            "/shm_ffwd_test_{}_{}",
            std::process::id(),
            TEST_COUNTER.fetch_add(1, Ordering::Relaxed)
        );

        unsafe {
            let server = Server::<u64, u64>::create(&path, 4, 16).unwrap();
            let mut client: SyncClient<u64, u64> = SyncClient::connect_sync(&path).unwrap();

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
    fn test_async_pipeline() {
        use std::cell::RefCell;
        use std::rc::Rc;

        let path = format!(
            "/shm_ffwd_test_{}_{}",
            std::process::id(),
            TEST_COUNTER.fetch_add(1, Ordering::Relaxed)
        );

        unsafe {
            let server = Server::<u64, u64>::create(&path, 4, 16).unwrap();

            let responses = Rc::new(RefCell::new(Vec::new()));
            let responses_clone = responses.clone();

            let mut client: Client<u64, u64, u64, _> =
                Client::connect(&path, move |id: u64, resp| {
                    responses_clone.borrow_mut().push((id, resp));
                })
                .unwrap();

            // Send 10 requests
            for i in 0..10 {
                client.call(i, i).unwrap();
            }

            // Process them
            while responses.borrow().len() < 10 {
                server.poll();
                while let Some(handle) = server.recv() {
                    let req = *handle.data();
                    handle.reply(req * 2);
                }
                client.poll().unwrap();
            }

            let mut responses = responses.borrow_mut();
            responses.sort_by_key(|(id, _)| *id);
            for (i, (id, resp)) in responses.iter().enumerate() {
                assert_eq!(*id, i as u64);
                assert_eq!(*resp, (i as u64) * 2);
            }
        }
    }

    #[test]
    fn test_multi_client() {
        let path = format!(
            "/shm_ffwd_test_{}_{}",
            std::process::id(),
            TEST_COUNTER.fetch_add(1, Ordering::Relaxed)
        );

        unsafe {
            let server = Server::<u64, u64>::create(&path, 4, 16).unwrap();

            let mut clients: Vec<SyncClient<u64, u64>> = vec![
                SyncClient::connect_sync(&path).unwrap(),
                SyncClient::connect_sync(&path).unwrap(),
                SyncClient::connect_sync(&path).unwrap(),
            ];

            assert_eq!(server.connected_clients(), 3);

            // Each client sends one request
            for (i, client) in clients.iter_mut().enumerate() {
                client.call(i as u64 * 100, ()).unwrap();
            }

            // Server processes all
            let mut count = 0;
            while count < 3 {
                count += server.poll();
                while let Some(handle) = server.recv() {
                    let req = *handle.data();
                    handle.reply(req + 1);
                }
            }

            // Clients receive responses
            for client in clients.iter_mut() {
                while client.pending_count() > 0 {
                    client.poll().unwrap();
                }
            }
        }
    }
}
