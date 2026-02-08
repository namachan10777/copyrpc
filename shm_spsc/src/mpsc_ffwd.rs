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
    // Per-client ring state
    req_tails: Vec<u64>,
    resp_heads: Vec<u64>,
    scan_pos: u32,
    // Zero-copy recv: per-client available count + lane-drain state
    req_avail: Vec<u32>,
    recv_start: u32,
    recv_scan_pos: u32,
    // Deferred responses (Vec + swap_remove)
    deferred_responses: Vec<(ClientId, Resp)>,
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
            req_tails: vec![0; max_clients as usize],
            resp_heads: vec![0; max_clients as usize],
            scan_pos: 0,
            req_avail: vec![0; max_clients as usize],
            recv_start: 0,
            recv_scan_pos: 0,
            deferred_responses: Vec::new(),
            _phantom: PhantomData,
        })
    }

    /// Try to flush deferred responses to their respective response rings.
    fn flush_deferred(&mut self) {
        let mut i = 0;
        while i < self.deferred_responses.len() {
            let (client_id, _) = &self.deferred_responses[i];
            let client_idx = client_id.0 as usize;
            let head = self.resp_heads[client_idx];
            let slot_idx = (head & self.ring_mask) as usize;

            let slot_ptr =
                unsafe { self.resp_ring_bases[client_idx].add(slot_idx * self.resp_slot_size) };
            let valid = unsafe { read_volatile(slot_ptr as *const u8) };

            if valid != 0 {
                i += 1;
                continue;
            }

            // O(1) swap_remove instead of O(n) VecDeque::remove
            let (_, resp) = self.deferred_responses.swap_remove(i);

            let resp_ptr = unsafe { slot_ptr.add(self.resp_data_offset) as *mut Resp };
            unsafe { write_volatile(resp_ptr, resp) };

            compiler_fence(Ordering::Release);
            unsafe { write_volatile(slot_ptr, 1) };

            self.resp_heads[client_idx] = head + 1;
            // Don't increment i — swap_remove moved last element here
        }
    }

    /// Scan all client request rings and count available requests.
    /// Returns the total number of requests available for recv().
    pub fn poll(&mut self) -> u32 {
        // Flush deferred responses only if needed
        if !self.deferred_responses.is_empty() {
            self.flush_deferred();
        }

        let recv_start = self.scan_pos;
        let mut count = 0u32;

        for _ in 0..self.max_clients {
            let client_idx = self.scan_pos as usize;
            self.scan_pos = (self.scan_pos + 1) % self.max_clients;

            // Check if client is alive (using cached pointer)
            let alive = unsafe { &*self.control_ptrs[client_idx] };
            if !alive.load(Ordering::Relaxed) {
                self.req_avail[client_idx] = 0;
                continue;
            }

            // Count available requests (valid flags only, no data read)
            // Limit to ring_depth to avoid infinite wraparound (valid isn't cleared until recv)
            let req_ring_base = self.req_ring_bases[client_idx];
            let ring_depth = (self.ring_mask + 1) as u32;
            let mut tail = self.req_tails[client_idx];
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
            self.req_avail[client_idx] = avail;
            count += avail;
        }

        self.recv_start = recv_start;
        self.recv_scan_pos = 0;
        count
    }

    /// Receive a request directly from SHM.
    /// Drains all requests from the same client before moving to the next.
    pub fn recv(&mut self) -> Option<(RequestToken, Req)> {
        while (self.recv_scan_pos as usize) < self.max_clients as usize {
            let client_idx =
                ((self.recv_start + self.recv_scan_pos) % self.max_clients) as usize;

            if self.req_avail[client_idx] > 0 {
                let tail = self.req_tails[client_idx];
                let slot_idx = (tail & self.ring_mask) as usize;
                let slot_ptr = unsafe {
                    self.req_ring_bases[client_idx].add(slot_idx * self.req_slot_size)
                };

                compiler_fence(Ordering::Acquire);
                let req = unsafe {
                    read_volatile(slot_ptr.add(self.req_data_offset) as *const Req)
                };
                compiler_fence(Ordering::Release);
                unsafe { write_volatile(slot_ptr, 0) };

                self.req_tails[client_idx] = tail + 1;
                self.req_avail[client_idx] -= 1;

                return Some((
                    RequestToken {
                        client_id: ClientId(client_idx as u32),
                    },
                    req,
                ));
            }

            self.recv_scan_pos += 1;
        }
        None
    }

    /// Send a response to a client.
    /// If the response ring is full, the response is buffered internally
    /// and will be flushed on the next poll() call.
    pub fn reply(&mut self, token: RequestToken, resp: Resp) {
        let client_idx = token.client_id.0 as usize;
        let head = self.resp_heads[client_idx];
        let slot_idx = (head & self.ring_mask) as usize;

        let slot_ptr =
            unsafe { self.resp_ring_bases[client_idx].add(slot_idx * self.resp_slot_size) };

        let valid = unsafe { read_volatile(slot_ptr as *const u8) };
        if valid != 0 {
            // Response ring full — defer this response
            self.deferred_responses.push((token.client_id, resp));
            return;
        }

        let resp_ptr = unsafe { slot_ptr.add(self.resp_data_offset) as *mut Resp };
        unsafe { write_volatile(resp_ptr, resp) };

        compiler_fence(Ordering::Release);
        unsafe { write_volatile(slot_ptr, 1) };

        self.resp_heads[client_idx] = head + 1;
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
            let mut server = Server::<u64, u64>::create(&path, 4, 8).unwrap();
            let mut client: SyncClient<u64, u64> = SyncClient::connect_sync(&path).unwrap();

            // Send request
            client.call(42, ()).unwrap();

            // Server processes
            assert_eq!(server.poll(), 1);
            let (token, req) = server.recv().unwrap();
            assert_eq!(req, 42);
            server.reply(token, req * 2);

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
            let mut server = Server::<u64, u64>::create(&path, 4, 16).unwrap();
            let mut client: SyncClient<u64, u64> = SyncClient::connect_sync(&path).unwrap();

            for i in 0..100 {
                client.call(i, ()).unwrap();
                server.poll();
                if let Some((token, req)) = server.recv() {
                    server.reply(token, req * 2);
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
            let mut server = Server::<u64, u64>::create(&path, 4, 16).unwrap();

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
                while let Some((token, req)) = server.recv() {
                    server.reply(token, req * 2);
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
            let mut server = Server::<u64, u64>::create(&path, 4, 16).unwrap();

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
                while let Some((token, req)) = server.recv() {
                    server.reply(token, req + 1);
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
