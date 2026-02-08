//! Shared-memory RPC using a shared MPSC ring buffer.
//!
//! Architecture:
//! - All clients write to ONE shared MPSC request ring using atomic fetch_add
//! - Server reads from the shared ring sequentially (single tail)
//! - Responses use per-client SPSC rings
//!
//! This differs from `mpsc_ffwd` where each client has its own request ring.
//! The shared MPSC ring reduces memory usage and simplifies server polling.

use crate::shm::SharedMemory;
use crate::{align_up, CallError, ClientId, ConnectError, Serial, CACHE_LINE_SIZE};
use std::collections::VecDeque;
use std::io;
use std::marker::PhantomData;
use std::path::Path;
use std::ptr::{read_volatile, write_volatile};
use std::sync::atomic::{compiler_fence, AtomicBool, AtomicU32, AtomicU64, Ordering};

const HEADER_SIZE: usize = 64;
const MPSC_RING_HEADER_SIZE: usize = 64;
const MAGIC: u64 = 0x5250_4353_4D50_5321; // "RPCSMPSC"
const VERSION: u32 = 2;

/// Shared memory header structure
#[repr(C)]
struct Header {
    magic: u64,
    version: u32,
    max_clients: u32,
    ring_depth: u32,
    mpsc_ring_depth: u32, // MPSC ring size (>= ring_depth * max_clients)
    req_size: u32,
    resp_size: u32,
    req_align: u32,
    resp_align: u32,
    server_alive: AtomicBool,
    next_client_id: AtomicU32,
}

/// MPSC ring header (contains shared producer head)
#[repr(C)]
struct MpscRingHeader {
    head: AtomicU64, // Shared producer head, incremented with fetch_add
}

/// Calculate size of a tagged request slot (valid + client_id + req)
fn calc_tagged_req_slot_size<Req: Serial>() -> usize {
    let data_align = std::mem::align_of::<Req>().max(8);
    let client_id_offset = 4; // align_up(1, 4)
    let req_offset = align_up(client_id_offset + 4, data_align); // after client_id
    align_up(req_offset + std::mem::size_of::<Req>(), CACHE_LINE_SIZE)
}

/// Calculate size of a response slot (valid + resp)
fn calc_resp_slot_size<Resp: Serial>() -> usize {
    let align = std::mem::align_of::<Resp>().max(8);
    let resp_offset = align_up(1, align);
    align_up(resp_offset + std::mem::size_of::<Resp>(), CACHE_LINE_SIZE)
}

/// Calculate per-client block size (control + response ring)
fn per_client_block_size<Resp: Serial>(ring_depth: u32) -> usize {
    let control_size = CACHE_LINE_SIZE; // 64B: alive flag + padding
    let resp_ring_size = calc_resp_slot_size::<Resp>() * ring_depth as usize;
    control_size + resp_ring_size
}

/// Calculate total shared memory size
fn total_shm_size<Req: Serial, Resp: Serial>(
    max_clients: u32,
    ring_depth: u32,
    mpsc_ring_depth: u32,
) -> usize {
    HEADER_SIZE
        + MPSC_RING_HEADER_SIZE
        + calc_tagged_req_slot_size::<Req>() * mpsc_ring_depth as usize
        + per_client_block_size::<Resp>(ring_depth) * max_clients as usize
}

/// Compute MPSC ring depth: at least ring_depth * max_clients, rounded up to power of 2.
fn compute_mpsc_ring_depth(ring_depth: u32, max_clients: u32) -> u32 {
    (ring_depth * max_clients).next_power_of_two()
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
    resp_ring_mask: u64,
    mpsc_ring_depth: u32,
    mpsc_ring_mask: u64,
    tagged_req_slot_size: usize,
    resp_slot_size: usize,
    client_block_size: usize,
    tail: u64, // Server's local tail for the shared MPSC ring
    resp_heads: Vec<u64>,
    request_buffer: VecDeque<(ClientId, Req)>,
    deferred_responses: VecDeque<(ClientId, Resp)>,
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

        let mpsc_ring_depth = compute_mpsc_ring_depth(ring_depth, max_clients);
        let size = total_shm_size::<Req, Resp>(max_clients, ring_depth, mpsc_ring_depth);
        let shm = unsafe { SharedMemory::create(path, size)? };

        let header = shm.as_ptr() as *mut Header;
        unsafe {
            (*header).magic = MAGIC;
            (*header).version = VERSION;
            (*header).max_clients = max_clients;
            (*header).ring_depth = ring_depth;
            (*header).mpsc_ring_depth = mpsc_ring_depth;
            (*header).req_size = std::mem::size_of::<Req>() as u32;
            (*header).resp_size = std::mem::size_of::<Resp>() as u32;
            (*header).req_align = std::mem::align_of::<Req>() as u32;
            (*header).resp_align = std::mem::align_of::<Resp>() as u32;
            (*header).server_alive = AtomicBool::new(true);
            (*header).next_client_id = AtomicU32::new(0);
        }

        // Initialize MPSC ring header
        let mpsc_ring_header = unsafe { shm.as_ptr().add(HEADER_SIZE) as *mut MpscRingHeader };
        unsafe {
            (*mpsc_ring_header).head = AtomicU64::new(0);
        }

        let tagged_req_slot_size = calc_tagged_req_slot_size::<Req>();
        let resp_slot_size = calc_resp_slot_size::<Resp>();
        let client_block_size = per_client_block_size::<Resp>(ring_depth);

        // Initialize all client control blocks
        unsafe {
            for i in 0..max_clients {
                let control_ptr = shm.as_ptr().add(
                    HEADER_SIZE
                        + MPSC_RING_HEADER_SIZE
                        + tagged_req_slot_size * mpsc_ring_depth as usize
                        + i as usize * client_block_size,
                );
                let alive = &mut *(control_ptr as *mut AtomicBool);
                *alive = AtomicBool::new(false);
            }
        }

        // Initialize all MPSC request ring slots (set valid = 0)
        unsafe {
            let req_ring_base = shm.as_ptr().add(HEADER_SIZE + MPSC_RING_HEADER_SIZE);
            for i in 0..mpsc_ring_depth {
                let slot_ptr = req_ring_base.add(i as usize * tagged_req_slot_size);
                write_volatile(slot_ptr, 0u8); // valid = 0
            }
        }

        Ok(Self {
            shm,
            header,
            max_clients,
            resp_ring_mask: (ring_depth - 1) as u64,
            mpsc_ring_depth,
            mpsc_ring_mask: (mpsc_ring_depth - 1) as u64,
            tagged_req_slot_size,
            resp_slot_size,
            client_block_size,
            tail: 0,
            resp_heads: vec![0; max_clients as usize],
            request_buffer: VecDeque::new(),
            deferred_responses: VecDeque::new(),
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
            let slot_idx = (head & self.resp_ring_mask) as usize;

            let resp_ring_base = unsafe {
                self.shm.as_ptr().add(
                    HEADER_SIZE
                        + MPSC_RING_HEADER_SIZE
                        + self.tagged_req_slot_size * self.mpsc_ring_depth as usize
                        + client_idx * self.client_block_size
                        + CACHE_LINE_SIZE,
                )
            };

            let slot_ptr = unsafe { resp_ring_base.add(slot_idx * self.resp_slot_size) };
            let valid = unsafe { read_volatile(slot_ptr as *const u8) };

            if valid != 0 {
                // Still full, skip this one
                i += 1;
                continue;
            }

            // Slot is free, write the deferred response
            let (_, resp) = self.deferred_responses.remove(i).unwrap();

            let resp_align = std::mem::align_of::<Resp>().max(8);
            let resp_offset = align_up(1, resp_align);
            let resp_ptr = unsafe { slot_ptr.add(resp_offset) as *mut Resp };
            unsafe { write_volatile(resp_ptr, resp) };

            compiler_fence(Ordering::Release);
            unsafe { write_volatile(slot_ptr, 1) };

            self.resp_heads[client_idx] = head + 1;
        }
    }

    /// Scan the shared MPSC request ring and buffer any pending requests.
    /// Returns the number of requests buffered.
    pub fn poll(&mut self) -> u32 {
        // Flush any deferred responses first
        self.flush_deferred();

        let mut count = 0;

        // Read from shared MPSC ring sequentially from tail
        loop {
            let slot_idx = (self.tail & self.mpsc_ring_mask) as usize;

            let req_ring_base =
                unsafe { self.shm.as_ptr().add(HEADER_SIZE + MPSC_RING_HEADER_SIZE) };
            let slot_ptr = unsafe { req_ring_base.add(slot_idx * self.tagged_req_slot_size) };

            let valid_ptr = slot_ptr as *const u8;
            let valid = unsafe { read_volatile(valid_ptr) };

            if valid == 0 {
                // No more entries available
                break;
            }

            // Acquire fence to ensure we see the data after seeing valid=1
            compiler_fence(Ordering::Acquire);

            // Read client_id (at offset 4)
            let client_id_offset = 4;
            let client_id_ptr = unsafe { slot_ptr.add(client_id_offset) as *const u32 };
            let client_id = unsafe { read_volatile(client_id_ptr) };

            // Read the request
            let data_align = std::mem::align_of::<Req>().max(8);
            let req_offset = align_up(client_id_offset + 4, data_align);
            let req_ptr = unsafe { slot_ptr.add(req_offset) as *const Req };
            let req = unsafe { read_volatile(req_ptr) };

            // Release fence to ensure read completes before clearing valid
            compiler_fence(Ordering::Release);

            // Clear the valid flag
            unsafe { write_volatile(valid_ptr as *mut u8, 0) };

            // Advance tail
            self.tail += 1;

            // Buffer the request
            self.request_buffer.push_back((ClientId(client_id), req));
            count += 1;
        }

        count
    }

    /// Receive a buffered request.
    pub fn recv(&mut self) -> Option<(RequestToken, Req)> {
        self.request_buffer
            .pop_front()
            .map(|(client_id, req)| (RequestToken { client_id }, req))
    }

    /// Send a response to a client.
    /// If the response ring is full, the response is buffered internally
    /// and will be flushed on the next poll() call.
    pub fn reply(&mut self, token: RequestToken, resp: Resp) {
        let client_idx = token.client_id.0 as usize;
        let head = self.resp_heads[client_idx];
        let slot_idx = (head & self.resp_ring_mask) as usize;

        let resp_ring_base = unsafe {
            self.shm.as_ptr().add(
                HEADER_SIZE
                    + MPSC_RING_HEADER_SIZE
                    + self.tagged_req_slot_size * self.mpsc_ring_depth as usize
                    + client_idx * self.client_block_size
                    + CACHE_LINE_SIZE, // skip control block
            )
        };

        let slot_ptr = unsafe { resp_ring_base.add(slot_idx * self.resp_slot_size) };

        let valid = unsafe { read_volatile(slot_ptr as *const u8) };
        if valid != 0 {
            // Response ring full — defer this response
            self.deferred_responses
                .push_back((token.client_id, resp));
            return;
        }

        // Write the response
        let resp_align = std::mem::align_of::<Resp>().max(8);
        let resp_offset = align_up(1, resp_align);
        let resp_ptr = unsafe { slot_ptr.add(resp_offset) as *mut Resp };
        unsafe { write_volatile(resp_ptr, resp) };

        // Release fence to ensure data is written before setting valid
        compiler_fence(Ordering::Release);

        // Mark slot as valid
        unsafe { write_volatile(slot_ptr, 1) };

        self.resp_heads[client_idx] = head + 1;
    }

    /// Check if a client is connected.
    pub fn is_client_connected(&self, client_id: ClientId) -> bool {
        let client_idx = client_id.0 as usize;
        if client_idx >= self.max_clients as usize {
            return false;
        }

        let control_ptr = unsafe {
            self.shm.as_ptr().add(
                HEADER_SIZE
                    + MPSC_RING_HEADER_SIZE
                    + self.tagged_req_slot_size * self.mpsc_ring_depth as usize
                    + client_idx * self.client_block_size,
            )
        };
        let alive = unsafe { &*(control_ptr as *const AtomicBool) };
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
    mpsc_ring_header: *const MpscRingHeader,
    client_id: ClientId,
    ring_depth: u32,
    resp_ring_mask: u64,
    mpsc_ring_mask: u64,
    mpsc_ring_depth: u32,
    tagged_req_slot_size: usize,
    resp_slot_size: usize,
    client_block_size: usize,
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

        let (max_clients, ring_depth, mpsc_ring_depth) = unsafe {
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

            (
                (*header_ptr).max_clients,
                (*header_ptr).ring_depth,
                (*header_ptr).mpsc_ring_depth,
            )
        };

        // Drop the header-only mapping
        drop(header_shm);

        // Now open with full size
        let size = total_shm_size::<Req, Resp>(max_clients, ring_depth, mpsc_ring_depth);
        let shm = unsafe { SharedMemory::open(path, size).map_err(ConnectError::Io)? };

        let header = shm.as_ptr() as *const Header;
        let mpsc_ring_header =
            unsafe { shm.as_ptr().add(HEADER_SIZE) as *const MpscRingHeader };

        // Allocate a client ID and mark alive
        unsafe {
            let client_idx = (*header).next_client_id.fetch_add(1, Ordering::Relaxed);
            if client_idx >= max_clients {
                return Err(ConnectError::ServerFull);
            }

            let tagged_req_slot_size = calc_tagged_req_slot_size::<Req>();
            let resp_slot_size = calc_resp_slot_size::<Resp>();
            let client_block_size = per_client_block_size::<Resp>(ring_depth);

            // Mark this client as alive
            let control_ptr = shm.as_ptr().add(
                HEADER_SIZE
                    + MPSC_RING_HEADER_SIZE
                    + tagged_req_slot_size * mpsc_ring_depth as usize
                    + client_idx as usize * client_block_size,
            );
            let alive = &*(control_ptr as *const AtomicBool);
            alive.store(true, Ordering::Release);

            Ok(Self {
                shm,
                header,
                mpsc_ring_header,
                client_id: ClientId(client_idx),
                ring_depth,
                resp_ring_mask: (ring_depth - 1) as u64,
                mpsc_ring_mask: (mpsc_ring_depth - 1) as u64,
                mpsc_ring_depth,
                tagged_req_slot_size,
                resp_slot_size,
                client_block_size,
                resp_tail: 0,
                in_flight: 0,
                on_response,
                user_data_queue: VecDeque::new(),
                _phantom: PhantomData,
            })
        }
    }

    /// Send a request with associated user data.
    pub fn call(&mut self, req: Req, user_data: U) -> Result<(), CallError> {
        // Check server is still alive
        if !self.is_server_alive() {
            return Err(CallError::ServerDisconnected);
        }

        // If ring is full, poll responses first
        while self.in_flight >= self.ring_depth {
            if self.poll()? == 0 {
                std::hint::spin_loop();
            }
        }

        // Atomically claim a slot from the shared MPSC ring
        let slot_idx = unsafe {
            (*self.mpsc_ring_header)
                .head
                .fetch_add(1, Ordering::Relaxed)
                & self.mpsc_ring_mask
        };

        let req_ring_base =
            unsafe { self.shm.as_ptr().add(HEADER_SIZE + MPSC_RING_HEADER_SIZE) };
        let slot_ptr = unsafe { req_ring_base.add(slot_idx as usize * self.tagged_req_slot_size) };
        let valid_ptr = slot_ptr;

        // Spin until slot is free (previous entry consumed by server)
        loop {
            let valid = unsafe { read_volatile(valid_ptr as *const u8) };
            if valid == 0 {
                break;
            }
            std::hint::spin_loop();
        }

        // Write client_id (at offset 4)
        let client_id_offset = 4;
        let client_id_ptr = unsafe { slot_ptr.add(client_id_offset) as *mut u32 };
        unsafe { write_volatile(client_id_ptr, self.client_id.0) };

        // Write the request
        let data_align = std::mem::align_of::<Req>().max(8);
        let req_offset = align_up(client_id_offset + 4, data_align);
        let req_ptr = unsafe { slot_ptr.add(req_offset) as *mut Req };
        unsafe { write_volatile(req_ptr, req) };

        // Release fence to ensure data is written before setting valid
        compiler_fence(Ordering::Release);

        // Mark slot as valid
        unsafe { write_volatile(valid_ptr, 1) };

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
            let slot_idx = (self.resp_tail & self.resp_ring_mask) as usize;

            let resp_ring_base = unsafe {
                self.shm.as_ptr().add(
                    HEADER_SIZE
                        + MPSC_RING_HEADER_SIZE
                        + self.tagged_req_slot_size * self.mpsc_ring_depth as usize
                        + self.client_id.0 as usize * self.client_block_size
                        + CACHE_LINE_SIZE, // skip control block
                )
            };

            let slot_ptr = unsafe { resp_ring_base.add(slot_idx * self.resp_slot_size) };
            let valid_ptr = slot_ptr as *const u8;

            let valid = unsafe { read_volatile(valid_ptr) };
            if valid == 0 {
                break;
            }

            // Acquire fence to ensure we see data after seeing valid=1
            compiler_fence(Ordering::Acquire);

            // Read the response
            let resp_align = std::mem::align_of::<Resp>().max(8);
            let resp_offset = align_up(1, resp_align);
            let resp_ptr = unsafe { slot_ptr.add(resp_offset) as *const Resp };
            let resp = unsafe { read_volatile(resp_ptr) };

            // Release fence to ensure read completes before clearing valid
            compiler_fence(Ordering::Release);

            // Clear the valid flag
            unsafe { write_volatile(valid_ptr as *mut u8, 0) };

            self.resp_tail += 1;
            self.in_flight -= 1;

            // Invoke callback
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
        let control_ptr = unsafe {
            self.shm.as_ptr().add(
                HEADER_SIZE
                    + MPSC_RING_HEADER_SIZE
                    + self.tagged_req_slot_size * self.mpsc_ring_depth as usize
                    + self.client_id.0 as usize * self.client_block_size,
            )
        };
        let alive = unsafe { &*(control_ptr as *const AtomicBool) };
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
        // Check server is still alive
        if !self.is_server_alive() {
            return Err(CallError::ServerDisconnected);
        }

        // If ring is full, poll responses first
        while self.in_flight >= self.ring_depth {
            if self.poll()? == 0 {
                std::hint::spin_loop();
            }
        }

        // Atomically claim a slot from the shared MPSC ring
        let slot_idx = unsafe {
            (*self.mpsc_ring_header)
                .head
                .fetch_add(1, Ordering::Relaxed)
                & self.mpsc_ring_mask
        };

        let req_ring_base =
            unsafe { self.shm.as_ptr().add(HEADER_SIZE + MPSC_RING_HEADER_SIZE) };
        let slot_ptr =
            unsafe { req_ring_base.add(slot_idx as usize * self.tagged_req_slot_size) };
        let valid_ptr = slot_ptr;

        // Spin until slot is free (previous entry consumed by server)
        loop {
            let valid = unsafe { read_volatile(valid_ptr as *const u8) };
            if valid == 0 {
                break;
            }
            std::hint::spin_loop();
        }

        // Write client_id (at offset 4)
        let client_id_offset = 4;
        let client_id_ptr = unsafe { slot_ptr.add(client_id_offset) as *mut u32 };
        unsafe { write_volatile(client_id_ptr, self.client_id.0) };

        // Write the request
        let data_align = std::mem::align_of::<Req>().max(8);
        let req_offset = align_up(client_id_offset + 4, data_align);
        let req_ptr = unsafe { slot_ptr.add(req_offset) as *mut Req };
        unsafe { write_volatile(req_ptr, req) };

        // Release fence to ensure data is written before setting valid
        compiler_fence(Ordering::Release);

        // Mark slot as valid
        unsafe { write_volatile(valid_ptr, 1) };

        self.in_flight += 1;

        // Wait for response
        let resp = loop {
            let resp_slot_idx = (self.resp_tail & self.resp_ring_mask) as usize;

            let resp_ring_base = unsafe {
                self.shm.as_ptr().add(
                    HEADER_SIZE
                        + MPSC_RING_HEADER_SIZE
                        + self.tagged_req_slot_size * self.mpsc_ring_depth as usize
                        + self.client_id.0 as usize * self.client_block_size
                        + CACHE_LINE_SIZE, // skip control block
                )
            };

            let resp_slot_ptr = unsafe { resp_ring_base.add(resp_slot_idx * self.resp_slot_size) };
            let resp_valid_ptr = resp_slot_ptr as *const u8;

            let valid = unsafe { read_volatile(resp_valid_ptr) };
            if valid != 0 {
                // Acquire fence to ensure we see data after seeing valid=1
                compiler_fence(Ordering::Acquire);

                // Read the response
                let resp_align = std::mem::align_of::<Resp>().max(8);
                let resp_offset = align_up(1, resp_align);
                let resp_ptr = unsafe { resp_slot_ptr.add(resp_offset) as *const Resp };
                let resp = unsafe { read_volatile(resp_ptr) };

                // Release fence to ensure read completes before clearing valid
                compiler_fence(Ordering::Release);

                // Clear the valid flag
                unsafe { write_volatile(resp_valid_ptr as *mut u8, 0) };

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
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn test_basic_rpc() {
        let path = format!(
            "/shm_shared_test_{}_{}",
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
            "/shm_shared_test_{}_{}",
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
            "/shm_shared_test_{}_{}",
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
            "/shm_shared_test_{}_{}",
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
            "/shm_shared_test_{}_{}",
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
