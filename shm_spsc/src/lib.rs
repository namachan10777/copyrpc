//! RPC slot-based communication over shared memory.
//!
//! Each client gets a group of 2^n dedicated request/response slots.
//! The server polls all slots and responds to each client via `process_batch()`.
//!
//! Uses a toggle-bit protocol (inspired by ffwd, SOSP 2017):
//! - `client_seq` incremented by client when posting a request
//! - `server_seq` incremented by server when posting a response
//! - `client_seq != server_seq` → request pending
//! - `client_seq == server_seq` → idle / response ready
//!
//! Clients can pipeline up to `slots_per_client` requests using `send()`/`recv()`.
//! Free slots are tracked with a bitmap; `trailing_zeros()` finds the next free slot.

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

/// Error returned when `Client::call()` or `Client::send()`/`Client::recv()` fails.
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
const VERSION: u32 = 5;
const HEADER_SIZE: usize = 64;
const CACHE_LINE_SIZE: usize = 64;
const MAX_SLOTS_PER_CLIENT: usize = 32;

/// Header stored at the beginning of shared memory.
/// Fixed 64 bytes.
#[repr(C)]
struct Header {
    magic: u64,                    // 8
    version: u32,                  // 4
    max_clients: u32,              // 4
    req_size: u32,                 // 4
    resp_size: u32,                // 4
    req_align: u32,                // 4
    resp_align: u32,               // 4
    server_alive: AtomicBool,      // 1
    _pad: [u8; 3],                 // 3
    next_client_id: AtomicU32,     // 4
    slots_per_client: u32,         // 4
    _reserved: [u8; 20],           // 20
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

fn calc_shm_size<Req, Resp>(max_clients: u32, slots_per_client: u32) -> usize {
    let layout = calc_slot_layout::<Req, Resp>();
    HEADER_SIZE + layout.slot_size * (max_clients as usize * slots_per_client as usize)
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
    slots_per_client: u32,
    server_seqs: Vec<u8>,
    deferred_bitmap: Vec<u32>,
    _marker: PhantomData<(Req, Resp)>,
}

unsafe impl<Req: Serial + Send, Resp: Serial + Send> Send for Server<Req, Resp> {}

impl<Req: Serial, Resp: Serial> Server<Req, Resp> {
    /// Creates a new server and shared memory region.
    ///
    /// `slots_per_client` must be a power of 2 (1, 2, 4, 8, ...).
    ///
    /// # Safety
    /// The caller must ensure that `path` is a valid shared memory path
    /// and that no other process is using the same path.
    pub unsafe fn create<P: AsRef<Path>>(
        path: P,
        max_clients: u32,
        slots_per_client: u32,
    ) -> io::Result<Self> {
        assert!(max_clients > 0, "max_clients must be > 0");
        assert!(max_clients <= 64, "max_clients must be <= 64");
        assert!(
            slots_per_client > 0 && slots_per_client.is_power_of_two(),
            "slots_per_client must be a power of 2"
        );
        assert!(
            (slots_per_client as usize) <= MAX_SLOTS_PER_CLIENT,
            "slots_per_client must be <= {}",
            MAX_SLOTS_PER_CLIENT
        );

        let size = calc_shm_size::<Req, Resp>(max_clients, slots_per_client);
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
                    slots_per_client,
                    _reserved: [0; 20],
                },
            );
        }

        let layout = calc_slot_layout::<Req, Resp>();
        let total_slots = max_clients * slots_per_client;

        // Initialize all slots
        for i in 0..total_slots {
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
            slots_per_client,
            server_seqs: vec![0u8; total_slots as usize],
            deferred_bitmap: vec![0u32; max_clients as usize],
            _marker: PhantomData,
        })
    }

    fn slot_base(&self, global_slot: u32) -> *mut u8 {
        unsafe {
            self.base
                .add(HEADER_SIZE + self.layout.slot_size * global_slot as usize)
        }
    }

    fn slot_client_seq(&self, global_slot: u32) -> &AtomicU8 {
        unsafe {
            &*(self.slot_base(global_slot).add(self.layout.client_seq_offset) as *const AtomicU8)
        }
    }

    fn slot_server_seq(&self, global_slot: u32) -> &AtomicU8 {
        unsafe {
            &*(self.slot_base(global_slot).add(self.layout.server_seq_offset) as *const AtomicU8)
        }
    }

    fn slot_alive(&self, global_slot: u32) -> &AtomicBool {
        unsafe {
            &*(self.slot_base(global_slot).add(self.layout.alive_offset) as *const AtomicBool)
        }
    }

    fn slot_req_ptr(&self, global_slot: u32) -> *const Req {
        unsafe { self.slot_base(global_slot).add(self.layout.req_offset) as *const Req }
    }

    fn slot_resp_ptr(&self, global_slot: u32) -> *mut Resp {
        unsafe { self.slot_base(global_slot).add(self.layout.resp_offset) as *mut Resp }
    }

    /// Processes all pending requests in a single sweep.
    ///
    /// Scans all allocated client slots linearly, calls `f` for each pending request
    /// to compute the response, and writes the response back immediately.
    ///
    /// # Design
    ///
    /// The toggle-bit protocol is inspired by **ffwd delegation** (Roghanchi et al.,
    /// SOSP 2017): dedicated per-client slots with a single server thread scanning
    /// all slots.
    ///
    /// Returns the number of requests processed.
    pub fn process_batch(&mut self, mut f: impl FnMut(ClientId, Req) -> Resp) -> u32 {
        let allocated = unsafe { (*self.header).next_client_id.load(Ordering::Relaxed) };
        if allocated == 0 {
            return 0;
        }

        let spc = self.slots_per_client;
        let mut count = 0u32;

        for client_idx in 0..allocated {
            let base_slot = client_idx * spc;
            // Check alive on first slot only
            if !self.slot_alive(base_slot).load(Ordering::Acquire) {
                continue;
            }
            for s in 0..spc {
                let global = base_slot + s;
                let seq_idx = global as usize;
                let client_seq = self.slot_client_seq(global).load(Ordering::Acquire);
                // SAFETY: seq_idx = client_idx * spc + s where client_idx < allocated <= max_clients
                // and s < spc, so seq_idx < max_clients * spc = server_seqs.len()
                let server_seq = unsafe { *self.server_seqs.get_unchecked(seq_idx) };
                if client_seq != server_seq {
                    let req = unsafe { std::ptr::read_volatile(self.slot_req_ptr(global)) };
                    let resp = f(ClientId(client_idx), req);
                    unsafe { std::ptr::write_volatile(self.slot_resp_ptr(global), resp) };
                    let new_seq = server_seq.wrapping_add(1);
                    unsafe { *self.server_seqs.get_unchecked_mut(seq_idx) = new_seq };
                    self.slot_server_seq(global)
                        .store(new_seq, Ordering::Release);
                    count += 1;
                }
            }
        }

        count
    }

    /// Processes all pending requests, allowing deferred responses.
    ///
    /// Like [`process_batch`], but the callback returns `Option<Resp>`:
    /// - `Some(resp)`: immediate response (same as `process_batch`)
    /// - `None`: deferred — the slot is marked pending and must be completed
    ///   later via [`complete_request`]
    ///
    /// The callback receives `(ClientId, u32, Req)` where the `u32` is the
    /// slot index within the client's slot group (0..slots_per_client).
    ///
    /// Returns the number of requests processed (both immediate and deferred).
    pub fn process_batch_deferred(
        &mut self,
        mut f: impl FnMut(ClientId, u32, Req) -> Option<Resp>,
    ) -> u32 {
        let allocated = unsafe { (*self.header).next_client_id.load(Ordering::Relaxed) };
        if allocated == 0 {
            return 0;
        }

        let spc = self.slots_per_client;
        let mut count = 0u32;

        for client_idx in 0..allocated {
            let base_slot = client_idx * spc;
            if !self.slot_alive(base_slot).load(Ordering::Acquire) {
                continue;
            }
            let bitmap = unsafe { *self.deferred_bitmap.get_unchecked(client_idx as usize) };
            for s in 0..spc {
                // Skip slots that are already deferred
                if bitmap & (1 << s) != 0 {
                    continue;
                }
                let global = base_slot + s;
                let seq_idx = global as usize;
                let client_seq = self.slot_client_seq(global).load(Ordering::Acquire);
                let server_seq = unsafe { *self.server_seqs.get_unchecked(seq_idx) };
                if client_seq != server_seq {
                    let req = unsafe { std::ptr::read_volatile(self.slot_req_ptr(global)) };
                    match f(ClientId(client_idx), s, req) {
                        Some(resp) => {
                            unsafe { std::ptr::write_volatile(self.slot_resp_ptr(global), resp) };
                            let new_seq = server_seq.wrapping_add(1);
                            unsafe { *self.server_seqs.get_unchecked_mut(seq_idx) = new_seq };
                            self.slot_server_seq(global)
                                .store(new_seq, Ordering::Release);
                        }
                        None => {
                            // Mark as deferred
                            unsafe {
                                *self.deferred_bitmap.get_unchecked_mut(client_idx as usize) |=
                                    1 << s;
                            }
                        }
                    }
                    count += 1;
                }
            }
        }

        count
    }

    /// Completes a previously deferred request by writing the response.
    ///
    /// # Panics
    /// Panics if the slot was not deferred.
    pub fn complete_request(&mut self, client_id: ClientId, slot_index: u32, resp: Resp) {
        let cid = client_id.0;
        let global = cid * self.slots_per_client + slot_index;
        let seq_idx = global as usize;

        debug_assert!(
            unsafe { *self.deferred_bitmap.get_unchecked(cid as usize) } & (1 << slot_index) != 0,
            "complete_request called on non-deferred slot"
        );

        unsafe { std::ptr::write_volatile(self.slot_resp_ptr(global), resp) };
        let server_seq = unsafe { *self.server_seqs.get_unchecked(seq_idx) };
        let new_seq = server_seq.wrapping_add(1);
        unsafe { *self.server_seqs.get_unchecked_mut(seq_idx) = new_seq };
        self.slot_server_seq(global)
            .store(new_seq, Ordering::Release);

        // Clear the deferred bit
        unsafe {
            *self.deferred_bitmap.get_unchecked_mut(cid as usize) &= !(1 << slot_index);
        }
    }

    /// Returns true if a specific client is connected.
    pub fn is_client_connected(&self, client_id: ClientId) -> bool {
        let id = client_id.0;
        let allocated = unsafe { (*self.header).next_client_id.load(Ordering::Relaxed) };
        if id >= allocated {
            return false;
        }
        let base_slot = id * self.slots_per_client;
        self.slot_alive(base_slot).load(Ordering::Acquire)
    }

    /// Returns the number of currently connected clients.
    pub fn connected_clients(&self) -> u32 {
        let allocated = unsafe { (*self.header).next_client_id.load(Ordering::Relaxed) };
        let spc = self.slots_per_client;
        let mut count = 0;
        for i in 0..allocated {
            let base_slot = i * spc;
            if self.slot_alive(base_slot).load(Ordering::Acquire) {
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
    base: *mut u8,
    layout: SlotLayout,
    client_id: u32,
    slots_per_client: u32,
    client_seqs: [u8; MAX_SLOTS_PER_CLIENT],
    free_bitmap: u32,
    pending_queue: [u8; MAX_SLOTS_PER_CLIENT],
    pq_head: u32,
    pq_tail: u32,
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

        let (max_clients, slots_per_client, shm_size) = unsafe {
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
            let spc = (*header).slots_per_client;
            (mc, spc, calc_shm_size::<Req, Resp>(mc, spc))
        };

        drop(header_shm);

        // Re-open with full size
        let shm = unsafe { SharedMemory::open(path, shm_size)? };
        let base = shm.as_ptr();
        let header = base as *mut Header;

        // Allocate a client slot group
        let client_id = unsafe { (*header).next_client_id.fetch_add(1, Ordering::Relaxed) };
        if client_id >= max_clients {
            return Err(ConnectError::ServerFull);
        }

        let layout = calc_slot_layout::<Req, Resp>();

        // Mark first slot as alive (used for liveness detection)
        let first_slot = client_id * slots_per_client;
        unsafe {
            let slot_ptr = base.add(HEADER_SIZE + layout.slot_size * first_slot as usize);
            let alive = &*(slot_ptr.add(layout.alive_offset) as *const AtomicBool);
            alive.store(true, Ordering::Release);
        }

        // Initialize free bitmap: all slots free
        let free_bitmap = (1u32 << slots_per_client) - 1;

        Ok(Self {
            _shm: shm,
            header,
            base,
            layout,
            client_id,
            slots_per_client,
            client_seqs: [0u8; MAX_SLOTS_PER_CLIENT],
            free_bitmap,
            pending_queue: [0u8; MAX_SLOTS_PER_CLIENT],
            pq_head: 0,
            pq_tail: 0,
            _marker: PhantomData,
        })
    }

    fn global_slot(&self, local_slot: u32) -> u32 {
        self.client_id * self.slots_per_client + local_slot
    }

    fn slot_base(&self, global_slot: u32) -> *mut u8 {
        unsafe {
            self.base
                .add(HEADER_SIZE + self.layout.slot_size * global_slot as usize)
        }
    }

    fn slot_client_seq(&self, global_slot: u32) -> &AtomicU8 {
        unsafe {
            &*(self.slot_base(global_slot).add(self.layout.client_seq_offset) as *const AtomicU8)
        }
    }

    fn slot_server_seq(&self, global_slot: u32) -> &AtomicU8 {
        unsafe {
            &*(self.slot_base(global_slot).add(self.layout.server_seq_offset) as *const AtomicU8)
        }
    }

    fn slot_req_ptr(&self, global_slot: u32) -> *mut Req {
        unsafe { self.slot_base(global_slot).add(self.layout.req_offset) as *mut Req }
    }

    fn slot_resp_ptr(&self, global_slot: u32) -> *const Resp {
        unsafe { self.slot_base(global_slot).add(self.layout.resp_offset) as *const Resp }
    }

    /// Sends a request without waiting for a response.
    ///
    /// If all slots are occupied, blocks until the oldest pending request completes,
    /// discarding its response.
    pub fn send(&mut self, req: Req) -> Result<(), CallError> {
        if self.free_bitmap == 0 {
            // All slots busy, drain the oldest
            let _ = self.recv()?;
        }
        let slot_idx = self.free_bitmap.trailing_zeros();
        self.free_bitmap &= !(1 << slot_idx);

        let global = self.global_slot(slot_idx);
        unsafe { std::ptr::write_volatile(self.slot_req_ptr(global), req) };
        // SAFETY: slot_idx = trailing_zeros(free_bitmap) < slots_per_client <= 32 = client_seqs.len()
        let new_seq = unsafe {
            let seq = self.client_seqs.get_unchecked_mut(slot_idx as usize);
            *seq = seq.wrapping_add(1);
            *seq
        };
        self.slot_client_seq(global).store(new_seq, Ordering::Release);

        let mask = self.slots_per_client - 1;
        // SAFETY: pq_tail is masked by (spc - 1) < 32 = pending_queue.len()
        unsafe { *self.pending_queue.get_unchecked_mut(self.pq_tail as usize) = slot_idx as u8 };
        self.pq_tail = (self.pq_tail + 1) & mask;
        Ok(())
    }

    /// Receives the response for the oldest pending request (blocks until ready).
    pub fn recv(&mut self) -> Result<Resp, CallError> {
        let mask = self.slots_per_client - 1;
        // SAFETY: pq_head is masked by (spc - 1) < 32 = pending_queue.len()
        let slot_idx = unsafe { *self.pending_queue.get_unchecked(self.pq_head as usize) } as u32;
        self.pq_head = (self.pq_head + 1) & mask;

        let global = self.global_slot(slot_idx);
        // SAFETY: slot_idx came from pending_queue which stores values < slots_per_client <= 32
        let expected_seq = unsafe { *self.client_seqs.get_unchecked(slot_idx as usize) };
        loop {
            if self.slot_server_seq(global).load(Ordering::Acquire) == expected_seq {
                break;
            }
            if !self.is_server_alive() {
                return Err(CallError::ServerDisconnected);
            }
            std::hint::spin_loop();
        }

        let resp = unsafe { std::ptr::read_volatile(self.slot_resp_ptr(global)) };
        self.free_bitmap |= 1 << slot_idx;
        Ok(resp)
    }

    /// Receives any completed response without ordering constraint (non-blocking).
    ///
    /// Scans all in-flight slots and returns the first completed one.
    /// Returns `Ok(None)` if no slot has completed yet.
    ///
    /// **Warning**: Do not mix with `recv()` — the pending_queue is not updated.
    pub fn try_recv_any(&mut self) -> Result<Option<Resp>, CallError> {
        let all_slots_mask = (1u32 << self.slots_per_client) - 1;
        let in_flight = !self.free_bitmap & all_slots_mask;
        if in_flight == 0 {
            return Ok(None);
        }

        let mut bits = in_flight;
        while bits != 0 {
            let slot_idx = bits.trailing_zeros();
            bits &= bits - 1;

            let global = self.global_slot(slot_idx);
            let expected_seq = unsafe { *self.client_seqs.get_unchecked(slot_idx as usize) };

            if self.slot_server_seq(global).load(Ordering::Acquire) == expected_seq {
                let resp = unsafe { std::ptr::read_volatile(self.slot_resp_ptr(global)) };
                self.free_bitmap |= 1 << slot_idx;
                return Ok(Some(resp));
            }
        }

        if !self.is_server_alive() {
            return Err(CallError::ServerDisconnected);
        }
        Ok(None)
    }

    /// Receives any completed response without ordering constraint (blocking).
    ///
    /// Spins until at least one in-flight slot completes, then returns its response.
    ///
    /// **Warning**: Do not mix with `recv()` — the pending_queue is not updated.
    pub fn recv_any(&mut self) -> Result<Resp, CallError> {
        loop {
            match self.try_recv_any() {
                Ok(Some(resp)) => return Ok(resp),
                Ok(None) => std::hint::spin_loop(),
                Err(e) => return Err(e),
            }
        }
    }

    /// Synchronous blocking RPC call.
    ///
    /// Equivalent to `send(req)` followed by `recv()`.
    pub fn call(&mut self, req: Req) -> Result<Resp, CallError> {
        self.send(req)?;
        self.recv()
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
        let first_global = self.global_slot(0);
        unsafe {
            let alive = &*(self.slot_base(first_global).add(self.layout.alive_offset)
                as *const AtomicBool);
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
            let mut server = Server::<u64, u64>::create(&name, 4, 1).unwrap();

            let name_clone = name.clone();
            let client_thread = std::thread::spawn(move || {
                std::thread::sleep(std::time::Duration::from_millis(10));
                let mut client = Client::<u64, u64>::connect(&name_clone).unwrap();
                let resp = client.call(42).unwrap();
                assert_eq!(resp, 43);
            });

            // Poll for request using process_batch
            loop {
                if server.process_batch(|_cid, req| req + 1) > 0 {
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
            let server = Server::<u64, u64>::create(&name, 4, 1).unwrap();

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
            let server = Server::<u64, u64>::create(&name, 4, 1).unwrap();
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
            let mut server = Server::<u64, u64>::create(&name, 4, 1).unwrap();

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
                served += server.process_batch(|_cid, req| req * 2);
                std::hint::spin_loop();
            }

            client_thread.join().unwrap();
        }
    }

    #[test]
    fn test_different_req_resp_types() {
        let name = format!("/shm_rpc_types_{}", std::process::id());

        unsafe {
            let mut server = Server::<u32, u64>::create(&name, 4, 1).unwrap();

            let name_clone = name.clone();
            let client_thread = std::thread::spawn(move || {
                std::thread::sleep(std::time::Duration::from_millis(10));
                let mut client = Client::<u32, u64>::connect(&name_clone).unwrap();
                let resp = client.call(10u32).unwrap();
                assert_eq!(resp, 100u64);
            });

            loop {
                if server.process_batch(|_cid, req| req as u64 * 10) > 0 {
                    break;
                }
                std::hint::spin_loop();
            }

            client_thread.join().unwrap();
        }
    }

    #[test]
    fn test_deferred_response() {
        let name = format!("/shm_rpc_defer_{}", std::process::id());

        unsafe {
            let mut server = Server::<u64, u64>::create(&name, 4, 4).unwrap();

            let name_clone = name.clone();
            let client_thread = std::thread::spawn(move || {
                std::thread::sleep(std::time::Duration::from_millis(10));
                let mut client = Client::<u64, u64>::connect(&name_clone).unwrap();

                // Send 4 requests (fills pipeline)
                for i in 0..4u64 {
                    client.send(i).unwrap();
                }

                // Receive all 4 responses (server will complete deferred ones)
                let mut results = Vec::new();
                for _ in 0..4 {
                    results.push(client.recv().unwrap());
                }
                results
            });

            // Collect deferred requests
            let mut deferred: Vec<(ClientId, u32, u64)> = Vec::new();
            let mut served = 0u32;

            while served < 4 {
                served += server.process_batch_deferred(|cid, slot_idx, req| {
                    if req % 2 == 0 {
                        // Defer even requests
                        deferred.push((cid, slot_idx, req));
                        None
                    } else {
                        // Respond immediately to odd requests
                        Some(req * 10)
                    }
                });
                std::hint::spin_loop();
            }

            // Complete deferred requests
            for (cid, slot_idx, req) in &deferred {
                server.complete_request(*cid, *slot_idx, req * 100);
            }

            let results = client_thread.join().unwrap();
            assert_eq!(results.len(), 4);
            // Slots are received in FIFO order. req 0 (even→*100), req 1 (odd→*10),
            // req 2 (even→*100), req 3 (odd→*10)
            assert_eq!(results[0], 0);   // 0 * 100
            assert_eq!(results[1], 10);  // 1 * 10
            assert_eq!(results[2], 200); // 2 * 100
            assert_eq!(results[3], 30);  // 3 * 10
        }
    }

    #[test]
    fn test_send_recv_pipeline() {
        let name = format!("/shm_rpc_pipe_{}", std::process::id());

        unsafe {
            let mut server = Server::<u64, u64>::create(&name, 4, 4).unwrap();

            let name_clone = name.clone();
            let client_thread = std::thread::spawn(move || {
                std::thread::sleep(std::time::Duration::from_millis(10));
                let mut client = Client::<u64, u64>::connect(&name_clone).unwrap();

                // Fill pipeline with 4 requests
                for i in 0..4u64 {
                    client.send(i).unwrap();
                }
                // Steady state: recv oldest, send new
                for i in 4..100u64 {
                    let resp = client.recv().unwrap();
                    assert_eq!(resp, (i - 4) + 1);
                    client.send(i).unwrap();
                }
                // Drain
                for i in 96..100u64 {
                    let resp = client.recv().unwrap();
                    assert_eq!(resp, i + 1);
                }
            });

            let mut served = 0;
            while served < 100 {
                served += server.process_batch(|_cid, req| req + 1);
                std::hint::spin_loop();
            }

            client_thread.join().unwrap();
        }
    }
}
