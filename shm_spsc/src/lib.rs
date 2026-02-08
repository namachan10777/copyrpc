//! RPC slot-based communication over shared memory.
//!
//! Uses a toggle-bit protocol (inspired by ffwd, SOSP 2017):
//! - `client_seq` incremented by client when posting a request
//! - `server_seq` incremented by server when posting a response
//! - `client_seq != server_seq` → request pending
//! - `client_seq == server_seq` → idle / response ready
//!
//! ## Client API (copyrpc-style async)
//!
//! - `call(req, user_data)` — non-blocking send; polls internally if all slots busy
//! - `poll()` — scans in-flight slots, fires `on_response(user_data, resp)` for completions
//! - `call_blocking(req)` — synchronous RPC (on `connect_sync()` clients)
//!
//! ## Server API (pull-based)
//!
//! - `poll()` — scans all client slots for new requests, buffers internally
//! - `recv()` — returns next buffered request with an opaque `RequestToken`
//! - `reply(token, resp)` — writes response (immediate or deferred)

pub mod mpsc_ffwd;
pub mod mpsc_shared;
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

/// Opaque token identifying a pending request on the server.
///
/// Returned by [`Server::recv()`] and consumed by [`Server::reply()`].
#[derive(Clone, Copy, Debug)]
pub struct RequestToken {
    client_id: ClientId,
    slot_index: u32,
}

impl RequestToken {
    /// Returns the client that sent this request.
    pub fn client_id(&self) -> ClientId {
        self.client_id
    }
}

/// Error returned when a client operation fails.
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
pub(crate) const CACHE_LINE_SIZE: usize = 64;
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

pub(crate) fn align_up(val: usize, align: usize) -> usize {
    (val + align - 1) & !(align - 1)
}

// =============================================================================
// Server
// =============================================================================

/// Server side of the RPC slot channel.
pub struct Server<Req: Serial, Resp: Serial> {
    _shm: SharedMemory,
    header: *mut Header,
    base: *mut u8,
    layout: SlotLayout,
    slots_per_client: u32,
    server_seqs: Vec<u8>,
    in_progress: Vec<u32>,    // bits: recv'd but not yet replied
    new_requests: Vec<u32>,   // bits: found by poll(), not yet recv'd
    recv_scan_pos: u32,
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
            in_progress: vec![0u32; max_clients as usize],
            new_requests: vec![0u32; max_clients as usize],
            recv_scan_pos: 0,
            _marker: PhantomData,
        })
    }

    #[inline(always)]
    fn slot_base(&self, flat_idx: u32) -> *mut u8 {
        unsafe {
            self.base
                .add(HEADER_SIZE + self.layout.slot_size * flat_idx as usize)
        }
    }

    #[inline(always)]
    fn slot_client_seq(&self, flat_idx: u32) -> &AtomicU8 {
        unsafe {
            &*(self.slot_base(flat_idx).add(self.layout.client_seq_offset) as *const AtomicU8)
        }
    }

    #[inline(always)]
    fn slot_server_seq(&self, flat_idx: u32) -> &AtomicU8 {
        unsafe {
            &*(self.slot_base(flat_idx).add(self.layout.server_seq_offset) as *const AtomicU8)
        }
    }

    #[inline(always)]
    fn slot_alive(&self, flat_idx: u32) -> &AtomicBool {
        unsafe {
            &*(self.slot_base(flat_idx).add(self.layout.alive_offset) as *const AtomicBool)
        }
    }

    #[inline(always)]
    fn slot_req_ptr(&self, flat_idx: u32) -> *const Req {
        unsafe { self.slot_base(flat_idx).add(self.layout.req_offset) as *const Req }
    }

    #[inline(always)]
    fn slot_resp_ptr(&self, flat_idx: u32) -> *mut Resp {
        unsafe { self.slot_base(flat_idx).add(self.layout.resp_offset) as *mut Resp }
    }

    /// Scans all client slots for new requests, marking them in an internal bitmap.
    ///
    /// Use [`recv()`](Self::recv) to retrieve marked requests (reads directly from
    /// shared memory — zero-copy). Slots that are already marked as new or
    /// in-progress (recv'd but not replied) are skipped.
    #[inline]
    pub fn poll(&mut self) -> u32 {
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
            let ci = client_idx as usize;
            let skip = unsafe {
                *self.in_progress.get_unchecked(ci) | *self.new_requests.get_unchecked(ci)
            };
            for s in 0..spc {
                if skip & (1 << s) != 0 {
                    continue;
                }
                let global = base_slot + s;
                let seq_idx = global as usize;
                let client_seq = self.slot_client_seq(global).load(Ordering::Acquire);
                let server_seq = unsafe { *self.server_seqs.get_unchecked(seq_idx) };
                if client_seq != server_seq {
                    unsafe { *self.new_requests.get_unchecked_mut(ci) |= 1 << s };
                    count += 1;
                }
            }
        }

        self.recv_scan_pos = 0;
        count
    }

    /// Returns the next new request, reading directly from shared memory.
    ///
    /// The returned [`RequestToken`] must be passed to [`reply()`](Self::reply)
    /// to complete the request.
    #[inline]
    pub fn recv(&mut self) -> Option<(RequestToken, Req)> {
        let allocated = unsafe { (*self.header).next_client_id.load(Ordering::Relaxed) };
        let spc = self.slots_per_client;

        while self.recv_scan_pos < allocated {
            let ci = self.recv_scan_pos as usize;
            let bits = unsafe { *self.new_requests.get_unchecked(ci) };
            if bits != 0 {
                let s = bits.trailing_zeros();
                unsafe { *self.new_requests.get_unchecked_mut(ci) &= !(1 << s) };
                unsafe { *self.in_progress.get_unchecked_mut(ci) |= 1 << s };
                let global = self.recv_scan_pos * spc + s;
                let req = unsafe { std::ptr::read_volatile(self.slot_req_ptr(global)) };
                return Some((
                    RequestToken {
                        client_id: ClientId(self.recv_scan_pos),
                        slot_index: s,
                    },
                    req,
                ));
            }
            self.recv_scan_pos += 1;
        }
        None
    }

    /// Writes a response for a previously received request.
    ///
    /// Can be called immediately after `recv()` or deferred to a later point.
    #[inline]
    pub fn reply(&mut self, token: RequestToken, resp: Resp) {
        let cid = token.client_id.0;
        let s = token.slot_index;
        let global = cid * self.slots_per_client + s;
        let seq_idx = global as usize;

        unsafe { std::ptr::write_volatile(self.slot_resp_ptr(global), resp) };
        let server_seq = unsafe { *self.server_seqs.get_unchecked(seq_idx) };
        let new_seq = server_seq.wrapping_add(1);
        unsafe { *self.server_seqs.get_unchecked_mut(seq_idx) = new_seq };
        self.slot_server_seq(global)
            .store(new_seq, Ordering::Release);

        unsafe { *self.in_progress.get_unchecked_mut(cid as usize) &= !(1 << s) };
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

// =============================================================================
// Client
// =============================================================================

/// Client side of the RPC slot channel.
///
/// Generic over user data `U` and response callback `F`.
/// Use [`connect_sync()`](Self::connect_sync) for synchronous-only usage.
pub struct Client<Req: Serial, Resp: Serial, U, F: FnMut(U, Resp)> {
    _shm: SharedMemory,
    header: *mut Header,
    base: *mut u8,
    layout: SlotLayout,
    client_id: u32,
    slots_per_client: u32,
    base_slot: u32, // client_id * slots_per_client (precomputed)
    client_seqs: [u8; MAX_SLOTS_PER_CLIENT],
    free_bitmap: u32,
    pending_queue: [u32; MAX_SLOTS_PER_CLIENT],
    pq_head: u32,
    pq_count: u32,
    user_data: [Option<U>; MAX_SLOTS_PER_CLIENT],
    on_response: F,
    _marker: PhantomData<(Req, Resp)>,
}

unsafe impl<Req: Serial + Send, Resp: Serial + Send, U: Send, F: FnMut(U, Resp) + Send> Send
    for Client<Req, Resp, U, F>
{
}

impl<Req: Serial, Resp: Serial, U, F: FnMut(U, Resp)> Client<Req, Resp, U, F> {
    /// Connects to an existing shared memory region.
    ///
    /// `on_response` is called for each completed async request during [`poll()`](Self::poll).
    ///
    /// # Safety
    /// The caller must ensure that a server has created the shared memory
    /// with the same types `Req` and `Resp`.
    pub unsafe fn connect<P: AsRef<Path>>(
        path: P,
        on_response: F,
    ) -> Result<Self, ConnectError> {
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

        let free_bitmap = (1u32 << slots_per_client) - 1;

        Ok(Self {
            _shm: shm,
            header,
            base,
            layout,
            client_id,
            slots_per_client,
            base_slot: client_id * slots_per_client,
            client_seqs: [0u8; MAX_SLOTS_PER_CLIENT],
            free_bitmap,
            pending_queue: [0u32; MAX_SLOTS_PER_CLIENT],
            pq_head: 0,
            pq_count: 0,
            user_data: std::array::from_fn(|_| None),
            on_response,
            _marker: PhantomData,
        })
    }

    #[inline(always)]
    fn slot_base(&self, flat_idx: u32) -> *mut u8 {
        unsafe {
            self.base
                .add(HEADER_SIZE + self.layout.slot_size * flat_idx as usize)
        }
    }

    #[inline(always)]
    fn slot_client_seq(&self, flat_idx: u32) -> &AtomicU8 {
        unsafe {
            &*(self.slot_base(flat_idx).add(self.layout.client_seq_offset) as *const AtomicU8)
        }
    }

    #[inline(always)]
    fn slot_server_seq(&self, flat_idx: u32) -> &AtomicU8 {
        unsafe {
            &*(self.slot_base(flat_idx).add(self.layout.server_seq_offset) as *const AtomicU8)
        }
    }

    #[inline(always)]
    fn slot_req_ptr(&self, flat_idx: u32) -> *mut Req {
        unsafe { self.slot_base(flat_idx).add(self.layout.req_offset) as *mut Req }
    }

    #[inline(always)]
    fn slot_resp_ptr(&self, flat_idx: u32) -> *const Resp {
        unsafe { self.slot_base(flat_idx).add(self.layout.resp_offset) as *const Resp }
    }

    /// Writes a request to the given slot and updates the client sequence number.
    #[inline(always)]
    fn write_slot(&mut self, slot_idx: u32, req: Req) -> u8 {
        let fi = self.base_slot + slot_idx;
        unsafe { std::ptr::write_volatile(self.slot_req_ptr(fi), req) };
        let new_seq = unsafe {
            let seq = self.client_seqs.get_unchecked_mut(slot_idx as usize);
            *seq = seq.wrapping_add(1);
            *seq
        };
        self.slot_client_seq(fi).store(new_seq, Ordering::Release);
        new_seq
    }

    /// Allocates a slot, writes the request, stores user data, and pushes to FIFO.
    #[inline(always)]
    fn send_internal(&mut self, req: Req, user_data: U) -> u32 {
        let slot_idx = self.free_bitmap.trailing_zeros();
        self.free_bitmap &= !(1 << slot_idx);
        self.write_slot(slot_idx, req);
        self.user_data[slot_idx as usize] = Some(user_data);

        // Push to FIFO pending queue
        let tail = (self.pq_head + self.pq_count) % MAX_SLOTS_PER_CLIENT as u32;
        self.pending_queue[tail as usize] = slot_idx;
        self.pq_count += 1;

        slot_idx
    }

    /// Sends a request asynchronously with associated user data.
    ///
    /// If all slots are occupied, internally calls [`poll()`](Self::poll) to drain
    /// completions until a slot becomes available.
    #[inline]
    pub fn call(&mut self, req: Req, user_data: U) -> Result<(), CallError> {
        while self.free_bitmap == 0 {
            self.poll()?;
            std::hint::spin_loop();
        }
        self.send_internal(req, user_data);
        Ok(())
    }

    /// Scans in-flight slots in FIFO order, invoking `on_response` for each completion.
    ///
    /// Checks the oldest pending slot first; stops at the first non-completed slot.
    #[inline]
    pub fn poll(&mut self) -> Result<u32, CallError> {
        if self.pq_count == 0 {
            return Ok(0);
        }

        let mut completed = 0u32;

        while self.pq_count > 0 {
            let slot_idx = self.pending_queue[self.pq_head as usize];
            let fi = self.base_slot + slot_idx;
            let expected_seq = unsafe { *self.client_seqs.get_unchecked(slot_idx as usize) };

            if self.slot_server_seq(fi).load(Ordering::Acquire) != expected_seq {
                break;
            }

            let resp = unsafe { std::ptr::read_volatile(self.slot_resp_ptr(fi)) };
            let ud = self.user_data[slot_idx as usize]
                .take()
                .expect("user_data missing for completed slot");
            self.free_bitmap |= 1 << slot_idx;
            self.pq_head = (self.pq_head + 1) % MAX_SLOTS_PER_CLIENT as u32;
            self.pq_count -= 1;
            (self.on_response)(ud, resp);
            completed += 1;
        }

        if completed == 0 && self.pq_count > 0 && !self.is_server_alive() {
            return Err(CallError::ServerDisconnected);
        }
        Ok(completed)
    }

    /// Returns the number of in-flight async requests (excludes blocking calls).
    #[inline]
    pub fn pending_count(&self) -> u32 {
        self.pq_count
    }

    /// Returns the number of slots available per client.
    #[inline]
    pub fn slots_per_client(&self) -> u32 {
        self.slots_per_client
    }

    /// Returns the client's slot ID.
    #[inline]
    pub fn id(&self) -> ClientId {
        ClientId(self.client_id)
    }

    /// Returns true if the server is still alive.
    #[inline]
    pub fn is_server_alive(&self) -> bool {
        unsafe { (*self.header).server_alive.load(Ordering::Acquire) }
    }
}

/// Type alias for synchronous-only clients (no async callback).
pub type SyncClient<Req, Resp> = Client<Req, Resp, (), fn((), Resp)>;

/// Synchronous convenience methods.
impl<Req: Serial, Resp: Serial> SyncClient<Req, Resp> {
    /// Connects with a no-op callback for synchronous-only usage.
    ///
    /// Use [`call_blocking()`](Self::call_blocking) for RPC calls.
    ///
    /// # Safety
    /// Same requirements as [`connect()`](Client::connect).
    pub unsafe fn connect_sync<P: AsRef<Path>>(path: P) -> Result<Self, ConnectError> {
        fn noop<R>(_: (), _: R) {}
        unsafe { Self::connect(path, noop::<Resp>) }
    }

    /// Synchronous blocking RPC call.
    ///
    /// Bypasses the FIFO pending queue entirely, spinning directly on the
    /// assigned slot for minimal overhead. If async calls are in flight,
    /// their callbacks are drained via `poll()` while waiting.
    #[inline]
    pub fn call_blocking(&mut self, req: Req) -> Result<Resp, CallError> {
        while self.free_bitmap == 0 {
            self.poll()?;
            std::hint::spin_loop();
        }
        // Allocate slot directly (not via send_internal — no pending_queue push)
        let slot_idx = self.free_bitmap.trailing_zeros();
        self.free_bitmap &= !(1 << slot_idx);
        let new_seq = self.write_slot(slot_idx, req);

        let fi = self.base_slot + slot_idx;
        let has_async = self.pq_count > 0;

        loop {
            if self.slot_server_seq(fi).load(Ordering::Acquire) == new_seq {
                let resp = unsafe { std::ptr::read_volatile(self.slot_resp_ptr(fi)) };
                self.free_bitmap |= 1 << slot_idx;
                return Ok(resp);
            }
            if has_async {
                self.poll()?;
            } else if !self.is_server_alive() {
                self.free_bitmap |= 1 << slot_idx;
                return Err(CallError::ServerDisconnected);
            }
            std::hint::spin_loop();
        }
    }
}

impl<Req: Serial, Resp: Serial, U, F: FnMut(U, Resp)> Drop for Client<Req, Resp, U, F> {
    fn drop(&mut self) {
        unsafe {
            let alive = &*(self.slot_base(self.base_slot).add(self.layout.alive_offset)
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
                let mut client = SyncClient::<u64, u64>::connect_sync(&name_clone).unwrap();
                let resp = client.call_blocking(42).unwrap();
                assert_eq!(resp, 43);
            });

            loop {
                server.poll();
                if let Some((token, req)) = server.recv() {
                    server.reply(token, req + 1);
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
                let client = SyncClient::<u64, u64>::connect_sync(&name_clone).unwrap();
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
            let client = SyncClient::<u64, u64>::connect_sync(&name_clone).unwrap();
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
                let mut client = SyncClient::<u64, u64>::connect_sync(&name_clone).unwrap();
                for i in 0..100u64 {
                    let resp = client.call_blocking(i).unwrap();
                    assert_eq!(resp, i * 2);
                }
            });

            let mut served = 0;
            while served < 100 {
                server.poll();
                while let Some((token, req)) = server.recv() {
                    server.reply(token, req * 2);
                    served += 1;
                }
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
                let mut client = SyncClient::<u32, u64>::connect_sync(&name_clone).unwrap();
                let resp = client.call_blocking(10u32).unwrap();
                assert_eq!(resp, 100u64);
            });

            loop {
                server.poll();
                if let Some((token, req)) = server.recv() {
                    server.reply(token, req as u64 * 10);
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
                let mut client = SyncClient::<u64, u64>::connect_sync(&name_clone).unwrap();

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
                while let Some((token, req)) = server.recv() {
                    served += 1;
                    if req % 2 == 0 {
                        deferred.push((token, req));
                    } else {
                        server.reply(token, req * 10);
                    }
                }
                for (token, req) in deferred.drain(..) {
                    server.reply(token, req * 100);
                }
                std::hint::spin_loop();
            }

            let results = client_thread.join().unwrap();
            assert_eq!(results.len(), 4);
            // call_blocking is serial: 0 (even→*100), 1 (odd→*10), 2 (even→*100), 3 (odd→*10)
            assert_eq!(results[0], 0);   // 0 * 100
            assert_eq!(results[1], 10);  // 1 * 10
            assert_eq!(results[2], 200); // 2 * 100
            assert_eq!(results[3], 30);  // 3 * 10
        }
    }

    #[test]
    fn test_async_pipeline() {
        let name = format!("/shm_rpc_pipe_{}", std::process::id());

        unsafe {
            let mut server = Server::<u64, u64>::create(&name, 4, 4).unwrap();

            let name_clone = name.clone();
            let client_thread = std::thread::spawn(move || {
                std::thread::sleep(std::time::Duration::from_millis(10));

                let count = std::cell::Cell::new(0u64);
                let mut client =
                    Client::<u64, u64, (), _>::connect(&name_clone, |(), _resp| {
                        count.set(count.get() + 1);
                    })
                    .unwrap();

                // Fill pipeline
                for i in 0..4u64 {
                    client.call(i, ()).unwrap();
                }
                // Steady state: call() will poll internally when full
                for i in 4..100u64 {
                    client.call(i, ()).unwrap();
                }
                // Drain
                while client.pending_count() > 0 {
                    client.poll().unwrap();
                    std::hint::spin_loop();
                }

                assert_eq!(count.get(), 100);
            });

            let mut served = 0u32;
            while served < 100 {
                server.poll();
                while let Some((token, req)) = server.recv() {
                    server.reply(token, req + 1);
                    served += 1;
                }
                std::hint::spin_loop();
            }

            client_thread.join().unwrap();
        }
    }
}
