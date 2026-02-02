//! RPC Server implementation.
//!
//! The server implements ScaleRPC's time-sharing scheduler (Section 3.2).
//!
//! ## Architecture (Figure 6)
//!
//! ```text
//! ┌────────────────┬────────────────┐
//! │ Processing Pool│  Warmup Pool   │
//! │  (Group_b処理) │ (Group_a準備)  │
//! └────────────────┴────────────────┘
//!          ↕ Context Switch ↕
//! ┌────────────────┬────────────────┐
//! │ Processing Pool│  Warmup Pool   │
//! │  (Group_a処理) │ (Group_c準備)  │
//! └────────────────┴────────────────┘
//! ```
//!
//! ## Time-Sharing Scheduler
//!
//! The server maintains connection groups and schedules them in a
//! round-robin fashion with configurable time slices (default 100µs).
//!
//! - Only one group is active (Processing) at a time
//! - The next group prepares requests in the Warmup pool
//! - On context switch, pools are swapped and clients are notified

use std::cell::{Cell, UnsafeCell};
use std::rc::Rc;

use minstant::Instant;

use mlx5::cq::{Cq, Cqe};
use mlx5::device::Context;
use mlx5::emit_wqe;
use mlx5::pd::{AccessFlags, MemoryRegion, Pd};
use mlx5::wqe::WqeFlags;

use crate::config::ServerConfig;
use crate::connection::{create_shared_cqs, Connection, ConnectionId, RemoteEndpoint};
use crate::error::{Error, Result};
use crate::mapping::VirtualMapping;
use crate::pool::MessagePool;
use crate::protocol::{
    ContextSwitchEvent, EndpointEntry, MessageTrailer, RequestHeader, ResponseHeader,
    MESSAGE_BLOCK_SIZE,
};

/// Request handler trait for processing RPC requests (zero-copy).
///
/// Takes RPC type, request payload, and a mutable response buffer.
/// Writes response directly to the buffer and returns (status, response_length).
/// This avoids memory allocation on every request.
///
/// The trait enables static dispatch and inlining of handler calls,
/// improving throughput compared to `Box<dyn Fn>`.
pub trait RequestHandler {
    /// Handle an RPC request.
    ///
    /// # Arguments
    /// * `rpc_type` - RPC type identifier
    /// * `payload` - Request payload bytes
    /// * `response_buf` - Mutable buffer to write response into
    ///
    /// # Returns
    /// (status_code, response_length)
    fn handle(&self, rpc_type: u16, payload: &[u8], response_buf: &mut [u8]) -> (u32, usize);
}

/// Blanket implementation for closures that match the handler signature.
impl<F> RequestHandler for F
where
    F: Fn(u16, &[u8], &mut [u8]) -> (u32, usize),
{
    #[inline]
    fn handle(&self, rpc_type: u16, payload: &[u8], response_buf: &mut [u8]) -> (u32, usize) {
        self(rpc_type, payload, response_buf)
    }
}

/// Marker type for RpcServer without a handler.
///
/// Used as the default type parameter for `RpcServer<H>` when no handler is set.
#[derive(Clone, Copy, Default)]
pub struct NoHandler;

impl RequestHandler for NoHandler {
    #[inline]
    fn handle(&self, _rpc_type: u16, _payload: &[u8], _response_buf: &mut [u8]) -> (u32, usize) {
        // NoHandler returns error status with no response
        (1, 0)
    }
}

/// Group ID type.
pub type GroupId = usize;

/// Per-connection warmup buffer information.
///
/// Stores the remote warmup buffer address and rkey for each connection,
/// allowing the server to RDMA READ requests directly from clients.
#[derive(Debug, Clone, Copy, Default)]
struct WarmupBufferInfo {
    /// Remote warmup buffer address.
    addr: u64,
    /// Remote warmup buffer rkey.
    rkey: u32,
    /// Number of slots in the warmup buffer.
    slots: u32,
}

/// Alignment for RDMA memory allocations (4KB page).
const RDMA_ALIGNMENT: usize = 4096;

/// Pool of endpoint entries for client warmup notification.
///
/// Each connection has an endpoint entry that the client writes to
/// via RDMA WRITE to notify the server that warmup requests are ready.
/// This allows the server to read only the required number of requests
/// instead of scanning all warmup slots.
struct EndpointEntryPool {
    /// Buffer memory.
    buffer: *mut u8,
    /// Memory region for RDMA access.
    mr: MemoryRegion,
    /// Number of entries in the pool.
    num_entries: usize,
    /// Last seen sequence number per connection.
    last_seqs: Vec<u32>,
}

impl EndpointEntryPool {
    /// Create a new endpoint entry pool.
    fn new(pd: &Pd, max_connections: usize) -> Result<Self> {
        let size = max_connections * EndpointEntry::SIZE;

        let buffer = unsafe {
            let mut ptr: *mut std::ffi::c_void = std::ptr::null_mut();
            let ret = libc::posix_memalign(&mut ptr, RDMA_ALIGNMENT, size);
            if ret != 0 {
                return Err(Error::Io(std::io::Error::from_raw_os_error(ret)));
            }
            std::ptr::write_bytes(ptr as *mut u8, 0, size);
            ptr as *mut u8
        };

        let access =
            AccessFlags::LOCAL_WRITE | AccessFlags::REMOTE_WRITE | AccessFlags::REMOTE_READ;
        let mr = unsafe { pd.register(buffer, size, access)? };

        Ok(Self {
            buffer,
            mr,
            num_entries: max_connections,
            last_seqs: vec![0; max_connections],
        })
    }

    /// Get the address for a specific connection's endpoint entry.
    fn addr(&self, conn_id: ConnectionId) -> u64 {
        self.buffer as u64 + (conn_id * EndpointEntry::SIZE) as u64
    }

    /// Get the rkey for RDMA WRITE.
    fn rkey(&self) -> u32 {
        self.mr.rkey()
    }

    /// Read the endpoint entry for a connection.
    fn read_entry(&self, conn_id: ConnectionId) -> EndpointEntry {
        if conn_id >= self.num_entries {
            return EndpointEntry::default();
        }
        let entry_ptr = unsafe { self.buffer.add(conn_id * EndpointEntry::SIZE) };
        unsafe { EndpointEntry::read_from(entry_ptr) }
    }

    /// Check if the entry has a new batch (sequence changed).
    fn has_new_batch(&mut self, conn_id: ConnectionId) -> Option<EndpointEntry> {
        if conn_id >= self.num_entries {
            return None;
        }
        let entry = self.read_entry(conn_id);
        if entry.seq > self.last_seqs[conn_id] && entry.batch_size > 0 {
            self.last_seqs[conn_id] = entry.seq;
            Some(entry)
        } else {
            None
        }
    }

    /// Clear the endpoint entry for a connection.
    fn clear_entry(&self, conn_id: ConnectionId) {
        if conn_id >= self.num_entries {
            return;
        }
        let entry_ptr = unsafe { self.buffer.add(conn_id * EndpointEntry::SIZE) };
        unsafe {
            // Clear batch_size to indicate processed
            let batch_size_ptr = entry_ptr.add(8) as *mut u32;
            std::ptr::write_volatile(batch_size_ptr, 0);
        }
    }
}

impl Drop for EndpointEntryPool {
    fn drop(&mut self) {
        unsafe {
            libc::free(self.buffer as *mut std::ffi::c_void);
        }
    }
}

/// Group scheduler state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchedulerState {
    /// Processing requests from the active group.
    Processing,
    /// Transitioning between groups.
    ContextSwitching,
}

/// Per-group context.
pub struct GroupContext {
    /// Group ID.
    pub group_id: GroupId,
    /// Connection IDs in this group.
    pub connections: Vec<ConnectionId>,
    /// Context switch sequence number for this group.
    pub context_switch_seq: u64,
}

impl GroupContext {
    /// Create a new group context.
    pub fn new(group_id: GroupId) -> Self {
        Self {
            group_id,
            connections: Vec::new(),
            context_switch_seq: 0,
        }
    }

    /// Add a connection to this group.
    pub fn add_connection(&mut self, conn_id: ConnectionId) {
        self.connections.push(conn_id);
    }
}

/// Group scheduler for time-sharing.
///
/// Implements the ScaleRPC connection grouping and time-sharing mechanism.
/// Uses Cell for interior mutability so context switches can happen during process().
pub struct GroupScheduler {
    /// Group contexts.
    groups: Vec<GroupContext>,
    /// Currently active (processing) group index.
    current_group: Cell<usize>,
    /// Next group (warmup) index.
    next_group: Cell<usize>,
    /// Time slice duration in microseconds.
    time_slice_us: u64,
    /// Last context switch time.
    last_switch: Cell<Instant>,
    /// Current scheduler state.
    state: Cell<SchedulerState>,
    /// Context switch event sequence number.
    context_switch_seq: Cell<u64>,
}

impl GroupScheduler {
    /// Create a new group scheduler.
    pub fn new(num_groups: usize, time_slice_us: u64) -> Self {
        let groups = (0..num_groups).map(GroupContext::new).collect();

        Self {
            groups,
            current_group: Cell::new(0),
            next_group: Cell::new(if num_groups > 1 { 1 } else { 0 }),
            time_slice_us,
            last_switch: Cell::new(Instant::now()),
            state: Cell::new(SchedulerState::Processing),
            context_switch_seq: Cell::new(0),
        }
    }

    /// Get the currently active group ID.
    pub fn current_group_id(&self) -> GroupId {
        self.current_group.get()
    }

    /// Get the next (warmup) group ID.
    pub fn next_group_id(&self) -> GroupId {
        self.next_group.get()
    }

    /// Check if a context switch is due.
    ///
    /// Returns false if there's only one group (no need to switch).
    pub fn should_switch(&self) -> bool {
        if self.groups.len() <= 1 {
            return false;
        }
        self.last_switch.get().elapsed().as_micros() as u64 >= self.time_slice_us
    }

    /// Get the current scheduler state.
    pub fn state(&self) -> SchedulerState {
        self.state.get()
    }

    /// Add a connection to a group.
    pub fn add_connection(&mut self, conn_id: ConnectionId) -> GroupId {
        // Round-robin assignment
        let group_id = conn_id % self.groups.len();
        self.groups[group_id].add_connection(conn_id);
        group_id
    }

    /// Add a connection to a specific group.
    pub fn add_connection_to_group(
        &mut self,
        conn_id: ConnectionId,
        group_id: GroupId,
    ) -> Result<()> {
        if group_id >= self.groups.len() {
            return Err(Error::GroupNotFound(group_id));
        }
        self.groups[group_id].add_connection(conn_id);
        Ok(())
    }

    /// Get connections in the current (processing) group.
    pub fn current_group_connections(&self) -> &[ConnectionId] {
        &self.groups[self.current_group.get()].connections
    }

    /// Get connections in the next (warmup) group.
    pub fn next_group_connections(&self) -> &[ConnectionId] {
        &self.groups[self.next_group.get()].connections
    }

    /// Perform context switch (interior mutability version for use in &self methods).
    ///
    /// Skips empty groups to avoid processing gaps.
    /// Returns the new context switch sequence number.
    pub fn do_context_switch(&self) -> u64 {
        self.state.set(SchedulerState::ContextSwitching);

        // Find next non-empty group (or wrap around to current if all others empty)
        let num_groups = self.groups.len();
        let current = self.current_group.get();
        let mut next = (current + 1) % num_groups;

        // Skip empty groups
        for _ in 0..num_groups {
            if !self.groups[next].connections.is_empty() {
                break;
            }
            next = (next + 1) % num_groups;
        }

        self.current_group.set(next);
        self.next_group.set((next + 1) % num_groups);

        // Update sequence number
        let new_seq = self.context_switch_seq.get() + 1;
        self.context_switch_seq.set(new_seq);
        // Note: can't update groups[].context_switch_seq here without RefCell

        // Update timing
        self.last_switch.set(Instant::now());
        self.state.set(SchedulerState::Processing);

        new_seq
    }

    /// Perform context switch (mutable version).
    ///
    /// Skips empty groups to avoid processing gaps.
    /// Returns the new context switch sequence number.
    pub fn context_switch(&mut self) -> u64 {
        self.state.set(SchedulerState::ContextSwitching);

        // Find next non-empty group (or wrap around to current if all others empty)
        let num_groups = self.groups.len();
        let current = self.current_group.get();
        let mut next = (current + 1) % num_groups;

        // Skip empty groups
        for _ in 0..num_groups {
            if !self.groups[next].connections.is_empty() {
                break;
            }
            next = (next + 1) % num_groups;
        }

        self.current_group.set(next);
        self.next_group.set((next + 1) % num_groups);

        // Update sequence number
        let new_seq = self.context_switch_seq.get() + 1;
        self.context_switch_seq.set(new_seq);
        self.groups[next].context_switch_seq = new_seq;

        // Update timing
        self.last_switch.set(Instant::now());
        self.state.set(SchedulerState::Processing);

        new_seq
    }

    /// Get the current context switch sequence number.
    pub fn context_switch_seq(&self) -> u64 {
        self.context_switch_seq.get()
    }

    /// Get the number of groups.
    pub fn num_groups(&self) -> usize {
        self.groups.len()
    }
}

/// Incoming RPC request (zero-copy).
///
/// The payload is accessed directly from the processing pool slot via pointer.
/// Response is written to a separate response_pool, so there's no overlap.
pub struct IncomingRequest {
    /// Request ID.
    pub req_id: u64,
    /// RPC type.
    pub rpc_type: u16,
    /// Pointer to request payload in processing pool (zero-copy).
    payload_ptr: *const u8,
    /// Length of request payload.
    payload_len: usize,
    /// Client's slot address for response.
    pub client_slot_addr: u64,
    /// Client's slot rkey.
    pub client_slot_rkey: u32,
    /// Connection ID this request came from.
    pub conn_id: ConnectionId,
    /// Slot index where request was received.
    pub slot_index: usize,
    /// Slot sequence number for credit-based flow control.
    pub slot_seq: u64,
}

impl IncomingRequest {
    /// Get the request payload as a slice (zero-copy).
    #[inline]
    pub fn payload(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.payload_ptr, self.payload_len) }
    }

    /// Get the payload length.
    #[inline]
    pub fn payload_len(&self) -> usize {
        self.payload_len
    }
}

// SAFETY: The payload pointer points to registered RDMA memory that lives
// as long as the RpcServer. The slot is marked as processed before the
// IncomingRequest is returned, ensuring the memory is valid.
unsafe impl Send for IncomingRequest {}
unsafe impl Sync for IncomingRequest {}

impl std::fmt::Debug for IncomingRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IncomingRequest")
            .field("req_id", &self.req_id)
            .field("rpc_type", &self.rpc_type)
            .field("payload_len", &self.payload_len)
            .field("client_slot_addr", &self.client_slot_addr)
            .field("client_slot_rkey", &self.client_slot_rkey)
            .field("conn_id", &self.conn_id)
            .field("slot_index", &self.slot_index)
            .field("slot_seq", &self.slot_seq)
            .finish()
    }
}

/// RPC Server.
///
/// Manages connections and processes incoming RPC requests.
/// Implements the ScaleRPC time-sharing scheduler when enabled.
///
/// # Type Parameter
/// * `H` - Request handler type, defaults to `NoHandler` when no handler is set
///
/// # Example
/// ```ignore
/// // Create server without handler
/// let server = RpcServer::new(&pd, config)?;
///
/// // Add handler using with_handler (returns new typed server)
/// let server = server.with_handler(|rpc_type, payload, response_buf| {
///     // Echo handler
///     let len = payload.len().min(response_buf.len());
///     response_buf[..len].copy_from_slice(&payload[..len]);
///     (0, len)
/// });
/// ```
pub struct RpcServer<H = NoHandler> {
    /// Processing pool for active group's requests (inbound).
    processing_pool: MessagePool,
    /// Warmup pool for next group's requests (fetched via RDMA READ).
    warmup_pool: MessagePool,
    /// Response pool for building responses before RDMA WRITE to client.
    /// Separate from processing_pool to enable zero-copy request handling.
    response_pool: MessagePool,
    /// Virtual mapping table.
    mapping: VirtualMapping,
    /// Connections indexed by ID.
    connections: Vec<Option<Connection>>,
    /// Configuration.
    #[allow(dead_code)]
    config: ServerConfig,
    /// Request handler.
    handler: H,
    /// Group scheduler for time-sharing.
    scheduler: GroupScheduler,
    /// Per-connection event buffer addresses for context switch notification.
    event_buffer_addrs: Vec<Option<(u64, u32)>>,
    /// Connections that need doorbell flush (for batching).
    ///
    /// # Safety
    /// Uses UnsafeCell for zero-overhead interior mutability.
    /// Safe because RpcServer is single-threaded (!Sync).
    needs_flush: UnsafeCell<Vec<ConnectionId>>,
    /// Per-connection warmup buffer info for RDMA READ.
    warmup_buffer_infos: Vec<Option<WarmupBufferInfo>>,
    /// Shared send completion queue (all QPs share this).
    shared_send_cq: Option<Rc<Cq>>,
    /// Shared receive completion queue (all QPs share this).
    shared_recv_cq: Option<Rc<Cq>>,
    /// Endpoint entries for each connection (client writes here to notify warmup ready).
    endpoint_entries: Option<EndpointEntryPool>,
    /// Pending context switch events per connection (fetched_seq to piggyback on responses).
    /// When set, the next response to this connection will carry the context switch event.
    ///
    /// # Safety
    /// Uses UnsafeCell for zero-overhead interior mutability.
    pending_context_switch: UnsafeCell<Vec<Option<(u64, u32)>>>, // (seq, fetched_seq)
    /// Per-connection next expected slot index for ring buffer polling.
    /// Reset on context switch when transitioning to Process mode.
    ///
    /// # Safety
    /// Uses UnsafeCell for zero-overhead interior mutability.
    next_expected_slot: UnsafeCell<Vec<usize>>,
    /// Reusable buffer for connection IDs during context switch.
    /// Avoids per-call Vec allocation in hot paths.
    ///
    /// # Safety
    /// Uses UnsafeCell for zero-overhead interior mutability.
    conn_buf: UnsafeCell<Vec<ConnectionId>>,
    /// Per-connection last processed slot sequence for credit-based flow control.
    /// Updated when processing requests, piggybacked on responses as acked_slot_seq.
    ///
    /// # Safety
    /// Uses UnsafeCell for zero-overhead interior mutability.
    last_processed_seq: UnsafeCell<Vec<u64>>,
}

impl RpcServer<NoHandler> {
    /// Create a new RPC server without a handler.
    ///
    /// Use `with_handler` to add a request handler.
    ///
    /// # Arguments
    /// * `pd` - Protection domain for memory registration
    /// * `config` - Server configuration
    ///
    /// # Example
    /// ```ignore
    /// let server = RpcServer::new(&pd, config)?
    ///     .with_handler(|rpc_type, payload, response_buf| {
    ///         (0, 0)  // No-op handler
    ///     });
    /// ```
    pub fn new(pd: &Pd, config: ServerConfig) -> Result<Self> {
        let max_connections = config.max_connections;
        let processing_pool = MessagePool::new(pd, &config.pool)?;
        let warmup_pool = MessagePool::new(pd, &config.pool)?;
        let response_pool = MessagePool::new(pd, &config.pool)?;
        let mapping = VirtualMapping::new();

        let scheduler = GroupScheduler::new(
            config.group.num_groups,
            config.group.time_slice_us,
        );

        // Create endpoint entry pool for warmup notification
        let endpoint_entries = EndpointEntryPool::new(pd, max_connections)?;

        Ok(Self {
            processing_pool,
            warmup_pool,
            response_pool,
            mapping,
            connections: Vec::new(),
            config,
            handler: NoHandler,
            scheduler,
            event_buffer_addrs: Vec::new(),
            needs_flush: UnsafeCell::new(Vec::new()),
            warmup_buffer_infos: Vec::new(),
            shared_send_cq: None,
            shared_recv_cq: None,
            endpoint_entries: Some(endpoint_entries),
            pending_context_switch: UnsafeCell::new(Vec::new()),
            next_expected_slot: UnsafeCell::new(Vec::new()),
            conn_buf: UnsafeCell::new(Vec::with_capacity(max_connections)),
            last_processed_seq: UnsafeCell::new(Vec::new()),
        })
    }
}

impl<H: RequestHandler> RpcServer<H> {
    /// Transform this server to use a different handler.
    ///
    /// This consumes the current server and returns a new one with the given handler.
    /// The handler type `H2` must implement `RequestHandler`.
    ///
    /// # Example
    /// ```ignore
    /// let server = RpcServer::new(&pd, config)?
    ///     .with_handler(|rpc_type, payload, response_buf| {
    ///         // Echo handler
    ///         let len = payload.len().min(response_buf.len());
    ///         response_buf[..len].copy_from_slice(&payload[..len]);
    ///         (0, len)
    ///     });
    /// ```
    pub fn with_handler<H2: RequestHandler>(self, handler: H2) -> RpcServer<H2> {
        RpcServer {
            processing_pool: self.processing_pool,
            warmup_pool: self.warmup_pool,
            response_pool: self.response_pool,
            mapping: self.mapping,
            connections: self.connections,
            config: self.config,
            handler,
            scheduler: self.scheduler,
            event_buffer_addrs: self.event_buffer_addrs,
            needs_flush: self.needs_flush,
            warmup_buffer_infos: self.warmup_buffer_infos,
            shared_send_cq: self.shared_send_cq,
            shared_recv_cq: self.shared_recv_cq,
            endpoint_entries: self.endpoint_entries,
            pending_context_switch: self.pending_context_switch,
            next_expected_slot: self.next_expected_slot,
            conn_buf: self.conn_buf,
            last_processed_seq: self.last_processed_seq,
        }
    }

    /// Get the group scheduler.
    pub fn scheduler(&self) -> &GroupScheduler {
        &self.scheduler
    }

    /// Get the group scheduler mutably.
    pub fn scheduler_mut(&mut self) -> &mut GroupScheduler {
        &mut self.scheduler
    }

    /// Register a client's event buffer address.
    ///
    /// The server will RDMA WRITE context switch events to this address.
    pub fn register_event_buffer(&mut self, conn_id: ConnectionId, addr: u64, rkey: u32) {
        while self.event_buffer_addrs.len() <= conn_id {
            self.event_buffer_addrs.push(None);
        }
        self.event_buffer_addrs[conn_id] = Some((addr, rkey));
    }

    /// Check if context switch is due and perform it if needed.
    ///
    /// Returns the new sequence number if a switch occurred.
    pub fn check_and_switch(&mut self) -> Option<u64> {
        if !self.scheduler.should_switch() {
            return None;
        }

        // Perform context switch
        let seq = self.scheduler.context_switch();

        // Swap pools
        std::mem::swap(&mut self.processing_pool, &mut self.warmup_pool);

        Some(seq)
    }

    /// Perform a complete context switch with all steps.
    ///
    /// This implements the full ScaleRPC context switch protocol:
    /// 1. Drain remaining requests from the processing pool
    /// 2. Notify clients of the context switch
    /// 3. Swap processing and warmup pools
    /// 4. Reset state for new active group
    ///
    /// Returns sequence_number if a switch occurred. The old group connections
    /// are stored in self.conn_buf for use by the caller.
    pub fn perform_context_switch(&mut self) -> Result<Option<u64>> {
        if !self.scheduler.should_switch() {
            return Ok(None);
        }

        // Step 1: Copy connections to reusable buffer (no allocation)
        // SAFETY: Single-threaded access, no aliasing within this block
        unsafe {
            let conn_buf = &mut *self.conn_buf.get();
            conn_buf.clear();
            conn_buf.extend_from_slice(self.scheduler.current_group_connections());
        }

        // Step 2: Perform the actual context switch in scheduler
        let seq = self.scheduler.context_switch();

        // Step 3: Swap pools
        std::mem::swap(&mut self.processing_pool, &mut self.warmup_pool);

        // Step 4: Clear the new warmup pool for incoming requests
        self.clear_pool_valid_flags(&self.warmup_pool);

        Ok(Some(seq))
    }

    /// Clear valid flags in all slots of a pool.
    fn clear_pool_valid_flags(&self, pool: &MessagePool) {
        for i in 0..pool.num_slots() {
            if let Some(slot) = pool.get_slot(i) {
                let slot_ptr = slot.data_ptr();
                unsafe {
                    // Clear magic number
                    std::ptr::write_volatile(slot_ptr as *mut u32, 0);
                    // Clear valid flag in trailer
                    let trailer_offset = MESSAGE_BLOCK_SIZE - 4;
                    let valid_ptr = slot_ptr.add(trailer_offset) as *mut u32;
                    std::ptr::write_volatile(valid_ptr, 0);
                }
            }
        }
    }

    /// Drain all pending requests from the processing pool.
    ///
    /// Returns the list of requests that were pending.
    pub fn drain_processing_pool(&self) -> Vec<IncomingRequest> {
        let mut requests = Vec::new();

        for slot_index in 0..self.processing_pool.num_slots() {
            if let Some(request) = self.check_slot_for_request(slot_index) {
                requests.push(request);
            }
        }

        requests
    }

    /// Process all requests from the drained pool and send responses.
    ///
    /// Returns the number of requests processed.
    pub fn process_drained_requests(&self, requests: Vec<IncomingRequest>) -> usize {
        let mut processed = 0;

        for request in requests {
            // Get the response_pool slot for zero-copy response building
            let slot = match self.response_pool.get_slot(request.slot_index) {
                Some(s) => s,
                None => continue,
            };

            // Get response buffer in response_pool
            let slot_ptr = slot.data_ptr();
            let response_buf = unsafe {
                let buf_ptr = slot_ptr.add(ResponseHeader::SIZE);
                let buf_len = MESSAGE_BLOCK_SIZE - ResponseHeader::SIZE - 8;
                std::slice::from_raw_parts_mut(buf_ptr, buf_len)
            };

            let (status, response_len) = self.handler.handle(request.rpc_type, request.payload(), response_buf);

            if let Err(e) = self.send_response_zero_copy(&request, status, response_len) {
                eprintln!("Failed to send response: {}", e);
            } else {
                processed += 1;
            }
        }

        processed
    }

    /// Run the server's main processing loop with time-sharing.
    ///
    /// This is the main entry point for ScaleRPC's time-sharing scheduler.
    /// It alternates between:
    /// - Processing requests from the active group
    /// - Fetching warmup requests from the next group
    /// - Performing context switches when the time slice expires
    ///
    /// Returns the total number of requests processed.
    pub fn run_scheduler_iteration(&mut self) -> Result<usize> {
        let mut total_processed = 0;

        // Check if we need to context switch
        if let Some(seq) = self.perform_context_switch()? {
            // Notify old group's clients (fetched_seq=0 for time-based switches)
            // conn_buf contains old connections from perform_context_switch
            // SAFETY: Single-threaded access
            let conn_buf = unsafe { &*self.conn_buf.get() };
            if let Err(e) = self.notify_context_switch(conn_buf, seq, 0) {
                eprintln!("Failed to notify context switch: {}", e);
            }
        }

        // Process requests from the processing pool
        total_processed += self.process();

        // Fetch warmup requests from next group (best effort)
        let _ = self.fetch_warmup_requests();

        Ok(total_processed)
    }

    /// Queue context switch events for piggybacking on responses.
    ///
    /// Instead of sending separate RDMA WRITEs, the context switch event will be
    /// piggybacked on the next response to each client. This reduces network overhead.
    pub fn queue_context_switch_for_piggyback(&self, conn_ids: &[ConnectionId], sequence: u64, fetched_seq: u32) {
        // SAFETY: Single-threaded access, no aliasing
        let pending = unsafe { &mut *self.pending_context_switch.get() };
        for &conn_id in conn_ids {
            while pending.len() <= conn_id {
                pending.push(None);
            }
            pending[conn_id] = Some((sequence, fetched_seq));
        }
    }

    /// Take pending context switch event for a connection (if any).
    ///
    /// Returns (sequence, fetched_seq) and clears the pending event.
    fn take_pending_context_switch(&self, conn_id: ConnectionId) -> Option<(u64, u32)> {
        // SAFETY: Single-threaded access, no aliasing
        let pending = unsafe { &mut *self.pending_context_switch.get() };
        if conn_id < pending.len() {
            pending[conn_id].take()
        } else {
            None
        }
    }

    /// Notify clients of context switch.
    ///
    /// Sends context switch events to all clients in the old active group.
    /// This method sends separate RDMA WRITEs for backward compatibility.
    /// For better efficiency, use queue_context_switch_for_piggyback() to piggyback
    /// events on responses.
    pub fn notify_context_switch(&self, conn_ids: &[ConnectionId], sequence: u64, fetched_seq: u32) -> Result<()> {
        // Queue for piggybacking on responses (primary method)
        self.queue_context_switch_for_piggyback(conn_ids, sequence, fetched_seq);

        // Also send via event buffer for backward compatibility and to handle
        // cases where there are no pending responses.
        let event = ContextSwitchEvent::new(sequence, fetched_seq);

        for &conn_id in conn_ids {
            if let Some(Some((addr, rkey))) = self.event_buffer_addrs.get(conn_id) {
                if let Some(Some(conn)) = self.connections.get(conn_id) {
                    // Send event via RDMA WRITE
                    let slot = self.processing_pool.alloc()?;
                    let slot_data_ptr = slot.slot().data_ptr();

                    unsafe {
                        event.write_to(slot_data_ptr);
                    }

                    {
                        let qp_ref = conn.qp().borrow();
                        let emit_ctx = qp_ref
                            .emit_ctx()
                            .map_err(|e| Error::Protocol(format!("emit_ctx failed: {:?}", e)))?;

                        emit_wqe!(&emit_ctx, write {
                            flags: WqeFlags::COMPLETION,
                            remote_addr: *addr,
                            rkey: *rkey,
                            sge: {
                                addr: slot.data_addr(),
                                len: ContextSwitchEvent::SIZE as u32,
                                lkey: self.processing_pool.lkey()
                            },
                            signaled: sequence,
                        })
                        .map_err(|e| Error::Protocol(format!("emit_wqe failed: {:?}", e)))?;
                    }
                    conn.ring_sq_doorbell();
                }
            }
        }

        Ok(())
    }

    /// Reset next_expected_slot for connections entering Process mode.
    ///
    /// Called after context switch when clients transition to Process mode.
    /// Clients start writing from their first slot, so we reset to match.
    /// Note: next_expected_slot stores position index (0..n-1), not slot value.
    fn reset_next_expected_slots(&self, conn_ids: &[ConnectionId]) {
        // SAFETY: Single-threaded access, no aliasing
        let next_slots = unsafe { &mut *self.next_expected_slot.get() };
        for &conn_id in conn_ids {
            if self.mapping.get_connection(conn_id).is_some() {
                if conn_id < next_slots.len() {
                    next_slots[conn_id] = 0; // Reset to first position
                }
            }
        }
    }


    /// Add a new connection to the server.
    ///
    /// # Arguments
    /// * `ctx` - Device context
    /// * `pd` - Protection domain
    /// * `port` - Local port number
    ///
    /// # Returns
    /// Connection ID for the new connection.
    pub fn add_connection(&mut self, ctx: &Context, pd: &Pd, port: u8) -> Result<ConnectionId> {
        let conn_id = self.connections.len();

        // Check max_connections limit
        if conn_id >= self.config.max_connections {
            return Err(Error::Protocol(format!(
                "max_connections limit ({}) reached",
                self.config.max_connections
            )));
        }

        // Create shared CQs on first connection
        if self.shared_send_cq.is_none() {
            // Size CQ to handle all connections: max_connections * 2048 WQEs each (deep pipelines)
            let cq_size = self.config.max_connections * 2048;
            let (send_cq, recv_cq) = create_shared_cqs(ctx, cq_size)?;
            self.shared_send_cq = Some(send_cq);
            self.shared_recv_cq = Some(recv_cq);
        }

        fn sq_callback(_cqe: Cqe, _entry: u64) {}
        fn rq_callback(_cqe: Cqe, _entry: u64) {}

        let send_cq = self.shared_send_cq.as_ref().unwrap().clone();
        let recv_cq = self.shared_recv_cq.as_ref().unwrap().clone();
        let conn = Connection::new(ctx, pd, conn_id, port, send_cq, recv_cq, sq_callback, rq_callback)?;

        self.mapping.register_connection(conn_id);

        // Virtualized Mapping: allocate fixed slot range for this connection
        let slots_per_conn = self.config.pool.num_slots / self.config.max_connections;
        let start_slot = conn_id * slots_per_conn;
        for i in 0..slots_per_conn {
            self.mapping.bind_slot(conn_id, start_slot + i)?;
        }

        self.connections.push(Some(conn));

        // Add to scheduler
        self.scheduler.add_connection(conn_id);

        // Initialize ring buffer polling state (stores position index, not slot value)
        // SAFETY: Single-threaded access, no aliasing
        unsafe {
            let next_slots = &mut *self.next_expected_slot.get();
            if conn_id >= next_slots.len() {
                next_slots.resize(conn_id + 1, 0);
            }
            next_slots[conn_id] = 0; // Start at first position (index 0 in entry.slots)
        }

        Ok(conn_id)
    }

    /// Connect to a client endpoint.
    ///
    /// # Arguments
    /// * `conn_id` - Local connection ID
    /// * `remote` - Remote (client) endpoint information
    ///
    /// When the remote provides event_buffer_addr/rkey, they are automatically
    /// registered for context switch notification.
    /// When the remote provides warmup_buffer_addr/rkey/slots, they are
    /// stored for RDMA READ during warmup phase.
    pub fn connect(&mut self, conn_id: ConnectionId, remote: RemoteEndpoint) -> Result<()> {
        // Store client slot info in mapping
        self.mapping
            .set_remote_slot(conn_id, remote.slot_addr, remote.slot_rkey)?;

        // Auto-register event buffer if provided
        if remote.event_buffer_addr != 0 {
            self.register_event_buffer(conn_id, remote.event_buffer_addr, remote.event_buffer_rkey);
        }

        // Store warmup buffer info if provided
        if remote.warmup_buffer_addr != 0 {
            while self.warmup_buffer_infos.len() <= conn_id {
                self.warmup_buffer_infos.push(None);
            }
            self.warmup_buffer_infos[conn_id] = Some(WarmupBufferInfo {
                addr: remote.warmup_buffer_addr,
                rkey: remote.warmup_buffer_rkey,
                slots: remote.warmup_buffer_slots,
            });
        }

        // Get connection and connect
        let conn = self
            .connections
            .get_mut(conn_id)
            .and_then(|c| c.as_mut())
            .ok_or(Error::ConnectionNotFound(conn_id))?;

        conn.connect(remote)?;
        Ok(())
    }

    /// Get local endpoint information for a connection.
    ///
    /// Note: slot_addr points to slot 0's data area (after slot header).
    /// endpoint_entry_addr/rkey are populated for client to RDMA WRITE warmup notification.
    /// server_conn_id is set so clients can use it as sender_conn_id in requests.
    pub fn local_endpoint(&self, conn_id: ConnectionId) -> Result<RemoteEndpoint> {
        let conn = self
            .connections
            .get(conn_id)
            .and_then(|c| c.as_ref())
            .ok_or(Error::ConnectionNotFound(conn_id))?;

        let mut endpoint = conn.local_endpoint();
        // Point to slot 0's data area (not the slot header)
        endpoint.slot_addr = self.processing_pool.slot_data_addr(0).unwrap();
        endpoint.slot_rkey = self.processing_pool.rkey();

        // Add endpoint entry info for client to RDMA WRITE warmup notification
        if let Some(ref entries) = self.endpoint_entries {
            endpoint.endpoint_entry_addr = entries.addr(conn_id);
            endpoint.endpoint_entry_rkey = entries.rkey();
        }

        // Set server_conn_id so client can use it as sender_conn_id
        endpoint.server_conn_id = conn_id as u32;

        // Set pool_num_slots so client knows how many server slots to cycle through
        endpoint.pool_num_slots = self.processing_pool.num_slots() as u32;

        Ok(endpoint)
    }

    /// Get the message pool (processing pool).
    pub fn pool(&self) -> &MessagePool {
        &self.processing_pool
    }

    /// Get the processing pool.
    pub fn processing_pool(&self) -> &MessagePool {
        &self.processing_pool
    }

    /// Get the warmup pool.
    pub fn warmup_pool(&self) -> &MessagePool {
        &self.warmup_pool
    }

    /// Poll for incoming requests.
    ///
    /// Scans only the slots owned by connections in the current active group.
    /// This is a key optimization from the ScaleRPC paper - instead of scanning
    /// all 1024 slots, we only scan slots belonging to active connections.
    ///
    /// For example, with 1024 slots and 64 max connections:
    /// - Each connection owns 16 slots
    /// - With 1 active connection, we scan 16 slots instead of 1024
    /// - Expected improvement: ~60x reduction in memory accesses
    ///
    /// Uses ring buffer polling: only checks next_expected_slot per connection.
    /// This assumes clients write to slots in order (which they do via direct_slot_index).
    pub fn poll_request(&self) -> Option<IncomingRequest> {
        // Get active group's connections
        let active_conns = self.scheduler.current_group_connections();

        // Ring buffer polling: check only the next expected slot per connection.
        // This is a key optimization: instead of scanning all slots (O(n)),
        // we check only one slot per connection (O(1) per connection).
        //
        // Synchronization: Client's direct_slot_index and server's next_expected_slot
        // must be in sync. This is achieved by:
        // 1. Server resets next_expected_slot on context switch (warmup fetch)
        // 2. Client sets direct_slot_index = warmup_request_count on Process mode entry
        // This accounts for warmup requests that were processed before Process mode starts.
        // SAFETY: Single-threaded access, no aliasing
        let next_slots = unsafe { &mut *self.next_expected_slot.get() };
        for &conn_id in active_conns {
            if let Some(entry) = self.mapping.get_connection(conn_id) {
                if entry.slots.is_empty() {
                    continue;
                }

                // Get next expected slot position (index into entry.slots array)
                let slot_pos = next_slots.get(conn_id).copied().unwrap_or(0) % entry.slots.len();
                let next_slot = entry.slots[slot_pos];

                if let Some(request) = self.check_slot_for_request(next_slot) {
                    // Advance to next position in ring buffer - O(1)
                    let next_pos = (slot_pos + 1) % entry.slots.len();
                    if conn_id < next_slots.len() {
                        next_slots[conn_id] = next_pos;
                    }
                    return Some(request);
                }

                // Fallback: if ring buffer is out of sync, scan all slots
                // This can happen during high-throughput scenarios where client and server
                // slot indices diverge due to timing
                for (idx, &slot) in entry.slots.iter().enumerate() {
                    if idx != slot_pos {
                        if let Some(request) = self.check_slot_for_request(slot) {
                            // Resync ring buffer - advance to next position
                            let next_pos = (idx + 1) % entry.slots.len();
                            if conn_id < next_slots.len() {
                                next_slots[conn_id] = next_pos;
                            }
                            return Some(request);
                        }
                    }
                }
            }
        }

        None
    }

    /// Check a specific slot for an incoming request using the Valid field.
    fn check_slot_for_request(&self, slot_index: usize) -> Option<IncomingRequest> {
        let slot = self.processing_pool.get_slot(slot_index)?;
        let slot_ptr = slot.data_ptr();

        // Read trailer (Valid field at end of block) - this is the poll target
        let (msg_len, valid) = unsafe { MessageTrailer::read_from(slot_ptr) };

        if valid == 0 || msg_len == 0 {
            return None;
        }

        // Memory fence to ensure we see the complete message
        std::sync::atomic::fence(std::sync::atomic::Ordering::Acquire);

        // Read the full request header
        let header = unsafe { RequestHeader::read_from(slot_ptr) };

        // Zero-copy: get pointer to payload in processing pool
        // Response will be built in separate response_pool, so no overlap
        let payload_len = header.payload_len as usize;
        let payload_ptr = unsafe { slot_ptr.add(RequestHeader::SIZE) };

        // Clear the valid flag to mark as processed
        unsafe {
            let trailer_offset = MESSAGE_BLOCK_SIZE - 4;
            let valid_ptr = slot_ptr.add(trailer_offset) as *mut u32;
            std::ptr::write_volatile(valid_ptr, 0);
        }

        // Use sender_conn_id from the request header for multi-QP routing
        let conn_id = { header.sender_conn_id } as usize;
        let slot_seq = { header.slot_seq };

        Some(IncomingRequest {
            req_id: header.req_id,
            rpc_type: header.rpc_type,
            payload_ptr,
            payload_len,
            client_slot_addr: header.client_slot_addr,
            client_slot_rkey: header.client_slot_rkey,
            conn_id,
            slot_index,
            slot_seq,
        })
    }

    /// Poll for incoming requests using the new message trailer format.
    ///
    /// Checks the Valid field in the message trailer for new messages.
    #[deprecated(note = "Use poll_request() instead, which now uses Valid field")]
    pub fn poll_request_trailer(&self) -> Option<IncomingRequest> {
        self.poll_request()
    }

    /// Send a response for a request.
    ///
    /// # Arguments
    /// * `request` - The original request
    /// * `status` - Response status code
    /// * `payload` - Response payload data
    ///
    /// Per the ScaleRPC paper, response is built in the response_pool (separate from
    /// processing_pool) and then RDMA WRITEd to the client. This enables zero-copy
    /// request handling since request payload remains valid in processing_pool.
    ///
    /// If a context switch event is pending for this connection, it will be
    /// piggybacked on the response (ScaleRPC optimization).
    ///
    /// Credit-based flow control: The response includes acked_slot_seq to acknowledge
    /// the highest slot sequence processed for this connection.
    pub fn send_response(
        &self,
        request: &IncomingRequest,
        status: u32,
        payload: &[u8],
    ) -> Result<()> {
        // Get the connection
        let conn = self
            .connections
            .get(request.conn_id)
            .and_then(|c| c.as_ref())
            .ok_or(Error::ConnectionNotFound(request.conn_id))?;

        // Use response_pool (separate from processing_pool) for building response
        let slot = self
            .response_pool
            .get_slot(request.slot_index)
            .ok_or(Error::Protocol(format!(
                "response slot {} not found",
                request.slot_index
            )))?;
        let slot_data_addr = self
            .response_pool
            .slot_data_addr(request.slot_index)
            .ok_or(Error::Protocol(format!(
                "response slot {} addr not found",
                request.slot_index
            )))?;

        // Update last_processed_seq for credit-based flow control
        // SAFETY: Single-threaded access, no aliasing
        let acked_seq = unsafe {
            let seqs = &mut *self.last_processed_seq.get();
            while seqs.len() <= request.conn_id {
                seqs.push(0);
            }
            // Update if this request's slot_seq is greater
            if request.slot_seq > seqs[request.conn_id] {
                seqs[request.conn_id] = request.slot_seq;
            }
            seqs[request.conn_id]
        };

        // Check for pending context switch event to piggyback
        let response_header = if let Some((seq, fetched_seq)) = self.take_pending_context_switch(request.conn_id) {
            if fetched_seq > 0 {
                // Warmup fetch complete → include processing pool info for Process mode transition
                ResponseHeader::with_processing_pool_info_and_credit_ack(
                    request.req_id,
                    status,
                    payload.len() as u32,
                    seq,
                    fetched_seq,
                    self.processing_pool.slot_data_addr(0).unwrap_or(0),
                    self.processing_pool.rkey(),
                    acked_seq,
                )
            } else {
                // Time-based context switch (Idle transition)
                ResponseHeader::with_context_switch_and_credit_ack(
                    request.req_id,
                    status,
                    payload.len() as u32,
                    seq,
                    fetched_seq,
                    acked_seq,
                )
            }
        } else {
            ResponseHeader::with_credit_ack(request.req_id, status, payload.len() as u32, acked_seq)
        };

        // Write response to response_pool slot
        let slot_data_ptr = slot.data_ptr();
        unsafe {
            response_header.write_to(slot_data_ptr);
            if !payload.is_empty() {
                std::ptr::copy_nonoverlapping(
                    payload.as_ptr(),
                    slot_data_ptr.add(ResponseHeader::SIZE),
                    payload.len(),
                );
            }
        }

        // Send response via RDMA WRITE to client
        let total_len = ResponseHeader::SIZE + payload.len();
        {
            let qp_ref = conn.qp().borrow();
            let emit_ctx = qp_ref
                .emit_ctx()
                .map_err(|e| Error::Protocol(format!("emit_ctx failed: {:?}", e)))?;

            emit_wqe!(&emit_ctx, write {
                flags: WqeFlags::COMPLETION,
                remote_addr: request.client_slot_addr,
                rkey: request.client_slot_rkey,
                sge: {
                    addr: slot_data_addr,
                    len: total_len as u32,
                    lkey: self.response_pool.lkey()
                },
                signaled: request.req_id,
            })
            .map_err(|e| Error::Protocol(format!("emit_wqe failed: {:?}", e)))?;
        }

        // Mark connection as needing doorbell flush (batched in poll())
        self.mark_needs_flush(request.conn_id);

        Ok(())
    }

    /// Send a response for a request (zero-copy version).
    ///
    /// The response payload is assumed to already be written in the slot
    /// (after ResponseHeader::SIZE offset). Only the header is written here.
    ///
    /// If a context switch event is pending for this connection, it will be
    /// piggybacked on the response (ScaleRPC optimization).
    ///
    /// Credit-based flow control: The response includes acked_slot_seq to acknowledge
    /// the highest slot sequence processed for this connection.
    ///
    /// # Arguments
    /// * `request` - The original request
    /// * `status` - Response status code
    /// * `payload_len` - Length of response payload already in slot
    fn send_response_zero_copy(
        &self,
        request: &IncomingRequest,
        status: u32,
        payload_len: usize,
    ) -> Result<()> {
        // Get the connection
        let conn = self
            .connections
            .get(request.conn_id)
            .and_then(|c| c.as_ref())
            .ok_or(Error::ConnectionNotFound(request.conn_id))?;

        // Use response_pool slot (payload already written by caller)
        let slot = self
            .response_pool
            .get_slot(request.slot_index)
            .ok_or(Error::Protocol(format!(
                "response slot {} not found",
                request.slot_index
            )))?;
        let slot_data_addr = self
            .response_pool
            .slot_data_addr(request.slot_index)
            .ok_or(Error::Protocol(format!(
                "response slot {} addr not found",
                request.slot_index
            )))?;

        // Update last_processed_seq for credit-based flow control
        // SAFETY: Single-threaded access, no aliasing
        let acked_seq = unsafe {
            let seqs = &mut *self.last_processed_seq.get();
            while seqs.len() <= request.conn_id {
                seqs.push(0);
            }
            // Update if this request's slot_seq is greater
            if request.slot_seq > seqs[request.conn_id] {
                seqs[request.conn_id] = request.slot_seq;
            }
            seqs[request.conn_id]
        };

        // Check for pending context switch event to piggyback
        let response_header = if let Some((seq, fetched_seq)) = self.take_pending_context_switch(request.conn_id) {
            if fetched_seq > 0 {
                // Warmup fetch complete → include processing pool info for Process mode transition
                ResponseHeader::with_processing_pool_info_and_credit_ack(
                    request.req_id,
                    status,
                    payload_len as u32,
                    seq,
                    fetched_seq,
                    self.processing_pool.slot_data_addr(0).unwrap_or(0),
                    self.processing_pool.rkey(),
                    acked_seq,
                )
            } else {
                // Time-based context switch (Idle transition)
                ResponseHeader::with_context_switch_and_credit_ack(
                    request.req_id,
                    status,
                    payload_len as u32,
                    seq,
                    fetched_seq,
                    acked_seq,
                )
            }
        } else {
            ResponseHeader::with_credit_ack(request.req_id, status, payload_len as u32, acked_seq)
        };

        unsafe {
            response_header.write_to(slot.data_ptr());
        }

        // Send response via RDMA WRITE to client
        let total_len = ResponseHeader::SIZE + payload_len;
        {
            let qp_ref = conn.qp().borrow();
            let emit_ctx = qp_ref
                .emit_ctx()
                .map_err(|e| Error::Protocol(format!("emit_ctx failed: {:?}", e)))?;

            emit_wqe!(&emit_ctx, write {
                flags: WqeFlags::COMPLETION,
                remote_addr: request.client_slot_addr,
                rkey: request.client_slot_rkey,
                sge: {
                    addr: slot_data_addr,
                    len: total_len as u32,
                    lkey: self.response_pool.lkey()
                },
                signaled: request.req_id,
            })
            .map_err(|e| Error::Protocol(format!("emit_wqe failed: {:?}", e)))?;
        }

        // Mark connection as needing doorbell flush (batched in poll())
        self.mark_needs_flush(request.conn_id);

        Ok(())
    }

    /// Process incoming requests using the registered handler.
    ///
    /// Polls for requests and processes them with the handler.
    /// Flushes doorbells and drains CQs after processing.
    /// Automatically performs context switch when time slice expires.
    /// Returns the number of requests processed.
    pub fn process(&self) -> usize {
        let mut processed = 0;

        while let Some(request) = self.poll_request() {
            // Get the response_pool slot for zero-copy response building
            let slot = match self.response_pool.get_slot(request.slot_index) {
                Some(s) => s,
                None => continue,
            };

            // Get response buffer in response_pool (after ResponseHeader)
            let slot_ptr = slot.data_ptr();
            let response_buf = unsafe {
                let buf_ptr = slot_ptr.add(ResponseHeader::SIZE);
                let buf_len = MESSAGE_BLOCK_SIZE - ResponseHeader::SIZE - 8; // -8 for trailer
                std::slice::from_raw_parts_mut(buf_ptr, buf_len)
            };

            // Call handler with request payload (zero-copy from processing_pool)
            // Uses static dispatch via RequestHandler trait for inlining
            let (status, response_len) = self.handler.handle(request.rpc_type, request.payload(), response_buf);

            // Send response (header only, payload already in response_pool slot)
            if let Err(e) = self.send_response_zero_copy(&request, status, response_len) {
                eprintln!("Failed to send response: {}", e);
            }

            processed += 1;
        }

        // Batch flush doorbells and drain CQs
        self.flush_doorbells();
        // SAFETY: Single-threaded access, no aliasing
        unsafe { (*self.needs_flush.get()).clear() };
        self.poll_cqs();

        // Check context switch after processing batch (amortize overhead)
        if self.scheduler.should_switch() {
            self.scheduler.do_context_switch();
        }

        processed
    }

    /// Poll shared CQs.
    ///
    /// All connections share the same CQs, so we only need to poll twice
    /// regardless of the number of connections.
    pub fn poll_cqs(&self) -> usize {
        let mut total = 0;
        if let Some(cq) = &self.shared_send_cq {
            total += cq.poll();
            cq.flush();
        }
        if let Some(cq) = &self.shared_recv_cq {
            total += cq.poll();
            cq.flush();
        }
        total
    }

    /// Mark a connection as needing doorbell flush.
    ///
    /// The doorbell will be batched and issued in the next `poll()` call.
    fn mark_needs_flush(&self, conn_id: ConnectionId) {
        // SAFETY: Single-threaded access, no aliasing
        let needs_flush = unsafe { &mut *self.needs_flush.get() };
        if !needs_flush.contains(&conn_id) {
            needs_flush.push(conn_id);
        }
    }

    /// Flush all pending doorbells.
    ///
    /// Issues doorbells for all connections with pending WQEs.
    /// This can be called directly to flush without draining CQs.
    pub fn flush_doorbells(&self) {
        // SAFETY: Single-threaded access, no aliasing
        let needs_flush = unsafe { &*self.needs_flush.get() };
        for &conn_id in needs_flush.iter() {
            if let Some(Some(conn)) = self.connections.get(conn_id) {
                conn.ring_sq_doorbell();
            }
        }
    }

    /// Clear the needs_flush list.
    pub fn clear_needs_flush(&self) {
        // SAFETY: Single-threaded access, no aliasing
        unsafe { (*self.needs_flush.get()).clear() };
    }

    /// Receive the next incoming request.
    ///
    /// This is the main entry point for receiving requests in the simplified API.
    /// Returns `Some(IncomingRequest)` if a request is available, `None` otherwise.
    pub fn recv(&self) -> Option<IncomingRequest> {
        self.poll_request()
    }

    /// Send a reply to a request.
    ///
    /// This is the main entry point for sending responses in the simplified API.
    ///
    /// # Arguments
    /// * `request` - The original request to reply to
    /// * `status` - Response status code (0 = success)
    /// * `payload` - Response payload data
    pub fn reply(&self, request: &IncomingRequest, status: u32, payload: &[u8]) -> Result<()> {
        self.send_response(request, status, payload)
    }

    /// Reply to a request (zero-copy version).
    ///
    /// The response payload is assumed to already be written in the slot.
    pub fn reply_zero_copy(&self, request: &IncomingRequest, status: u32, payload_len: usize) -> Result<()> {
        self.send_response_zero_copy(request, status, payload_len)
    }

    /// Main server loop - one iteration.
    ///
    /// This is the simplified API for running the server. It handles:
    /// - Flushing pending doorbells (batch doorbell)
    /// - Draining CQs
    /// - Fetching warmup requests (BEFORE context switch)
    /// - Context switch (time-sharing scheduler)
    /// - Processing incoming requests (if handler is set)
    ///
    /// The order is important: fetch warmup requests FIRST, then context switch.
    /// This way, fetched requests are in warmup_pool, and after context switch,
    /// warmup_pool becomes processing_pool where recv() can find them.
    ///
    /// Returns the number of CQ entries processed (or requests if handler is set).
    pub fn poll(&mut self) -> usize {
        // 1. Flush all pending doorbells
        self.flush_doorbells();
        // SAFETY: Single-threaded access, no aliasing
        unsafe { (*self.needs_flush.get()).clear() };

        // 2. Drain all CQs
        let cq_processed = self.poll_cqs();

        // 3. Fetch warmup requests from next group (BEFORE context switch)
        //    This puts requests into warmup_pool
        let (fetched, fetched_seq) = self.fetch_warmup_requests().unwrap_or((0, 0));

        // 4. Check for context switch (swaps warmup_pool <-> processing_pool)
        //    After this, the warmup_pool (with fetched requests) becomes processing_pool
        //    If we fetched requests, force a context switch to move them to processing_pool
        let _did_switch = if fetched > 0 {
            // Force context switch to make fetched requests available for processing
            let seq = self.scheduler.do_context_switch();
            // SAFETY: Single-threaded access, no aliasing within this block
            unsafe {
                let conn_buf = &mut *self.conn_buf.get();
                conn_buf.clear();
                conn_buf.extend_from_slice(self.scheduler.current_group_connections());
            }
            std::mem::swap(&mut self.processing_pool, &mut self.warmup_pool);
            self.clear_pool_valid_flags(&self.warmup_pool);
            // Reset ring buffer pointers for new Process mode session
            // SAFETY: Single-threaded access
            self.reset_next_expected_slots(unsafe { &*self.conn_buf.get() });
            let _ = self.notify_context_switch(unsafe { &*self.conn_buf.get() }, seq, fetched_seq);
            true
        } else if let Ok(Some(seq)) = self.perform_context_switch() {
            // SAFETY: Single-threaded access
            let _ = self.notify_context_switch(unsafe { &*self.conn_buf.get() }, seq, 0);
            true
        } else {
            false
        };

        // 5. Process requests using the handler
        //    recv() scans processing_pool (which was warmup_pool before context switch)
        let mut requests_processed = 0;
        while let Some(request) = self.recv() {
            // Get the response_pool slot for zero-copy response building
            let slot = match self.response_pool.get_slot(request.slot_index) {
                Some(s) => s,
                None => continue,
            };

            // Get response buffer in response_pool
            let slot_ptr = slot.data_ptr();
            let response_buf = unsafe {
                let buf_ptr = slot_ptr.add(ResponseHeader::SIZE);
                let buf_len = MESSAGE_BLOCK_SIZE - ResponseHeader::SIZE - 8;
                std::slice::from_raw_parts_mut(buf_ptr, buf_len)
            };

            // Uses static dispatch via RequestHandler trait for inlining
            let (status, response_len) = self.handler.handle(request.rpc_type, request.payload(), response_buf);
            if self.reply_zero_copy(&request, status, response_len).is_ok() {
                requests_processed += 1;
            }
        }

        // 6. Flush doorbells for responses
        self.flush_doorbells();
        // SAFETY: Single-threaded access, no aliasing
        unsafe { (*self.needs_flush.get()).clear() };

        // 7. Drain CQs to ensure response WRITEs complete before next poll iteration
        // This prevents WRITE completions from being counted as READ completions in fetch_warmup_batch
        if requests_processed > 0 {
            // Only drain if we sent responses (have outstanding WRITEs)
            loop {
                let n = self.poll_cqs();
                if n == 0 {
                    break;
                }
            }
        }

        if requests_processed > 0 {
            requests_processed
        } else {
            cq_processed
        }
    }

    /// Fetch warmup requests from clients via RDMA READ.
    ///
    /// Uses the endpoint entry mechanism: client writes <addr, batch_size, seq>
    /// to notify the server, and the server reads only batch_size requests.
    /// This avoids wasted bandwidth from reading empty slots.
    ///
    /// Returns (number of requests fetched, last fetched endpoint entry seq).
    pub fn fetch_warmup_requests(&mut self) -> Result<(usize, u32)> {
        let mut fetched = 0;
        let mut last_seq = 0u32;

        // Copy connections to reusable buffer (no allocation)
        // SAFETY: Single-threaded access, no aliasing within this block
        unsafe {
            let conn_buf = &mut *self.conn_buf.get();
            conn_buf.clear();
            conn_buf.extend_from_slice(self.scheduler.next_group_connections());
        }

        // Iterate by index to avoid borrow conflicts
        // SAFETY: Single-threaded access
        let num_conns = unsafe { (&*self.conn_buf.get()).len() };
        for i in 0..num_conns {
            // SAFETY: Single-threaded access
            let conn_id = unsafe { (&*self.conn_buf.get())[i] };
            // Check if this connection has a new batch via endpoint entry
            let entry_opt = self.endpoint_entries.as_mut().and_then(|e| e.has_new_batch(conn_id));
            let entry = match entry_opt {
                Some(e) => e,
                None => continue,
            };

            let warmup_info = match self.warmup_buffer_infos.get(conn_id).and_then(|w| w.as_ref()) {
                Some(info) => *info,
                None => continue,
            };

            let conn = match self.connections.get(conn_id).and_then(|c| c.as_ref()) {
                Some(c) => c,
                None => continue,
            };

            // Fetch all batch_size slots at once (issue all RDMA READs, then wait)
            let batch_size = entry.batch_size.min(warmup_info.slots);
            let batch_fetched = self.fetch_warmup_batch(conn, conn_id, &warmup_info, batch_size)?;
            fetched += batch_fetched;
            last_seq = entry.seq;

            // Clear the endpoint entry to indicate processed
            if let Some(ref entries) = self.endpoint_entries {
                entries.clear_entry(conn_id);
            }
        }

        Ok((fetched, last_seq))
    }

    /// Fetch a batch of warmup requests at once.
    ///
    /// Issues all RDMA READs first, then waits for all completions.
    /// This is more efficient than waiting for each READ individually.
    fn fetch_warmup_batch(
        &self,
        conn: &Connection,
        conn_id: ConnectionId,
        warmup_info: &WarmupBufferInfo,
        batch_size: u32,
    ) -> Result<usize> {
        if batch_size == 0 {
            return Ok(0);
        }

        // Drain any stale completions from previous operations before issuing new READs
        // This prevents counting old WRITE completions as READ completions
        loop {
            let n = self.poll_cqs();
            if n == 0 {
                break;
            }
        }

        // Issue all RDMA READs
        for slot_idx in 0..batch_size {
            self.issue_warmup_read(conn, conn_id, warmup_info, slot_idx)?;
        }

        // Ring doorbell once for all WQEs
        conn.ring_sq_doorbell();

        // Wait for all completions - now we know all completions are from our READs
        let start = Instant::now();
        let timeout = std::time::Duration::from_millis(100);
        let mut completions = 0u32;

        while completions < batch_size {
            let n = self.poll_cqs();
            if n > 0 {
                completions += n as u32;
            }
            if start.elapsed() > timeout {
                return Err(Error::Protocol(format!(
                    "RDMA READ batch timeout: expected {}, got {}",
                    batch_size, completions
                )));
            }
            std::hint::spin_loop();
        }

        Ok(batch_size as usize)
    }

    /// Issue a single RDMA READ for a warmup slot (without waiting).
    fn issue_warmup_read(
        &self,
        conn: &Connection,
        conn_id: ConnectionId,
        warmup_info: &WarmupBufferInfo,
        slot_idx: u32,
    ) -> Result<()> {
        // Get the server slot index from the mapping
        let server_slot_idx = self
            .mapping
            .get_connection(conn_id)
            .and_then(|entry| entry.slots.get(slot_idx as usize).copied())
            .ok_or(Error::Protocol(format!(
                "No mapping slot for conn_id={}, slot_idx={}",
                conn_id, slot_idx
            )))?;

        let slot_data_addr = self
            .warmup_pool
            .slot_data_addr(server_slot_idx)
            .ok_or(Error::Protocol(format!(
                "Warmup pool slot {} addr not found",
                server_slot_idx
            )))?;

        // Calculate source address (client's warmup buffer + slot offset)
        let src_addr = warmup_info.addr + (slot_idx as u64 * MESSAGE_BLOCK_SIZE as u64);

        // Issue RDMA READ (don't ring doorbell yet)
        {
            let qp_ref = conn.qp().borrow();
            let emit_ctx = qp_ref
                .emit_ctx()
                .map_err(|e| Error::Protocol(format!("emit_ctx failed: {:?}", e)))?;

            emit_wqe!(&emit_ctx, read {
                flags: WqeFlags::COMPLETION,
                remote_addr: src_addr,
                rkey: warmup_info.rkey,
                sge: {
                    addr: slot_data_addr,
                    len: MESSAGE_BLOCK_SIZE as u32,
                    lkey: self.warmup_pool.lkey()
                },
                signaled: (conn_id as u64) << 32 | slot_idx as u64,
            })
            .map_err(|e| Error::Protocol(format!("emit_wqe failed: {:?}", e)))?;
        }

        Ok(())
    }

    /// Process warmup requests that have been fetched into the warmup pool.
    ///
    /// This is called after a context switch when the warmup pool becomes
    /// the processing pool.
    pub fn drain_warmup_pool(&self) -> Vec<IncomingRequest> {
        let mut requests = Vec::new();
        let num_slots = self.warmup_pool.num_slots();

        for slot_index in 0..num_slots {
            if let Some(request) = self.check_warmup_slot(slot_index) {
                requests.push(request);
            }
        }

        requests
    }

    /// Check a warmup pool slot for a request (zero-copy).
    fn check_warmup_slot(&self, slot_index: usize) -> Option<IncomingRequest> {
        let slot = self.warmup_pool.get_slot(slot_index)?;
        let data_ptr = slot.data_ptr();

        // Check trailer first
        let (msg_len, valid) = unsafe { MessageTrailer::read_from(data_ptr) };

        if valid == 0 || msg_len == 0 {
            // Fall back to checking magic number
            let header = unsafe { RequestHeader::read_from(data_ptr) };
            if !header.is_valid() {
                return None;
            }

            // Zero-copy: get pointer to payload
            let payload_len = header.payload_len as usize;
            let payload_ptr = unsafe { data_ptr.add(RequestHeader::SIZE) };

            // Clear magic
            unsafe {
                std::ptr::write_volatile(data_ptr as *mut u32, 0);
            }

            // Use sender_conn_id from the request header for multi-QP routing
            let conn_id = { header.sender_conn_id } as usize;
            let slot_seq = { header.slot_seq };

            return Some(IncomingRequest {
                req_id: header.req_id,
                rpc_type: header.rpc_type,
                payload_ptr,
                payload_len,
                client_slot_addr: header.client_slot_addr,
                client_slot_rkey: header.client_slot_rkey,
                conn_id,
                slot_index,
                slot_seq,
            });
        }

        // Valid trailer - read header
        let header = unsafe { RequestHeader::read_from(data_ptr) };

        if !header.is_valid() {
            return None;
        }

        // Zero-copy: get pointer to payload
        let payload_len = header.payload_len as usize;
        let payload_ptr = unsafe { data_ptr.add(RequestHeader::SIZE) };

        // Clear both magic and valid
        unsafe {
            std::ptr::write_volatile(data_ptr as *mut u32, 0);
            let trailer_offset = MESSAGE_BLOCK_SIZE - 4;
            let valid_ptr = data_ptr.add(trailer_offset) as *mut u32;
            std::ptr::write_volatile(valid_ptr, 0);
        }

        // Use sender_conn_id from the request header for multi-QP routing
        let conn_id = { header.sender_conn_id } as usize;
        let slot_seq = { header.slot_seq };

        Some(IncomingRequest {
            req_id: header.req_id,
            rpc_type: header.rpc_type,
            payload_ptr,
            payload_len,
            client_slot_addr: header.client_slot_addr,
            client_slot_rkey: header.client_slot_rkey,
            conn_id,
            slot_index,
            slot_seq,
        })
    }

}
