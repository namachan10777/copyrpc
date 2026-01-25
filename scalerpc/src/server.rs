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

use std::cell::{Cell, RefCell};
use std::time::Instant;

use mlx5::cq::Cqe;
use mlx5::device::Context;
use mlx5::emit_wqe;
use mlx5::pd::{AccessFlags, MemoryRegion, Pd};
use mlx5::wqe::WqeFlags;

use crate::config::ServerConfig;
use crate::connection::{Connection, ConnectionId, RemoteEndpoint};
use crate::error::{Error, Result};
use crate::mapping::VirtualMapping;
use crate::pool::MessagePool;
use crate::protocol::{
    ContextSwitchEvent, EndpointEntry, MessageTrailer, RequestHeader, ResponseHeader,
    MESSAGE_BLOCK_SIZE,
};

/// Request handler function type.
///
/// Takes RPC type, request payload, and returns (status, response_payload).
pub type RequestHandler = Box<dyn Fn(u16, &[u8]) -> (u32, Vec<u8>)>;

/// Group ID type.
pub type GroupId = usize;

/// Group scheduler state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchedulerState {
    /// Processing requests from the active group.
    Processing,
    /// Transitioning between groups.
    ContextSwitching,
}

/// Endpoint entries buffer for warmup mechanism.
///
/// Holds endpoint entries from clients in the warmup group.
/// The server scans these to know where to RDMA READ requests.
pub struct EndpointEntryBuffer {
    /// Buffer memory.
    buffer: *mut u8,
    /// Buffer size in bytes.
    size: usize,
    /// Memory region.
    mr: MemoryRegion,
    /// Maximum number of entries.
    max_entries: usize,
    /// Number of valid entries.
    num_entries: Cell<usize>,
}

impl EndpointEntryBuffer {
    /// Create a new endpoint entry buffer.
    pub fn new(pd: &Pd, max_entries: usize) -> Result<Self> {
        let size = max_entries * EndpointEntry::SIZE;

        let buffer = unsafe {
            let mut ptr: *mut std::ffi::c_void = std::ptr::null_mut();
            let ret = libc::posix_memalign(&mut ptr, 64, size);
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
            size,
            mr,
            max_entries,
            num_entries: Cell::new(0),
        })
    }

    /// Get the base address.
    pub fn addr(&self) -> u64 {
        self.buffer as u64
    }

    /// Get the rkey.
    pub fn rkey(&self) -> u32 {
        self.mr.rkey()
    }

    /// Get the lkey.
    pub fn lkey(&self) -> u32 {
        self.mr.lkey()
    }

    /// Get the address for a specific entry slot.
    pub fn entry_addr(&self, index: usize) -> Option<u64> {
        if index >= self.max_entries {
            return None;
        }
        Some(self.buffer as u64 + (index * EndpointEntry::SIZE) as u64)
    }

    /// Read an entry at the given index.
    pub fn read_entry(&self, index: usize) -> Option<EndpointEntry> {
        if index >= self.max_entries {
            return None;
        }
        let entry_ptr = unsafe { self.buffer.add(index * EndpointEntry::SIZE) };
        let entry = unsafe { std::ptr::read_unaligned(entry_ptr as *const EndpointEntry) };
        Some(entry)
    }

    /// Scan all valid entries.
    ///
    /// Returns entries that have batch_size > 0.
    pub fn scan_valid_entries(&self) -> Vec<EndpointEntry> {
        let mut entries = Vec::new();
        for i in 0..self.max_entries {
            if let Some(entry) = self.read_entry(i) {
                if entry.is_valid() {
                    entries.push(entry);
                }
            }
        }
        self.num_entries.set(entries.len());
        entries
    }

    /// Clear all entries.
    pub fn clear(&self) {
        unsafe {
            std::ptr::write_bytes(self.buffer, 0, self.size);
        }
        self.num_entries.set(0);
    }

    /// Get the maximum number of entries.
    pub fn max_entries(&self) -> usize {
        self.max_entries
    }
}

impl Drop for EndpointEntryBuffer {
    fn drop(&mut self) {
        unsafe {
            libc::free(self.buffer as *mut std::ffi::c_void);
        }
    }
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
pub struct GroupScheduler {
    /// Group contexts.
    groups: Vec<GroupContext>,
    /// Currently active (processing) group index.
    current_group: usize,
    /// Next group (warmup) index.
    next_group: usize,
    /// Time slice duration in microseconds.
    time_slice_us: u64,
    /// Last context switch time.
    last_switch: Instant,
    /// Current scheduler state.
    state: SchedulerState,
    /// Endpoint entries for warmup clients.
    endpoint_entries: EndpointEntryBuffer,
    /// Context switch event sequence number.
    context_switch_seq: u64,
}

impl GroupScheduler {
    /// Create a new group scheduler.
    pub fn new(pd: &Pd, num_groups: usize, time_slice_us: u64, max_entries: usize) -> Result<Self> {
        let groups = (0..num_groups).map(GroupContext::new).collect();
        let endpoint_entries = EndpointEntryBuffer::new(pd, max_entries)?;

        Ok(Self {
            groups,
            current_group: 0,
            next_group: if num_groups > 1 { 1 } else { 0 },
            time_slice_us,
            last_switch: Instant::now(),
            state: SchedulerState::Processing,
            endpoint_entries,
            context_switch_seq: 0,
        })
    }

    /// Get the currently active group ID.
    pub fn current_group_id(&self) -> GroupId {
        self.current_group
    }

    /// Get the next (warmup) group ID.
    pub fn next_group_id(&self) -> GroupId {
        self.next_group
    }

    /// Get the endpoint entries buffer.
    pub fn endpoint_entries(&self) -> &EndpointEntryBuffer {
        &self.endpoint_entries
    }

    /// Check if a context switch is due.
    pub fn should_switch(&self) -> bool {
        self.last_switch.elapsed().as_micros() as u64 >= self.time_slice_us
    }

    /// Get the current scheduler state.
    pub fn state(&self) -> SchedulerState {
        self.state
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
        &self.groups[self.current_group].connections
    }

    /// Get connections in the next (warmup) group.
    pub fn next_group_connections(&self) -> &[ConnectionId] {
        &self.groups[self.next_group].connections
    }

    /// Get endpoint entry address for a connection slot.
    ///
    /// Used by clients to know where to WRITE their endpoint entry.
    pub fn endpoint_entry_addr_for(&self, slot: usize) -> Option<(u64, u32)> {
        self.endpoint_entries
            .entry_addr(slot)
            .map(|addr| (addr, self.endpoint_entries.rkey()))
    }

    /// Scan endpoint entries for pending warmup requests.
    pub fn scan_warmup_entries(&self) -> Vec<EndpointEntry> {
        self.endpoint_entries.scan_valid_entries()
    }

    /// Perform context switch.
    ///
    /// Returns the new context switch sequence number.
    pub fn context_switch(&mut self) -> u64 {
        self.state = SchedulerState::ContextSwitching;

        // Advance to next group
        self.current_group = self.next_group;
        self.next_group = (self.next_group + 1) % self.groups.len();

        // Update sequence number
        self.context_switch_seq += 1;
        self.groups[self.current_group].context_switch_seq = self.context_switch_seq;

        // Clear endpoint entries for new warmup group
        self.endpoint_entries.clear();

        // Update timing
        self.last_switch = Instant::now();
        self.state = SchedulerState::Processing;

        self.context_switch_seq
    }

    /// Get the current context switch sequence number.
    pub fn context_switch_seq(&self) -> u64 {
        self.context_switch_seq
    }

    /// Get the number of groups.
    pub fn num_groups(&self) -> usize {
        self.groups.len()
    }
}

/// Incoming RPC request.
#[derive(Debug)]
pub struct IncomingRequest {
    /// Request ID.
    pub req_id: u64,
    /// RPC type.
    pub rpc_type: u16,
    /// Request payload.
    pub payload: Vec<u8>,
    /// Client's slot address for response.
    pub client_slot_addr: u64,
    /// Client's slot rkey.
    pub client_slot_rkey: u32,
    /// Connection ID this request came from.
    pub conn_id: ConnectionId,
    /// Slot index where request was received.
    pub slot_index: usize,
}

/// RPC Server.
///
/// Manages connections and processes incoming RPC requests.
/// Implements the ScaleRPC time-sharing scheduler when enabled.
pub struct RpcServer {
    /// Processing pool for active group's requests.
    processing_pool: MessagePool,
    /// Warmup pool for next group's requests (fetched via RDMA READ).
    warmup_pool: MessagePool,
    /// Virtual mapping table.
    mapping: VirtualMapping,
    /// Connections indexed by ID.
    connections: Vec<Option<Connection>>,
    /// Configuration.
    #[allow(dead_code)]
    config: ServerConfig,
    /// Request handler.
    handler: Option<RequestHandler>,
    /// Next slot to check for incoming requests (Cell for interior mutability).
    next_check_slot: Cell<usize>,
    /// Group scheduler for time-sharing (None if disabled).
    scheduler: Option<GroupScheduler>,
    /// Per-connection event buffer addresses for context switch notification.
    event_buffer_addrs: Vec<Option<(u64, u32)>>,
    /// Connections that need doorbell flush (for batching).
    needs_flush: RefCell<Vec<ConnectionId>>,
}

impl RpcServer {
    /// Create a new RPC server.
    ///
    /// # Arguments
    /// * `pd` - Protection domain for memory registration
    /// * `config` - Server configuration
    pub fn new(pd: &Pd, config: ServerConfig) -> Result<Self> {
        let processing_pool = MessagePool::new(pd, &config.pool)?;
        let warmup_pool = MessagePool::new(pd, &config.pool)?;
        let mapping = VirtualMapping::new();

        let scheduler = if config.enable_scheduler {
            Some(GroupScheduler::new(
                pd,
                config.group.num_groups,
                config.group.time_slice_us,
                config.group.max_endpoint_entries,
            )?)
        } else {
            None
        };

        Ok(Self {
            processing_pool,
            warmup_pool,
            mapping,
            connections: Vec::new(),
            config,
            handler: None,
            next_check_slot: Cell::new(0),
            scheduler,
            event_buffer_addrs: Vec::new(),
            needs_flush: RefCell::new(Vec::new()),
        })
    }

    /// Get the group scheduler (if enabled).
    pub fn scheduler(&self) -> Option<&GroupScheduler> {
        self.scheduler.as_ref()
    }

    /// Get the group scheduler mutably (if enabled).
    pub fn scheduler_mut(&mut self) -> Option<&mut GroupScheduler> {
        self.scheduler.as_mut()
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
        let scheduler = self.scheduler.as_mut()?;

        if !scheduler.should_switch() {
            return None;
        }

        // Perform context switch
        let seq = scheduler.context_switch();

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
    /// Returns (sequence_number, old_group_connections) if a switch occurred.
    pub fn perform_context_switch(&mut self) -> Result<Option<(u64, Vec<ConnectionId>)>> {
        let scheduler = match self.scheduler.as_mut() {
            Some(s) => s,
            None => return Ok(None),
        };

        if !scheduler.should_switch() {
            return Ok(None);
        }

        // Step 1: Get connections in the current (soon to be old) group
        let old_group_connections: Vec<ConnectionId> =
            scheduler.current_group_connections().to_vec();

        // Step 2: Perform the actual context switch in scheduler
        let seq = scheduler.context_switch();

        // Step 3: Swap pools
        std::mem::swap(&mut self.processing_pool, &mut self.warmup_pool);

        // Step 4: Clear the new warmup pool for incoming requests
        self.clear_pool_valid_flags(&self.warmup_pool);

        Ok(Some((seq, old_group_connections)))
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
        let handler = match &self.handler {
            Some(h) => h,
            None => return 0,
        };

        let mut processed = 0;

        for request in requests {
            let (status, response_payload) = handler(request.rpc_type, &request.payload);

            if let Err(e) = self.send_response(&request, status, &response_payload) {
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
        if let Some((seq, old_connections)) = self.perform_context_switch()? {
            // Notify old group's clients
            if let Err(e) = self.notify_context_switch(&old_connections, seq) {
                eprintln!("Failed to notify context switch: {}", e);
            }
        }

        // Process requests from the processing pool
        total_processed += self.process();

        // Fetch warmup requests from next group (best effort)
        let _ = self.fetch_warmup_requests();

        Ok(total_processed)
    }

    /// Notify clients of context switch.
    ///
    /// Sends context switch events to all clients in the old active group.
    pub fn notify_context_switch(&self, conn_ids: &[ConnectionId], sequence: u64) -> Result<()> {
        let event = ContextSwitchEvent::new(sequence);

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

    /// Set the request handler.
    pub fn set_handler<F>(&mut self, handler: F)
    where
        F: Fn(u16, &[u8]) -> (u32, Vec<u8>) + 'static,
    {
        self.handler = Some(Box::new(handler));
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

        fn sq_callback(_cqe: Cqe, _entry: u64) {}
        fn rq_callback(_cqe: Cqe, _entry: u64) {}

        let conn = Connection::new(ctx, pd, conn_id, port, sq_callback, rq_callback)?;

        self.mapping.register_connection(conn_id);
        self.connections.push(Some(conn));

        // Add to scheduler if enabled
        if let Some(scheduler) = &mut self.scheduler {
            scheduler.add_connection(conn_id);
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
    pub fn connect(&mut self, conn_id: ConnectionId, remote: RemoteEndpoint) -> Result<()> {
        // Store client slot info in mapping
        self.mapping
            .set_remote_slot(conn_id, remote.slot_addr, remote.slot_rkey)?;

        // Auto-register event buffer if provided
        if remote.event_buffer_addr != 0 {
            self.register_event_buffer(conn_id, remote.event_buffer_addr, remote.event_buffer_rkey);
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
    /// When scheduler is enabled, endpoint_entry_addr/rkey are populated
    /// for client warmup registration.
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

        // If scheduler is enabled, provide endpoint entry address for warmup registration
        if let Some(scheduler) = &self.scheduler {
            if let Some((addr, rkey)) = scheduler.endpoint_entry_addr_for(conn_id) {
                endpoint.endpoint_entry_addr = addr;
                endpoint.endpoint_entry_rkey = rkey;
            }
        }

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
    /// Scans message pool slots for new requests.
    /// Returns the first request found, or None if no requests are pending.
    pub fn poll_request(&self) -> Option<IncomingRequest> {
        let num_slots = self.processing_pool.num_slots();

        // Scan all slots to find any pending request
        for _ in 0..num_slots {
            let slot_index = self.next_check_slot.get();
            self.next_check_slot.set((slot_index + 1) % num_slots);

            if let Some(request) = self.check_slot_for_request(slot_index) {
                return Some(request);
            }
        }

        None
    }

    /// Check a specific slot for an incoming request.
    fn check_slot_for_request(&self, slot_index: usize) -> Option<IncomingRequest> {
        let slot = self.processing_pool.get_slot(slot_index)?;
        let data_ptr = slot.data_ptr();

        // Use volatile read for magic to see RDMA writes from client
        let magic = unsafe { std::ptr::read_volatile(data_ptr as *const u32) };
        if magic != crate::protocol::REQUEST_MAGIC {
            return None;
        }

        // Memory fence to ensure we see the complete header
        std::sync::atomic::fence(std::sync::atomic::Ordering::Acquire);

        // Read the full request header
        let header = unsafe { RequestHeader::read_from(data_ptr) };

        if !header.is_valid() {
            return None;
        }

        // Valid request found - extract payload
        let payload_len = header.payload_len as usize;
        let payload = if payload_len > 0 {
            unsafe {
                let payload_ptr = data_ptr.add(RequestHeader::SIZE);
                std::slice::from_raw_parts(payload_ptr, payload_len).to_vec()
            }
        } else {
            Vec::new()
        };

        // Clear the magic to mark as processed
        unsafe {
            std::ptr::write_volatile(data_ptr as *mut u32, 0);
        }

        // Use sender_conn_id from the request header for multi-QP routing
        let conn_id = { header.sender_conn_id } as usize;

        Some(IncomingRequest {
            req_id: header.req_id,
            rpc_type: header.rpc_type,
            payload,
            client_slot_addr: header.client_slot_addr,
            client_slot_rkey: header.client_slot_rkey,
            conn_id,
            slot_index,
        })
    }

    /// Poll for incoming requests using the new message trailer format.
    ///
    /// Checks the Valid field in the message trailer for new messages.
    pub fn poll_request_trailer(&self) -> Option<IncomingRequest> {
        let num_slots = self.processing_pool.num_slots();

        for _ in 0..num_slots {
            let slot_index = self.next_check_slot.get();
            self.next_check_slot.set((slot_index + 1) % num_slots);

            if let Some(request) = self.check_slot_trailer(slot_index) {
                return Some(request);
            }
        }

        None
    }

    /// Check a slot using the message trailer format.
    fn check_slot_trailer(&self, slot_index: usize) -> Option<IncomingRequest> {
        let slot = self.processing_pool.get_slot(slot_index)?;
        let slot_ptr = slot.data_ptr();

        // Read trailer (at end of block)
        let (msg_len, valid) = unsafe { MessageTrailer::read_from(slot_ptr) };

        if valid == 0 || msg_len == 0 {
            return None;
        }

        // Valid message found - read the request header
        let header = unsafe { RequestHeader::read_from(slot_ptr) };

        if !header.is_valid() {
            return None;
        }

        // Extract payload
        let payload_len = header.payload_len as usize;
        let payload = if payload_len > 0 {
            unsafe {
                let payload_ptr = slot_ptr.add(RequestHeader::SIZE);
                std::slice::from_raw_parts(payload_ptr, payload_len).to_vec()
            }
        } else {
            Vec::new()
        };

        // Clear the valid flag
        unsafe {
            let trailer_offset = MESSAGE_BLOCK_SIZE - 4;
            let valid_ptr = slot_ptr.add(trailer_offset) as *mut u32;
            std::ptr::write_volatile(valid_ptr, 0);
        }

        // Use sender_conn_id from the request header for multi-QP routing
        let conn_id = { header.sender_conn_id } as usize;

        Some(IncomingRequest {
            req_id: header.req_id,
            rpc_type: header.rpc_type,
            payload,
            client_slot_addr: header.client_slot_addr,
            client_slot_rkey: header.client_slot_rkey,
            conn_id,
            slot_index,
        })
    }

    /// Send a response for a request.
    ///
    /// # Arguments
    /// * `request` - The original request
    /// * `status` - Response status code
    /// * `payload` - Response payload data
    ///
    /// Per the ScaleRPC paper (Section 3.3), the message pool is "stateless" and
    /// slots become obsolete immediately after a request is processed. Therefore,
    /// we reuse the same slot where the request was received for sending the response.
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

        // Reuse the request slot for the response (per ScaleRPC paper Section 3.3)
        let slot = self
            .processing_pool
            .get_slot(request.slot_index)
            .ok_or(Error::Protocol(format!(
                "slot {} not found",
                request.slot_index
            )))?;
        let slot_data_addr = self
            .processing_pool
            .slot_data_addr(request.slot_index)
            .ok_or(Error::Protocol(format!(
                "slot {} addr not found",
                request.slot_index
            )))?;

        // Build response header
        let response_header =
            ResponseHeader::new(request.req_id, status, payload.len() as u32);

        // Write response to slot
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

        // Send response via RDMA WRITE
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
                    lkey: self.processing_pool.lkey()
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
    /// Returns the number of requests processed.
    pub fn process(&self) -> usize {
        let handler = match &self.handler {
            Some(h) => h,
            None => return 0,
        };

        let mut processed = 0;

        while let Some(request) = self.poll_request() {
            // Call handler
            let (status, response_payload) = handler(request.rpc_type, &request.payload);

            // Send response (WQE only, no doorbell)
            if let Err(e) = self.send_response(&request, status, &response_payload) {
                eprintln!("Failed to send response: {}", e);
            }

            processed += 1;
        }

        // Batch flush doorbells and drain CQs
        self.flush_doorbells();
        self.needs_flush.borrow_mut().clear();
        self.poll_cqs();

        processed
    }

    /// Poll all connection CQs.
    pub fn poll_cqs(&self) -> usize {
        let mut total = 0;
        for conn in self.connections.iter().flatten() {
            total += conn.poll_send_cq();
            total += conn.poll_recv_cq();
        }
        total
    }

    /// Mark a connection as needing doorbell flush.
    ///
    /// The doorbell will be batched and issued in the next `poll()` call.
    fn mark_needs_flush(&self, conn_id: ConnectionId) {
        let mut needs_flush = self.needs_flush.borrow_mut();
        if !needs_flush.contains(&conn_id) {
            needs_flush.push(conn_id);
        }
    }

    /// Flush all pending doorbells.
    ///
    /// Issues doorbells for all connections with pending WQEs.
    /// This can be called directly to flush without draining CQs.
    pub fn flush_doorbells(&self) {
        let needs_flush = self.needs_flush.borrow();
        for &conn_id in needs_flush.iter() {
            if let Some(Some(conn)) = self.connections.get(conn_id) {
                conn.ring_sq_doorbell();
            }
        }
    }

    /// Clear the needs_flush list.
    pub fn clear_needs_flush(&self) {
        self.needs_flush.borrow_mut().clear();
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

    /// Main server loop - one iteration.
    ///
    /// This is the simplified API for running the server. It handles:
    /// - Flushing pending doorbells (batch doorbell)
    /// - Draining CQs
    /// - Context switch when scheduler is enabled
    /// - Fetching warmup requests
    /// - Processing incoming requests (if handler is set)
    ///
    /// Returns the number of CQ entries processed (or requests if handler is set).
    pub fn poll(&mut self) -> usize {
        // 1. Flush all pending doorbells
        self.flush_doorbells();
        self.needs_flush.borrow_mut().clear();

        // 2. Drain all CQs
        let cq_processed = self.poll_cqs();

        // Handle scheduler tasks if enabled
        if self.scheduler.is_some() {
            // Check for context switch
            if let Ok(Some((seq, old_conns))) = self.perform_context_switch() {
                let _ = self.notify_context_switch(&old_conns, seq);
            }

            // Fetch warmup requests from next group
            let _ = self.fetch_warmup_requests();
        }

        // Process requests if handler is set
        let mut requests_processed = 0;
        if self.handler.is_some() {
            while let Some(request) = self.recv() {
                if let Some(handler) = &self.handler {
                    let (status, response) = handler(request.rpc_type, &request.payload);
                    if self.reply(&request, status, &response).is_ok() {
                        requests_processed += 1;
                    }
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
    /// Scans the endpoint entries buffer and issues RDMA READs to fetch
    /// requests from clients in the warmup group.
    ///
    /// Returns the number of requests fetched.
    pub fn fetch_warmup_requests(&self) -> Result<usize> {
        let scheduler = match &self.scheduler {
            Some(s) => s,
            None => return Ok(0),
        };

        let entries = scheduler.scan_warmup_entries();
        if entries.is_empty() {
            return Ok(0);
        }

        let mut fetched = 0;

        for entry in &entries {
            if !entry.is_valid() {
                continue;
            }

            // Find connection for this client
            let conn_id = entry.client_id as usize;
            let conn = match self.connections.get(conn_id).and_then(|c| c.as_ref()) {
                Some(c) => c,
                None => continue,
            };

            // Fetch each request in the batch
            for batch_idx in 0..entry.batch_size {
                let result = self.fetch_single_request(conn, &entry, batch_idx);
                if result.is_ok() {
                    fetched += 1;
                }
            }
        }

        Ok(fetched)
    }

    /// Fetch a single request from a client's warmup buffer via RDMA READ.
    fn fetch_single_request(
        &self,
        conn: &Connection,
        entry: &EndpointEntry,
        batch_idx: u32,
    ) -> Result<()> {
        // Allocate a slot in the warmup pool
        let slot = self.warmup_pool.alloc()?;

        // Calculate source address (client's warmup buffer + offset)
        let src_addr = entry.req_addr + (batch_idx as u64 * MESSAGE_BLOCK_SIZE as u64);

        // Issue RDMA READ
        {
            let qp_ref = conn.qp().borrow();
            let emit_ctx = qp_ref
                .emit_ctx()
                .map_err(|e| Error::Protocol(format!("emit_ctx failed: {:?}", e)))?;

            emit_wqe!(&emit_ctx, read {
                flags: WqeFlags::COMPLETION,
                remote_addr: src_addr,
                rkey: entry.rkey,
                sge: {
                    addr: slot.data_addr(),
                    len: MESSAGE_BLOCK_SIZE as u32,
                    lkey: self.warmup_pool.lkey()
                },
                signaled: entry.client_id as u64,
            })
            .map_err(|e| Error::Protocol(format!("emit_wqe failed: {:?}", e)))?;
        }
        conn.ring_sq_doorbell();

        // Wait for read completion
        let start = std::time::Instant::now();
        let timeout = std::time::Duration::from_millis(10);
        loop {
            if conn.poll_send_cq() > 0 {
                break;
            }
            if start.elapsed() > timeout {
                return Err(Error::Protocol("RDMA READ timeout".to_string()));
            }
            std::hint::spin_loop();
        }

        // Release the slot handle but keep the data (it's now in the warmup pool)
        let _ = slot.release();

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

    /// Check a warmup pool slot for a request.
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

            let payload_len = header.payload_len as usize;
            let payload = if payload_len > 0 {
                unsafe {
                    let payload_ptr = data_ptr.add(RequestHeader::SIZE);
                    std::slice::from_raw_parts(payload_ptr, payload_len).to_vec()
                }
            } else {
                Vec::new()
            };

            // Clear magic
            unsafe {
                std::ptr::write_volatile(data_ptr as *mut u32, 0);
            }

            // Use sender_conn_id from the request header for multi-QP routing
            let conn_id = { header.sender_conn_id } as usize;

            return Some(IncomingRequest {
                req_id: header.req_id,
                rpc_type: header.rpc_type,
                payload,
                client_slot_addr: header.client_slot_addr,
                client_slot_rkey: header.client_slot_rkey,
                conn_id,
                slot_index,
            });
        }

        // Valid trailer - read header
        let header = unsafe { RequestHeader::read_from(data_ptr) };

        if !header.is_valid() {
            return None;
        }

        let payload_len = header.payload_len as usize;
        let payload = if payload_len > 0 {
            unsafe {
                let payload_ptr = data_ptr.add(RequestHeader::SIZE);
                std::slice::from_raw_parts(payload_ptr, payload_len).to_vec()
            }
        } else {
            Vec::new()
        };

        // Clear both magic and valid
        unsafe {
            std::ptr::write_volatile(data_ptr as *mut u32, 0);
            let trailer_offset = MESSAGE_BLOCK_SIZE - 4;
            let valid_ptr = data_ptr.add(trailer_offset) as *mut u32;
            std::ptr::write_volatile(valid_ptr, 0);
        }

        // Use sender_conn_id from the request header for multi-QP routing
        let conn_id = { header.sender_conn_id } as usize;

        Some(IncomingRequest {
            req_id: header.req_id,
            rpc_type: header.rpc_type,
            payload,
            client_slot_addr: header.client_slot_addr,
            client_slot_rkey: header.client_slot_rkey,
            conn_id,
            slot_index,
        })
    }

    /// Get endpoint entry address for a connection slot.
    ///
    /// Returns (address, rkey) that clients should use to WRITE their
    /// endpoint entry for warmup registration.
    pub fn endpoint_entry_addr_for(&self, slot: usize) -> Option<(u64, u32)> {
        self.scheduler
            .as_ref()
            .and_then(|s| s.endpoint_entry_addr_for(slot))
    }
}
