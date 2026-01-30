//! RPC Client implementation.
//!
//! The client implements the ScaleRPC client-side state machine (Figure 7):
//!
//! ```text
//! CONNECT → WARMUP → PROCESS → IDLE → WARMUP → ...
//!               ↑                  |
//!               └──────────────────┘
//! ```
//!
//! ## State Descriptions
//!
//! - **Warmup**: Client prepares requests in a local buffer. Server fetches
//!   these via RDMA READ using the endpoint entry information.
//! - **Process**: Client's group is active. Requests can be sent directly
//!   via RDMA WRITE to the server's processing pool.
//! - **Idle**: Client's group has been switched out. Client waits for the
//!   next time slice when its group becomes active again.
//!
//! ## Request Flow
//!
//! 1. Client in Warmup: Writes request to local buffer, updates endpoint entry
//! 2. Server performs RDMA READ to fetch request to warmup pool
//! 3. Context switch: warmup pool becomes processing pool
//! 4. Client transitions to Process state
//! 5. In Process state: client can RDMA WRITE directly to processing pool
//! 6. On context_switch_event: client transitions to Idle
//! 7. Client prepares next request, transitions to Warmup

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};

use mlx5::cq::{Cq, Cqe};
use mlx5::device::Context;
use mlx5::emit_wqe;
use mlx5::pd::{AccessFlags, MemoryRegion, Pd};
use mlx5::wqe::WqeFlags;

use crate::config::ClientConfig;
use crate::connection::{create_shared_cqs, Connection, ConnectionId, RemoteEndpoint};
use crate::error::{Error, Result};
use crate::mapping::VirtualMapping;
use crate::pool::MessagePool;
use crate::protocol::{
    ContextSwitchEvent, EndpointEntry, MessageTrailer, RequestHeader, ResponseHeader,
    MESSAGE_BLOCK_SIZE,
};

/// Request ID generator.
static NEXT_REQ_ID: AtomicU64 = AtomicU64::new(1);

/// Generate a unique request ID.
fn next_req_id() -> u64 {
    NEXT_REQ_ID.fetch_add(1, Ordering::Relaxed)
}

/// Client state in the ScaleRPC protocol (Figure 7).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientState {
    /// Initial state before first request.
    Connected,
    /// Preparing requests in local buffer for server RDMA READ.
    Warmup,
    /// Group is active, can RDMA WRITE directly to processing pool.
    Process,
    /// Group switched out, waiting for next time slice.
    Idle,
}

impl Default for ClientState {
    fn default() -> Self {
        Self::Connected
    }
}

/// Local request buffer for warmup mechanism.
///
/// When in Warmup state, the client prepares requests here.
/// The server uses RDMA READ to fetch them.
pub struct WarmupBuffer {
    /// Buffer memory.
    buffer: *mut u8,
    /// Buffer size.
    size: usize,
    /// Memory region for RDMA access.
    mr: MemoryRegion,
    /// Number of requests currently in buffer.
    request_count: usize,
    /// Current write offset.
    write_offset: usize,
}

/// Alignment for RDMA memory allocations (4KB page).
const RDMA_ALIGNMENT: usize = 4096;

impl WarmupBuffer {
    /// Create a new warmup buffer.
    pub fn new(pd: &Pd, num_slots: usize) -> Result<Self> {
        let size = num_slots * MESSAGE_BLOCK_SIZE;

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
            size,
            mr,
            request_count: 0,
            write_offset: 0,
        })
    }

    /// Get the base address for RDMA READ.
    pub fn addr(&self) -> u64 {
        self.buffer as u64
    }

    /// Get the rkey for RDMA READ.
    pub fn rkey(&self) -> u32 {
        self.mr.rkey()
    }

    /// Get the lkey for local access.
    pub fn lkey(&self) -> u32 {
        self.mr.lkey()
    }

    /// Get the number of message blocks that fit in this buffer.
    pub fn capacity(&self) -> usize {
        self.size / MESSAGE_BLOCK_SIZE
    }

    /// Get the current number of requests in the buffer.
    pub fn request_count(&self) -> usize {
        self.request_count
    }

    /// Write a request to the warmup buffer.
    ///
    /// Returns the slot index where the request was written.
    pub fn write_request(&mut self, header: &RequestHeader, payload: &[u8]) -> Result<usize> {
        if self.request_count >= self.capacity() {
            return Err(Error::NoFreeSlots);
        }

        let slot_index = self.request_count;
        let slot_ptr = unsafe { self.buffer.add(slot_index * MESSAGE_BLOCK_SIZE) };

        // Write header at start of data area
        unsafe {
            header.write_to(slot_ptr);
            if !payload.is_empty() {
                std::ptr::copy_nonoverlapping(
                    payload.as_ptr(),
                    slot_ptr.add(RequestHeader::SIZE),
                    payload.len(),
                );
            }

            // Write message trailer (right-aligned)
            let msg_len = (RequestHeader::SIZE + payload.len()) as u32;
            MessageTrailer::write_to(slot_ptr, msg_len);
        }

        self.request_count += 1;
        Ok(slot_index)
    }

    /// Clear the buffer for reuse.
    pub fn clear(&mut self) {
        self.request_count = 0;
        self.write_offset = 0;
        // Zero out valid flags
        for i in 0..self.capacity() {
            let slot_ptr = unsafe { self.buffer.add(i * MESSAGE_BLOCK_SIZE) };
            unsafe {
                let trailer_offset = MESSAGE_BLOCK_SIZE - 8;
                let valid_ptr = slot_ptr.add(trailer_offset + 4) as *mut u32;
                std::ptr::write_volatile(valid_ptr, 0);
            }
        }
    }

    /// Get the address of a specific slot.
    pub fn slot_addr(&self, index: usize) -> Option<u64> {
        if index >= self.capacity() {
            return None;
        }
        Some(self.buffer as u64 + (index * MESSAGE_BLOCK_SIZE) as u64)
    }
}

impl Drop for WarmupBuffer {
    fn drop(&mut self) {
        unsafe {
            libc::free(self.buffer as *mut std::ffi::c_void);
        }
    }
}

/// Context switch event buffer.
///
/// The server writes context switch events here to notify the client.
pub struct EventBuffer {
    /// Buffer memory.
    buffer: *mut u8,
    /// Memory region.
    mr: MemoryRegion,
    /// Last seen sequence number.
    last_sequence: Cell<u64>,
}

impl EventBuffer {
    /// Create a new event buffer.
    pub fn new(pd: &Pd) -> Result<Self> {
        let size = std::mem::size_of::<ContextSwitchEvent>();

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
            mr,
            last_sequence: Cell::new(0),
        })
    }

    /// Get the address for RDMA WRITE.
    pub fn addr(&self) -> u64 {
        self.buffer as u64
    }

    /// Get the rkey.
    pub fn rkey(&self) -> u32 {
        self.mr.rkey()
    }

    /// Check for new context switch event.
    ///
    /// Returns (scheduler sequence, fetched endpoint entry seq) if an event occurred.
    pub fn poll(&self) -> Option<(u64, u32)> {
        let event = unsafe { ContextSwitchEvent::read_from(self.buffer) };

        if event.is_valid() && event.sequence > self.last_sequence.get() {
            self.last_sequence.set(event.sequence);
            Some((event.sequence, event.fetched_seq))
        } else {
            None
        }
    }

    /// Clear the event buffer.
    pub fn clear(&self) {
        unsafe {
            std::ptr::write_bytes(self.buffer, 0, std::mem::size_of::<ContextSwitchEvent>());
        }
        self.last_sequence.set(0);
    }
}

impl Drop for EventBuffer {
    fn drop(&mut self) {
        unsafe {
            libc::free(self.buffer as *mut std::ffi::c_void);
        }
    }
}

/// Per-connection client state.
struct ConnectionState {
    /// Current state in the ScaleRPC protocol.
    state: Cell<ClientState>,
    /// Warmup buffer for this connection.
    warmup_buffer: RefCell<WarmupBuffer>,
    /// Event buffer for context switch notifications.
    event_buffer: EventBuffer,
    /// Server's endpoint entry address (client writes to this).
    endpoint_entry_addr: Cell<u64>,
    /// Server's endpoint entry rkey.
    endpoint_entry_rkey: Cell<u32>,
    /// Current sequence number for endpoint entry.
    endpoint_entry_seq: Cell<u32>,
    /// Number of requests in warmup buffer when last notified.
    last_notified_count: Cell<usize>,
    /// True if a notification has been sent and we're waiting for server to fetch.
    /// This prevents overwriting the endpoint entry before the server reads it.
    pending_notification: Cell<bool>,
    /// Server-assigned connection ID (used as sender_conn_id in requests).
    server_conn_id: Cell<u32>,
}

/// RPC Client.
///
/// Manages connections, message pools, and RPC request/response handling.
/// Implements the ScaleRPC client-side state machine (Figure 7).
pub struct RpcClient {
    /// Message pool for request/response data.
    pool: MessagePool,
    /// Virtual mapping table.
    mapping: VirtualMapping,
    /// Connections indexed by ID.
    connections: Vec<Option<Connection>>,
    /// Per-connection state.
    connection_states: Vec<Option<ConnectionState>>,
    /// Configuration.
    config: ClientConfig,
    /// Protection domain reference for buffer allocation.
    #[allow(dead_code)]
    pd_ptr: *const Pd,
    /// Connections that need doorbell flush (for batching).
    needs_flush: RefCell<Vec<ConnectionId>>,
    /// Shared send completion queue (all QPs share this).
    shared_send_cq: Option<Rc<Cq>>,
    /// Shared receive completion queue (all QPs share this).
    shared_recv_cq: Option<Rc<Cq>>,
}

impl RpcClient {
    /// Create a new RPC client.
    ///
    /// # Arguments
    /// * `pd` - Protection domain for memory registration
    /// * `config` - Client configuration
    pub fn new(pd: &Pd, config: ClientConfig) -> Result<Self> {
        let pool = MessagePool::new(pd, &config.pool)?;
        let mapping = VirtualMapping::new();

        Ok(Self {
            pool,
            mapping,
            connections: Vec::new(),
            connection_states: Vec::new(),
            config,
            pd_ptr: pd as *const Pd,
            needs_flush: RefCell::new(Vec::new()),
            shared_send_cq: None,
            shared_recv_cq: None,
        })
    }

    /// Get the current state of a connection.
    pub fn state(&self, conn_id: ConnectionId) -> Option<ClientState> {
        self.connection_states
            .get(conn_id)
            .and_then(|s| s.as_ref())
            .map(|s| s.state.get())
    }

    /// Transition connection to a new state.
    #[allow(dead_code)]
    fn set_state(&self, conn_id: ConnectionId, new_state: ClientState) {
        if let Some(Some(state)) = self.connection_states.get(conn_id) {
            state.state.set(new_state);
        }
    }

    /// Get the event buffer address for a connection.
    pub fn event_buffer_addr(&self, conn_id: ConnectionId) -> Option<(u64, u32)> {
        self.connection_states
            .get(conn_id)
            .and_then(|s| s.as_ref())
            .map(|s| (s.event_buffer.addr(), s.event_buffer.rkey()))
    }

    /// Add a new connection to the client.
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
            // Size CQ to handle all connections: max_connections * 256 WQEs each
            let cq_size = self.config.max_connections * 256;
            let (send_cq, recv_cq) = create_shared_cqs(ctx, cq_size)?;
            self.shared_send_cq = Some(send_cq);
            self.shared_recv_cq = Some(recv_cq);
        }

        fn sq_callback(_cqe: Cqe, _entry: u64) {
            // Entry contains request ID - we could track completions here
        }
        fn rq_callback(_cqe: Cqe, _entry: u64) {
            // RQ completions not used in client (one-sided)
        }

        let send_cq = self.shared_send_cq.as_ref().unwrap().clone();
        let recv_cq = self.shared_recv_cq.as_ref().unwrap().clone();
        let conn = Connection::new(ctx, pd, conn_id, port, send_cq, recv_cq, sq_callback, rq_callback)?;

        // Register with mapping
        self.mapping.register_connection(conn_id);

        // Virtualized Mapping: allocate fixed slot range for this connection
        let slots_per_conn = self.config.pool.num_slots / self.config.max_connections;
        let start_slot = conn_id * slots_per_conn;
        for i in 0..slots_per_conn {
            self.mapping.bind_slot(conn_id, start_slot + i)?;
        }

        // Create per-connection state with warmup and event buffers
        let warmup_buffer = WarmupBuffer::new(pd, 16)?; // 16 slots for warmup batching
        let event_buffer = EventBuffer::new(pd)?;

        let conn_state = ConnectionState {
            state: Cell::new(ClientState::Connected),
            warmup_buffer: RefCell::new(warmup_buffer),
            event_buffer,
            endpoint_entry_addr: Cell::new(0),
            endpoint_entry_rkey: Cell::new(0),
            endpoint_entry_seq: Cell::new(0),
            last_notified_count: Cell::new(0),
            pending_notification: Cell::new(false),
            server_conn_id: Cell::new(0),
        };

        self.connections.push(Some(conn));
        self.connection_states.push(Some(conn_state));

        Ok(conn_id)
    }

    /// Connect to a remote endpoint.
    ///
    /// # Arguments
    /// * `conn_id` - Local connection ID
    /// * `remote` - Remote endpoint information
    pub fn connect(&mut self, conn_id: ConnectionId, remote: RemoteEndpoint) -> Result<()> {
        let conn = self
            .connections
            .get_mut(conn_id)
            .and_then(|c| c.as_mut())
            .ok_or(Error::ConnectionNotFound(conn_id))?;

        // Store remote slot info in mapping
        self.mapping
            .set_remote_slot(conn_id, remote.slot_addr, remote.slot_rkey)?;

        // Store endpoint entry info for warmup notification and server_conn_id
        if let Some(Some(state)) = self.connection_states.get(conn_id) {
            state.endpoint_entry_addr.set(remote.endpoint_entry_addr);
            state.endpoint_entry_rkey.set(remote.endpoint_entry_rkey);
            state.server_conn_id.set(remote.server_conn_id);
        }

        conn.connect(remote)?;
        Ok(())
    }

    /// Get local endpoint information for a connection.
    ///
    /// Note: slot_addr points to the pool's data region base for response write-back.
    /// event_buffer_addr/rkey are populated for context switch notification.
    /// warmup_buffer_addr/rkey/slots are populated for server to RDMA READ warmup requests.
    pub fn local_endpoint(&self, conn_id: ConnectionId) -> Result<RemoteEndpoint> {
        let conn = self
            .connections
            .get(conn_id)
            .and_then(|c| c.as_ref())
            .ok_or(Error::ConnectionNotFound(conn_id))?;

        let mut endpoint = conn.local_endpoint();
        // Note: actual slot address for response is provided in RequestHeader
        endpoint.slot_addr = self.pool.base_addr();
        endpoint.slot_rkey = self.pool.rkey();

        // Add event buffer info for context switch notification
        if let Some((addr, rkey)) = self.event_buffer_addr(conn_id) {
            endpoint.event_buffer_addr = addr;
            endpoint.event_buffer_rkey = rkey;
        }

        // Add warmup buffer info for server to RDMA READ requests
        if let Some(Some(state)) = self.connection_states.get(conn_id) {
            let warmup_buffer = state.warmup_buffer.borrow();
            endpoint.warmup_buffer_addr = warmup_buffer.addr();
            endpoint.warmup_buffer_rkey = warmup_buffer.rkey();
            endpoint.warmup_buffer_slots = warmup_buffer.capacity() as u32;
        }

        Ok(endpoint)
    }

    /// Get the message pool.
    pub fn pool(&self) -> &MessagePool {
        &self.pool
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
        let mut needs_flush = self.needs_flush.borrow_mut();
        if !needs_flush.contains(&conn_id) {
            needs_flush.push(conn_id);
        }
    }

    /// Batch doorbell flush and CQ drain.
    ///
    /// This is the main polling method that should be called periodically.
    /// It:
    /// 1. Notifies server of new warmup requests (batched)
    /// 2. Issues doorbells for all connections with pending WQEs
    /// 3. Drains all connection CQs
    ///
    /// Returns the total number of CQ entries processed.
    pub fn poll(&self) -> usize {
        // 1. Check for context switch events first (to clear pending_notification)
        for conn_id in 0..self.connection_states.len() {
            self.check_events(conn_id);
        }

        // 2. Notify server of new warmup requests (batched notification)
        // Skip if there's already a pending notification to avoid overwriting
        // the endpoint entry before the server reads it.
        for conn_id in 0..self.connection_states.len() {
            if let Some(Some(state)) = self.connection_states.get(conn_id) {
                // Don't send new notification if previous one hasn't been processed
                if state.pending_notification.get() {
                    continue;
                }
                let warmup_count = state.warmup_buffer.borrow().request_count();
                let last_notified = state.last_notified_count.get();
                if warmup_count > last_notified {
                    // New requests since last notification
                    let _ = self.notify_warmup_ready(conn_id);
                    state.last_notified_count.set(warmup_count);
                    state.pending_notification.set(true);
                }
            }
        }

        // 2. Flush all pending doorbells
        {
            let needs_flush = self.needs_flush.borrow();
            for &conn_id in needs_flush.iter() {
                if let Some(Some(conn)) = self.connections.get(conn_id) {
                    conn.ring_sq_doorbell();
                }
            }
        }
        self.needs_flush.borrow_mut().clear();

        // 3. Drain all CQs
        self.poll_cqs()
    }

    /// Check for context switch events on a specific connection.
    ///
    /// Called automatically by `call()` to handle state transitions.
    /// Context switch events indicate that warmup requests were fetched by server.
    /// Since call_direct is disabled, we just clear the buffer and stay in Warmup.
    fn check_events(&self, conn_id: ConnectionId) {
        if let Some(Some(state)) = self.connection_states.get(conn_id) {
            if let Some((_sched_seq, fetched_seq)) = state.event_buffer.poll() {
                // Context switch event received - server fetched warmup requests
                // Only clear buffer if the fetched_seq matches our last notified seq
                // This prevents clearing the buffer for events that correspond to older batches
                let notified_seq = state.endpoint_entry_seq.get();
                if fetched_seq == notified_seq && state.pending_notification.get() {
                    state.pending_notification.set(false);
                    // Clear the warmup buffer so we can send new requests
                    if state.state.get() == ClientState::Warmup && state.last_notified_count.get() > 0 {
                        state.warmup_buffer.borrow_mut().clear();
                        state.last_notified_count.set(0);
                    }
                }
            }
        }
    }

    /// Notify server that warmup requests are ready via RDMA WRITE to endpoint entry.
    ///
    /// This is the key optimization from the paper: instead of the server blindly
    /// reading all warmup slots, the client notifies exactly how many requests
    /// are ready and their location.
    fn notify_warmup_ready(&self, conn_id: ConnectionId) -> Result<()> {
        let conn = self
            .connections
            .get(conn_id)
            .and_then(|c| c.as_ref())
            .ok_or(Error::ConnectionNotFound(conn_id))?;

        let conn_state = self
            .connection_states
            .get(conn_id)
            .and_then(|s| s.as_ref())
            .ok_or(Error::ConnectionNotFound(conn_id))?;

        let warmup_buffer = conn_state.warmup_buffer.borrow();
        let batch_size = warmup_buffer.request_count() as u32;

        if batch_size == 0 {
            return Ok(()); // Nothing to notify
        }

        let endpoint_entry_addr = conn_state.endpoint_entry_addr.get();
        let endpoint_entry_rkey = conn_state.endpoint_entry_rkey.get();

        // Skip if no endpoint entry configured (backward compatibility)
        if endpoint_entry_addr == 0 {
            return Ok(());
        }

        // Increment sequence
        let new_seq = conn_state.endpoint_entry_seq.get() + 1;
        conn_state.endpoint_entry_seq.set(new_seq);

        // Build endpoint entry
        let entry = EndpointEntry::new(warmup_buffer.addr(), batch_size, new_seq);

        // Allocate a small buffer for RDMA WRITE
        let slot = self.pool.alloc()?;
        unsafe {
            entry.write_to(slot.slot().data_ptr());
        }

        // RDMA WRITE to server's endpoint entry
        {
            let qp_ref = conn.qp().borrow();
            let emit_ctx = qp_ref
                .emit_ctx()
                .map_err(|e| Error::Protocol(format!("emit_ctx failed: {:?}", e)))?;

            emit_wqe!(&emit_ctx, write {
                flags: WqeFlags::COMPLETION,
                remote_addr: endpoint_entry_addr,
                rkey: endpoint_entry_rkey,
                sge: {
                    addr: slot.data_addr(),
                    len: EndpointEntry::SIZE as u32,
                    lkey: self.pool.lkey()
                },
                signaled: new_seq as u64,
            })
            .map_err(|e| Error::Protocol(format!("emit_wqe failed: {:?}", e)))?;
        }

        // Mark connection as needing doorbell flush
        self.mark_needs_flush(conn_id);

        // Release the slot - the data has been queued for RDMA WRITE
        // The doorbell will be flushed in the next poll() call
        drop(slot);

        Ok(())
    }

    /// Send request via warmup mechanism.
    ///
    /// Writes request to warmup buffer. The server will RDMA READ from this
    /// buffer using the warmup buffer info exchanged during connection setup.
    fn send_warmup(
        &self,
        conn_id: ConnectionId,
        rpc_type: u16,
        payload: &[u8],
    ) -> Result<PendingRpc<'_>> {
        let conn_state = self
            .connection_states
            .get(conn_id)
            .and_then(|s| s.as_ref())
            .ok_or(Error::ConnectionNotFound(conn_id))?;

        // Auto-transition Idle/Connected -> Warmup
        let current = conn_state.state.get();
        if current == ClientState::Idle || current == ClientState::Connected {
            conn_state.warmup_buffer.borrow_mut().clear();
            conn_state.last_notified_count.set(0); // Reset notification tracking
            conn_state.state.set(ClientState::Warmup);
        }

        // Check if a notification is pending (server hasn't fetched yet)
        // We can't add more requests because the batch size has already been sent
        // Adding more would result in those requests being lost when warmup_buffer is cleared
        if conn_state.pending_notification.get() {
            return Err(Error::NoFreeSlots);
        }

        // Check if warmup buffer is full - return error instead of clearing
        // Clearing while server is still reading would cause a race condition
        // and lose requests. The caller should wait for responses (which will
        // trigger context_switch_event and state transition to Idle->Warmup).
        {
            let warmup_buffer = conn_state.warmup_buffer.borrow();
            if warmup_buffer.request_count() >= warmup_buffer.capacity() {
                return Err(Error::NoFreeSlots);
            }
        }

        // Allocate a response slot
        let response_slot = self.pool.alloc()?;
        let req_id = next_req_id();

        // Use server_conn_id so server can route response through correct QP
        let sender_conn_id = conn_state.server_conn_id.get();

        // Build request header
        let header = RequestHeader::new(
            req_id,
            rpc_type,
            payload.len() as u16,
            response_slot.data_addr(),
            self.pool.rkey(),
            sender_conn_id,
        );

        // Write to warmup buffer
        {
            let mut warmup_buffer = conn_state.warmup_buffer.borrow_mut();
            warmup_buffer.write_request(&header, payload)?;
        }

        // Note: We don't notify immediately. Notification is batched in poll()
        // to avoid RDMA WRITE ordering issues when multiple requests are queued.

        Ok(PendingRpc {
            pool: &self.pool,
            slot_index: response_slot.release(),
            req_id,
        })
    }

    /// Make an asynchronous RPC call.
    ///
    /// Returns immediately with a `PendingRpc` handle that can be polled
    /// for completion. Always uses the warmup mechanism where the server
    /// fetches requests via RDMA READ.
    ///
    /// Note: The direct RDMA WRITE optimization (call_direct) is currently
    /// disabled because pool swapping during context switch causes requests
    /// to end up in the wrong pool.
    ///
    /// State machine (Figure 7):
    /// CONNECT → WARMUP → (server fetches, context switch) → PROCESS → IDLE → WARMUP → ...
    ///
    /// # Arguments
    /// * `conn_id` - Connection to use
    /// * `rpc_type` - RPC method identifier
    /// * `payload` - Request payload data
    pub fn call_async(
        &self,
        conn_id: ConnectionId,
        rpc_type: u16,
        payload: &[u8],
    ) -> Result<PendingRpc<'_>> {
        // Check for context switch events
        self.check_events(conn_id);

        // Always use warmup mechanism for now
        // TODO: Re-enable call_direct when pool addressing is fixed
        self.send_warmup(conn_id, rpc_type, payload)
    }

    /// Poll for any completed responses.
    ///
    /// Checks all connections for context switch events.
    /// Note: This method is a placeholder for more sophisticated
    /// response tracking in the future.
    pub fn recv(&self) -> Option<RpcResponse> {
        // Check all connections for events
        for conn_id in 0..self.connection_states.len() {
            self.check_events(conn_id);
        }

        // TODO: Implement pending request tracking for async recv
        None
    }
}

/// A pending RPC request waiting for response.
///
/// The caller should poll for completion using `poll()`. When the response
/// is received, it is returned. Flow control is handled by slot allocation:
/// if all slots are occupied waiting for responses, new requests cannot be sent.
pub struct PendingRpc<'a> {
    pool: &'a MessagePool,
    slot_index: usize,
    req_id: u64,
}

impl<'a> PendingRpc<'a> {
    /// Get the request ID.
    pub fn req_id(&self) -> u64 {
        self.req_id
    }

    /// Get the slot index.
    pub fn slot_index(&self) -> usize {
        self.slot_index
    }

    /// Poll for response completion.
    ///
    /// Returns `Some(RpcResponse)` if the response is ready,
    /// `None` if still pending.
    ///
    /// Flow control: The slot remains allocated until a response is received.
    /// This naturally limits outstanding requests to the number of available slots.
    pub fn poll(&self) -> Option<RpcResponse> {
        let slot = self.pool.get_slot(self.slot_index)?;

        // Check if response magic is present
        // Use volatile read to ensure we see RDMA writes from the NIC
        let data_ptr = slot.data_ptr();

        // Read magic and req_id with volatile to prevent caching/optimization
        let magic = unsafe { std::ptr::read_volatile(data_ptr as *const u32) };
        if magic != crate::protocol::RESPONSE_MAGIC {
            return None;
        }

        // Read the full header now that we know it's valid
        // Add a memory fence to ensure ordering
        std::sync::atomic::fence(std::sync::atomic::Ordering::Acquire);
        let header = unsafe { ResponseHeader::read_from(data_ptr) };

        if header.req_id == self.req_id {
            Some(RpcResponse {
                header,
                payload_ptr: unsafe { data_ptr.add(ResponseHeader::SIZE) },
            })
        } else {
            None
        }
    }

    /// Release the slot back to the pool without dropping.
    ///
    /// This consumes the PendingRpc and returns the slot to the free list.
    pub fn release(self) {
        self.pool.free_slot_by_index(self.slot_index);
        std::mem::forget(self); // Prevent Drop from running
    }
}

impl Drop for PendingRpc<'_> {
    fn drop(&mut self) {
        // Return the slot to the free list when PendingRpc is dropped
        self.pool.free_slot_by_index(self.slot_index);
    }
}

/// RPC Response.
pub struct RpcResponse {
    header: ResponseHeader,
    payload_ptr: *const u8,
}

impl RpcResponse {
    /// Get the response status.
    pub fn status(&self) -> u32 {
        self.header.status
    }

    /// Check if the response indicates success.
    pub fn is_success(&self) -> bool {
        self.header.is_success()
    }

    /// Get the payload length.
    pub fn payload_len(&self) -> usize {
        self.header.payload_len as usize
    }

    /// Get the payload data.
    pub fn payload(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(self.payload_ptr, self.header.payload_len as usize)
        }
    }

    /// Copy payload to a buffer.
    pub fn copy_payload(&self, buf: &mut [u8]) -> usize {
        let len = buf.len().min(self.payload_len());
        buf[..len].copy_from_slice(&self.payload()[..len]);
        len
    }
}
