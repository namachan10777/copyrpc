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
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use mlx5::cq::Cqe;
use mlx5::device::Context;
use mlx5::emit_wqe;
use mlx5::pd::{AccessFlags, MemoryRegion, Pd};
use mlx5::wqe::WqeFlags;

use crate::config::ClientConfig;
use crate::connection::{Connection, ConnectionId, RemoteEndpoint};
use crate::error::{Error, Result, SlotState};
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

impl WarmupBuffer {
    /// Create a new warmup buffer.
    pub fn new(pd: &Pd, num_slots: usize) -> Result<Self> {
        let size = num_slots * MESSAGE_BLOCK_SIZE;

        let buffer = unsafe {
            let mut ptr: *mut std::ffi::c_void = std::ptr::null_mut();
            let ret = libc::posix_memalign(&mut ptr, MESSAGE_BLOCK_SIZE, size);
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
    /// Returns the new sequence number if an event occurred.
    pub fn poll(&self) -> Option<u64> {
        let event = unsafe { ContextSwitchEvent::read_from(self.buffer) };

        if event.is_valid() && event.sequence > self.last_sequence.get() {
            self.last_sequence.set(event.sequence);
            Some(event.sequence)
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
    /// Endpoint entry to register with server.
    endpoint_entry: RefCell<EndpointEntry>,
    /// Server's endpoint entry address (for registering warmup info).
    server_endpoint_entry_addr: Cell<u64>,
    /// Server's endpoint entry rkey.
    server_endpoint_entry_rkey: Cell<u32>,
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

        fn sq_callback(_cqe: Cqe, _entry: u64) {
            // Entry contains request ID - we could track completions here
        }
        fn rq_callback(_cqe: Cqe, _entry: u64) {
            // RQ completions not used in client (one-sided)
        }

        let conn = Connection::new(ctx, pd, conn_id, port, sq_callback, rq_callback)?;

        // Register with mapping
        self.mapping.register_connection(conn_id);

        // Create per-connection state with warmup and event buffers
        let warmup_buffer = WarmupBuffer::new(pd, 16)?; // 16 slots for warmup batching
        let event_buffer = EventBuffer::new(pd)?;

        let conn_state = ConnectionState {
            state: Cell::new(ClientState::Connected),
            warmup_buffer: RefCell::new(warmup_buffer),
            event_buffer,
            endpoint_entry: RefCell::new(EndpointEntry::default()),
            server_endpoint_entry_addr: Cell::new(0),
            server_endpoint_entry_rkey: Cell::new(0),
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
    ///
    /// When the remote provides endpoint_entry_addr/rkey (scheduler enabled),
    /// they are automatically saved for warmup registration.
    pub fn connect(&mut self, conn_id: ConnectionId, remote: RemoteEndpoint) -> Result<()> {
        let conn = self
            .connections
            .get_mut(conn_id)
            .and_then(|c| c.as_mut())
            .ok_or(Error::ConnectionNotFound(conn_id))?;

        // Store remote slot info in mapping
        self.mapping
            .set_remote_slot(conn_id, remote.slot_addr, remote.slot_rkey)?;

        // Auto-save endpoint entry info if provided (for warmup registration)
        if remote.endpoint_entry_addr != 0 {
            if let Some(Some(state)) = self.connection_states.get(conn_id) {
                state.server_endpoint_entry_addr.set(remote.endpoint_entry_addr);
                state.server_endpoint_entry_rkey.set(remote.endpoint_entry_rkey);
            }
        }

        conn.connect(remote)?;
        Ok(())
    }

    /// Get local endpoint information for a connection.
    ///
    /// Note: slot_addr points to the pool's data region base for response write-back.
    /// event_buffer_addr/rkey are populated for context switch notification.
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

        Ok(endpoint)
    }

    /// Get the message pool.
    pub fn pool(&self) -> &MessagePool {
        &self.pool
    }

    /// Make a blocking RPC call.
    ///
    /// Sends the request and waits for the response. The behavior depends
    /// on the client's current state:
    ///
    /// - **Process/Connected**: Sends directly via RDMA WRITE
    /// - **Warmup/Idle**: Uses warmup mechanism with endpoint entry
    ///
    /// For non-blocking operation, use `call_async()` instead.
    ///
    /// # Arguments
    /// * `conn_id` - Connection to use
    /// * `rpc_type` - RPC method identifier
    /// * `payload` - Request payload data
    ///
    /// # Returns
    /// The RPC response on success.
    pub fn call(
        &self,
        conn_id: ConnectionId,
        rpc_type: u16,
        payload: &[u8],
    ) -> Result<RpcResponse> {
        let pending = self.call_async(conn_id, rpc_type, payload)?;
        // Flush doorbell immediately for blocking call
        self.poll();
        pending.wait()
    }

    /// Make a direct RPC call via RDMA WRITE.
    ///
    /// This bypasses the state machine and sends directly.
    /// Used in Process state or for backward compatibility.
    fn call_direct(
        &self,
        conn_id: ConnectionId,
        rpc_type: u16,
        payload: &[u8],
    ) -> Result<PendingRpc<'_>> {
        // Allocate a slot for this request
        let slot = self.pool.alloc()?;
        let slot_index = slot.index();

        // Get connection and remote info
        let conn = self
            .connections
            .get(conn_id)
            .and_then(|c| c.as_ref())
            .ok_or(Error::ConnectionNotFound(conn_id))?;

        let mapping_entry = self
            .mapping
            .get_connection(conn_id)
            .ok_or(Error::ConnectionNotFound(conn_id))?;

        // Calculate remote slot address based on client slot index
        // This ensures each request goes to a different server slot
        let remote_base_addr = mapping_entry.remote_slot_addr;
        let remote_addr = remote_base_addr + (slot_index * MESSAGE_BLOCK_SIZE) as u64;
        let remote_rkey = mapping_entry.remote_slot_rkey;

        // Generate request ID
        let req_id = next_req_id();

        // Build request header
        // client_slot_addr points to data area where response will be written
        let header = RequestHeader::new(
            req_id,
            rpc_type,
            payload.len() as u16,
            slot.data_addr(),
            self.pool.rkey(),
            conn_id as u32,
        );

        // Write header and payload to slot
        // First, clear any previous response magic to avoid false positives in poll()
        let slot_data_ptr = slot.slot().data_ptr();
        unsafe {
            // Clear response magic (first 4 bytes)
            std::ptr::write_volatile(slot_data_ptr as *mut u32, 0);

            header.write_to(slot_data_ptr);
            if !payload.is_empty() {
                std::ptr::copy_nonoverlapping(
                    payload.as_ptr(),
                    slot_data_ptr.add(RequestHeader::SIZE),
                    payload.len(),
                );
            }
        }

        // Update slot state
        slot.set_state(SlotState::RequestPending);

        // Send request via RDMA WRITE
        let total_len = RequestHeader::SIZE + payload.len();
        {
            let qp_ref = conn.qp().borrow();
            let emit_ctx = qp_ref
                .emit_ctx()
                .map_err(|e| Error::Protocol(format!("emit_ctx failed: {:?}", e)))?;

            emit_wqe!(&emit_ctx, write {
                flags: WqeFlags::COMPLETION,
                remote_addr: remote_addr,
                rkey: remote_rkey,
                sge: {
                    addr: slot.data_addr(),
                    len: total_len as u32,
                    lkey: self.pool.lkey()
                },
                signaled: req_id,
            })
            .map_err(|e| Error::Protocol(format!("emit_wqe failed: {:?}", e)))?;
        }

        // Mark connection as needing doorbell flush (batched in poll())
        self.mark_needs_flush(conn_id);

        Ok(PendingRpc {
            pool: &self.pool,
            slot_index: slot.release(),
            req_id,
            timeout_ms: self.config.timeout_ms,
        })
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

    /// Batch doorbell flush and CQ drain.
    ///
    /// This is the main polling method that should be called periodically.
    /// It:
    /// 1. Issues doorbells for all connections with pending WQEs
    /// 2. Drains all connection CQs
    ///
    /// Returns the total number of CQ entries processed.
    pub fn poll(&self) -> usize {
        // 1. Flush all pending doorbells
        {
            let needs_flush = self.needs_flush.borrow();
            for &conn_id in needs_flush.iter() {
                if let Some(Some(conn)) = self.connections.get(conn_id) {
                    conn.ring_sq_doorbell();
                }
            }
        }
        self.needs_flush.borrow_mut().clear();

        // 2. Drain all CQs
        self.poll_cqs()
    }

    /// Check for context switch events on a specific connection.
    ///
    /// Called automatically by `call()` to handle state transitions.
    fn check_events(&self, conn_id: ConnectionId) {
        if let Some(Some(state)) = self.connection_states.get(conn_id) {
            if state.event_buffer.poll().is_some() {
                // Context switch event received - transition to Idle
                state.state.set(ClientState::Idle);
            }
        }
    }

    /// Send endpoint entry to the server via RDMA WRITE.
    ///
    /// This registers the client's warmup buffer with the server,
    /// allowing the server to fetch requests via RDMA READ.
    fn send_endpoint_entry(&self, conn_id: ConnectionId) -> Result<()> {
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

        let server_addr = conn_state.server_endpoint_entry_addr.get();
        let server_rkey = conn_state.server_endpoint_entry_rkey.get();

        if server_addr == 0 {
            // Server doesn't support warmup (scheduler disabled)
            return Ok(());
        }

        // Allocate a temporary slot for the endpoint entry
        let slot = self.pool.alloc()?;
        let entry = conn_state.endpoint_entry.borrow();

        // Write endpoint entry to slot
        let slot_data_ptr = slot.slot().data_ptr();
        unsafe {
            std::ptr::copy_nonoverlapping(
                &*entry as *const EndpointEntry as *const u8,
                slot_data_ptr,
                EndpointEntry::SIZE,
            );
        }

        // Send via RDMA WRITE
        {
            let qp_ref = conn.qp().borrow();
            let emit_ctx = qp_ref
                .emit_ctx()
                .map_err(|e| Error::Protocol(format!("emit_ctx failed: {:?}", e)))?;

            emit_wqe!(&emit_ctx, write {
                flags: WqeFlags::COMPLETION,
                remote_addr: server_addr,
                rkey: server_rkey,
                sge: {
                    addr: slot.data_addr(),
                    len: EndpointEntry::SIZE as u32,
                    lkey: self.pool.lkey()
                },
                signaled: conn_id as u64,
            })
            .map_err(|e| Error::Protocol(format!("emit_wqe failed: {:?}", e)))?;
        }
        conn.ring_sq_doorbell();

        // Poll for completion
        let start = Instant::now();
        let timeout = Duration::from_millis(self.config.timeout_ms);
        loop {
            if conn.poll_send_cq() > 0 {
                break;
            }
            if start.elapsed() > timeout {
                return Err(Error::Protocol("endpoint entry send timeout".to_string()));
            }
            std::hint::spin_loop();
        }

        Ok(())
    }

    /// Send request via warmup mechanism.
    ///
    /// Writes request to warmup buffer and sends endpoint entry to server.
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

        // Auto-transition Idle -> Warmup
        if conn_state.state.get() == ClientState::Idle {
            conn_state.warmup_buffer.borrow_mut().clear();
            conn_state.state.set(ClientState::Warmup);
        }

        // Allocate a response slot
        let response_slot = self.pool.alloc()?;
        let req_id = next_req_id();

        // Build request header
        let header = RequestHeader::new(
            req_id,
            rpc_type,
            payload.len() as u16,
            response_slot.data_addr(),
            self.pool.rkey(),
            conn_id as u32,
        );

        // Write to warmup buffer
        {
            let mut warmup_buffer = conn_state.warmup_buffer.borrow_mut();
            warmup_buffer.write_request(&header, payload)?;
        }

        // Update endpoint entry
        {
            let warmup_buffer = conn_state.warmup_buffer.borrow();
            let mut entry = conn_state.endpoint_entry.borrow_mut();
            entry.req_addr = warmup_buffer.addr();
            entry.batch_size = warmup_buffer.request_count() as u32;
            entry.client_id = conn_id as u32;
            entry.rkey = warmup_buffer.rkey();
        }

        // Send endpoint entry to server
        self.send_endpoint_entry(conn_id)?;

        Ok(PendingRpc {
            pool: &self.pool,
            slot_index: response_slot.release(),
            req_id,
            timeout_ms: self.config.timeout_ms,
        })
    }

    /// Make an asynchronous RPC call.
    ///
    /// Returns immediately with a `PendingRpc` handle that can be polled
    /// for completion. The request behavior depends on the current state:
    ///
    /// - **Process/Connected**: Sends directly via RDMA WRITE
    /// - **Warmup/Idle**: Uses warmup mechanism with endpoint entry
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

        let current_state = self.state(conn_id).ok_or(Error::ConnectionNotFound(conn_id))?;

        match current_state {
            ClientState::Process | ClientState::Connected => {
                // Active: send directly via RDMA WRITE
                self.call_direct(conn_id, rpc_type, payload)
            }
            ClientState::Warmup | ClientState::Idle => {
                // Inactive: use warmup mechanism
                self.send_warmup(conn_id, rpc_type, payload)
            }
        }
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
pub struct PendingRpc<'a> {
    pool: &'a MessagePool,
    slot_index: usize,
    req_id: u64,
    timeout_ms: u64,
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

    /// Wait for response with timeout.
    pub fn wait(&self) -> Result<RpcResponse> {
        let start = Instant::now();
        let timeout = Duration::from_millis(self.timeout_ms);

        loop {
            if let Some(response) = self.poll() {
                return Ok(response);
            }
            if start.elapsed() > timeout {
                return Err(Error::Protocol("response timeout".to_string()));
            }
            std::hint::spin_loop();
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
