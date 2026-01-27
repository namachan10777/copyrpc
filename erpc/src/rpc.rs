//! Main RPC implementation.
//!
//! The Rpc struct provides the primary API for eRPC operations.

use std::cell::{Cell, RefCell};

use mlx5::device::Context;

use crate::buffer::ZeroCopyPool;
use crate::config::RpcConfig;
use crate::error::{Error, Result};
use crate::packet::{PktHdr, PktType, SmPktHdr, SmPktType, PKT_HDR_SIZE, SM_PKT_HDR_SIZE};
use crate::session::{SessionHandle, SessionState, SessionTable};
use crate::timing::{TimerEntry, TimingWheel, current_time_us};
use crate::transport::{BufferType, GRH_SIZE, RemoteInfo, TransportEntry, UdTransport};

/// Reserved request type for Session Management packets.
const SM_REQ_TYPE: u8 = 0xFF;

/// Response callback type (copyrpc-style).
pub type OnResponse<U> = Box<dyn Fn(U, &[u8])>;

/// Incoming request from a client (copyrpc-style API).
///
/// Zero-copy design: the request holds a reference to the receive buffer
/// instead of copying the data. Call `data()` to access the payload,
/// and the buffer is automatically released when `reply()` is called.
pub struct IncomingRequest {
    /// Request type (application-defined).
    pub req_type: u8,
    /// Receive buffer index (for zero-copy access).
    buf_idx: usize,
    /// Offset to payload within the buffer (after GRH + PKT_HDR).
    payload_offset: usize,
    /// Payload length.
    payload_len: usize,
    /// Session number that sent this request.
    pub session_num: u16,
    /// Request number for correlating with response.
    pub req_num: u64,
}

impl IncomingRequest {
    /// Get the payload data as a slice.
    ///
    /// This returns a reference to the receive buffer without copying.
    /// The buffer remains valid until `reply()` is called.
    #[inline]
    pub fn data<'a, U: 'static>(&self, rpc: &'a Rpc<U>) -> &'a [u8] {
        rpc.get_recv_buffer_slice(self.buf_idx, self.payload_offset, self.payload_len)
    }
}

/// eRPC instance.
///
/// The main struct for eRPC operations. Manages sessions, buffers,
/// and the event loop.
///
/// Generic parameter `U` is the user data type passed to call() and
/// returned in the on_response callback.
pub struct Rpc<U>
where
    U: 'static,
{
    /// UD transport layer.
    transport: UdTransport,
    /// Session table.
    sessions: RefCell<SessionTable<PendingRequest<U>>>,
    /// Response callback (copyrpc-style).
    on_response: OnResponse<U>,
    /// Receive queue for incoming requests.
    recv_queue: RefCell<Vec<IncomingRequest>>,
    /// Configuration.
    config: RpcConfig,
    /// Timing wheel for retransmission timeouts.
    timing_wheel: RefCell<TimingWheel>,
    /// Receive buffer pool (zero-copy).
    recv_buffers: RefCell<ZeroCopyPool>,
    /// Send buffer pool (zero-copy).
    send_buffers: RefCell<ZeroCopyPool>,
    /// MTU in bytes.
    mtu: usize,
    /// Number of currently posted receive buffers.
    posted_recv_count: Cell<usize>,
    /// Threshold for batch reposting receive buffers (num_recv_buffers / 2).
    recv_repost_threshold: usize,
    /// Reusable buffer for batch recv posting to avoid allocation.
    recv_post_buf: RefCell<Vec<(usize, u64, u32, u32)>>,
    /// Reusable buffer for expired timers to avoid allocation in event loop.
    expired_timers: RefCell<Vec<TimerEntry>>,
}

/// Pending request state.
struct PendingRequest<U> {
    /// User data to pass to on_response callback.
    user_data: Option<U>,
    /// Single packet buffer index (avoids Vec allocation for 99% of requests).
    single_buf_idx: Option<usize>,
    /// Multi-packet buffer indices (only allocated for large requests).
    multi_buf_indices: Option<Vec<usize>>,
}

impl<U> PendingRequest<U> {
    /// Create a new pending request for a single-packet request.
    #[inline]
    fn new_single(user_data: U, buf_idx: usize) -> Self {
        Self {
            user_data: Some(user_data),
            single_buf_idx: Some(buf_idx),
            multi_buf_indices: None,
        }
    }

    /// Create a new pending request for a multi-packet request.
    #[inline]
    fn new_multi(user_data: U, buf_indices: Vec<usize>) -> Self {
        Self {
            user_data: Some(user_data),
            single_buf_idx: None,
            multi_buf_indices: Some(buf_indices),
        }
    }
}

/// Buffer index for tracking allocated send buffers.
struct BufInfo {
    idx: usize,
}

impl<U: 'static> Rpc<U> {
    /// Create a new RPC instance.
    ///
    /// # Arguments
    /// * `ctx` - RDMA device context
    /// * `port` - Port number
    /// * `config` - RPC configuration
    /// * `on_response` - Callback invoked when a response is received for a call()
    pub fn new<F>(ctx: &Context, port: u8, config: RpcConfig, on_response: F) -> Result<Self>
    where
        F: Fn(U, &[u8]) + 'static,
    {
        let transport = UdTransport::new(ctx, port, &config)?;
        let mtu = transport.mtu();
        let pd = transport.pd();

        // Create buffer pools
        // send_buffers needs to be large enough to hold in-flight requests/responses
        // until send completions are processed. Use 4x max_send_wr to provide headroom.
        let recv_buf_size = mtu + GRH_SIZE;
        // Use ZeroCopyPool for receive buffers (single MR, zero-copy access)
        let recv_buffers = ZeroCopyPool::new(config.num_recv_buffers, recv_buf_size, pd)?;
        // Use ZeroCopyPool for send buffers (single MR, zero-copy access)
        let send_buffers = ZeroCopyPool::new((config.max_send_wr as usize) * 4, mtu, pd)?;

        let mut timing_wheel = TimingWheel::default_for_rpc();
        timing_wheel.init(current_time_us());

        let num_recv_buffers = config.num_recv_buffers;
        // Repost threshold: when posted count drops to 1/4, batch repost all free buffers
        // Balance between batch efficiency and avoiding RQ starvation
        let recv_repost_threshold = num_recv_buffers / 4;
        let rpc = Self {
            transport,
            sessions: RefCell::new(SessionTable::new(config.max_sessions)),
            on_response: Box::new(on_response),
            recv_queue: RefCell::new(Vec::new()),
            config,
            timing_wheel: RefCell::new(timing_wheel),
            recv_buffers: RefCell::new(recv_buffers),
            send_buffers: RefCell::new(send_buffers),
            mtu,
            posted_recv_count: Cell::new(0),
            recv_repost_threshold,
            recv_post_buf: RefCell::new(Vec::with_capacity(num_recv_buffers)),
            expired_timers: RefCell::new(Vec::new()),
        };

        // Post initial receive buffers
        rpc.post_recv_buffers()?;
        // After initial post, all buffers are posted
        rpc.posted_recv_count.set(num_recv_buffers);

        Ok(rpc)
    }

    /// Get the local endpoint information.
    pub fn local_info(&self) -> crate::transport::LocalInfo {
        self.transport.local_info()
    }

    /// Create a new session to a remote endpoint.
    ///
    /// This initiates a session handshake by sending a ConnectRequest.
    /// The session is not fully connected until a ConnectResponse is received.
    pub fn create_session(&self, remote: &RemoteInfo) -> Result<SessionHandle> {
        let local_info = self.transport.local_info();
        let handle = {
            let mut sessions = self.sessions.borrow_mut();
            let handle = sessions.create_session(*remote, &self.config)?;

            // Create address handle for the session
            let session = sessions.get_mut(handle).unwrap();
            let ah = self.transport.create_ah(remote)?;
            session.set_ah(ah);
            session.state = SessionState::Connecting;

            handle
        };

        // Send ConnectRequest
        let sm_hdr = SmPktHdr::new(
            SmPktType::ConnectRequest,
            handle.session_num(),
            0, // Server session num not known yet
            local_info.qpn,
            local_info.lid,
        );

        self.send_sm_packet(handle, &sm_hdr)?;

        Ok(handle)
    }

    /// Accept incoming session requests (server-side).
    ///
    /// This should be called after setting up the RPC to allow incoming connections.
    pub fn accept_sessions(&self) -> bool {
        // Sessions are automatically accepted in handle_sm_packet
        // This method exists for API completeness
        true
    }

    /// Check if a session is connected.
    pub fn is_session_connected(&self, handle: SessionHandle) -> bool {
        let sessions = self.sessions.borrow();
        sessions.get(handle).map_or(false, |s| s.is_connected())
    }

    /// Prepare and send an SM packet.
    fn send_sm_packet(&self, session: SessionHandle, sm_hdr: &SmPktHdr) -> Result<()> {
        // Prepare SM packet buffer
        let (buf_idx, addr, lkey) = {
            let mut send_buffers = self.send_buffers.borrow_mut();
            let (buf_idx, buf) = send_buffers.alloc().ok_or(Error::RequestQueueFull)?;

            // Write SM header with SM_REQ_TYPE marker in a wrapper PktHdr
            let pkt_hdr = PktHdr::new(
                SM_REQ_TYPE,
                SM_PKT_HDR_SIZE,
                sm_hdr.client_session_num,
                PktType::Req,
                0,
                0,
            );

            unsafe {
                pkt_hdr.write_to(buf.as_mut_ptr());
                sm_hdr.write_to(buf.as_mut_ptr().add(PKT_HDR_SIZE));
            }

            let addr = send_buffers.slot_addr(buf_idx);
            let lkey = send_buffers.lkey();
            (buf_idx, addr, lkey)
        };

        // Post send
        let av = {
            let sessions = self.sessions.borrow();
            let sess = sessions.get(session).ok_or(Error::SessionNotFound(session.session_num()))?;
            let ah = sess.ah.as_ref().ok_or(Error::SessionNotConnected(session.session_num()))?;
            UdTransport::ah_to_av(ah)
        };

        let entry = TransportEntry {
            buf_idx,
            session_num: session.session_num(),
            context: 0,
            buf_type: BufferType::Response, // SM packets can be freed immediately
        };

        let len = (PKT_HDR_SIZE + SM_PKT_HDR_SIZE) as u32;
        self.transport.post_send_raw(av, addr, len, lkey, entry)?;
        // Doorbell is batched in run_event_loop_once()

        Ok(())
    }

    /// Allocate and prepare a send buffer.
    fn prepare_send_buffer(&self, data: &[u8], hdr: &PktHdr) -> Result<BufInfo> {
        let mut send_buffers = self.send_buffers.borrow_mut();
        let (buf_idx, buf) = send_buffers.alloc().ok_or(Error::RequestQueueFull)?;

        // Write header and data to buffer
        unsafe {
            hdr.write_to(buf.as_mut_ptr());
            std::ptr::copy_nonoverlapping(
                data.as_ptr(),
                buf.as_mut_ptr().add(PKT_HDR_SIZE),
                data.len(),
            );
        }

        Ok(BufInfo { idx: buf_idx })
    }

    /// Check if sending is allowed now based on Timely rate limiting.
    /// Reserved for future use when blocking/queuing on rate limit is implemented.
    #[allow(dead_code)]
    fn can_send_now(&self, session: SessionHandle) -> bool {
        if !self.config.enable_cc {
            return true;
        }

        let sessions = self.sessions.borrow();
        if let Some(sess) = sessions.get(session) {
            if sess.cc_state.is_some() {
                return current_time_us() >= sess.next_send_time_us.get();
            }
        }
        true
    }

    /// Update the next allowed send time based on Timely rate and bytes sent.
    fn update_next_send_time(&self, session: SessionHandle, bytes_sent: usize, now: u64) {
        if !self.config.enable_cc {
            return;
        }

        let sessions = self.sessions.borrow();
        if let Some(sess) = sessions.get(session) {
            if let Some(ref cc) = sess.cc_state {
                let rate = cc.rate(); // packets per microsecond (Mpps)
                if rate > 0.0 {
                    // rate is in Mpps (millions of packets per second)
                    // Convert to bytes/μs: rate * MTU
                    let bytes_per_us = rate * (self.mtu as f64);
                    if bytes_per_us > 0.0 {
                        let interval_us = (bytes_sent as f64 / bytes_per_us) as u64;
                        sess.next_send_time_us.set(now + interval_us);
                    }
                }
            }
        }
    }

    /// Send an asynchronous RPC request (copyrpc-style API).
    ///
    /// When the response is received, the `on_response` callback (passed to `Rpc::new()`)
    /// will be called with the `user_data` and response payload.
    ///
    /// Returns the request number (req_num) on success.
    ///
    /// # Arguments
    /// * `session` - Session handle for the target endpoint
    /// * `req_type` - Application-defined request type
    /// * `data` - Request payload
    /// * `user_data` - User data to be passed to on_response callback
    #[inline]
    pub fn call(
        &self,
        session: SessionHandle,
        req_type: u8,
        data: &[u8],
        user_data: U,
    ) -> Result<u64> {
        // Phase 1: Session setup and get AV (single borrow)
        let (req_num, sslot_idx, remote_session_num, av) = {
            let mut sessions = self.sessions.borrow_mut();
            let sess = sessions
                .get_mut(session)
                .ok_or(Error::SessionNotFound(session.session_num()))?;

            if !sess.is_connected() {
                return Err(Error::SessionNotConnected(session.session_num()));
            }

            // Get request number first (eRPC pattern: sslot_idx = req_num % req_window)
            let req_num = sess.next_req_num();

            // Allocate slot using fixed assignment
            let sslot_idx = sess.alloc_sslot(req_num).ok_or(Error::NoAvailableSlots)?;

            // Check credits
            sess.consume_credit()?;

            let remote_session_num = sess.remote_session_num;

            // Get AV while we have the session borrowed
            let ah = sess.ah.as_ref().ok_or(Error::SessionNotConnected(session.session_num()))?;
            let av = UdTransport::ah_to_av(ah);

            (req_num, sslot_idx, remote_session_num, av)
        };

        // Phase 2: Calculate packet count and prepare buffers
        let msg_size = data.len();
        let data_per_pkt = self.mtu - PKT_HDR_SIZE;
        let num_pkts = PktHdr::calc_num_pkts(msg_size, self.mtu);

        // Get timestamp once for this call
        let now = current_time_us();

        // Single-packet fast path (99% of requests)
        if num_pkts == 1 {
            let hdr = PktHdr::new(
                req_type,
                msg_size,
                remote_session_num,
                PktType::ReqForResp,
                0,
                req_num,
            );

            let buf_idx = self.prepare_send_buffer(data, &hdr)?.idx;

            // Post send
            {
                let send_buffers = self.send_buffers.borrow();
                let lkey = send_buffers.lkey();
                let pkt_len = (PKT_HDR_SIZE + msg_size) as u32;
                let entry = TransportEntry {
                    buf_idx,
                    session_num: session.session_num(),
                    context: req_num,
                    buf_type: BufferType::Request,
                };
                let addr = send_buffers.slot_addr(buf_idx);
                self.transport.post_send_raw(av, addr, pkt_len, lkey, entry)?;
            }

            // Update session state and start timer
            {
                let timer_entry = TimerEntry {
                    session_num: session.session_num(),
                    sslot_idx,
                    req_num,
                    expires_at: now + self.config.rto_us,
                };
                let wheel_slot = self.timing_wheel.borrow_mut().insert(timer_entry);

                let mut sessions = self.sessions.borrow_mut();
                let sess = sessions.get_mut(session).unwrap();
                let sslot = sess.sslot_mut(sslot_idx).unwrap();

                // Use single-packet pending request (no Vec allocation)
                let pending = PendingRequest::new_single(user_data, buf_idx);
                sslot.start_request(req_num, req_type, pending);
                sslot.req_msg_size = msg_size;
                sslot.req_num_pkts = 1;
                sslot.req_pkts_sent = 1;
                sslot.tx_ts = now;
                sslot.timer_wheel_slot = wheel_slot;
                sslot.wait_response(1);
            }

            self.update_next_send_time(session, msg_size + PKT_HDR_SIZE, now);
            return Ok(req_num);
        }

        // Multi-packet path (rare, large requests)
        let mut buf_indices = Vec::with_capacity(num_pkts as usize);
        for pkt_num in 0..num_pkts {
            let offset = (pkt_num as usize) * data_per_pkt;
            let pkt_data_len = if pkt_num == num_pkts - 1 {
                msg_size.saturating_sub(offset)
            } else {
                data_per_pkt
            };
            let pkt_data = &data[offset..offset + pkt_data_len];

            let hdr = PktHdr::new(
                req_type,
                msg_size,
                remote_session_num,
                PktType::Req,
                pkt_num,
                req_num,
            );

            let buf_info = self.prepare_send_buffer(pkt_data, &hdr)?;
            buf_indices.push(buf_info.idx);
        }

        // Post send for all packets
        {
            let send_buffers = self.send_buffers.borrow();
            let lkey = send_buffers.lkey();
            let pkt_len = (PKT_HDR_SIZE + data_per_pkt.min(msg_size)) as u32;
            for &buf_idx in &buf_indices {
                let entry = TransportEntry {
                    buf_idx,
                    session_num: session.session_num(),
                    context: req_num,
                    buf_type: BufferType::Request,
                };
                let addr = send_buffers.slot_addr(buf_idx);
                self.transport.post_send_raw(av, addr, pkt_len, lkey, entry)?;
            }
        }

        // Update session state and start timer
        {
            let timer_entry = TimerEntry {
                session_num: session.session_num(),
                sslot_idx,
                req_num,
                expires_at: now + self.config.rto_us,
            };
            let wheel_slot = self.timing_wheel.borrow_mut().insert(timer_entry);

            let mut sessions = self.sessions.borrow_mut();
            let sess = sessions.get_mut(session).unwrap();
            let sslot = sess.sslot_mut(sslot_idx).unwrap();

            // Use multi-packet pending request
            let pending = PendingRequest::new_multi(user_data, buf_indices);
            sslot.start_request(req_num, req_type, pending);
            sslot.req_msg_size = msg_size;
            sslot.req_num_pkts = num_pkts;
            sslot.req_pkts_sent = num_pkts;
            sslot.tx_ts = now;
            sslot.timer_wheel_slot = wheel_slot;
            sslot.wait_response(1);
        }

        self.update_next_send_time(session, msg_size + PKT_HDR_SIZE * (num_pkts as usize), now);

        Ok(req_num)
    }

    /// Poll for an incoming request (non-blocking, copyrpc-style API).
    ///
    /// Returns `Some(IncomingRequest)` if a request is available,
    /// `None` if the queue is empty.
    pub fn recv(&self) -> Option<IncomingRequest> {
        self.recv_queue.borrow_mut().pop()
    }

    /// Send a response to a request (copyrpc-style API).
    ///
    /// This also releases the receive buffer associated with the request.
    ///
    /// # Arguments
    /// * `request` - The incoming request to respond to
    /// * `data` - Response payload
    pub fn reply(&self, request: &IncomingRequest, data: &[u8]) -> Result<()> {
        let result = self.send_response(request.session_num, request.req_num, request.req_type, data);
        // Release the receive buffer after sending response
        self.release_recv_buffer(request.buf_idx);
        result
    }

    /// Release a receive buffer without sending a response.
    ///
    /// Use this when you want to drop a request without responding.
    pub fn release_request(&self, request: &IncomingRequest) {
        self.release_recv_buffer(request.buf_idx);
    }

    /// Internal: release a receive buffer and mark it for reposting.
    fn release_recv_buffer(&self, buf_idx: usize) {
        self.recv_buffers.borrow_mut().free(buf_idx);
    }

    /// Internal: get a slice of the receive buffer for zero-copy access.
    fn get_recv_buffer_slice(&self, buf_idx: usize, offset: usize, len: usize) -> &[u8] {
        let recv_buffers = self.recv_buffers.borrow();
        let slice = recv_buffers.slot_slice(buf_idx);
        // Safety: we return a reference that is valid as long as the buffer is not freed.
        // The caller must ensure the buffer is not freed while the slice is in use.
        // This is guaranteed because:
        // 1. The buffer is only freed in release_recv_buffer()
        // 2. release_recv_buffer() is only called after reply() or release_request()
        // 3. The IncomingRequest holds the buf_idx, preventing accidental double-free
        unsafe {
            std::slice::from_raw_parts(
                slice.as_ptr().add(offset),
                len.min(slice.len().saturating_sub(offset)),
            )
        }
    }

    /// Send a response to a request.
    ///
    /// Uses inline send for small responses (data copied to WQE),
    /// which avoids buffer allocation overhead.
    fn send_response(
        &self,
        session_num: u16,
        req_num: u64,
        req_type: u8,
        data: &[u8],
    ) -> Result<()> {
        // Phase 1: Get session info
        let remote_session_num = {
            let sessions = self.sessions.borrow();
            let handle = SessionHandle(session_num);
            let sess = sessions
                .get(handle)
                .ok_or(Error::SessionNotFound(session_num))?;
            sess.remote_session_num
        };

        // Phase 2: Prepare inline packet (header + data)
        let hdr = PktHdr::new(
            req_type,
            data.len(),
            remote_session_num,
            PktType::Resp,
            0,
            req_num,
        );

        // Build inline packet: header + data
        let total_len = PKT_HDR_SIZE + data.len();
        let max_inline = self.config.max_inline_data as usize;

        // Phase 3: Get address vector
        let av = {
            let sessions = self.sessions.borrow();
            let handle = SessionHandle(session_num);
            let sess = sessions.get(handle).ok_or(Error::SessionNotFound(session_num))?;
            let ah = sess.ah.as_ref().ok_or(Error::SessionNotConnected(session_num))?;
            UdTransport::ah_to_av(ah)
        };

        // Phase 4: Send - use buffered send for all responses
        // (inline with always signaled has higher CQ overhead than buffered)
        if false && total_len <= max_inline {
            // Build inline packet on stack
            let mut inline_buf = [0u8; 256]; // Stack buffer for inline data
            unsafe {
                hdr.write_to(inline_buf.as_mut_ptr());
                std::ptr::copy_nonoverlapping(
                    data.as_ptr(),
                    inline_buf.as_mut_ptr().add(PKT_HDR_SIZE),
                    data.len(),
                );
            }

            // Post inline send - uses signaled interval internally
            // buf_idx = usize::MAX indicates no buffer to free
            let entry = TransportEntry {
                buf_idx: usize::MAX,
                session_num,
                context: 0,
                buf_type: BufferType::Request, // Treated as no-op in completion (buf_idx check)
            };
            self.transport.post_send_inline(av, &inline_buf[..total_len], true, entry)?;
        } else {
            // Fall back to buffered send for large responses
            let buf_info = self.prepare_send_buffer(data, &hdr)?;

            let entry = TransportEntry {
                buf_idx: buf_info.idx,
                session_num,
                context: req_num,
                buf_type: BufferType::Response, // Free on send completion
            };

            let send_buffers = self.send_buffers.borrow();
            let addr = send_buffers.slot_addr(buf_info.idx);
            let lkey = send_buffers.lkey();
            self.transport.post_send_raw(av, addr, total_len as u32, lkey, entry)?;
        }
        // Doorbell is batched in run_event_loop_once()

        // Note: Credit is implicitly returned with the response packet.
        // The client's handle_response() calls sess.return_credit() upon receiving this.
        // Explicit CreditReturn packets are only needed for one-way messages without responses.

        Ok(())
    }

    /// Send a CreditReturn packet to return credits to the client.
    /// Used for one-way messages where no response is sent.
    #[allow(dead_code)]
    fn send_credit_return(&self, session_num: u16) -> Result<()> {
        // Get session info
        let remote_session_num = {
            let sessions = self.sessions.borrow();
            let handle = SessionHandle(session_num);
            let sess = sessions
                .get(handle)
                .ok_or(Error::SessionNotFound(session_num))?;
            sess.remote_session_num
        };

        // Prepare CreditReturn packet (header only, no payload)
        let hdr = PktHdr::new(
            0,                   // req_type (unused for CreditReturn)
            1,                   // msg_size: 1 credit being returned
            remote_session_num,
            PktType::CreditReturn,
            0,                   // pkt_num
            0,                   // req_num (unused)
        );

        // Allocate and prepare buffer
        let (buf_idx, addr, lkey) = {
            let mut send_buffers = self.send_buffers.borrow_mut();
            let (buf_idx, buf) = send_buffers.alloc().ok_or(Error::RequestQueueFull)?;

            unsafe {
                hdr.write_to(buf.as_mut_ptr());
            }

            let addr = send_buffers.slot_addr(buf_idx);
            let lkey = send_buffers.lkey();
            (buf_idx, addr, lkey)
        };

        // Post send
        let av = {
            let sessions = self.sessions.borrow();
            let handle = SessionHandle(session_num);
            let sess = sessions.get(handle).ok_or(Error::SessionNotFound(session_num))?;
            let ah = sess.ah.as_ref().ok_or(Error::SessionNotConnected(session_num))?;
            UdTransport::ah_to_av(ah)
        };

        let entry = TransportEntry {
            buf_idx,
            session_num,
            context: 0,
            buf_type: BufferType::Response, // Free on send completion
        };

        self.transport.post_send_raw(av, addr, PKT_HDR_SIZE as u32, lkey, entry)?;
        // Doorbell is batched in run_event_loop_once()

        Ok(())
    }

    /// Post receive buffers to the transport.
    fn post_recv_buffers(&self) -> Result<()> {
        let mut recv_buffers = self.recv_buffers.borrow_mut();
        let lkey = recv_buffers.lkey();
        let slot_size = recv_buffers.slot_size() as u32;

        // Collect buffer info first
        let mut buf_infos = Vec::new();
        while let Some((idx, _slice)) = recv_buffers.alloc() {
            let addr = recv_buffers.slot_addr(idx);
            buf_infos.push((idx, addr, slot_size, lkey));
        }
        drop(recv_buffers);

        // Post receives using the collected info
        for (idx, addr, capacity, lkey) in buf_infos {
            let entry = TransportEntry {
                buf_idx: idx,
                session_num: 0,
                context: 0,
                buf_type: BufferType::Request, // Receive buffers use default type
            };
            self.transport.post_recv_raw(addr, capacity, lkey, entry)?;
        }
        self.transport.ring_rq_doorbell();

        Ok(())
    }

    /// Process a received packet.
    #[inline]
    fn process_recv(&self, buf_idx: usize, byte_cnt: u32, batch_ts: u64) -> Result<()> {
        // Validate packet size and read header (zero-copy)
        let hdr = {
            let recv_buffers = self.recv_buffers.borrow();
            let buf_ptr = recv_buffers.slot_ptr(buf_idx);

            if (byte_cnt as usize) < GRH_SIZE + PKT_HDR_SIZE {
                // Invalid packet, free buffer and return error
                drop(recv_buffers);
                self.recv_buffers.borrow_mut().free(buf_idx);
                self.posted_recv_count.set(self.posted_recv_count.get().saturating_sub(1));
                return Err(Error::BufferTooSmall {
                    required: GRH_SIZE + PKT_HDR_SIZE,
                    available: byte_cnt as usize,
                });
            }

            // Read header
            let hdr_ptr = unsafe { buf_ptr.add(GRH_SIZE) };
            let hdr = unsafe { PktHdr::read_from(hdr_ptr) };
            hdr.validate()?;
            hdr
        };

        let payload_offset = GRH_SIZE + PKT_HDR_SIZE;
        let payload_len = (byte_cnt as usize).saturating_sub(payload_offset);

        // Check if this is an SM (Session Management) packet
        if hdr.req_type() == SM_REQ_TYPE {
            // SM packets: copy payload and free buffer immediately
            let payload = {
                let recv_buffers = self.recv_buffers.borrow();
                let buf_ptr = recv_buffers.slot_ptr(buf_idx);
                if payload_len > 0 {
                    let mut payload = Vec::with_capacity(payload_len);
                    unsafe {
                        std::ptr::copy_nonoverlapping(
                            buf_ptr.add(payload_offset),
                            payload.as_mut_ptr(),
                            payload_len,
                        );
                        payload.set_len(payload_len);
                    }
                    payload
                } else {
                    Vec::new()
                }
            };
            // Free buffer for SM packets
            self.recv_buffers.borrow_mut().free(buf_idx);
            self.posted_recv_count.set(self.posted_recv_count.get().saturating_sub(1));
            return self.handle_sm_packet(&hdr, &payload);
        }

        // Process based on packet type
        match hdr.pkt_type() {
            PktType::Req | PktType::ReqForResp => {
                // Zero-copy: pass buf_idx and offsets to handle_request
                // Buffer will be freed when reply() is called
                self.handle_request_zero_copy(&hdr, buf_idx, payload_offset, payload_len)?;
                // Decrement posted recv count (buffer is held by IncomingRequest)
                self.posted_recv_count.set(self.posted_recv_count.get().saturating_sub(1));
            }
            PktType::Resp => {
                // Response packets: copy payload and free buffer, then process
                // Copy is required to avoid holding recv_buffers borrow during callback
                let payload = {
                    let recv_buffers = self.recv_buffers.borrow();
                    let buf_ptr = recv_buffers.slot_ptr(buf_idx);
                    if payload_len > 0 {
                        let mut payload = Vec::with_capacity(payload_len);
                        unsafe {
                            std::ptr::copy_nonoverlapping(
                                buf_ptr.add(payload_offset),
                                payload.as_mut_ptr(),
                                payload_len,
                            );
                            payload.set_len(payload_len);
                        }
                        payload
                    } else {
                        Vec::new()
                    }
                };
                self.recv_buffers.borrow_mut().free(buf_idx);
                self.posted_recv_count.set(self.posted_recv_count.get().saturating_sub(1));

                // Check if single-packet response (most common case)
                let msg_size = hdr.msg_size();
                let num_pkts = PktHdr::calc_num_pkts(msg_size, self.mtu);

                if num_pkts == 1 {
                    // Single-packet response: optimized path
                    self.handle_response_single_packet(&hdr, &payload, batch_ts)?;
                } else {
                    // Multi-packet response: reassembly path
                    self.handle_response_multi_packet(&hdr, &payload, batch_ts)?;
                }
            }
            PktType::CreditReturn => {
                // Free buffer immediately for credit return
                self.recv_buffers.borrow_mut().free(buf_idx);
                self.posted_recv_count.set(self.posted_recv_count.get().saturating_sub(1));
                self.handle_credit_return(&hdr)?;
            }
        }

        Ok(())
    }

    /// Handle an SM (Session Management) packet.
    fn handle_sm_packet(&self, _pkt_hdr: &PktHdr, payload: &[u8]) -> Result<()> {
        if payload.len() < SM_PKT_HDR_SIZE {
            return Err(Error::BufferTooSmall {
                required: SM_PKT_HDR_SIZE,
                available: payload.len(),
            });
        }

        let sm_hdr = unsafe { SmPktHdr::read_from(payload.as_ptr()) };
        sm_hdr.validate()?;

        let sm_type = sm_hdr.pkt_type()?;
        match sm_type {
            SmPktType::ConnectRequest => {
                self.handle_connect_request(&sm_hdr)?;
            }
            SmPktType::ConnectResponse => {
                self.handle_connect_response(&sm_hdr)?;
            }
            SmPktType::DisconnectRequest | SmPktType::DisconnectResponse => {
                // Not implemented yet
            }
        }

        Ok(())
    }

    /// Handle a ConnectRequest from a client.
    fn handle_connect_request(&self, sm_hdr: &SmPktHdr) -> Result<()> {
        // Create remote info from SM header
        let remote = RemoteInfo {
            qpn: sm_hdr.qpn,
            qkey: self.config.qkey,
            lid: sm_hdr.lid,
        };

        let local_info = self.transport.local_info();

        // Create a new session for the client
        let handle = {
            let mut sessions = self.sessions.borrow_mut();
            let handle = sessions.create_session(remote, &self.config)?;

            let session = sessions.get_mut(handle).unwrap();
            let ah = self.transport.create_ah(&remote)?;
            session.set_ah(ah);
            // Set the remote session number from the client's local session number
            session.connect(sm_hdr.client_session_num);

            handle
        };

        // Send ConnectResponse
        let resp_hdr = SmPktHdr::new(
            SmPktType::ConnectResponse,
            sm_hdr.client_session_num, // Echo back client's session num
            handle.session_num(),      // Our local session num
            local_info.qpn,
            local_info.lid,
        );

        self.send_sm_packet(handle, &resp_hdr)?;

        Ok(())
    }

    /// Handle a ConnectResponse from a server.
    fn handle_connect_response(&self, sm_hdr: &SmPktHdr) -> Result<()> {
        let client_session_num = sm_hdr.client_session_num;
        let server_session_num = sm_hdr.server_session_num;

        let mut sessions = self.sessions.borrow_mut();
        let handle = SessionHandle(client_session_num);

        if let Some(session) = sessions.get_mut(handle) {
            if session.state == SessionState::Connecting {
                // Set the correct remote session number from the server
                session.connect(server_session_num);
            }
        }

        Ok(())
    }

    /// Batch repost all free receive buffers.
    /// Called when posted_recv_count drops below recv_repost_threshold.
    fn batch_repost_recv_buffers(&self) {
        // Use pre-allocated buffer to avoid allocation
        let mut buf_infos = self.recv_post_buf.borrow_mut();
        buf_infos.clear();

        // Collect buffer info first to avoid holding borrow during post
        {
            let mut recv_buffers = self.recv_buffers.borrow_mut();
            let lkey = recv_buffers.lkey();
            let slot_size = recv_buffers.slot_size() as u32;
            while let Some((idx, _slice)) = recv_buffers.alloc() {
                let addr = recv_buffers.slot_addr(idx);
                buf_infos.push((idx, addr, slot_size, lkey));
            }
        }

        // Post receives using the collected info
        let mut count = 0;
        for &(idx, addr, capacity, lkey) in buf_infos.iter() {
            let entry = TransportEntry {
                buf_idx: idx,
                session_num: 0,
                context: 0,
                buf_type: BufferType::Request, // Receive buffers use default type
            };
            if self.transport.post_recv_raw(addr, capacity, lkey, entry).is_ok() {
                count += 1;
            }
        }

        // Update posted count
        self.posted_recv_count.set(self.posted_recv_count.get() + count);
        // RQ doorbell is batched in run_event_loop_once()
    }

    /// Handle an incoming request by pushing it to the recv_queue (zero-copy).
    fn handle_request_zero_copy(
        &self,
        hdr: &PktHdr,
        buf_idx: usize,
        payload_offset: usize,
        payload_len: usize,
    ) -> Result<()> {
        self.recv_queue.borrow_mut().push(IncomingRequest {
            req_type: hdr.req_type(),
            buf_idx,
            payload_offset,
            payload_len,
            session_num: hdr.dest_session_num(),
            req_num: hdr.req_num(),
        });
        Ok(())
    }

    /// Handle a single-packet response.
    #[inline]
    fn handle_response_single_packet(
        &self,
        hdr: &PktHdr,
        payload: &[u8],
        batch_ts: u64,
    ) -> Result<()> {
        let dest_session = hdr.dest_session_num();

        let pending = {
            let mut sessions = self.sessions.borrow_mut();
            let handle = SessionHandle(dest_session);
            let sess = sessions.get_mut(handle).ok_or(Error::SessionNotFound(dest_session))?;

            // Find the matching slot
            let sslot_idx = match sess.find_sslot_by_req_num(hdr.req_num()) {
                Some(idx) => idx,
                None => return Ok(()), // Silently drop duplicate/stale response
            };

            // Return credit
            sess.return_credit();

            // Get the slot and extract user_data
            let sslot = sess.sslot_mut(sslot_idx).unwrap();

            // Cancel the retransmission timer (fast path with known wheel_slot)
            if let Some(wheel_slot) = sslot.timer_wheel_slot {
                self.timing_wheel.borrow_mut().cancel_fast(wheel_slot, hdr.req_num());
            }

            let tx_ts = sslot.tx_ts;
            let pending = sslot.user_data.take();

            // Mark slot as free
            sslot.reset();

            // Update Timely congestion control with RTT measurement
            if tx_ts > 0 {
                let rtt = batch_ts.saturating_sub(tx_ts);
                if let Some(ref cc_state) = sess.cc_state {
                    cc_state.update(rtt);
                }
            }

            pending
        };

        // Free request buffers now that response is received
        if let Some(ref p) = pending {
            let mut send_buffers = self.send_buffers.borrow_mut();
            if let Some(idx) = p.single_buf_idx {
                send_buffers.free(idx);
            }
        }

        // Call callback
        if let Some(p) = pending {
            if let Some(ud) = p.user_data {
                (self.on_response)(ud, payload);
            }
        }

        Ok(())
    }

    /// Handle a multi-packet response.
    fn handle_response_multi_packet(&self, hdr: &PktHdr, payload: &[u8], batch_ts: u64) -> Result<()> {
        let dest_session = hdr.dest_session_num();
        let pkt_num = hdr.pkt_num();
        let msg_size = hdr.msg_size();
        let num_pkts = PktHdr::calc_num_pkts(msg_size, self.mtu);

        let (is_complete, pending, data) = {
            let mut sessions = self.sessions.borrow_mut();
            let handle = SessionHandle(dest_session);
            let sess = sessions.get_mut(handle).ok_or(Error::SessionNotFound(dest_session))?;

            // Find the matching slot
            let sslot_idx = match sess.find_sslot_by_req_num(hdr.req_num()) {
                Some(idx) => idx,
                None => return Ok(()), // Silently drop duplicate/stale response
            };

            // Get the slot and process
            {
                let sslot = sess.sslot_mut(sslot_idx).unwrap();

                // Initialize response buffer on first packet
                if sslot.resp_buf.is_none() {
                    sslot.init_resp_buf(msg_size, num_pkts);
                }

                // Copy payload to reassembly buffer
                let data_per_pkt = self.mtu - PKT_HDR_SIZE;
                let offset = (pkt_num as usize) * data_per_pkt;

                if let Some(buf) = sslot.resp_buf_mut() {
                    let copy_len = payload.len().min(buf.len().saturating_sub(offset));
                    if copy_len > 0 && offset < buf.len() {
                        buf[offset..offset + copy_len].copy_from_slice(&payload[..copy_len]);
                    }
                }

                // Record received packet
                if !sslot.record_recv_pkt(pkt_num) {
                    // Duplicate packet, ignore
                    return Ok(());
                }
            }

            // Check completion after releasing sslot borrow
            let is_complete = sess.sslot(sslot_idx).unwrap().is_response_complete();

            if is_complete {
                // Return credit
                sess.return_credit();

                // Get the slot again and extract data
                let sslot = sess.sslot_mut(sslot_idx).unwrap();

                // Cancel the retransmission timer (fast path with known wheel_slot)
                if let Some(wheel_slot) = sslot.timer_wheel_slot {
                    self.timing_wheel.borrow_mut().cancel_fast(wheel_slot, hdr.req_num());
                }

                let tx_ts = sslot.tx_ts;
                let pending = sslot.user_data.take();
                let resp_data = sslot.take_resp_buf();

                // Mark slot as free
                sslot.reset();

                // Update Timely congestion control with RTT measurement
                if tx_ts > 0 {
                    let rtt = batch_ts.saturating_sub(tx_ts);
                    if let Some(ref cc_state) = sess.cc_state {
                        cc_state.update(rtt);
                    }
                }

                (true, pending, resp_data)
            } else {
                (false, None, None)
            }
        };

        // Free all request buffers now that response is received
        if let Some(ref p) = pending {
            let mut send_buffers = self.send_buffers.borrow_mut();
            // Free single buffer (common case, no iteration overhead)
            if let Some(idx) = p.single_buf_idx {
                send_buffers.free(idx);
            }
            // Free multi-packet buffers
            if let Some(ref indices) = p.multi_buf_indices {
                for &idx in indices {
                    send_buffers.free(idx);
                }
            }
        }

        // Call on_response callback outside of borrow
        if is_complete {
            if let Some(p) = pending {
                if let Some(ud) = p.user_data {
                    if let Some(resp_data) = data {
                        (self.on_response)(ud, &resp_data);
                    }
                }
            }
        }

        Ok(())
    }

    /// Handle a credit return packet.
    fn handle_credit_return(&self, hdr: &PktHdr) -> Result<()> {
        let mut sessions = self.sessions.borrow_mut();
        let handle = SessionHandle(hdr.dest_session_num());
        if let Some(sess) = sessions.get_mut(handle) {
            sess.return_credit();
        }
        Ok(())
    }

    /// Process retransmission timeouts.
    fn process_timeouts(&self, batch_ts: u64) {
        // Use pre-allocated buffer to avoid allocation in hot path
        let mut expired = self.expired_timers.borrow_mut();
        expired.clear();
        self.timing_wheel.borrow_mut().advance_into(batch_ts, &mut expired);

        for entry in expired.drain(..) {
            self.handle_timeout(entry, batch_ts);
        }
    }

    /// Handle a single timeout.
    fn handle_timeout(&self, entry: TimerEntry, now: u64) {
        // For retransmit: (single_buf_idx, multi_buf_indices)
        let should_retransmit: Option<(Option<usize>, Option<Vec<usize>>)> = {
            let mut sessions = self.sessions.borrow_mut();
            let handle = SessionHandle(entry.session_num);

            if let Some(sess) = sessions.get_mut(handle) {
                if let Some(sslot) = sess.sslot_mut(entry.sslot_idx) {
                    if sslot.req_num == entry.req_num && !sslot.is_free() {
                        sslot.retries += 1;

                        if sslot.retries > self.config.max_retries {
                            // Max retries exceeded, fail the request
                            // Note: user_data is dropped here without calling on_response
                            // (timeout failure case)
                            let pending = sslot.user_data.take();
                            sslot.reset();

                            if let Some(p) = pending {
                                let mut send_buffers = self.send_buffers.borrow_mut();
                                if let Some(idx) = p.single_buf_idx {
                                    send_buffers.free(idx);
                                }
                                if let Some(indices) = p.multi_buf_indices {
                                    for idx in indices {
                                        send_buffers.free(idx);
                                    }
                                }
                            }
                            None
                        } else {
                            // Need to retransmit all request packets
                            sslot.user_data.as_ref().map(|p| {
                                (p.single_buf_idx, p.multi_buf_indices.clone())
                            })
                        }
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            }
        };

        // Retransmit if needed
        if let Some((single_idx, multi_indices)) = should_retransmit {
            // Single-packet fast path
            if let Some(idx) = single_idx {
                let _ = self.retransmit_single(entry.session_num, idx, entry.sslot_idx, entry.req_num, now);
            }
            // Multi-packet path
            if let Some(ref indices) = multi_indices {
                let _ = self.retransmit(entry.session_num, indices, entry.sslot_idx, entry.req_num, now);
            }
        }
    }

    /// Retransmit a single-packet request.
    fn retransmit_single(&self, session_num: u16, buf_idx: usize, sslot_idx: usize, req_num: u64, now: u64) -> Result<()> {
        let av = {
            let sessions = self.sessions.borrow();
            let handle = SessionHandle(session_num);
            let sess = sessions.get(handle).ok_or(Error::SessionNotFound(session_num))?;
            let ah = sess.ah.as_ref().ok_or(Error::SessionNotConnected(session_num))?;
            UdTransport::ah_to_av(ah)
        };

        {
            let send_buffers = self.send_buffers.borrow();
            let lkey = send_buffers.lkey();
            let pkt_len = self.mtu as u32;
            let entry = TransportEntry {
                buf_idx,
                session_num,
                context: req_num,
                buf_type: BufferType::Request,
            };
            let addr = send_buffers.slot_addr(buf_idx);
            self.transport.post_send_raw(av, addr, pkt_len, lkey, entry)?;
        }

        // Re-arm timer
        let timer_entry = TimerEntry {
            session_num,
            sslot_idx,
            req_num,
            expires_at: now + self.config.rto_us,
        };
        let wheel_slot = self.timing_wheel.borrow_mut().insert(timer_entry);

        // Update timer_wheel_slot in sslot
        {
            let mut sessions = self.sessions.borrow_mut();
            let handle = SessionHandle(session_num);
            if let Some(sess) = sessions.get_mut(handle) {
                if let Some(sslot) = sess.sslot_mut(sslot_idx) {
                    sslot.timer_wheel_slot = wheel_slot;
                }
            }
        }

        Ok(())
    }

    /// Retransmit all packets of a request.
    fn retransmit(&self, session_num: u16, req_buf_indices: &[usize], sslot_idx: usize, req_num: u64, now: u64) -> Result<()> {
        // Get address vector before dropping session borrow
        let av = {
            let sessions = self.sessions.borrow();
            let handle = SessionHandle(session_num);
            let sess = sessions.get(handle).ok_or(Error::SessionNotFound(session_num))?;
            let ah = sess.ah.as_ref().ok_or(Error::SessionNotConnected(session_num))?;
            UdTransport::ah_to_av(ah)
        };

        // Retransmit all packets
        {
            let send_buffers = self.send_buffers.borrow();
            let lkey = send_buffers.lkey();
            // Use MTU as packet size (receiver uses header's msg_size to interpret)
            let pkt_len = self.mtu as u32;
            for &req_buf_idx in req_buf_indices {
                let entry = TransportEntry {
                    buf_idx: req_buf_idx,
                    session_num,
                    context: req_num,
                    buf_type: BufferType::Request, // Keep until response received
                };

                let addr = send_buffers.slot_addr(req_buf_idx);
                self.transport.post_send_raw(av, addr, pkt_len, lkey, entry)?;
            }
        }
        // Doorbell is batched in run_event_loop_once()

        // Re-arm timer and update wheel_slot in sslot
        let timer_entry = TimerEntry {
            session_num,
            sslot_idx,
            req_num,
            expires_at: now + self.config.rto_us,
        };
        let wheel_slot = self.timing_wheel.borrow_mut().insert(timer_entry);

        // Update timer_wheel_slot in sslot for next cancel_fast
        {
            let mut sessions = self.sessions.borrow_mut();
            let handle = SessionHandle(session_num);
            if let Some(sess) = sessions.get_mut(handle) {
                if let Some(sslot) = sess.sslot_mut(sslot_idx) {
                    sslot.timer_wheel_slot = wheel_slot;
                }
            }
        }

        Ok(())
    }

    /// Run one iteration of the event loop.
    ///
    /// Returns the number of events processed.
    #[inline]
    pub fn run_event_loop_once(&self) -> usize {
        let mut events = 0;

        // Get batch timestamp once for all operations in this iteration
        let batch_ts = current_time_us();

        // Poll and process send completions
        // Request buffers are kept until response is received (for retransmission)
        // Response buffers are freed immediately on send completion
        // Note: buf_idx == usize::MAX indicates inline send with no buffer to free
        events += self.transport.poll_send_completions(|comps| {
            for comp in comps {
                if comp.buf_type == BufferType::Response && comp.buf_idx != usize::MAX {
                    self.send_buffers.borrow_mut().free(comp.buf_idx);
                }
            }
            comps.len()
        });

        // Poll and process receive completions
        events += self.transport.poll_recv_completions(|comps| {
            for comp in comps {
                if let Err(e) = self.process_recv(comp.buf_idx, comp.byte_cnt, batch_ts) {
                    eprintln!("process_recv error: {:?}", e);
                }
            }
            comps.len()
        });

        // Batch repost receive buffers when below threshold
        if self.posted_recv_count.get() <= self.recv_repost_threshold {
            self.batch_repost_recv_buffers();
        }

        // Process timeouts
        self.process_timeouts(batch_ts);

        // Batch doorbell: ring doorbells only if there are pending operations
        if self.transport.pending_sends() > 0 {
            self.transport.ring_sq_doorbell();
        }
        if self.transport.pending_recvs() > 0 {
            self.transport.ring_rq_doorbell();
        }

        events
    }

    /// Get the MTU.
    pub fn mtu(&self) -> usize {
        self.mtu
    }

    /// Get the configuration.
    pub fn config(&self) -> &RpcConfig {
        &self.config
    }

    /// Get the number of active sessions.
    pub fn active_sessions(&self) -> usize {
        self.sessions.borrow().active_count()
    }
}
