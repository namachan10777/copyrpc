//! Main RPC implementation.
//!
//! The Rpc struct provides the primary API for eRPC operations.

use std::cell::{Cell, RefCell};

use mlx5::device::Context;

use crate::buffer::BufferPool;
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
pub struct IncomingRequest {
    /// Request type (application-defined).
    pub req_type: u8,
    /// Request data (payload).
    pub data: Vec<u8>,
    /// Session number that sent this request.
    pub session_num: u16,
    /// Request number for correlating with response.
    pub req_num: u64,
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
    /// Receive buffer pool.
    recv_buffers: RefCell<BufferPool>,
    /// Send buffer pool.
    send_buffers: RefCell<BufferPool>,
    /// MTU in bytes.
    mtu: usize,
    /// Number of currently posted receive buffers.
    posted_recv_count: Cell<usize>,
    /// Threshold for batch reposting receive buffers (num_recv_buffers / 2).
    recv_repost_threshold: usize,
    /// Reusable buffer for batch recv posting to avoid allocation.
    recv_post_buf: RefCell<Vec<(usize, u64, u32, u32)>>,
}

/// Pending request state.
struct PendingRequest<U> {
    /// User data to pass to on_response callback.
    user_data: Option<U>,
    /// Request buffer indices for all packets (for retransmission).
    req_buf_indices: Vec<usize>,
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
        let recv_buffers = BufferPool::new(config.num_recv_buffers, recv_buf_size, pd)?;
        let send_buffers = BufferPool::new((config.max_send_wr as usize) * 4, mtu, pd)?;

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
        let buf_idx = {
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
            buf.set_len(PKT_HDR_SIZE + SM_PKT_HDR_SIZE);

            buf_idx
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

        {
            let send_buffers = self.send_buffers.borrow();
            let buf = send_buffers.get(buf_idx).ok_or(Error::InvalidPacket)?;
            self.transport.post_send(av, buf, entry)?;
        }
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
        buf.set_len(PKT_HDR_SIZE + data.len());

        // Verify buffer is registered
        let _ = buf.lkey().ok_or_else(|| {
            Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Buffer not registered",
            ))
        })?;

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
    fn update_next_send_time(&self, session: SessionHandle, bytes_sent: usize) {
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
                        let now = current_time_us();
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
    pub fn call(
        &self,
        session: SessionHandle,
        req_type: u8,
        data: &[u8],
        user_data: U,
    ) -> Result<u64> {
        // Phase 1: Session setup
        let (req_num, sslot_idx, remote_session_num) = {
            let mut sessions = self.sessions.borrow_mut();
            let sess = sessions
                .get_mut(session)
                .ok_or(Error::SessionNotFound(session.session_num()))?;

            if !sess.is_connected() {
                return Err(Error::SessionNotConnected(session.session_num()));
            }

            // Find a free slot
            let sslot_idx = sess.alloc_sslot().ok_or(Error::NoAvailableSlots)?;

            // Check credits
            sess.consume_credit()?;

            // Get request number
            let req_num = sess.next_req_num();
            let remote_session_num = sess.remote_session_num;

            (req_num, sslot_idx, remote_session_num)
        };

        // Phase 2: Calculate packet count and prepare buffers
        let msg_size = data.len();
        let data_per_pkt = self.mtu - PKT_HDR_SIZE;
        let num_pkts = PktHdr::calc_num_pkts(msg_size, self.mtu);

        // Allocate and prepare buffers for all packets
        let mut buf_indices = Vec::with_capacity(num_pkts as usize);
        for pkt_num in 0..num_pkts {
            let offset = (pkt_num as usize) * data_per_pkt;
            let pkt_data_len = if pkt_num == num_pkts - 1 {
                // Last packet
                msg_size.saturating_sub(offset)
            } else {
                data_per_pkt
            };
            let pkt_data = &data[offset..offset + pkt_data_len];

            // Determine packet type
            let pkt_type = if num_pkts == 1 {
                PktType::ReqForResp
            } else {
                PktType::Req
            };

            let hdr = PktHdr::new(
                req_type,
                msg_size,
                remote_session_num,
                pkt_type,
                pkt_num,
                req_num,
            );

            let buf_info = self.prepare_send_buffer(pkt_data, &hdr)?;
            buf_indices.push(buf_info.idx);
        }

        // Phase 3: Update session state
        {
            let mut sessions = self.sessions.borrow_mut();
            let sess = sessions.get_mut(session).unwrap();

            let pending = PendingRequest {
                user_data: Some(user_data),
                req_buf_indices: buf_indices.clone(),
            };

            let sslot = sess.sslot_mut(sslot_idx).unwrap();
            sslot.start_request(req_num, req_type, pending);
            sslot.req_msg_size = msg_size;
            sslot.req_num_pkts = num_pkts;
            sslot.req_pkts_sent = 0;
            sslot.req_buf_indices = buf_indices.clone();
            sslot.tx_ts = current_time_us();
            sslot.wait_response(1); // Will be updated when response arrives

            // Register req_num -> sslot_idx for O(1) lookup
            sess.register_req_num(req_num, sslot_idx);
        }

        // Phase 4: Post send for all packets
        let av = {
            let sessions = self.sessions.borrow();
            let sess = sessions.get(session).ok_or(Error::SessionNotFound(session.session_num()))?;
            let ah = sess.ah.as_ref().ok_or(Error::SessionNotConnected(session.session_num()))?;
            UdTransport::ah_to_av(ah)
        };

        for buf_idx in &buf_indices {
            let entry = TransportEntry {
                buf_idx: *buf_idx,
                session_num: session.session_num(),
                context: req_num,
                buf_type: BufferType::Request, // Keep until response received
            };

            let send_buffers = self.send_buffers.borrow();
            let buf = send_buffers.get(*buf_idx).ok_or(Error::InvalidPacket)?;
            self.transport.post_send(av, buf, entry)?;
        }
        // Doorbell is batched in run_event_loop_once()

        // Update packets sent count and rate limiting
        {
            let mut sessions = self.sessions.borrow_mut();
            let sess = sessions.get_mut(session).unwrap();
            let sslot = sess.sslot_mut(sslot_idx).unwrap();
            sslot.req_pkts_sent = num_pkts;
        }

        // Update next send time based on Timely rate
        self.update_next_send_time(session, msg_size + PKT_HDR_SIZE * (num_pkts as usize));

        // Phase 5: Start timer
        let timer_entry = TimerEntry {
            session_num: session.session_num(),
            sslot_idx,
            req_num,
            expires_at: current_time_us() + self.config.rto_us,
        };
        self.timing_wheel.borrow_mut().insert(timer_entry);

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
    /// # Arguments
    /// * `request` - The incoming request to respond to
    /// * `data` - Response payload
    pub fn reply(&self, request: &IncomingRequest, data: &[u8]) -> Result<()> {
        self.send_response(request.session_num, request.req_num, request.req_type, data)
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
            let buf = send_buffers.get(buf_info.idx).ok_or(Error::InvalidPacket)?;
            self.transport.post_send(av, buf, entry)?;
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
        let buf_idx = {
            let mut send_buffers = self.send_buffers.borrow_mut();
            let (buf_idx, buf) = send_buffers.alloc().ok_or(Error::RequestQueueFull)?;

            unsafe {
                hdr.write_to(buf.as_mut_ptr());
            }
            buf.set_len(PKT_HDR_SIZE);

            buf_idx
        };

        // Post send
        let av = {
            let sessions = self.sessions.borrow();
            let handle = SessionHandle(session_num);
            let sess = sessions.get(handle).ok_or(Error::SessionNotFound(session_num))?;
            let ah = sess.ah.as_ref().ok_or(Error::SessionNotConnected(session_num))?;
            UdTransport::ah_to_av(ah)
        };

        {
            let entry = TransportEntry {
                buf_idx,
                session_num,
                context: 0,
                buf_type: BufferType::Response, // Free on send completion
            };

            let send_buffers = self.send_buffers.borrow();
            let buf = send_buffers.get(buf_idx).ok_or(Error::InvalidPacket)?;
            self.transport.post_send(av, buf, entry)?;
        }
        // Doorbell is batched in run_event_loop_once()

        Ok(())
    }

    /// Post receive buffers to the transport.
    fn post_recv_buffers(&self) -> Result<()> {
        let mut recv_buffers = self.recv_buffers.borrow_mut();

        // Collect buffer info first
        let mut buf_infos = Vec::new();
        while let Some((idx, buf)) = recv_buffers.alloc() {
            let lkey = buf.lkey().ok_or_else(|| {
                Error::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Buffer not registered",
                ))
            })?;
            buf_infos.push((idx, buf.addr(), buf.capacity() as u32, lkey));
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
    fn process_recv(&self, buf_idx: usize, byte_cnt: u32) -> Result<()> {
        // Read packet data
        let (hdr, payload) = {
            let recv_buffers = self.recv_buffers.borrow();
            let buf = recv_buffers.get(buf_idx).ok_or(Error::InvalidPacket)?;

            if (byte_cnt as usize) < GRH_SIZE + PKT_HDR_SIZE {
                return Err(Error::BufferTooSmall {
                    required: GRH_SIZE + PKT_HDR_SIZE,
                    available: byte_cnt as usize,
                });
            }

            // Read header
            let hdr_ptr = unsafe { buf.as_ptr().add(GRH_SIZE) };
            let hdr = unsafe { PktHdr::read_from(hdr_ptr) };
            hdr.validate()?;

            // Copy payload (single copy, no zero-init)
            let payload_offset = GRH_SIZE + PKT_HDR_SIZE;
            let payload_len = (byte_cnt as usize).saturating_sub(payload_offset);
            let payload = if payload_len > 0 {
                let mut payload = Vec::with_capacity(payload_len);
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        buf.as_ptr().add(payload_offset),
                        payload.as_mut_ptr(),
                        payload_len,
                    );
                    payload.set_len(payload_len);
                }
                payload
            } else {
                Vec::new()
            };

            (hdr, payload)
        };

        // Return buffer to pool
        self.recv_buffers.borrow_mut().free(buf_idx);

        // Decrement posted recv count (batch repost will be done in run_event_loop_once)
        self.posted_recv_count.set(self.posted_recv_count.get().saturating_sub(1));

        // Check if this is an SM (Session Management) packet
        if hdr.req_type() == SM_REQ_TYPE {
            return self.handle_sm_packet(&hdr, &payload);
        }

        // Process based on packet type
        match hdr.pkt_type() {
            PktType::Req | PktType::ReqForResp => {
                // Pass payload by value - avoids second copy in handle_request
                self.handle_request(&hdr, payload)?;
            }
            PktType::Resp => {
                self.handle_response(&hdr, &payload)?;
            }
            PktType::CreditReturn => {
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
            while let Some((idx, buf)) = recv_buffers.alloc() {
                if let Some(lkey) = buf.lkey() {
                    buf_infos.push((idx, buf.addr(), buf.capacity() as u32, lkey));
                }
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

    /// Handle an incoming request by pushing it to the recv_queue.
    fn handle_request(&self, hdr: &PktHdr, payload: Vec<u8>) -> Result<()> {
        self.recv_queue.borrow_mut().push(IncomingRequest {
            req_type: hdr.req_type(),
            data: payload,  // Move, no copy
            session_num: hdr.dest_session_num(),
            req_num: hdr.req_num(),
        });
        Ok(())
    }

    /// Handle an incoming response.
    fn handle_response(&self, hdr: &PktHdr, payload: &[u8]) -> Result<()> {
        let dest_session = hdr.dest_session_num();
        let pkt_num = hdr.pkt_num();
        let msg_size = hdr.msg_size();
        let num_pkts = PktHdr::calc_num_pkts(msg_size, self.mtu);

        let (is_complete, user_data, data, req_buf_indices) = {
            let mut sessions = self.sessions.borrow_mut();
            let handle = SessionHandle(dest_session);
            let sess = sessions.get_mut(handle).ok_or(Error::SessionNotFound(dest_session))?;

            // Find the matching slot
            // Note: Slot may not be found if this is a duplicate response after
            // the original response was already processed. This is normal behavior.
            let sslot_idx = match sess.find_sslot_by_req_num(hdr.req_num()) {
                Some(idx) => idx,
                None => return Ok(()), // Silently drop duplicate/stale response
            };

            // For multi-packet responses
            if num_pkts > 1 {
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
                    // Cancel the retransmission timer
                    self.timing_wheel.borrow_mut().cancel(
                        dest_session,
                        sslot_idx,
                        hdr.req_num(),
                    );

                    // Return credit
                    sess.return_credit();

                    // Get the slot again and extract data
                    let sslot = sess.sslot_mut(sslot_idx).unwrap();
                    let tx_ts = sslot.tx_ts;
                    let req_num_for_unregister = sslot.req_num;
                    let pending = sslot.user_data.take();
                    let resp_data = sslot.take_resp_buf();

                    // Extract request buffer indices and user_data from pending
                    let (ud, req_bufs) = pending
                        .map(|p| (p.user_data, p.req_buf_indices))
                        .unwrap_or((None, Vec::new()));

                    // Mark slot as free
                    sslot.reset();

                    // Unregister req_num from HashMap
                    sess.unregister_req_num(req_num_for_unregister);

                    // Update Timely congestion control with RTT measurement
                    if tx_ts > 0 {
                        let rtt = current_time_us().saturating_sub(tx_ts);
                        if let Some(ref cc_state) = sess.cc_state {
                            cc_state.update(rtt);
                        }
                    }

                    (true, ud, resp_data, req_bufs)
                } else {
                    (false, None, None, Vec::new())
                }
            } else {
                // Single-packet response
                // Cancel the retransmission timer
                self.timing_wheel.borrow_mut().cancel(
                    dest_session,
                    sslot_idx,
                    hdr.req_num(),
                );

                // Return credit
                sess.return_credit();

                // Get the slot and extract user_data
                let sslot = sess.sslot_mut(sslot_idx).unwrap();
                let tx_ts = sslot.tx_ts;
                let req_num_for_unregister = sslot.req_num;
                let pending = sslot.user_data.take();

                // Extract request buffer indices and user_data from pending
                let (ud, req_bufs) = pending
                    .map(|p| (p.user_data, p.req_buf_indices))
                    .unwrap_or((None, Vec::new()));

                // Mark slot as free
                sslot.reset();

                // Unregister req_num from HashMap
                sess.unregister_req_num(req_num_for_unregister);

                // Update Timely congestion control with RTT measurement
                if tx_ts > 0 {
                    let rtt = current_time_us().saturating_sub(tx_ts);
                    if let Some(ref cc_state) = sess.cc_state {
                        cc_state.update(rtt);
                    }
                }

                (true, ud, None, req_bufs)
            }
        };

        // Free all request buffers now that response is received
        {
            let mut send_buffers = self.send_buffers.borrow_mut();
            for buf_idx in req_buf_indices {
                send_buffers.free(buf_idx);
            }
        }

        // Call on_response callback outside of borrow
        if is_complete {
            if let Some(ud) = user_data {
                if let Some(resp_data) = data {
                    // Multi-packet response
                    (self.on_response)(ud, &resp_data);
                } else {
                    // Single-packet response
                    (self.on_response)(ud, payload);
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
    fn process_timeouts(&self) {
        let now = current_time_us();
        let expired = self.timing_wheel.borrow_mut().advance(now);

        for entry in expired {
            self.handle_timeout(entry, now);
        }
    }

    /// Handle a single timeout.
    fn handle_timeout(&self, entry: TimerEntry, now: u64) {
        let should_retransmit: Option<Vec<usize>> = {
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

                            // Unregister req_num from HashMap
                            sess.unregister_req_num(entry.req_num);

                            if let Some(p) = pending {
                                let mut send_buffers = self.send_buffers.borrow_mut();
                                for buf_idx in p.req_buf_indices {
                                    send_buffers.free(buf_idx);
                                }
                            }
                            None
                        } else {
                            // Need to retransmit all request packets
                            sslot.user_data.as_ref().map(|p| p.req_buf_indices.clone())
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
        if let Some(req_buf_indices) = should_retransmit {
            let _ = self.retransmit(entry.session_num, &req_buf_indices, entry.sslot_idx, entry.req_num, now);
        }
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
        for &req_buf_idx in req_buf_indices {
            let entry = TransportEntry {
                buf_idx: req_buf_idx,
                session_num,
                context: req_num,
                buf_type: BufferType::Request, // Keep until response received
            };

            {
                let send_buffers = self.send_buffers.borrow();
                let buf = send_buffers.get(req_buf_idx).ok_or(Error::InvalidPacket)?;
                self.transport.post_send(av, buf, entry)?;
            }
        }
        // Doorbell is batched in run_event_loop_once()

        // Re-arm timer
        let timer_entry = TimerEntry {
            session_num,
            sslot_idx,
            req_num,
            expires_at: now + self.config.rto_us,
        };
        self.timing_wheel.borrow_mut().insert(timer_entry);

        Ok(())
    }

    /// Run one iteration of the event loop.
    ///
    /// Returns the number of events processed.
    #[inline]
    pub fn run_event_loop_once(&self) -> usize {
        let mut events = 0;

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
                if let Err(e) = self.process_recv(comp.buf_idx, comp.byte_cnt) {
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
        self.process_timeouts();

        // Batch doorbell: ring both SQ and RQ doorbells once per event loop iteration
        self.transport.ring_sq_doorbell();
        self.transport.ring_rq_doorbell();

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
