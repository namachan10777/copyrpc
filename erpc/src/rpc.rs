//! Main RPC implementation.
//!
//! The Rpc struct provides the primary API for eRPC operations.

use std::cell::RefCell;

use mlx5::device::Context;

use crate::buffer::BufferPool;
use crate::config::RpcConfig;
use crate::error::{Error, Result};
use crate::packet::{PktHdr, PktType, PKT_HDR_SIZE};
use crate::session::{SessionHandle, SessionTable};
use crate::timing::{TimerEntry, TimingWheel, current_time_us};
use crate::transport::{GRH_SIZE, RemoteInfo, TransportEntry, UdTransport};

/// Request context passed to the request handler.
pub struct ReqContext<'a> {
    /// Request type.
    pub req_type: u8,
    /// Request data (excluding header).
    pub data: &'a [u8],
    /// Session number.
    pub session_num: u16,
    /// Request number.
    pub req_num: u64,
}

/// Response handle for sending responses.
pub struct RespHandle<'a> {
    rpc: &'a Rpc,
    session_num: u16,
    req_num: u64,
    req_type: u8,
}

impl RespHandle<'_> {
    /// Send the response.
    pub fn respond(self, data: &[u8]) -> Result<()> {
        self.rpc.send_response(self.session_num, self.req_num, self.req_type, data)
    }
}

/// Continuation for asynchronous response handling.
pub type Continuation<U> = Box<dyn FnOnce(U, &[u8])>;

/// Request handler function type.
pub type ReqHandler = Box<dyn Fn(ReqContext<'_>, RespHandle<'_>)>;

/// eRPC instance.
///
/// The main struct for eRPC operations. Manages sessions, buffers,
/// and the event loop.
pub struct Rpc {
    /// UD transport layer.
    transport: UdTransport,
    /// Session table.
    sessions: RefCell<SessionTable<PendingRequest>>,
    /// Request handler callback.
    req_handler: RefCell<Option<ReqHandler>>,
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
}

/// Pending request state.
struct PendingRequest {
    /// Continuation to call when response is received.
    continuation: Option<Continuation<()>>,
    /// Request buffer index (for retransmission).
    req_buf_idx: Option<usize>,
}

/// Buffer index for tracking allocated send buffers.
struct BufInfo {
    idx: usize,
}

impl Rpc {
    /// Create a new RPC instance.
    pub fn new(ctx: &Context, port: u8, config: RpcConfig) -> Result<Self> {
        let transport = UdTransport::new(ctx, port, &config)?;
        let mtu = transport.mtu();
        let pd = transport.pd();

        // Create buffer pools
        let recv_buf_size = mtu + GRH_SIZE;
        let recv_buffers = BufferPool::new(config.num_recv_buffers, recv_buf_size, pd)?;
        let send_buffers = BufferPool::new(config.max_send_wr as usize, mtu, pd)?;

        let mut timing_wheel = TimingWheel::default_for_rpc();
        timing_wheel.init(current_time_us());

        let rpc = Self {
            transport,
            sessions: RefCell::new(SessionTable::new(config.max_sessions)),
            req_handler: RefCell::new(None),
            config,
            timing_wheel: RefCell::new(timing_wheel),
            recv_buffers: RefCell::new(recv_buffers),
            send_buffers: RefCell::new(send_buffers),
            mtu,
        };

        // Post initial receive buffers
        rpc.post_recv_buffers()?;

        Ok(rpc)
    }

    /// Set the request handler.
    pub fn set_req_handler<F>(&self, handler: F)
    where
        F: Fn(ReqContext<'_>, RespHandle<'_>) + 'static,
    {
        *self.req_handler.borrow_mut() = Some(Box::new(handler));
    }

    /// Get the local endpoint information.
    pub fn local_info(&self) -> crate::transport::LocalInfo {
        self.transport.local_info()
    }

    /// Create a new session to a remote endpoint.
    pub fn create_session(&self, remote: &RemoteInfo) -> Result<SessionHandle> {
        let mut sessions = self.sessions.borrow_mut();
        let handle = sessions.create_session(*remote, &self.config)?;

        // Create address handle for the session
        let session = sessions.get_mut(handle).unwrap();
        let ah = self.transport.create_ah(remote)?;
        session.set_ah(ah);
        session.connect(handle.session_num()); // For simplicity, use local num as remote

        Ok(handle)
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

    /// Enqueue a request.
    ///
    /// The continuation will be called when the response is received.
    pub fn enqueue_request<F>(
        &self,
        session: SessionHandle,
        req_type: u8,
        req_data: &[u8],
        cont: F,
    ) -> Result<()>
    where
        F: FnOnce((), &[u8]) + 'static,
    {
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

        // Phase 2: Prepare buffer
        let msg_size = req_data.len();
        let pkt_type = if msg_size + PKT_HDR_SIZE <= self.mtu {
            PktType::ReqForResp
        } else {
            PktType::Req
        };

        let hdr = PktHdr::new(
            req_type,
            msg_size,
            remote_session_num,
            pkt_type,
            0,
            req_num,
        );

        let buf_info = self.prepare_send_buffer(req_data, &hdr)?;

        // Phase 3: Update session state
        {
            let mut sessions = self.sessions.borrow_mut();
            let sess = sessions.get_mut(session).unwrap();

            let pending = PendingRequest {
                continuation: Some(Box::new(cont)),
                req_buf_idx: Some(buf_info.idx),
            };

            let sslot = sess.sslot_mut(sslot_idx).unwrap();
            sslot.start_request(req_num, req_type, pending);
            sslot.wait_response(1);
        }

        // Phase 4: Post send
        let av = {
            let sessions = self.sessions.borrow();
            let sess = sessions.get(session).ok_or(Error::SessionNotFound(session.session_num()))?;
            let ah = sess.ah.as_ref().ok_or(Error::SessionNotConnected(session.session_num()))?;
            UdTransport::ah_to_av(ah)
        };

        {
            let entry = TransportEntry {
                buf_idx: buf_info.idx,
                session_num: session.session_num(),
                context: req_num,
            };

            let send_buffers = self.send_buffers.borrow();
            let buf = send_buffers.get(buf_info.idx).ok_or(Error::InvalidPacket)?;
            self.transport.post_send(av, buf, entry)?;
            drop(send_buffers);
            self.transport.ring_sq_doorbell();
        }

        // Phase 5: Start timer
        let timer_entry = TimerEntry {
            session_num: session.session_num(),
            sslot_idx,
            req_num,
            expires_at: current_time_us() + self.config.rto_us,
        };
        self.timing_wheel.borrow_mut().insert(timer_entry);

        Ok(())
    }

    /// Send a response to a request.
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

        // Phase 2: Prepare buffer
        let hdr = PktHdr::new(
            req_type,
            data.len(),
            remote_session_num,
            PktType::Resp,
            0,
            req_num,
        );

        let buf_info = self.prepare_send_buffer(data, &hdr)?;

        // Phase 3: Post send
        let av = {
            let sessions = self.sessions.borrow();
            let handle = SessionHandle(session_num);
            let sess = sessions.get(handle).ok_or(Error::SessionNotFound(session_num))?;
            let ah = sess.ah.as_ref().ok_or(Error::SessionNotConnected(session_num))?;
            UdTransport::ah_to_av(ah)
        };

        {
            let entry = TransportEntry {
                buf_idx: buf_info.idx,
                session_num,
                context: req_num,
            };

            let send_buffers = self.send_buffers.borrow();
            let buf = send_buffers.get(buf_info.idx).ok_or(Error::InvalidPacket)?;
            self.transport.post_send(av, buf, entry)?;
            drop(send_buffers);
            self.transport.ring_sq_doorbell();
        }

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
            };
            self.transport.post_recv_raw(addr, capacity, lkey, entry)?;
        }
        self.transport.ring_rq_doorbell();

        Ok(())
    }

    /// Process a received packet.
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

            // Copy payload
            let payload_offset = GRH_SIZE + PKT_HDR_SIZE;
            let payload_len = (byte_cnt as usize).saturating_sub(payload_offset);
            let mut payload = vec![0u8; payload_len];
            if payload_len > 0 {
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        buf.as_ptr().add(payload_offset),
                        payload.as_mut_ptr(),
                        payload_len,
                    );
                }
            }

            (hdr, payload)
        };

        // Return buffer to pool
        self.recv_buffers.borrow_mut().free(buf_idx);

        // Repost a receive buffer
        self.repost_recv_buffer()?;

        // Process based on packet type
        match hdr.pkt_type() {
            PktType::Req | PktType::ReqForResp => {
                self.handle_request(&hdr, &payload)?;
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

    /// Repost a single receive buffer.
    fn repost_recv_buffer(&self) -> Result<()> {
        let buf_info = {
            let mut recv_buffers = self.recv_buffers.borrow_mut();
            if let Some((idx, buf)) = recv_buffers.alloc() {
                let lkey = buf.lkey().ok_or_else(|| {
                    Error::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "Buffer not registered",
                    ))
                })?;
                Some((idx, buf.addr(), buf.capacity() as u32, lkey))
            } else {
                None
            }
        };

        if let Some((idx, addr, capacity, lkey)) = buf_info {
            let entry = TransportEntry {
                buf_idx: idx,
                session_num: 0,
                context: 0,
            };
            self.transport.post_recv_raw(addr, capacity, lkey, entry)?;
            self.transport.ring_rq_doorbell();
        }

        Ok(())
    }

    /// Handle an incoming request.
    fn handle_request(&self, hdr: &PktHdr, payload: &[u8]) -> Result<()> {
        let handler = self.req_handler.borrow();
        if let Some(ref h) = *handler {
            let ctx = ReqContext {
                req_type: hdr.req_type(),
                data: payload,
                session_num: hdr.dest_session_num(),
                req_num: hdr.req_num(),
            };

            let resp_handle = RespHandle {
                rpc: self,
                session_num: hdr.dest_session_num(),
                req_num: hdr.req_num(),
                req_type: hdr.req_type(),
            };

            h(ctx, resp_handle);
        }

        Ok(())
    }

    /// Handle an incoming response.
    fn handle_response(&self, hdr: &PktHdr, payload: &[u8]) -> Result<()> {
        let dest_session = hdr.dest_session_num();
        let pkt_num = hdr.pkt_num();
        let msg_size = hdr.msg_size();
        let num_pkts = PktHdr::calc_num_pkts(msg_size, self.mtu);

        let (is_complete, continuation, data) = {
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
                    let pending = sslot.user_data.take();
                    let resp_data = sslot.take_resp_buf();

                    // Note: Request buffer is freed on send completion, not here

                    // Mark slot as free
                    sslot.reset();

                    (true, pending.and_then(|p| p.continuation), resp_data)
                } else {
                    (false, None, None)
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

                // Get the slot and extract continuation
                let sslot = sess.sslot_mut(sslot_idx).unwrap();
                let pending = sslot.user_data.take();

                // Note: Request buffer is freed on send completion, not here

                // Mark slot as free
                sslot.reset();

                (true, pending.and_then(|p| p.continuation), None)
            }
        };

        // Call continuation outside of borrow
        if is_complete {
            if let Some(cont) = continuation {
                if let Some(data) = data {
                    // Multi-packet response
                    cont((), &data);
                } else {
                    // Single-packet response
                    cont((), payload);
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
        let should_retransmit = {
            let mut sessions = self.sessions.borrow_mut();
            let handle = SessionHandle(entry.session_num);

            if let Some(sess) = sessions.get_mut(handle) {
                if let Some(sslot) = sess.sslot_mut(entry.sslot_idx) {
                    if sslot.req_num == entry.req_num && !sslot.is_free() {
                        sslot.retries += 1;

                        if sslot.retries > self.config.max_retries {
                            // Max retries exceeded, fail the request
                            let pending = sslot.user_data.take();
                            sslot.reset();

                            if let Some(p) = pending {
                                if let Some(req_buf_idx) = p.req_buf_idx {
                                    self.send_buffers.borrow_mut().free(req_buf_idx);
                                }
                            }
                            None
                        } else {
                            // Need to retransmit
                            sslot.user_data.as_ref().and_then(|p| p.req_buf_idx)
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
        if let Some(req_buf_idx) = should_retransmit {
            let _ = self.retransmit(entry.session_num, req_buf_idx, entry.sslot_idx, entry.req_num, now);
        }
    }

    /// Retransmit a request.
    fn retransmit(&self, session_num: u16, req_buf_idx: usize, sslot_idx: usize, req_num: u64, now: u64) -> Result<()> {
        // Get address vector before dropping session borrow
        let av = {
            let sessions = self.sessions.borrow();
            let handle = SessionHandle(session_num);
            let sess = sessions.get(handle).ok_or(Error::SessionNotFound(session_num))?;
            let ah = sess.ah.as_ref().ok_or(Error::SessionNotConnected(session_num))?;
            UdTransport::ah_to_av(ah)
        };

        let entry = TransportEntry {
            buf_idx: req_buf_idx,
            session_num,
            context: req_num,
        };

        {
            let send_buffers = self.send_buffers.borrow();
            let buf = send_buffers.get(req_buf_idx).ok_or(Error::InvalidPacket)?;
            self.transport.post_send(av, buf, entry)?;
        }
        self.transport.ring_sq_doorbell();

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
    pub fn run_event_loop_once(&self) -> usize {
        let mut events = 0;

        // Poll and process send completions
        // Note: Currently we free all send buffers on completion. This means
        // request buffers are freed immediately, which prevents retransmission.
        // TODO: Distinguish between request and response buffers:
        // - Request buffers: Keep until response received (for retransmission)
        // - Response buffers: Free on send completion
        for comp in self.transport.poll_send_completions() {
            self.send_buffers.borrow_mut().free(comp.buf_idx);
            events += 1;
        }

        // Poll and process receive completions
        for comp in self.transport.poll_recv_completions() {
            if let Err(e) = self.process_recv(comp.buf_idx, comp.byte_cnt) {
                eprintln!("process_recv error: {:?}", e);
            }
            events += 1;
        }

        // Process timeouts
        self.process_timeouts();

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
