//! Session and SSlot management for eRPC.
//!
//! A session represents a connection to a remote endpoint.
//! SSlots (session slots) track individual request/response transactions.

use std::cell::Cell;

use mlx5::pd::AddressHandle;

use crate::config::RpcConfig;
use crate::error::{Error, Result};
use crate::flow_control::TimelyState;
use crate::transport::RemoteInfo;

/// Session state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionState {
    /// Session is not yet connected.
    Disconnected,
    /// Connection request sent, waiting for response.
    Connecting,
    /// Session is connected and ready for requests.
    Connected,
    /// Session is being disconnected.
    Disconnecting,
    /// Session has encountered an error.
    Error,
}

/// SSlot state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SSlotState {
    /// Slot is free and can be used for a new request.
    Free,
    /// Request is being transmitted.
    TxRequest,
    /// Waiting for response.
    WaitResponse,
    /// Response is being received.
    RxResponse,
    /// Request/response completed.
    Complete,
}

/// Session Slot (SSlot) for tracking a single request/response transaction.
///
/// Each session has `req_window` SSlots, allowing up to `req_window`
/// concurrent requests per session.
pub struct SSlot<U> {
    /// Slot index within the session (0..req_window).
    pub index: usize,
    /// Current request number for this slot.
    pub req_num: u64,
    /// Current slot state.
    pub state: SSlotState,
    /// Timestamp when the request was sent (for RTT measurement).
    pub tx_ts: u64,
    /// Number of packets sent for the current request.
    pub pkts_sent: u16,
    /// Number of packets received for the current response.
    pub pkts_recvd: u16,
    /// Total number of packets expected in the response.
    pub num_pkts_total: u16,
    /// Number of retransmissions for this request.
    pub retries: u32,
    /// User-defined data associated with this request.
    pub user_data: Option<U>,
    /// Request type (application-defined).
    pub req_type: u8,
    /// Request buffer index (for multi-packet requests).
    pub req_buf_idx: Option<usize>,
    /// Response buffer index.
    pub resp_buf_idx: Option<usize>,
    /// Response message size.
    pub resp_msg_size: usize,
    /// Multi-packet response reassembly buffer.
    pub resp_buf: Option<Vec<u8>>,
    /// Next expected packet number for Go-Back-N protocol.
    pub expected_pkt_num: u16,
    /// Request message size (for multi-packet requests).
    pub req_msg_size: usize,
    /// Total number of packets in the request (for multi-packet requests).
    pub req_num_pkts: u16,
    /// Number of request packets sent so far.
    pub req_pkts_sent: u16,
    /// Request buffer indices for all packets (for retransmission).
    pub req_buf_indices: Vec<usize>,
    /// Timing wheel slot for O(1) timer cancellation.
    pub timer_wheel_slot: Option<usize>,
}

impl<U> SSlot<U> {
    /// Create a new free SSlot.
    pub fn new(index: usize) -> Self {
        Self {
            index,
            req_num: 0,
            state: SSlotState::Free,
            tx_ts: 0,
            pkts_sent: 0,
            pkts_recvd: 0,
            num_pkts_total: 0,
            retries: 0,
            user_data: None,
            req_type: 0,
            req_buf_idx: None,
            resp_buf_idx: None,
            resp_msg_size: 0,
            resp_buf: None,
            expected_pkt_num: 0,
            req_msg_size: 0,
            req_num_pkts: 0,
            req_pkts_sent: 0,
            req_buf_indices: Vec::new(),
            timer_wheel_slot: None,
        }
    }

    /// Check if the slot is free.
    #[inline]
    pub fn is_free(&self) -> bool {
        self.state == SSlotState::Free
    }

    /// Reset the slot to free state.
    pub fn reset(&mut self) {
        self.state = SSlotState::Free;
        self.tx_ts = 0;
        self.pkts_sent = 0;
        self.pkts_recvd = 0;
        self.num_pkts_total = 0;
        self.retries = 0;
        self.user_data = None;
        self.req_type = 0;
        self.req_buf_idx = None;
        self.resp_buf_idx = None;
        self.resp_msg_size = 0;
        self.resp_buf = None;
        self.expected_pkt_num = 0;
        self.req_msg_size = 0;
        self.req_num_pkts = 0;
        self.req_pkts_sent = 0;
        self.req_buf_indices.clear();
        self.timer_wheel_slot = None;
    }

    /// Start a new request.
    pub fn start_request(&mut self, req_num: u64, req_type: u8, user_data: U) {
        self.req_num = req_num;
        self.req_type = req_type;
        self.state = SSlotState::TxRequest;
        self.pkts_sent = 0;
        self.pkts_recvd = 0;
        self.num_pkts_total = 0;
        self.retries = 0;
        self.user_data = Some(user_data);
    }

    /// Transition to waiting for response.
    pub fn wait_response(&mut self, num_pkts: u16) {
        self.state = SSlotState::WaitResponse;
        self.num_pkts_total = num_pkts;
    }

    /// Record a received packet.
    pub fn record_recv(&mut self) {
        self.pkts_recvd += 1;
        if self.pkts_recvd >= self.num_pkts_total {
            self.state = SSlotState::Complete;
        } else {
            self.state = SSlotState::RxResponse;
        }
    }

    /// Check if the given packet number is the expected one (Go-Back-N).
    #[inline]
    pub fn is_expected_pkt(&self, pkt_num: u16) -> bool {
        pkt_num == self.expected_pkt_num
    }

    /// Advance the expected packet number and record receipt (Go-Back-N).
    pub fn advance_expected(&mut self) {
        self.expected_pkt_num += 1;
        self.pkts_recvd += 1;

        if self.pkts_recvd >= self.num_pkts_total {
            self.state = SSlotState::Complete;
        } else {
            self.state = SSlotState::RxResponse;
        }
    }

    /// Record a received packet with packet number (Go-Back-N protocol).
    ///
    /// Returns true if this is the expected packet and was accepted.
    /// Out-of-order packets are rejected (Go-Back-N semantics).
    pub fn record_recv_pkt(&mut self, pkt_num: u16) -> bool {
        // Go-Back-N: Only accept packets with the expected sequence number
        if !self.is_expected_pkt(pkt_num) {
            return false; // Out-of-order or duplicate, discard
        }

        self.advance_expected();
        true
    }

    /// Check if response is complete.
    #[inline]
    pub fn is_response_complete(&self) -> bool {
        self.state == SSlotState::Complete || self.pkts_recvd >= self.num_pkts_total
    }

    /// Initialize multi-packet response buffer.
    pub fn init_resp_buf(&mut self, msg_size: usize, num_pkts: u16) {
        self.resp_buf = Some(vec![0u8; msg_size]);
        self.resp_msg_size = msg_size;
        self.num_pkts_total = num_pkts;
        self.expected_pkt_num = 0;
        self.pkts_recvd = 0;
    }

    /// Get the response buffer for multi-packet reassembly.
    pub fn resp_buf_mut(&mut self) -> Option<&mut Vec<u8>> {
        self.resp_buf.as_mut()
    }

    /// Take the completed response buffer.
    pub fn take_resp_buf(&mut self) -> Option<Vec<u8>> {
        self.resp_buf.take()
    }
}

/// A handle to a session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SessionHandle(pub u16);

impl SessionHandle {
    /// Get the session number.
    #[inline]
    pub fn session_num(&self) -> u16 {
        self.0
    }
}

/// Session for eRPC.
///
/// A session represents a connection to a remote endpoint and manages
/// concurrent request/response transactions through SSlots.
pub struct Session<U> {
    /// Local session number.
    pub local_session_num: u16,
    /// Remote session number.
    pub remote_session_num: u16,
    /// Session state.
    pub state: SessionState,
    /// Remote endpoint information.
    pub remote: RemoteInfo,
    /// Address Handle for sending to the remote endpoint.
    pub ah: Option<AddressHandle>,
    /// Session slots for concurrent requests.
    pub sslots: Vec<SSlot<U>>,
    /// Available credits for sending.
    pub credits: Cell<usize>,
    /// Next request number.
    pub next_req_num: Cell<u64>,
    /// Timely congestion control state.
    pub cc_state: Option<TimelyState>,
    /// Request window size.
    pub req_window: usize,
    /// Next allowed send time (μs) for rate limiting.
    pub next_send_time_us: Cell<u64>,
}

impl<U> Session<U> {
    /// Create a new session.
    pub fn new(local_session_num: u16, remote: RemoteInfo, config: &RpcConfig) -> Self {
        let req_window = config.req_window;
        let sslots = (0..req_window).map(SSlot::new).collect();

        let cc_state = if config.enable_cc {
            Some(TimelyState::new())
        } else {
            None
        };

        Self {
            local_session_num,
            remote_session_num: 0,
            state: SessionState::Disconnected,
            remote,
            ah: None,
            sslots,
            credits: Cell::new(config.session_credits),
            next_req_num: Cell::new(0),
            cc_state,
            req_window,
            next_send_time_us: Cell::new(0),
        }
    }

    /// Get the session handle.
    #[inline]
    pub fn handle(&self) -> SessionHandle {
        SessionHandle(self.local_session_num)
    }

    /// Check if the session is connected.
    #[inline]
    pub fn is_connected(&self) -> bool {
        self.state == SessionState::Connected
    }

    /// Set the Address Handle.
    pub fn set_ah(&mut self, ah: AddressHandle) {
        self.ah = Some(ah);
    }

    /// Set the remote session number and mark as connected.
    pub fn connect(&mut self, remote_session_num: u16) {
        self.remote_session_num = remote_session_num;
        self.state = SessionState::Connected;
    }

    /// Allocate an SSlot for a given request number.
    ///
    /// Uses eRPC's fixed slot assignment: sslot_idx = req_num % req_window.
    /// Returns the slot index if the slot is free, None if occupied.
    #[inline]
    pub fn alloc_sslot(&mut self, req_num: u64) -> Option<usize> {
        let idx = (req_num % self.req_window as u64) as usize;
        if self.sslots[idx].is_free() {
            Some(idx)
        } else {
            None
        }
    }

    /// Get a reference to an SSlot.
    #[inline]
    pub fn sslot(&self, idx: usize) -> Option<&SSlot<U>> {
        self.sslots.get(idx)
    }

    /// Get a mutable reference to an SSlot.
    #[inline]
    pub fn sslot_mut(&mut self, idx: usize) -> Option<&mut SSlot<U>> {
        self.sslots.get_mut(idx)
    }

    /// Find an SSlot by request number.
    ///
    /// Uses eRPC's fixed slot assignment: sslot_idx = req_num % req_window.
    /// O(1) lookup without HashMap.
    #[inline]
    pub fn find_sslot_by_req_num(&self, req_num: u64) -> Option<usize> {
        let idx = (req_num % self.req_window as u64) as usize;
        if !self.sslots[idx].is_free() && self.sslots[idx].req_num == req_num {
            Some(idx)
        } else {
            None
        }
    }

    /// Get the next request number.
    pub fn next_req_num(&self) -> u64 {
        let num = self.next_req_num.get();
        self.next_req_num.set(num.wrapping_add(1));
        num
    }

    /// Check if credits are available.
    #[inline]
    pub fn has_credits(&self) -> bool {
        self.credits.get() > 0
    }

    /// Consume a credit.
    pub fn consume_credit(&self) -> Result<()> {
        let credits = self.credits.get();
        if credits == 0 {
            return Err(Error::NoCredits);
        }
        self.credits.set(credits - 1);
        Ok(())
    }

    /// Return a credit.
    pub fn return_credit(&self) {
        self.credits.set(self.credits.get() + 1);
    }

    /// Get the number of available credits.
    #[inline]
    pub fn available_credits(&self) -> usize {
        self.credits.get()
    }

    /// Get the number of active (non-free) slots.
    pub fn active_slots(&self) -> usize {
        self.sslots.iter().filter(|s| !s.is_free()).count()
    }

    /// Get the number of free slots.
    pub fn free_slots(&self) -> usize {
        self.sslots.iter().filter(|s| s.is_free()).count()
    }
}

/// Session table for managing multiple sessions.
pub struct SessionTable<U> {
    sessions: Vec<Option<Session<U>>>,
    /// Next session number to allocate.
    next_session_num: u16,
}

impl<U> SessionTable<U> {
    /// Create a new session table.
    pub fn new(max_sessions: usize) -> Self {
        Self {
            sessions: (0..max_sessions).map(|_| None).collect(),
            next_session_num: 0,
        }
    }

    /// Allocate a new session number.
    fn alloc_session_num(&mut self) -> Option<u16> {
        let start = self.next_session_num as usize;
        for i in 0..self.sessions.len() {
            let idx = (start + i) % self.sessions.len();
            if self.sessions[idx].is_none() {
                self.next_session_num = ((idx + 1) % self.sessions.len()) as u16;
                return Some(idx as u16);
            }
        }
        None
    }

    /// Create a new session.
    pub fn create_session(
        &mut self,
        remote: RemoteInfo,
        config: &RpcConfig,
    ) -> Result<SessionHandle> {
        let session_num = self
            .alloc_session_num()
            .ok_or(Error::InvalidConfig("No available session slots".into()))?;

        let session = Session::new(session_num, remote, config);
        let handle = session.handle();

        self.sessions[session_num as usize] = Some(session);
        Ok(handle)
    }

    /// Get a reference to a session.
    pub fn get(&self, handle: SessionHandle) -> Option<&Session<U>> {
        self.sessions
            .get(handle.0 as usize)
            .and_then(|s| s.as_ref())
    }

    /// Get a mutable reference to a session.
    pub fn get_mut(&mut self, handle: SessionHandle) -> Option<&mut Session<U>> {
        self.sessions
            .get_mut(handle.0 as usize)
            .and_then(|s| s.as_mut())
    }

    /// Remove a session.
    pub fn remove(&mut self, handle: SessionHandle) -> Option<Session<U>> {
        self.sessions
            .get_mut(handle.0 as usize)
            .and_then(|s| s.take())
    }

    /// Get the number of active sessions.
    pub fn active_count(&self) -> usize {
        self.sessions.iter().filter(|s| s.is_some()).count()
    }

    /// Iterate over all active sessions.
    pub fn iter(&self) -> impl Iterator<Item = &Session<U>> {
        self.sessions.iter().filter_map(|s| s.as_ref())
    }

    /// Iterate mutably over all active sessions.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut Session<U>> {
        self.sessions.iter_mut().filter_map(|s| s.as_mut())
    }
}
