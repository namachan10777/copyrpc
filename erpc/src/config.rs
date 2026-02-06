//! Configuration types for eRPC.

/// RPC configuration.
///
/// Controls the behavior of the RPC system, including request multiplexing,
/// flow control, and reliability parameters.
#[derive(Debug, Clone)]
pub struct RpcConfig {
    /// Request window size (number of concurrent requests per session).
    /// Default: 8
    pub req_window: usize,
    /// Session credits for flow control.
    /// Default: 32
    pub session_credits: usize,
    /// Retransmission timeout in microseconds.
    /// Default: 5000 (5ms)
    pub rto_us: u64,
    /// Maximum number of retransmission attempts.
    /// Default: 5
    pub max_retries: u32,
    /// Enable Timely congestion control.
    /// Default: false
    pub enable_cc: bool,
    /// UD QP Q_Key.
    /// Default: 0x11111111
    pub qkey: u32,
    /// Maximum inline data size.
    /// Default: 64
    pub max_inline_data: u32,
    /// Maximum number of sessions.
    /// Default: 256
    pub max_sessions: usize,
    /// Maximum send queue depth.
    /// Default: 256
    pub max_send_wr: u32,
    /// Maximum receive queue depth.
    /// Default: 256
    pub max_recv_wr: u32,
    /// Number of receive buffers to post.
    /// Default: 128
    pub num_recv_buffers: usize,
}

impl Default for RpcConfig {
    fn default() -> Self {
        Self {
            req_window: 8,
            session_credits: 32,
            rto_us: 5000,
            max_retries: 5,
            enable_cc: false,
            qkey: 0x11111111,
            max_inline_data: 64,
            max_sessions: 256,
            max_send_wr: 256,
            max_recv_wr: 256,
            num_recv_buffers: 128,
        }
    }
}

impl RpcConfig {
    /// Create a new configuration with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the request window size.
    pub fn with_req_window(mut self, req_window: usize) -> Self {
        self.req_window = req_window;
        self
    }

    /// Set the session credits.
    pub fn with_session_credits(mut self, session_credits: usize) -> Self {
        self.session_credits = session_credits;
        self
    }

    /// Set the retransmission timeout.
    pub fn with_rto_us(mut self, rto_us: u64) -> Self {
        self.rto_us = rto_us;
        self
    }

    /// Set the maximum retries.
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Enable or disable Timely congestion control.
    pub fn with_cc(mut self, enable_cc: bool) -> Self {
        self.enable_cc = enable_cc;
        self
    }

    /// Set the Q_Key.
    pub fn with_qkey(mut self, qkey: u32) -> Self {
        self.qkey = qkey;
        self
    }

    /// Set the maximum number of sessions.
    pub fn with_max_sessions(mut self, max_sessions: usize) -> Self {
        self.max_sessions = max_sessions;
        self
    }

    /// Set the maximum send queue depth.
    pub fn with_max_send_wr(mut self, max_send_wr: u32) -> Self {
        self.max_send_wr = max_send_wr;
        self
    }

    /// Set the maximum receive queue depth.
    pub fn with_max_recv_wr(mut self, max_recv_wr: u32) -> Self {
        self.max_recv_wr = max_recv_wr;
        self
    }

    /// Set the number of receive buffers.
    pub fn with_num_recv_buffers(mut self, num_recv_buffers: usize) -> Self {
        self.num_recv_buffers = num_recv_buffers;
        self
    }
}

/// Session-specific configuration.
#[derive(Debug, Clone)]
#[derive(Default)]
pub struct SessionConfig {
    /// Request window size for this session.
    /// If None, uses the RpcConfig default.
    pub req_window: Option<usize>,
    /// Session credits for this session.
    /// If None, uses the RpcConfig default.
    pub session_credits: Option<usize>,
}

