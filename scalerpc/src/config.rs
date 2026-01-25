//! Configuration types for ScaleRPC.
//!
//! # ScaleRPC Configuration
//!
//! This module defines configuration parameters for ScaleRPC based on
//! the EuroSys 2019 paper's recommendations.

/// Configuration for the message pool.
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Number of message slots in the pool.
    pub num_slots: usize,
    /// Size of payload data in each slot (bytes).
    pub slot_data_size: usize,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            num_slots: 1024,
            slot_data_size: 4080, // 4096 - 16 bytes for header
        }
    }
}

/// Configuration for connection grouping and time-sharing (Section 3.2).
#[derive(Debug, Clone)]
pub struct GroupConfig {
    /// Maximum number of active connections per group.
    pub max_active_per_group: usize,
    /// Number of connection groups.
    pub num_groups: usize,
    /// Time slice duration in microseconds for group scheduling.
    /// Default is 100µs as recommended in the paper.
    pub time_slice_us: u64,
    /// Maximum number of endpoint entries for warmup mechanism.
    pub max_endpoint_entries: usize,
}

impl Default for GroupConfig {
    fn default() -> Self {
        Self {
            max_active_per_group: 4,
            num_groups: 8,
            time_slice_us: 100, // 100µs as per paper
            max_endpoint_entries: 256,
        }
    }
}

/// Configuration for RPC client.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// Message pool configuration.
    pub pool: PoolConfig,
    /// Connection grouping configuration.
    pub group: GroupConfig,
    /// Timeout for RPC calls in milliseconds.
    pub timeout_ms: u64,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            pool: PoolConfig::default(),
            group: GroupConfig::default(),
            timeout_ms: 5000,
        }
    }
}

/// Configuration for RPC server.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Message pool configuration (used for both processing and warmup pools).
    pub pool: PoolConfig,
    /// Number of receive slots to post.
    pub num_recv_slots: usize,
    /// Group scheduling configuration.
    pub group: GroupConfig,
    /// Enable ScaleRPC time-sharing scheduler.
    /// When disabled, falls back to simple direct processing.
    pub enable_scheduler: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            pool: PoolConfig::default(),
            num_recv_slots: 256,
            group: GroupConfig::default(),
            enable_scheduler: true,
        }
    }
}
