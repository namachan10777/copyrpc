//! Reliability mechanisms for eRPC.
//!
//! Implements Go-Back-N protocol for reliable message delivery over UD.

use crate::config::RpcConfig;
use crate::packet::MAX_PKT_NUM;

/// Go-Back-N state for a single request.
#[derive(Debug, Clone)]
pub struct GoBackNState {
    /// Base sequence number (first unacknowledged packet).
    base: u16,
    /// Next sequence number to send.
    next_seq: u16,
    /// Window size.
    window_size: u16,
    /// Number of packets in the message.
    num_pkts: u16,
    /// Bitmap of received packets (for out-of-order tracking).
    recv_bitmap: u64,
}

impl GoBackNState {
    /// Create a new Go-Back-N state.
    pub fn new(num_pkts: u16, window_size: u16) -> Self {
        Self {
            base: 0,
            next_seq: 0,
            window_size,
            num_pkts,
            recv_bitmap: 0,
        }
    }

    /// Get the base sequence number.
    #[inline]
    pub fn base(&self) -> u16 {
        self.base
    }

    /// Get the next sequence number to send.
    #[inline]
    pub fn next_seq(&self) -> u16 {
        self.next_seq
    }

    /// Get the window size.
    #[inline]
    pub fn window_size(&self) -> u16 {
        self.window_size
    }

    /// Check if we can send more packets.
    #[inline]
    pub fn can_send(&self) -> bool {
        let in_flight = self.next_seq.wrapping_sub(self.base);
        in_flight < self.window_size && self.next_seq < self.num_pkts
    }

    /// Get the number of packets that can be sent.
    #[inline]
    pub fn sendable_count(&self) -> u16 {
        let in_flight = self.next_seq.wrapping_sub(self.base);
        let window_available = self.window_size.saturating_sub(in_flight);
        let remaining = self.num_pkts.saturating_sub(self.next_seq);
        window_available.min(remaining)
    }

    /// Advance the send pointer after sending a packet.
    pub fn advance_send(&mut self) {
        if self.next_seq < self.num_pkts {
            self.next_seq = self.next_seq.wrapping_add(1);
        }
    }

    /// Record a received packet (for the receiver side).
    ///
    /// Returns true if this is a new packet (not a duplicate).
    pub fn record_recv(&mut self, pkt_num: u16) -> bool {
        if pkt_num >= self.num_pkts {
            return false;
        }

        let bit_idx = pkt_num as u64;
        if bit_idx >= 64 {
            // For messages with more than 64 packets, we only track the first 64
            // This is a simplification; real implementation would use larger bitmap
            return true;
        }

        let mask = 1u64 << bit_idx;
        if self.recv_bitmap & mask != 0 {
            return false; // Duplicate
        }

        self.recv_bitmap |= mask;

        // Advance base if possible (in-order delivery)
        while self.base < self.num_pkts {
            let base_mask = 1u64 << (self.base as u64);
            if self.recv_bitmap & base_mask != 0 {
                self.base = self.base.wrapping_add(1);
            } else {
                break;
            }
        }

        true
    }

    /// Handle an acknowledgment.
    ///
    /// In eRPC, the response acts as an implicit ACK.
    /// Returns true if the ACK advanced the base.
    pub fn handle_ack(&mut self, ack_num: u16) -> bool {
        // ACK acknowledges all packets up to and including ack_num
        if ack_num >= self.base {
            let old_base = self.base;
            self.base = ack_num.wrapping_add(1).min(self.num_pkts);
            self.base != old_base
        } else {
            false
        }
    }

    /// Reset for retransmission (Go-Back-N style).
    ///
    /// Resets the send pointer to the base to retransmit all unacknowledged packets.
    pub fn go_back_n(&mut self) {
        self.next_seq = self.base;
    }

    /// Check if all packets have been acknowledged.
    #[inline]
    pub fn is_complete(&self) -> bool {
        self.base >= self.num_pkts
    }

    /// Check if all packets have been received.
    #[inline]
    pub fn all_received(&self) -> bool {
        self.base >= self.num_pkts
    }

    /// Get the number of unacknowledged packets.
    #[inline]
    pub fn unacked_count(&self) -> u16 {
        self.next_seq.wrapping_sub(self.base)
    }
}

/// Request state for reliability tracking.
#[derive(Debug, Clone)]
pub struct RequestReliability {
    /// Go-Back-N state for the request.
    gbn: GoBackNState,
    /// Number of retransmissions.
    retries: u32,
    /// Maximum retries allowed.
    max_retries: u32,
    /// Timestamp of last transmission (microseconds).
    last_tx_ts: u64,
    /// Retransmission timeout (microseconds).
    rto: u64,
}

impl RequestReliability {
    /// Create a new request reliability state.
    pub fn new(num_pkts: u16, config: &RpcConfig) -> Self {
        Self {
            gbn: GoBackNState::new(num_pkts, config.req_window as u16),
            retries: 0,
            max_retries: config.max_retries,
            last_tx_ts: 0,
            rto: config.rto_us,
        }
    }

    /// Get the Go-Back-N state.
    #[inline]
    pub fn gbn(&self) -> &GoBackNState {
        &self.gbn
    }

    /// Get a mutable reference to the Go-Back-N state.
    #[inline]
    pub fn gbn_mut(&mut self) -> &mut GoBackNState {
        &mut self.gbn
    }

    /// Check if retransmission is needed based on timeout.
    pub fn needs_retransmit(&self, current_ts: u64) -> bool {
        if self.gbn.is_complete() {
            return false;
        }
        current_ts.saturating_sub(self.last_tx_ts) >= self.rto
    }

    /// Trigger retransmission.
    ///
    /// Returns Ok(()) if retransmission is allowed, Err if max retries exceeded.
    #[allow(clippy::result_unit_err)]
    pub fn trigger_retransmit(&mut self) -> Result<(), ()> {
        if self.retries >= self.max_retries {
            return Err(());
        }
        self.retries += 1;
        self.gbn.go_back_n();
        Ok(())
    }

    /// Update the last transmission timestamp.
    #[inline]
    pub fn update_tx_ts(&mut self, ts: u64) {
        self.last_tx_ts = ts;
    }

    /// Get the last transmission timestamp.
    #[inline]
    pub fn last_tx_ts(&self) -> u64 {
        self.last_tx_ts
    }

    /// Get the number of retries.
    #[inline]
    pub fn retries(&self) -> u32 {
        self.retries
    }

    /// Get the RTO.
    #[inline]
    pub fn rto(&self) -> u64 {
        self.rto
    }

    /// Update RTO based on RTT measurement.
    pub fn update_rto(&mut self, rtt: u64) {
        // Simple RTO calculation: 2 * RTT + margin
        self.rto = (rtt * 2).clamp(1000, 100_000);
    }
}

/// Response state for reliability tracking.
#[derive(Debug, Clone)]
pub struct ResponseReliability {
    /// Go-Back-N state for receiving response packets.
    gbn: GoBackNState,
    /// Expected total message size (reserved for future use).
    #[allow(dead_code)]
    msg_size: usize,
    /// Bytes received so far.
    bytes_recvd: usize,
}

impl ResponseReliability {
    /// Create a new response reliability state.
    pub fn new(num_pkts: u16, msg_size: usize) -> Self {
        Self {
            gbn: GoBackNState::new(num_pkts, MAX_PKT_NUM),
            msg_size,
            bytes_recvd: 0,
        }
    }

    /// Get the Go-Back-N state.
    #[inline]
    pub fn gbn(&self) -> &GoBackNState {
        &self.gbn
    }

    /// Get a mutable reference to the Go-Back-N state.
    #[inline]
    pub fn gbn_mut(&mut self) -> &mut GoBackNState {
        &mut self.gbn
    }

    /// Record received bytes.
    pub fn add_bytes(&mut self, bytes: usize) {
        self.bytes_recvd += bytes;
    }

    /// Get bytes received.
    #[inline]
    pub fn bytes_recvd(&self) -> usize {
        self.bytes_recvd
    }

    /// Check if the response is complete.
    #[inline]
    pub fn is_complete(&self) -> bool {
        self.gbn.all_received()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gbn_basic() {
        let mut gbn = GoBackNState::new(4, 2);

        assert!(gbn.can_send());
        assert_eq!(gbn.sendable_count(), 2);

        gbn.advance_send();
        gbn.advance_send();
        assert!(!gbn.can_send());
        assert_eq!(gbn.unacked_count(), 2);

        gbn.handle_ack(0);
        assert!(gbn.can_send());
        assert_eq!(gbn.sendable_count(), 1);

        gbn.advance_send();
        gbn.handle_ack(1);
        gbn.advance_send();
        gbn.handle_ack(3);

        assert!(gbn.is_complete());
    }

    #[test]
    fn test_gbn_recv() {
        let mut gbn = GoBackNState::new(4, 4);

        // Receive out of order
        assert!(gbn.record_recv(2));
        assert_eq!(gbn.base(), 0); // Base doesn't advance

        assert!(gbn.record_recv(0));
        assert_eq!(gbn.base(), 1); // Base advances to 1

        assert!(gbn.record_recv(1));
        assert_eq!(gbn.base(), 3); // Base advances to 3 (skipping received 2)

        assert!(gbn.record_recv(3));
        assert!(gbn.all_received());
    }

    #[test]
    fn test_gbn_go_back_n() {
        let mut gbn = GoBackNState::new(4, 2);

        gbn.advance_send();
        gbn.advance_send();
        assert_eq!(gbn.next_seq(), 2);

        gbn.go_back_n();
        assert_eq!(gbn.next_seq(), 0);
    }

    #[test]
    fn test_request_reliability() {
        let config = RpcConfig::default();
        let mut rel = RequestReliability::new(4, &config);

        rel.update_tx_ts(0);
        assert!(!rel.needs_retransmit(1000));
        assert!(rel.needs_retransmit(10000));

        assert!(rel.trigger_retransmit().is_ok());
        assert_eq!(rel.retries(), 1);
    }
}
