//! RPC Protocol definitions.
//!
//! Defines the wire format for ScaleRPC requests and responses.
//!
//! # ScaleRPC Message Format (Section 3.1)
//!
//! Messages use a right-aligned layout for efficient polling:
//! ```text
//! [ Padding | Data | MsgLen | Valid ]
//!                           ^^^^^^^^
//!                      Poll this field
//! ```
//!
//! The Valid field is polled to detect new messages. When a message arrives,
//! the valid field is set to a non-zero value, and the message length indicates
//! how much data precedes it.

use std::sync::atomic::{AtomicU32, Ordering};

/// Magic number for request headers.
pub const REQUEST_MAGIC: u32 = 0xDEAD_BEEF;

/// Magic number for response headers.
pub const RESPONSE_MAGIC: u32 = 0xBEEF_DEAD;

/// Valid field value indicating a valid message.
pub const MESSAGE_VALID: u32 = 1;

/// Valid field value indicating no message (slot is empty).
pub const MESSAGE_INVALID: u32 = 0;

/// Message block size (matches slot data size: 4KB - 16 byte slot header).
/// Note: The slot header is implementation overhead, leaving 4080 bytes for message data.
pub const MESSAGE_BLOCK_SIZE: usize = 4080;

/// Size of the message trailer (msg_len + valid).
pub const MESSAGE_TRAILER_SIZE: usize = 8;

/// Maximum data size in a message block (4080 - 8 trailer = 4072 bytes).
pub const MESSAGE_MAX_DATA_SIZE: usize = MESSAGE_BLOCK_SIZE - MESSAGE_TRAILER_SIZE;

/// Right-aligned message trailer.
///
/// This structure is placed at the END of each message block.
/// The layout is: `[ ... data ... | MsgLen(4) | Valid(4) ]`
#[repr(C)]
#[derive(Debug)]
pub struct MessageTrailer {
    /// Length of the message data (excluding trailer).
    pub msg_len: u32,
    /// Valid flag: 0 = no message, non-zero = valid message.
    pub valid: AtomicU32,
}

impl MessageTrailer {
    /// Check if there's a valid message.
    #[inline]
    pub fn is_valid(&self) -> bool {
        self.valid.load(Ordering::Acquire) != MESSAGE_INVALID
    }

    /// Get the message length.
    #[inline]
    pub fn msg_len(&self) -> u32 {
        self.msg_len
    }

    /// Set the message as valid with the given length.
    #[inline]
    pub fn set_valid(&mut self, len: u32) {
        self.msg_len = len;
        self.valid.store(MESSAGE_VALID, Ordering::Release);
    }

    /// Clear the message (mark as invalid).
    #[inline]
    pub fn clear(&mut self) {
        self.valid.store(MESSAGE_INVALID, Ordering::Release);
        self.msg_len = 0;
    }

    /// Write trailer to a buffer at the correct offset.
    ///
    /// # Safety
    /// `dst` must point to a buffer of at least `MESSAGE_BLOCK_SIZE` bytes.
    #[inline]
    pub unsafe fn write_to(dst: *mut u8, msg_len: u32) {
        let trailer_offset = MESSAGE_BLOCK_SIZE - MESSAGE_TRAILER_SIZE;
        let trailer_ptr = dst.add(trailer_offset) as *mut u32;
        // Write msg_len first
        std::ptr::write_volatile(trailer_ptr, msg_len);
        // Memory barrier then write valid
        std::sync::atomic::fence(Ordering::Release);
        std::ptr::write_volatile(trailer_ptr.add(1), MESSAGE_VALID);
    }

    /// Read trailer from a buffer.
    ///
    /// # Safety
    /// `src` must point to a buffer of at least `MESSAGE_BLOCK_SIZE` bytes.
    #[inline]
    pub unsafe fn read_from(src: *const u8) -> (u32, u32) {
        let trailer_offset = MESSAGE_BLOCK_SIZE - MESSAGE_TRAILER_SIZE;
        let trailer_ptr = src.add(trailer_offset) as *const u32;
        // Read valid first (poll target)
        let valid = std::ptr::read_volatile(trailer_ptr.add(1));
        std::sync::atomic::fence(Ordering::Acquire);
        let msg_len = std::ptr::read_volatile(trailer_ptr);
        (msg_len, valid)
    }

    /// Get the data area pointer for a message block.
    ///
    /// Data is right-aligned: starts at (block_end - trailer_size - msg_len).
    ///
    /// # Safety
    /// `block_ptr` must point to a valid message block.
    #[inline]
    pub unsafe fn data_ptr(block_ptr: *const u8, msg_len: u32) -> *const u8 {
        let data_end = MESSAGE_BLOCK_SIZE - MESSAGE_TRAILER_SIZE;
        block_ptr.add(data_end - msg_len as usize)
    }

    /// Get the mutable data area pointer for writing.
    ///
    /// For writing, data starts at the beginning of the data area.
    ///
    /// # Safety
    /// `block_ptr` must point to a valid message block.
    #[inline]
    pub unsafe fn data_ptr_mut(block_ptr: *mut u8) -> *mut u8 {
        block_ptr
    }
}

/// Request header sent from client to server.
///
/// The header is placed at the beginning of the message slot data area.
#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
pub struct RequestHeader {
    /// Magic number for validation.
    pub magic: u32,
    /// Unique request identifier.
    pub req_id: u64,
    /// RPC type/method identifier.
    pub rpc_type: u16,
    /// Length of payload data following the header.
    pub payload_len: u16,
    /// Client's slot address for response write-back.
    pub client_slot_addr: u64,
    /// Client's slot rkey for response write-back.
    pub client_slot_rkey: u32,
    /// Sender's connection ID (for multi-QP routing).
    pub sender_conn_id: u32,
    /// Slot sequence number for credit-based flow control.
    /// Incremented by client for each request sent via call_direct().
    pub slot_seq: u64,
}

impl RequestHeader {
    /// Size of the request header in bytes.
    pub const SIZE: usize = std::mem::size_of::<Self>();

    /// Create a new request header.
    pub fn new(
        req_id: u64,
        rpc_type: u16,
        payload_len: u16,
        client_slot_addr: u64,
        client_slot_rkey: u32,
        sender_conn_id: u32,
    ) -> Self {
        Self {
            magic: REQUEST_MAGIC,
            req_id,
            rpc_type,
            payload_len,
            client_slot_addr,
            client_slot_rkey,
            sender_conn_id,
            slot_seq: 0,
        }
    }

    /// Create a new request header with slot sequence for flow control.
    pub fn new_with_seq(
        req_id: u64,
        rpc_type: u16,
        payload_len: u16,
        client_slot_addr: u64,
        client_slot_rkey: u32,
        sender_conn_id: u32,
        slot_seq: u64,
    ) -> Self {
        Self {
            magic: REQUEST_MAGIC,
            req_id,
            rpc_type,
            payload_len,
            client_slot_addr,
            client_slot_rkey,
            sender_conn_id,
            slot_seq,
        }
    }

    /// Validate the magic number.
    pub fn is_valid(&self) -> bool {
        self.magic == REQUEST_MAGIC
    }

    /// Write the header to a byte slice.
    ///
    /// # Safety
    /// The destination must be at least `SIZE` bytes.
    pub unsafe fn write_to(&self, dst: *mut u8) {
        std::ptr::copy_nonoverlapping(
            self as *const Self as *const u8,
            dst,
            Self::SIZE,
        );
    }

    /// Read a header from a byte slice.
    ///
    /// # Safety
    /// The source must be at least `SIZE` bytes and properly aligned.
    pub unsafe fn read_from(src: *const u8) -> Self {
        std::ptr::read_unaligned(src as *const Self)
    }
}

/// Response header flags.
pub mod response_flags {
    /// Context switch event flag.
    /// When set, indicates that the client's group has been switched out.
    /// The client should transition to Idle state.
    pub const FLAG_CONTEXT_SWITCH: u32 = 0x1;

    /// Processing pool info flag.
    /// When set, the response contains the server's processing pool address and rkey.
    /// The client should update its mapping and transition to Process state.
    pub const FLAG_PROCESSING_POOL_INFO: u32 = 0x2;

    /// Credit acknowledgment flag.
    /// When set, the response contains acked_slot_seq indicating how many
    /// slot sequences the server has processed.
    pub const FLAG_CREDIT_ACK: u32 = 0x4;
}

/// Response header sent from server to client.
///
/// The header is placed at the beginning of the response data.
/// Includes flags for piggybacking context switch events (ScaleRPC optimization).
#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
pub struct ResponseHeader {
    /// Magic number for validation.
    pub magic: u32,
    /// Request identifier (matches the request).
    pub req_id: u64,
    /// Status code (0 = success).
    pub status: u32,
    /// Length of payload data following the header.
    pub payload_len: u32,
    /// Flags for context switch events and other metadata.
    pub flags: u32,
    /// Context switch sequence number (valid when FLAG_CONTEXT_SWITCH is set).
    pub context_switch_seq: u64,
    /// Server's processing pool address (valid when FLAG_PROCESSING_POOL_INFO is set).
    pub processing_pool_addr: u64,
    /// Server's processing pool rkey (valid when FLAG_PROCESSING_POOL_INFO is set).
    pub processing_pool_rkey: u32,
    /// Padding for alignment.
    pub _padding: u32,
    /// Acknowledged slot sequence for credit-based flow control.
    /// Valid when FLAG_CREDIT_ACK is set. Indicates the highest slot_seq
    /// that the server has processed for this connection.
    pub acked_slot_seq: u64,
}

impl ResponseHeader {
    /// Size of the response header in bytes.
    pub const SIZE: usize = std::mem::size_of::<Self>();

    /// Create a new response header.
    pub fn new(req_id: u64, status: u32, payload_len: u32) -> Self {
        Self {
            magic: RESPONSE_MAGIC,
            req_id,
            status,
            payload_len,
            flags: 0,
            context_switch_seq: 0,
            processing_pool_addr: 0,
            processing_pool_rkey: 0,
            _padding: 0,
            acked_slot_seq: 0,
        }
    }

    /// Create a new response header with credit acknowledgment.
    pub fn with_credit_ack(req_id: u64, status: u32, payload_len: u32, acked_seq: u64) -> Self {
        Self {
            magic: RESPONSE_MAGIC,
            req_id,
            status,
            payload_len,
            flags: response_flags::FLAG_CREDIT_ACK,
            context_switch_seq: 0,
            processing_pool_addr: 0,
            processing_pool_rkey: 0,
            _padding: 0,
            acked_slot_seq: acked_seq,
        }
    }

    /// Create a new response header with context switch event piggybacked.
    ///
    /// The context_switch_seq field encodes both the scheduler sequence and the fetched
    /// endpoint entry sequence:
    /// - Lower 32 bits: scheduler sequence (truncated)
    /// - Upper 32 bits: fetched endpoint entry sequence (fetched_seq)
    pub fn with_context_switch(req_id: u64, status: u32, payload_len: u32, scheduler_seq: u64, fetched_seq: u32) -> Self {
        // Encode both seq and fetched_seq in context_switch_seq
        // Lower 32 bits: scheduler_seq (truncated), Upper 32 bits: fetched_seq
        let context_switch_seq = ((fetched_seq as u64) << 32) | (scheduler_seq as u32 as u64);
        Self {
            magic: RESPONSE_MAGIC,
            req_id,
            status,
            payload_len,
            flags: response_flags::FLAG_CONTEXT_SWITCH,
            context_switch_seq,
            processing_pool_addr: 0,
            processing_pool_rkey: 0,
            _padding: 0,
            acked_slot_seq: 0,
        }
    }

    /// Create a new response header with context switch event AND processing pool info.
    ///
    /// This is used when the server has fetched warmup requests and wants to notify
    /// the client to transition to Process state with the processing pool address.
    pub fn with_processing_pool_info(
        req_id: u64,
        status: u32,
        payload_len: u32,
        scheduler_seq: u64,
        fetched_seq: u32,
        pool_addr: u64,
        pool_rkey: u32,
    ) -> Self {
        let context_switch_seq = ((fetched_seq as u64) << 32) | (scheduler_seq as u32 as u64);
        Self {
            magic: RESPONSE_MAGIC,
            req_id,
            status,
            payload_len,
            flags: response_flags::FLAG_CONTEXT_SWITCH | response_flags::FLAG_PROCESSING_POOL_INFO,
            context_switch_seq,
            processing_pool_addr: pool_addr,
            processing_pool_rkey: pool_rkey,
            _padding: 0,
            acked_slot_seq: 0,
        }
    }

    /// Create a new response header with context switch event AND credit acknowledgment.
    pub fn with_context_switch_and_credit_ack(
        req_id: u64,
        status: u32,
        payload_len: u32,
        scheduler_seq: u64,
        fetched_seq: u32,
        acked_seq: u64,
    ) -> Self {
        let context_switch_seq = ((fetched_seq as u64) << 32) | (scheduler_seq as u32 as u64);
        Self {
            magic: RESPONSE_MAGIC,
            req_id,
            status,
            payload_len,
            flags: response_flags::FLAG_CONTEXT_SWITCH | response_flags::FLAG_CREDIT_ACK,
            context_switch_seq,
            processing_pool_addr: 0,
            processing_pool_rkey: 0,
            _padding: 0,
            acked_slot_seq: acked_seq,
        }
    }

    /// Create a new response header with processing pool info AND credit acknowledgment.
    pub fn with_processing_pool_info_and_credit_ack(
        req_id: u64,
        status: u32,
        payload_len: u32,
        scheduler_seq: u64,
        fetched_seq: u32,
        pool_addr: u64,
        pool_rkey: u32,
        acked_seq: u64,
    ) -> Self {
        let context_switch_seq = ((fetched_seq as u64) << 32) | (scheduler_seq as u32 as u64);
        Self {
            magic: RESPONSE_MAGIC,
            req_id,
            status,
            payload_len,
            flags: response_flags::FLAG_CONTEXT_SWITCH | response_flags::FLAG_PROCESSING_POOL_INFO | response_flags::FLAG_CREDIT_ACK,
            context_switch_seq,
            processing_pool_addr: pool_addr,
            processing_pool_rkey: pool_rkey,
            _padding: 0,
            acked_slot_seq: acked_seq,
        }
    }

    /// Create a success response header.
    pub fn success(req_id: u64, payload_len: u32) -> Self {
        Self::new(req_id, 0, payload_len)
    }

    /// Create an error response header.
    pub fn error(req_id: u64, status: u32) -> Self {
        Self::new(req_id, status, 0)
    }

    /// Validate the magic number.
    pub fn is_valid(&self) -> bool {
        self.magic == RESPONSE_MAGIC
    }

    /// Check if the response indicates success.
    pub fn is_success(&self) -> bool {
        self.status == 0
    }

    /// Check if the response has a context switch event piggybacked.
    pub fn has_context_switch(&self) -> bool {
        self.flags & response_flags::FLAG_CONTEXT_SWITCH != 0
    }

    /// Check if the response has processing pool info.
    pub fn has_processing_pool_info(&self) -> bool {
        self.flags & response_flags::FLAG_PROCESSING_POOL_INFO != 0
    }

    /// Check if the response has credit acknowledgment.
    pub fn has_credit_ack(&self) -> bool {
        self.flags & response_flags::FLAG_CREDIT_ACK != 0
    }

    /// Get the acknowledged slot sequence (valid when has_credit_ack() is true).
    pub fn get_acked_slot_seq(&self) -> u64 {
        self.acked_slot_seq
    }

    /// Get the scheduler's context switch sequence (valid when has_context_switch() is true).
    ///
    /// This is the lower 32 bits of context_switch_seq.
    pub fn get_context_switch_seq(&self) -> u64 {
        (self.context_switch_seq & 0xFFFFFFFF) as u64
    }

    /// Get the fetched endpoint entry sequence (valid when has_context_switch() is true).
    ///
    /// This is the upper 32 bits of context_switch_seq. The client should compare this
    /// with its endpoint_entry_seq to verify the context switch is for the expected batch.
    pub fn get_fetched_seq(&self) -> u32 {
        (self.context_switch_seq >> 32) as u32
    }

    /// Get the processing pool address (valid when has_processing_pool_info() is true).
    pub fn get_processing_pool_addr(&self) -> Option<u64> {
        if self.has_processing_pool_info() {
            Some(self.processing_pool_addr)
        } else {
            None
        }
    }

    /// Get the processing pool rkey (valid when has_processing_pool_info() is true).
    pub fn get_processing_pool_rkey(&self) -> Option<u32> {
        if self.has_processing_pool_info() {
            Some(self.processing_pool_rkey)
        } else {
            None
        }
    }

    /// Write the header to a byte slice.
    ///
    /// # Safety
    /// The destination must be at least `SIZE` bytes.
    pub unsafe fn write_to(&self, dst: *mut u8) {
        std::ptr::copy_nonoverlapping(
            self as *const Self as *const u8,
            dst,
            Self::SIZE,
        );
    }

    /// Read a header from a byte slice.
    ///
    /// # Safety
    /// The source must be at least `SIZE` bytes.
    pub unsafe fn read_from(src: *const u8) -> Self {
        std::ptr::read_unaligned(src as *const Self)
    }
}

/// Status codes for RPC responses.
pub mod status {
    /// Success.
    pub const OK: u32 = 0;
    /// Unknown RPC type.
    pub const UNKNOWN_RPC: u32 = 1;
    /// Invalid request format.
    pub const INVALID_REQUEST: u32 = 2;
    /// Internal server error.
    pub const INTERNAL_ERROR: u32 = 3;
    /// Request timeout.
    pub const TIMEOUT: u32 = 4;
}

/// Endpoint entry for warmup notification.
///
/// Client writes this to the server's endpoint entry buffer via RDMA WRITE
/// to notify that warmup requests are ready. This allows the server to
/// know exactly how many requests to RDMA READ, avoiding wasted bandwidth
/// on empty slots.
#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct EndpointEntry {
    /// Client's warmup buffer address.
    pub req_addr: u64,
    /// Number of pending requests (batch_size).
    pub batch_size: u32,
    /// Sequence number for change detection.
    pub seq: u32,
}

impl EndpointEntry {
    /// Size of the endpoint entry in bytes.
    pub const SIZE: usize = 16; // 8 + 4 + 4

    /// Create a new endpoint entry.
    pub fn new(req_addr: u64, batch_size: u32, seq: u32) -> Self {
        Self {
            req_addr,
            batch_size,
            seq,
        }
    }

    /// Read from a buffer.
    ///
    /// # Safety
    /// `src` must point to at least `SIZE` bytes.
    pub unsafe fn read_from(src: *const u8) -> Self {
        std::ptr::read_unaligned(src as *const Self)
    }

    /// Write to a buffer.
    ///
    /// # Safety
    /// `dst` must point to at least `SIZE` bytes.
    pub unsafe fn write_to(&self, dst: *mut u8) {
        std::ptr::copy_nonoverlapping(self as *const Self as *const u8, dst, Self::SIZE);
    }
}

/// Context switch event notification.
///
/// Sent from server to client when the client's group is being
/// switched out of the processing state.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ContextSwitchEvent {
    /// Magic number for validation.
    pub magic: u32,
    /// Endpoint entry sequence that was fetched.
    pub fetched_seq: u32,
    /// Sequence number for ordering.
    pub sequence: u64,
}

impl ContextSwitchEvent {
    /// Magic number for context switch events.
    pub const MAGIC: u32 = 0xC5E7_C5E7; // CSEt_CSEt

    /// Size of the event structure.
    pub const SIZE: usize = std::mem::size_of::<Self>();

    /// Create a new context switch event.
    pub fn new(sequence: u64, fetched_seq: u32) -> Self {
        Self {
            magic: Self::MAGIC,
            fetched_seq,
            sequence,
        }
    }

    /// Validate the magic number.
    pub fn is_valid(&self) -> bool {
        self.magic == Self::MAGIC
    }

    /// Write to a buffer.
    ///
    /// # Safety
    /// `dst` must point to at least `SIZE` bytes.
    pub unsafe fn write_to(&self, dst: *mut u8) {
        std::ptr::copy_nonoverlapping(
            self as *const Self as *const u8,
            dst,
            Self::SIZE,
        );
    }

    /// Read from a buffer.
    ///
    /// # Safety
    /// `src` must point to at least `SIZE` bytes.
    pub unsafe fn read_from(src: *const u8) -> Self {
        std::ptr::read_unaligned(src as *const Self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_header_size() {
        // Expected size: 4 + 8 + 2 + 2 + 8 + 4 + 4 + 8 = 40 bytes
        assert_eq!(RequestHeader::SIZE, 40);
    }

    #[test]
    fn test_response_header_size() {
        // Expected size: 4 + 8 + 4 + 4 + 4 + 8 + 8 + 4 + 4 + 8 = 56 bytes
        assert_eq!(ResponseHeader::SIZE, 56);
    }

    #[test]
    fn test_request_header_serialization() {
        let header = RequestHeader::new(123, 1, 100, 0x1000, 0xABCD, 5);
        assert!(header.is_valid());

        let mut buf = [0u8; RequestHeader::SIZE];
        unsafe {
            header.write_to(buf.as_mut_ptr());
            let read_back = RequestHeader::read_from(buf.as_ptr());
            // Copy fields to local variables to avoid unaligned access
            let magic = { read_back.magic };
            let req_id = { read_back.req_id };
            let rpc_type = { read_back.rpc_type };
            let payload_len = { read_back.payload_len };
            let client_slot_addr = { read_back.client_slot_addr };
            let client_slot_rkey = { read_back.client_slot_rkey };
            let sender_conn_id = { read_back.sender_conn_id };
            let slot_seq = { read_back.slot_seq };
            assert_eq!(magic, REQUEST_MAGIC);
            assert_eq!(req_id, 123);
            assert_eq!(rpc_type, 1);
            assert_eq!(payload_len, 100);
            assert_eq!(client_slot_addr, 0x1000);
            assert_eq!(client_slot_rkey, 0xABCD);
            assert_eq!(sender_conn_id, 5);
            assert_eq!(slot_seq, 0); // Default is 0
        }
    }

    #[test]
    fn test_request_header_with_seq() {
        let header = RequestHeader::new_with_seq(123, 1, 100, 0x1000, 0xABCD, 5, 42);
        assert!(header.is_valid());

        let mut buf = [0u8; RequestHeader::SIZE];
        unsafe {
            header.write_to(buf.as_mut_ptr());
            let read_back = RequestHeader::read_from(buf.as_ptr());
            let slot_seq = { read_back.slot_seq };
            assert_eq!(slot_seq, 42);
        }
    }

    #[test]
    fn test_response_header_serialization() {
        let header = ResponseHeader::success(456, 50);
        assert!(header.is_valid());
        assert!(header.is_success());
        assert!(!header.has_context_switch());

        let mut buf = [0u8; ResponseHeader::SIZE];
        unsafe {
            header.write_to(buf.as_mut_ptr());
            let read_back = ResponseHeader::read_from(buf.as_ptr());
            // Copy fields to local variables to avoid unaligned access
            let magic = { read_back.magic };
            let req_id = { read_back.req_id };
            let status = { read_back.status };
            let payload_len = { read_back.payload_len };
            let flags = { read_back.flags };
            let context_switch_seq = { read_back.context_switch_seq };
            assert_eq!(magic, RESPONSE_MAGIC);
            assert_eq!(req_id, 456);
            assert_eq!(status, 0);
            assert_eq!(payload_len, 50);
            assert_eq!(flags, 0);
            assert_eq!(context_switch_seq, 0);
        }
    }

    #[test]
    fn test_response_header_with_context_switch() {
        // scheduler_seq = 42, fetched_seq = 5
        let header = ResponseHeader::with_context_switch(789, 0, 100, 42, 5);
        assert!(header.is_valid());
        assert!(header.is_success());
        assert!(header.has_context_switch());
        assert!(!header.has_processing_pool_info());
        // get_context_switch_seq returns lower 32 bits (scheduler_seq)
        assert_eq!(header.get_context_switch_seq(), 42);
        // get_fetched_seq returns upper 32 bits
        assert_eq!(header.get_fetched_seq(), 5);

        let mut buf = [0u8; ResponseHeader::SIZE];
        unsafe {
            header.write_to(buf.as_mut_ptr());
            let read_back = ResponseHeader::read_from(buf.as_ptr());
            let flags = { read_back.flags };
            assert_eq!(flags, response_flags::FLAG_CONTEXT_SWITCH);
            // context_switch_seq encodes both: (fetched_seq << 32) | scheduler_seq
            assert_eq!(read_back.get_context_switch_seq(), 42);
            assert_eq!(read_back.get_fetched_seq(), 5);
        }
    }

    #[test]
    fn test_response_header_with_processing_pool_info() {
        // scheduler_seq = 42, fetched_seq = 5, pool_addr = 0x1234_5678_9ABC_DEF0, pool_rkey = 0xABCD
        let header = ResponseHeader::with_processing_pool_info(
            789, 0, 100, 42, 5,
            0x1234_5678_9ABC_DEF0, 0xABCD
        );
        assert!(header.is_valid());
        assert!(header.is_success());
        assert!(header.has_context_switch());
        assert!(header.has_processing_pool_info());
        assert_eq!(header.get_context_switch_seq(), 42);
        assert_eq!(header.get_fetched_seq(), 5);
        assert_eq!(header.get_processing_pool_addr(), Some(0x1234_5678_9ABC_DEF0));
        assert_eq!(header.get_processing_pool_rkey(), Some(0xABCD));

        let mut buf = [0u8; ResponseHeader::SIZE];
        unsafe {
            header.write_to(buf.as_mut_ptr());
            let read_back = ResponseHeader::read_from(buf.as_ptr());
            let flags = { read_back.flags };
            assert_eq!(flags, response_flags::FLAG_CONTEXT_SWITCH | response_flags::FLAG_PROCESSING_POOL_INFO);
            assert_eq!(read_back.get_context_switch_seq(), 42);
            assert_eq!(read_back.get_fetched_seq(), 5);
            assert_eq!(read_back.get_processing_pool_addr(), Some(0x1234_5678_9ABC_DEF0));
            assert_eq!(read_back.get_processing_pool_rkey(), Some(0xABCD));
        }
    }

    #[test]
    fn test_message_trailer_constants() {
        // Trailer is 8 bytes (4 + 4)
        assert_eq!(MESSAGE_TRAILER_SIZE, 8);
        // Block is 4080 bytes (4KB slot - 16 byte header)
        assert_eq!(MESSAGE_BLOCK_SIZE, 4080);
        // Max data is block - trailer
        assert_eq!(MESSAGE_MAX_DATA_SIZE, 4072);
    }

    #[test]
    fn test_message_trailer_write_read() {
        let mut buf = vec![0u8; MESSAGE_BLOCK_SIZE];

        // Write a message trailer
        unsafe {
            MessageTrailer::write_to(buf.as_mut_ptr(), 100);
        }

        // Read it back
        let (msg_len, valid) = unsafe { MessageTrailer::read_from(buf.as_ptr()) };
        assert_eq!(msg_len, 100);
        assert_eq!(valid, MESSAGE_VALID);

        // Check that the trailer is at the correct offset
        let trailer_offset = MESSAGE_BLOCK_SIZE - MESSAGE_TRAILER_SIZE;
        let trailer_ptr = unsafe { buf.as_ptr().add(trailer_offset) as *const u32 };
        assert_eq!(unsafe { *trailer_ptr }, 100); // msg_len
        assert_eq!(unsafe { *trailer_ptr.add(1) }, MESSAGE_VALID); // valid
    }

    #[test]
    fn test_context_switch_event() {
        let event = ContextSwitchEvent::new(12345, 42);
        assert!(event.is_valid());
        assert_eq!(event.sequence, 12345);
        assert_eq!(event.fetched_seq, 42);

        let mut buf = [0u8; ContextSwitchEvent::SIZE];
        unsafe {
            event.write_to(buf.as_mut_ptr());
            let read_back = ContextSwitchEvent::read_from(buf.as_ptr());
            assert!(read_back.is_valid());
            assert_eq!(read_back.sequence, 12345);
        }
    }

    #[test]
    fn test_endpoint_entry_size() {
        // Expected size: 8 + 4 + 4 = 16 bytes
        assert_eq!(EndpointEntry::SIZE, 16);
        assert_eq!(std::mem::size_of::<EndpointEntry>(), 16);
    }

    #[test]
    fn test_endpoint_entry_serialization() {
        let entry = EndpointEntry::new(0x1234_5678_9ABC_DEF0, 42, 123);
        assert_eq!(entry.req_addr, 0x1234_5678_9ABC_DEF0);
        assert_eq!(entry.batch_size, 42);
        assert_eq!(entry.seq, 123);

        let mut buf = [0u8; EndpointEntry::SIZE];
        unsafe {
            entry.write_to(buf.as_mut_ptr());
            let read_back = EndpointEntry::read_from(buf.as_ptr());
            assert_eq!(read_back.req_addr, 0x1234_5678_9ABC_DEF0);
            assert_eq!(read_back.batch_size, 42);
            assert_eq!(read_back.seq, 123);
        }
    }

    #[test]
    fn test_endpoint_entry_default() {
        let entry = EndpointEntry::default();
        assert_eq!(entry.req_addr, 0);
        assert_eq!(entry.batch_size, 0);
        assert_eq!(entry.seq, 0);
    }

    #[test]
    fn test_response_header_with_credit_ack() {
        let header = ResponseHeader::with_credit_ack(123, 0, 50, 42);
        assert!(header.is_valid());
        assert!(header.is_success());
        assert!(header.has_credit_ack());
        assert!(!header.has_context_switch());
        assert_eq!(header.get_acked_slot_seq(), 42);

        let mut buf = [0u8; ResponseHeader::SIZE];
        unsafe {
            header.write_to(buf.as_mut_ptr());
            let read_back = ResponseHeader::read_from(buf.as_ptr());
            let flags = { read_back.flags };
            assert_eq!(flags, response_flags::FLAG_CREDIT_ACK);
            assert_eq!(read_back.get_acked_slot_seq(), 42);
        }
    }

    #[test]
    fn test_response_header_with_context_switch_and_credit_ack() {
        let header = ResponseHeader::with_context_switch_and_credit_ack(123, 0, 50, 10, 5, 42);
        assert!(header.is_valid());
        assert!(header.has_context_switch());
        assert!(header.has_credit_ack());
        assert_eq!(header.get_context_switch_seq(), 10);
        assert_eq!(header.get_fetched_seq(), 5);
        assert_eq!(header.get_acked_slot_seq(), 42);

        let mut buf = [0u8; ResponseHeader::SIZE];
        unsafe {
            header.write_to(buf.as_mut_ptr());
            let read_back = ResponseHeader::read_from(buf.as_ptr());
            assert!(read_back.has_context_switch());
            assert!(read_back.has_credit_ack());
        }
    }
}
