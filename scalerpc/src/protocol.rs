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

/// Response header sent from server to client.
///
/// The header is placed at the beginning of the response data.
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
        // Expected size: 4 + 8 + 2 + 2 + 8 + 4 + 4 = 32 bytes
        assert_eq!(RequestHeader::SIZE, 32);
    }

    #[test]
    fn test_response_header_size() {
        // Expected size: 4 + 8 + 4 + 4 = 20 bytes
        assert_eq!(ResponseHeader::SIZE, 20);
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
            assert_eq!(magic, REQUEST_MAGIC);
            assert_eq!(req_id, 123);
            assert_eq!(rpc_type, 1);
            assert_eq!(payload_len, 100);
            assert_eq!(client_slot_addr, 0x1000);
            assert_eq!(client_slot_rkey, 0xABCD);
            assert_eq!(sender_conn_id, 5);
        }
    }

    #[test]
    fn test_response_header_serialization() {
        let header = ResponseHeader::success(456, 50);
        assert!(header.is_valid());
        assert!(header.is_success());

        let mut buf = [0u8; ResponseHeader::SIZE];
        unsafe {
            header.write_to(buf.as_mut_ptr());
            let read_back = ResponseHeader::read_from(buf.as_ptr());
            // Copy fields to local variables to avoid unaligned access
            let magic = { read_back.magic };
            let req_id = { read_back.req_id };
            let status = { read_back.status };
            let payload_len = { read_back.payload_len };
            assert_eq!(magic, RESPONSE_MAGIC);
            assert_eq!(req_id, 456);
            assert_eq!(status, 0);
            assert_eq!(payload_len, 50);
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
}
