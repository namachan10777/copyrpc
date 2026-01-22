//! Immediate value encoding/decoding for RDMA WRITE IMM operations.
//!
//! The immediate value (32 bits) encodes the virtual offset delta:
//! - The delta is the increment from the previous position (not absolute)
//! - Divided by 32 to allow representation of up to 128GB delta in 32 bits

/// Alignment granularity for ring buffer positions.
/// All message sizes are padded to this boundary.
pub const ALIGNMENT: u64 = 32;

/// Encode a virtual offset delta into an immediate value.
///
/// The delta is divided by ALIGNMENT to pack more range into 32 bits.
///
/// # Arguments
/// * `delta` - The offset delta (must be aligned to ALIGNMENT)
///
/// # Returns
/// The encoded immediate value.
#[inline]
pub fn encode_imm(delta: u64) -> u32 {
    debug_assert!(
        delta % ALIGNMENT == 0,
        "delta must be aligned to {}",
        ALIGNMENT
    );
    (delta / ALIGNMENT) as u32
}

/// Decode an immediate value back to a virtual offset delta.
///
/// # Arguments
/// * `imm` - The immediate value from the CQE
///
/// # Returns
/// The decoded offset delta.
#[inline]
pub fn decode_imm(imm: u32) -> u64 {
    (imm as u64) * ALIGNMENT
}

/// Message header size in bytes.
/// Layout: call_id (4) + piggyback (4) + len (4) = 12 bytes
pub const HEADER_SIZE: usize = 12;

/// Bit flag in call_id indicating this is a response (not a request).
pub const RESPONSE_FLAG: u32 = 0x8000_0000;

/// Special call_id indicating a padding marker for wrap-around.
/// When the ring buffer wraps, we write this marker at the end and jump to the beginning.
pub const PADDING_MARKER: u32 = 0xFFFF_FFFF;

/// Encode a message header into the buffer.
///
/// Header layout:
/// - call_id: 4 bytes (request if MSB=0, response if MSB=1)
/// - piggyback: 4 bytes (sender's consumer position for flow control)
/// - len: 4 bytes (payload length)
///
/// # Safety
/// The buffer must have at least HEADER_SIZE bytes available.
#[inline]
pub unsafe fn encode_header(buf: *mut u8, call_id: u32, piggyback: u64, payload_len: u32) {
    let ptr = buf as *mut u32;
    std::ptr::write(ptr, call_id.to_le());
    std::ptr::write(ptr.add(1), (piggyback as u32).to_le());
    std::ptr::write(ptr.add(2), payload_len.to_le());
}

/// Decode a message header from the buffer.
///
/// # Safety
/// The buffer must have at least HEADER_SIZE bytes of valid data.
///
/// # Returns
/// (call_id, piggyback, payload_len)
#[inline]
pub unsafe fn decode_header(buf: *const u8) -> (u32, u32, u32) {
    let ptr = buf as *const u32;
    let call_id = u32::from_le(std::ptr::read(ptr));
    let piggyback = u32::from_le(std::ptr::read(ptr.add(1)));
    let payload_len = u32::from_le(std::ptr::read(ptr.add(2)));
    (call_id, piggyback, payload_len)
}

/// Check if a call_id indicates a response message.
#[inline]
pub fn is_response(call_id: u32) -> bool {
    call_id != PADDING_MARKER && (call_id & RESPONSE_FLAG) != 0
}

/// Check if a call_id indicates a padding marker (wrap-around).
#[inline]
pub fn is_padding_marker(call_id: u32) -> bool {
    call_id == PADDING_MARKER
}

/// Convert a request call_id to a response call_id.
#[inline]
pub fn to_response_id(call_id: u32) -> u32 {
    call_id | RESPONSE_FLAG
}

/// Extract the original call_id from a response call_id.
#[inline]
pub fn from_response_id(call_id: u32) -> u32 {
    call_id & !RESPONSE_FLAG
}

/// Calculate the padded message size (aligned to ALIGNMENT).
///
/// Total message size = HEADER_SIZE + payload_len, padded to ALIGNMENT boundary.
#[inline]
pub fn padded_message_size(payload_len: u32) -> u64 {
    let total = HEADER_SIZE as u64 + payload_len as u64;
    (total + ALIGNMENT - 1) & !(ALIGNMENT - 1)
}

/// Write a padding marker at the given buffer position.
///
/// The padding marker fills the remaining space at the end of the ring buffer
/// when a message would wrap around. The `remaining` parameter is the number
/// of bytes from the current position to the end of the ring.
///
/// # Safety
/// The buffer must have at least HEADER_SIZE bytes available.
#[inline]
pub unsafe fn write_padding_marker(buf: *mut u8, remaining: u32) {
    let ptr = buf as *mut u32;
    std::ptr::write(ptr, PADDING_MARKER.to_le());
    // Store remaining space in piggyback field for receiver to know how much to skip
    std::ptr::write(ptr.add(1), remaining.to_le());
    std::ptr::write(ptr.add(2), 0u32.to_le()); // payload_len = 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_imm_encoding() {
        assert_eq!(encode_imm(0), 0);
        assert_eq!(encode_imm(32), 1);
        assert_eq!(encode_imm(64), 2);
        assert_eq!(encode_imm(1024), 32);

        assert_eq!(decode_imm(0), 0);
        assert_eq!(decode_imm(1), 32);
        assert_eq!(decode_imm(2), 64);
        assert_eq!(decode_imm(32), 1024);
    }

    #[test]
    fn test_imm_roundtrip() {
        for delta in (0..1024).map(|x| x * 32) {
            assert_eq!(decode_imm(encode_imm(delta)), delta);
        }
    }

    #[test]
    fn test_header_encoding() {
        let mut buf = [0u8; HEADER_SIZE];
        unsafe {
            encode_header(buf.as_mut_ptr(), 42, 1000, 256);
            let (call_id, piggyback, len) = decode_header(buf.as_ptr());
            assert_eq!(call_id, 42);
            assert_eq!(piggyback, 1000);
            assert_eq!(len, 256);
        }
    }

    #[test]
    fn test_response_flag() {
        let request_id = 123;
        assert!(!is_response(request_id));

        let response_id = to_response_id(request_id);
        assert!(is_response(response_id));
        assert_eq!(from_response_id(response_id), request_id);
    }

    #[test]
    fn test_padded_message_size() {
        assert_eq!(padded_message_size(0), 32); // 12 bytes header -> 32
        assert_eq!(padded_message_size(20), 32); // 12 + 20 = 32 -> 32
        assert_eq!(padded_message_size(21), 64); // 12 + 21 = 33 -> 64
        assert_eq!(padded_message_size(52), 64); // 12 + 52 = 64 -> 64
        assert_eq!(padded_message_size(53), 96); // 12 + 53 = 65 -> 96
    }
}
