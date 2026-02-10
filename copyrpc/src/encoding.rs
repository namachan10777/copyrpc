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
        delta.is_multiple_of(ALIGNMENT),
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

/// Message count value indicating a wrap-around in the ring buffer.
/// When message_count == WRAP_MESSAGE_COUNT, the receiver should wrap to the beginning.
pub const WRAP_MESSAGE_COUNT: u32 = u32::MAX;

/// Flow control metadata size in bytes.
/// Contains consumer position, credit grant, and message count.
pub const FLOW_METADATA_SIZE: usize = 32;

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
    (call_id & RESPONSE_FLAG) != 0
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

/// Encode flow control metadata into the buffer.
///
/// Metadata layout (32 bytes):
/// - offset 0-7: ring_credit_return (u64 LE) — bytes consumed since last notification
/// - offset 8-15: credit_grant (u64 LE)
/// - offset 16-19: message_count (u32 LE)
/// - offset 20-31: zero padding
///
/// # Safety
/// The buffer must have at least FLOW_METADATA_SIZE bytes available.
#[inline]
pub unsafe fn encode_flow_metadata(
    buf: *mut u8,
    ring_credit_return: u64,
    credit_grant: u64,
    message_count: u32,
) {
    let ptr = buf as *mut u64;
    std::ptr::write(ptr, ring_credit_return.to_le());
    std::ptr::write(ptr.add(1), credit_grant.to_le());
    let ptr32 = buf.add(16) as *mut u32;
    std::ptr::write(ptr32, message_count.to_le());
    // Zero out padding bytes 20-31
    std::ptr::write_bytes(buf.add(20), 0, 12);
}

/// Decode flow control metadata from the buffer.
///
/// # Safety
/// The buffer must have at least FLOW_METADATA_SIZE bytes of valid data.
///
/// # Returns
/// (ring_credit_return, credit_grant, message_count)
#[inline]
pub unsafe fn decode_flow_metadata(buf: *const u8) -> (u64, u64, u32) {
    let ptr = buf as *const u64;
    let ring_credit_return = u64::from_le(std::ptr::read(ptr));
    let credit_grant = u64::from_le(std::ptr::read(ptr.add(1)));
    let ptr32 = buf.add(16) as *const u32;
    let message_count = u32::from_le(std::ptr::read(ptr32));
    (ring_credit_return, credit_grant, message_count)
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

    #[test]
    fn test_flow_metadata_encoding() {
        let mut buf = [0u8; FLOW_METADATA_SIZE];
        unsafe {
            encode_flow_metadata(buf.as_mut_ptr(), 12345, 67890, 42);
            let (ring_credit_return, credit_grant, message_count) =
                decode_flow_metadata(buf.as_ptr());
            assert_eq!(ring_credit_return, 12345);
            assert_eq!(credit_grant, 67890);
            assert_eq!(message_count, 42);

            // Verify zero padding
            for i in 20..32 {
                assert_eq!(buf[i], 0);
            }
        }
    }

    #[test]
    fn test_flow_metadata_wrap_message_count() {
        let mut buf = [0u8; FLOW_METADATA_SIZE];
        unsafe {
            encode_flow_metadata(buf.as_mut_ptr(), 1000, 2000, WRAP_MESSAGE_COUNT);
            let (ring_credit_return, credit_grant, message_count) =
                decode_flow_metadata(buf.as_ptr());
            assert_eq!(ring_credit_return, 1000);
            assert_eq!(credit_grant, 2000);
            assert_eq!(message_count, WRAP_MESSAGE_COUNT);
            assert_eq!(message_count, u32::MAX);
        }
    }

    #[test]
    fn test_flow_metadata_roundtrip() {
        let test_cases = [
            (0u64, 0u64, 0u32),
            (1, 1, 1),
            (u64::MAX, u64::MAX, u32::MAX),
            (0x1234_5678_9ABC_DEF0, 0xFEDC_BA98_7654_3210, 0x1234_5678),
        ];

        for (ring_credit_return, credit_grant, message_count) in test_cases {
            let mut buf = [0u8; FLOW_METADATA_SIZE];
            unsafe {
                encode_flow_metadata(
                    buf.as_mut_ptr(),
                    ring_credit_return,
                    credit_grant,
                    message_count,
                );
                let (decoded_ring_credit_return, decoded_credit_grant, decoded_message_count) =
                    decode_flow_metadata(buf.as_ptr());
                assert_eq!(decoded_ring_credit_return, ring_credit_return);
                assert_eq!(decoded_credit_grant, credit_grant);
                assert_eq!(decoded_message_count, message_count);
            }
        }
    }
}
