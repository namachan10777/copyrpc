//! Packet header and packet types for eRPC.
//!
//! The packet header is 16 bytes and contains all metadata needed for
//! request/response correlation and reliability.

use crate::error::{Error, Result};

/// Packet header size in bytes.
pub const PKT_HDR_SIZE: usize = 16;

/// Magic number for valid packets.
pub const ERPC_MAGIC: u8 = 0xEC;

/// Maximum message size (24 bits).
pub const MAX_MSG_SIZE: usize = (1 << 24) - 1;

/// Maximum request number (44 bits).
pub const MAX_REQ_NUM: u64 = (1 << 44) - 1;

/// Maximum packet number (14 bits).
pub const MAX_PKT_NUM: u16 = (1 << 14) - 1;

/// Packet type (2 bits).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PktType {
    /// Request packet (first or middle packet of a request).
    Req = 0,
    /// Request packet (first packet, expect single-packet response).
    ReqForResp = 1,
    /// Response packet (first or middle packet of a response).
    Resp = 2,
    /// Explicit credit return (no payload).
    CreditReturn = 3,
}

impl TryFrom<u8> for PktType {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(PktType::Req),
            1 => Ok(PktType::ReqForResp),
            2 => Ok(PktType::Resp),
            3 => Ok(PktType::CreditReturn),
            _ => Err(Error::InvalidPacketType(value)),
        }
    }
}

/// Packet header (16 bytes).
///
/// Layout:
/// ```text
/// Offset  Size  Field
/// 0       1     req_type
/// 1       3     msg_size (24-bit)
/// 4       2     dest_session_num
/// 6       2     pkt_type (2 bits) + pkt_num (14 bits)
/// 8       6     req_num (44 bits, 2 bytes padding)
/// 14      1     reserved
/// 15      1     magic
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C, packed)]
pub struct PktHdr {
    /// Request type (application-defined).
    pub req_type: u8,
    /// Message size in bytes (24 bits, stored as 3 bytes little-endian).
    msg_size_bytes: [u8; 3],
    /// Destination session number.
    pub dest_session_num: u16,
    /// Packet type (2 bits) and packet number (14 bits).
    pkt_type_num: u16,
    /// Request number (44 bits stored in 6 bytes).
    req_num_bytes: [u8; 6],
    /// Reserved byte.
    reserved: u8,
    /// Magic number for validation.
    pub magic: u8,
}

impl PktHdr {
    /// Create a new packet header.
    pub fn new(
        req_type: u8,
        msg_size: usize,
        dest_session_num: u16,
        pkt_type: PktType,
        pkt_num: u16,
        req_num: u64,
    ) -> Self {
        debug_assert!(msg_size <= MAX_MSG_SIZE);
        debug_assert!(pkt_num <= MAX_PKT_NUM);
        debug_assert!(req_num <= MAX_REQ_NUM);

        let mut hdr = Self {
            req_type,
            msg_size_bytes: [0; 3],
            dest_session_num,
            pkt_type_num: 0,
            req_num_bytes: [0; 6],
            reserved: 0,
            magic: ERPC_MAGIC,
        };

        hdr.set_msg_size(msg_size);
        hdr.set_pkt_type_num(pkt_type, pkt_num);
        hdr.set_req_num(req_num);

        hdr
    }

    /// Get the message size.
    #[inline]
    pub fn msg_size(&self) -> usize {
        (self.msg_size_bytes[0] as usize)
            | ((self.msg_size_bytes[1] as usize) << 8)
            | ((self.msg_size_bytes[2] as usize) << 16)
    }

    /// Set the message size.
    #[inline]
    fn set_msg_size(&mut self, size: usize) {
        self.msg_size_bytes[0] = (size & 0xFF) as u8;
        self.msg_size_bytes[1] = ((size >> 8) & 0xFF) as u8;
        self.msg_size_bytes[2] = ((size >> 16) & 0xFF) as u8;
    }

    /// Get the packet type.
    #[inline]
    pub fn pkt_type(&self) -> PktType {
        // Safe because we only store valid values
        match (self.pkt_type_num >> 14) & 0x03 {
            0 => PktType::Req,
            1 => PktType::ReqForResp,
            2 => PktType::Resp,
            _ => PktType::CreditReturn,
        }
    }

    /// Get the packet number.
    #[inline]
    pub fn pkt_num(&self) -> u16 {
        self.pkt_type_num & MAX_PKT_NUM
    }

    /// Set the packet type and packet number.
    #[inline]
    fn set_pkt_type_num(&mut self, pkt_type: PktType, pkt_num: u16) {
        self.pkt_type_num = ((pkt_type as u16) << 14) | (pkt_num & MAX_PKT_NUM);
    }

    /// Get the request number.
    #[inline]
    pub fn req_num(&self) -> u64 {
        (self.req_num_bytes[0] as u64)
            | ((self.req_num_bytes[1] as u64) << 8)
            | ((self.req_num_bytes[2] as u64) << 16)
            | ((self.req_num_bytes[3] as u64) << 24)
            | ((self.req_num_bytes[4] as u64) << 32)
            | (((self.req_num_bytes[5] & 0x0F) as u64) << 40)
    }

    /// Set the request number.
    #[inline]
    fn set_req_num(&mut self, req_num: u64) {
        self.req_num_bytes[0] = (req_num & 0xFF) as u8;
        self.req_num_bytes[1] = ((req_num >> 8) & 0xFF) as u8;
        self.req_num_bytes[2] = ((req_num >> 16) & 0xFF) as u8;
        self.req_num_bytes[3] = ((req_num >> 24) & 0xFF) as u8;
        self.req_num_bytes[4] = ((req_num >> 32) & 0xFF) as u8;
        self.req_num_bytes[5] = ((req_num >> 40) & 0x0F) as u8;
    }

    /// Get the request type.
    #[inline]
    pub fn req_type(&self) -> u8 {
        self.req_type
    }

    /// Get the destination session number.
    #[inline]
    pub fn dest_session_num(&self) -> u16 {
        self.dest_session_num
    }

    /// Check if this is a request packet.
    #[inline]
    pub fn is_request(&self) -> bool {
        matches!(self.pkt_type(), PktType::Req | PktType::ReqForResp)
    }

    /// Check if this is a response packet.
    #[inline]
    pub fn is_response(&self) -> bool {
        self.pkt_type() == PktType::Resp
    }

    /// Check if the magic number is valid.
    #[inline]
    pub fn is_valid(&self) -> bool {
        let magic = self.magic;
        magic == ERPC_MAGIC
    }

    /// Validate the packet header.
    pub fn validate(&self) -> Result<()> {
        let magic = self.magic;
        if magic != ERPC_MAGIC {
            return Err(Error::InvalidMagic {
                expected: ERPC_MAGIC,
                got: magic,
            });
        }
        Ok(())
    }

    /// Serialize the header to a byte slice.
    ///
    /// # Safety
    /// The destination buffer must be at least PKT_HDR_SIZE bytes.
    #[inline]
    pub unsafe fn write_to(&self, dst: *mut u8) {
        unsafe {
            std::ptr::copy_nonoverlapping(self as *const Self as *const u8, dst, PKT_HDR_SIZE);
        }
    }

    /// Deserialize a header from a byte slice.
    ///
    /// # Safety
    /// The source buffer must be at least PKT_HDR_SIZE bytes.
    #[inline]
    pub unsafe fn read_from(src: *const u8) -> Self {
        unsafe {
            let mut hdr = std::mem::MaybeUninit::<Self>::uninit();
            std::ptr::copy_nonoverlapping(src, hdr.as_mut_ptr() as *mut u8, PKT_HDR_SIZE);
            hdr.assume_init()
        }
    }

    /// Create a header from a byte slice.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < PKT_HDR_SIZE {
            return Err(Error::BufferTooSmall {
                required: PKT_HDR_SIZE,
                available: bytes.len(),
            });
        }
        let hdr = unsafe { Self::read_from(bytes.as_ptr()) };
        hdr.validate()?;
        Ok(hdr)
    }

    /// Calculate the number of packets needed for a message of given size.
    pub fn calc_num_pkts(msg_size: usize, mtu: usize) -> u16 {
        let data_per_pkt = mtu - PKT_HDR_SIZE;
        if msg_size == 0 {
            1
        } else {
            ((msg_size + data_per_pkt - 1) / data_per_pkt) as u16
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pkt_hdr_size() {
        assert_eq!(std::mem::size_of::<PktHdr>(), PKT_HDR_SIZE);
    }

    #[test]
    fn test_pkt_hdr_roundtrip() {
        let hdr = PktHdr::new(42, 0x123456, 0x1234, PktType::Req, 0x3FFF, 0x0FFF_FFFF_FFFF);

        assert_eq!(hdr.req_type(), 42);
        assert_eq!(hdr.msg_size(), 0x123456);
        assert_eq!(hdr.dest_session_num(), 0x1234);
        assert_eq!(hdr.pkt_type(), PktType::Req);
        assert_eq!(hdr.pkt_num(), 0x3FFF);
        assert_eq!(hdr.req_num(), 0x0FFF_FFFF_FFFF);
        assert!(hdr.is_valid());
    }

    #[test]
    fn test_pkt_hdr_serialize() {
        let hdr = PktHdr::new(1, 100, 5, PktType::Resp, 10, 12345);
        let mut buf = [0u8; PKT_HDR_SIZE];

        unsafe {
            hdr.write_to(buf.as_mut_ptr());
            let hdr2 = PktHdr::read_from(buf.as_ptr());
            assert_eq!(hdr, hdr2);
        }
    }

    #[test]
    fn test_pkt_types() {
        for pkt_type in [PktType::Req, PktType::ReqForResp, PktType::Resp, PktType::CreditReturn] {
            let hdr = PktHdr::new(0, 0, 0, pkt_type, 0, 0);
            assert_eq!(hdr.pkt_type(), pkt_type);
        }
    }

    #[test]
    fn test_calc_num_pkts() {
        let mtu = 4096;
        let data_per_pkt = mtu - PKT_HDR_SIZE;

        assert_eq!(PktHdr::calc_num_pkts(0, mtu), 1);
        assert_eq!(PktHdr::calc_num_pkts(1, mtu), 1);
        assert_eq!(PktHdr::calc_num_pkts(data_per_pkt, mtu), 1);
        assert_eq!(PktHdr::calc_num_pkts(data_per_pkt + 1, mtu), 2);
        assert_eq!(PktHdr::calc_num_pkts(data_per_pkt * 2, mtu), 2);
        assert_eq!(PktHdr::calc_num_pkts(data_per_pkt * 2 + 1, mtu), 3);
    }
}
