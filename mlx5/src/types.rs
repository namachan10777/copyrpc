//! RDMA types with documentation.
//!
//! These types are layout-compatible with the corresponding mlx5_sys types,
//! allowing safe pointer casting while providing documented field access.

use std::net::{Ipv4Addr, Ipv6Addr};

use bitflags::bitflags;

/// Logical port state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum PortState {
    /// No state change.
    Nop = 0,
    /// Port is down.
    Down = 1,
    /// Port is initializing.
    Init = 2,
    /// Port is armed and ready to transition to active.
    Armed = 3,
    /// Port is active and fully operational.
    Active = 4,
    /// Port is active but deferred for link training.
    ActiveDefer = 5,
}

impl From<u32> for PortState {
    fn from(v: u32) -> Self {
        match v {
            0 => Self::Nop,
            1 => Self::Down,
            2 => Self::Init,
            3 => Self::Armed,
            4 => Self::Active,
            5 => Self::ActiveDefer,
            _ => Self::Nop,
        }
    }
}

/// MTU (Maximum Transmission Unit) size.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum Mtu {
    /// 256 bytes.
    Mtu256 = 1,
    /// 512 bytes.
    Mtu512 = 2,
    /// 1024 bytes.
    Mtu1024 = 3,
    /// 2048 bytes.
    Mtu2048 = 4,
    /// 4096 bytes.
    Mtu4096 = 5,
}

impl From<u32> for Mtu {
    fn from(v: u32) -> Self {
        match v {
            1 => Self::Mtu256,
            2 => Self::Mtu512,
            3 => Self::Mtu1024,
            4 => Self::Mtu2048,
            5 => Self::Mtu4096,
            _ => Self::Mtu256,
        }
    }
}

impl Mtu {
    /// Returns the MTU size in bytes.
    pub fn bytes(&self) -> usize {
        match self {
            Self::Mtu256 => 256,
            Self::Mtu512 => 512,
            Self::Mtu1024 => 1024,
            Self::Mtu2048 => 2048,
            Self::Mtu4096 => 4096,
        }
    }
}

/// Atomic operations support level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum AtomicCap {
    /// No atomic operations supported.
    None = 0,
    /// Atomic operations supported within the HCA.
    Hca = 1,
    /// Atomic operations supported globally.
    Glob = 2,
}

impl From<u32> for AtomicCap {
    fn from(v: u32) -> Self {
        match v {
            0 => Self::None,
            1 => Self::Hca,
            2 => Self::Glob,
            _ => Self::None,
        }
    }
}

bitflags! {
    /// HCA device capability flags.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct DeviceCapFlags: u32 {
        /// Device supports Reliable Connection (RC) transport.
        const RESIZE_MAX_WR = 1 << 0;
        /// Device supports bad P_Key counter.
        const BAD_PKEY_CNTR = 1 << 1;
        /// Device supports bad Q_Key counter.
        const BAD_QKEY_CNTR = 1 << 2;
        /// Device supports Raw Packet QP.
        const RAW_MULTI = 1 << 3;
        /// Device supports auto path migration.
        const AUTO_PATH_MIG = 1 << 4;
        /// Device supports change of physical port number.
        const CHANGE_PHY_PORT = 1 << 5;
        /// Device supports Address Handle port checking.
        const UD_AV_PORT_ENFORCE = 1 << 6;
        /// Device supports current QP state modification.
        const CURR_QP_STATE_MOD = 1 << 7;
        /// Device supports shutdown port.
        const SHUTDOWN_PORT = 1 << 8;
        /// Device supports initialization type.
        const INIT_TYPE = 1 << 9;
        /// Device supports port active event.
        const PORT_ACTIVE_EVENT = 1 << 10;
        /// Device supports system image GUID modification.
        const SYS_IMAGE_GUID = 1 << 11;
        /// Device supports RC RNR-NAK generation.
        const RC_RNR_NAK_GEN = 1 << 12;
        /// Device supports SRQ resize.
        const SRQ_RESIZE = 1 << 13;
        /// Device supports N_notify_CQ.
        const N_NOTIFY_CQ = 1 << 14;
        /// Device supports memory window type 2.
        const MEM_WINDOW_TYPE_2A = 1 << 17;
        /// Device supports memory window type 2b.
        const MEM_WINDOW_TYPE_2B = 1 << 18;
        /// Device supports RC IP CSUM offload.
        const RC_IP_CSUM = 1 << 19;
        /// Device supports XRC.
        const XRC = 1 << 20;
        /// Device supports Raw Packet QP.
        const RAW_IP_CSUM = 1 << 21;
        /// Device supports managed flow steering.
        const MANAGED_FLOW_STEERING = 1 << 29;
    }
}

/// Link layer protocol type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum LinkLayer {
    /// Unspecified link layer.
    Unspecified = 0,
    /// InfiniBand link layer.
    InfiniBand = 1,
    /// Ethernet link layer (RoCE).
    Ethernet = 2,
}

impl From<u8> for LinkLayer {
    fn from(v: u8) -> Self {
        match v {
            1 => Self::InfiniBand,
            2 => Self::Ethernet,
            _ => Self::Unspecified,
        }
    }
}

/// Device attributes returned by `ibv_query_device`.
///
/// This struct is layout-compatible with `mlx5_sys::ibv_device_attr`.
#[repr(C)]
#[derive(Debug, Clone)]
pub struct DeviceAttr {
    /// Firmware version string.
    pub fw_ver: [std::ffi::c_char; 64],
    /// Node GUID (in network byte order).
    pub node_guid: u64,
    /// System image GUID (in network byte order).
    pub sys_image_guid: u64,
    /// Largest contiguous block that can be registered.
    pub max_mr_size: u64,
    /// Supported memory page sizes as a bitmask.
    pub page_size_cap: u64,
    /// Vendor ID, per IEEE.
    pub vendor_id: u32,
    /// Vendor supplied part ID.
    pub vendor_part_id: u32,
    /// Hardware version.
    pub hw_ver: u32,
    /// Maximum number of supported QPs.
    pub max_qp: i32,
    /// Maximum number of outstanding WR on any work queue.
    pub max_qp_wr: i32,
    /// HCA capabilities mask.
    pub device_cap_flags: u32,
    /// Maximum number of scatter/gather entries per WR for non-RDMA Read operations.
    pub max_sge: i32,
    /// Maximum number of scatter/gather entries per WR for RDMA Read operations.
    pub max_sge_rd: i32,
    /// Maximum number of supported CQs.
    pub max_cq: i32,
    /// Maximum number of CQE capacity per CQ.
    pub max_cqe: i32,
    /// Maximum number of supported MRs.
    pub max_mr: i32,
    /// Maximum number of supported PDs.
    pub max_pd: i32,
    /// Maximum number of RDMA Read & Atomic operations that can be outstanding per QP.
    pub max_qp_rd_atom: i32,
    /// Maximum number of RDMA Read & Atomic operations that can be outstanding per EEC.
    pub max_ee_rd_atom: i32,
    /// Maximum number of resources used for RDMA Read & Atomic operations by this HCA as the Target.
    pub max_res_rd_atom: i32,
    /// Maximum depth per QP for initiation of RDMA Read & Atomic operations.
    pub max_qp_init_rd_atom: i32,
    /// Maximum depth per EEC for initiation of RDMA Read & Atomic operations.
    pub max_ee_init_rd_atom: i32,
    /// Atomic operations support level.
    pub atomic_cap: AtomicCap,
    /// Maximum number of supported EE contexts.
    pub max_ee: i32,
    /// Maximum number of supported RD domains.
    pub max_rdd: i32,
    /// Maximum number of supported Memory Windows.
    pub max_mw: i32,
    /// Maximum number of supported raw IPv6 datagram QPs.
    pub max_raw_ipv6_qp: i32,
    /// Maximum number of supported Ethertype datagram QPs.
    pub max_raw_ethy_qp: i32,
    /// Maximum number of supported multicast groups.
    pub max_mcast_grp: i32,
    /// Maximum number of QPs per multicast group which can be attached.
    pub max_mcast_qp_attach: i32,
    /// Maximum number of QPs which can be attached to multicast groups.
    pub max_total_mcast_qp_attach: i32,
    /// Maximum number of supported address handles.
    pub max_ah: i32,
    /// Maximum number of supported FMRs.
    pub max_fmr: i32,
    /// Maximum number of (re)maps per FMR before an unmap operation is required.
    pub max_map_per_fmr: i32,
    /// Maximum number of supported SRQs.
    pub max_srq: i32,
    /// Maximum number of WRs per SRQ.
    pub max_srq_wr: i32,
    /// Maximum number of scatter/gather entries per SRQ.
    pub max_srq_sge: i32,
    /// Maximum number of partitions.
    pub max_pkeys: u16,
    /// Local CA ACK delay.
    pub local_ca_ack_delay: u8,
    /// Number of physical ports.
    pub phys_port_cnt: u8,
}

/// Port attributes returned by `ibv_query_port`.
///
/// This struct is layout-compatible with `mlx5_sys::ibv_port_attr`.
#[repr(C)]
#[derive(Debug, Clone)]
pub struct PortAttr {
    /// Logical port state.
    pub state: PortState,
    /// Maximum MTU supported by the port.
    pub max_mtu: Mtu,
    /// Currently active MTU.
    pub active_mtu: Mtu,
    /// Length of the source GID table.
    pub gid_tbl_len: i32,
    /// Port capabilities flags.
    pub port_cap_flags: u32,
    /// Maximum message size.
    pub max_msg_sz: u32,
    /// Bad P_Key counter.
    pub bad_pkey_cntr: u32,
    /// Q_Key violation counter.
    pub qkey_viol_cntr: u32,
    /// Length of the partition table.
    pub pkey_tbl_len: u16,
    /// Base port LID (Local Identifier).
    pub lid: u16,
    /// Subnet Manager LID.
    pub sm_lid: u16,
    /// LID Mask Control (number of low-order bits of LID that can be used for path selection).
    pub lmc: u8,
    /// Maximum number of Virtual Lanes.
    pub max_vl_num: u8,
    /// Subnet Manager Service Level.
    pub sm_sl: u8,
    /// Subnet propagation delay.
    pub subnet_timeout: u8,
    /// Type of initialization performed by Subnet Manager.
    pub init_type_reply: u8,
    /// Currently active link width.
    pub active_width: u8,
    /// Currently active link speed.
    pub active_speed: u8,
    /// Physical port state.
    pub phys_state: u8,
    /// Link layer protocol (InfiniBand or Ethernet/RoCE).
    pub link_layer: u8,
    /// Port flags (e.g., IBV_QPF_GRH_REQUIRED).
    pub flags: u8,
    /// Extended port capabilities flags.
    pub port_cap_flags2: u16,
    /// Extended active link speed (use this if non-zero, otherwise use active_speed).
    pub active_speed_ex: u32,
}

bitflags! {
    /// mlx5 device context flags.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct Mlx5ContextFlags: u64 {
        /// CQE version 1 is needed.
        const CQE_V1 = 1 << 0;
        /// Obsolete flag, do not use.
        const OBSOLETE = 1 << 1;
        /// Multi-packet WQE is allowed.
        const MPW_ALLOWED = 1 << 2;
        /// Enhanced multi-packet WQE is supported.
        const ENHANCED_MPW = 1 << 3;
        /// CQE 128B compression is supported.
        const CQE_128B_COMP = 1 << 4;
        /// CQE 128B padding is supported.
        const CQE_128B_PAD = 1 << 5;
        /// Packet-based credit mode in RC QP is supported.
        const PACKET_BASED_CREDIT_MODE = 1 << 6;
        /// CQE timestamp will be in real time format.
        const REAL_TIME_TS = 1 << 7;
    }
}

/// CQE compression capabilities.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct CqeCompCaps {
    /// Maximum number of CQE compression entries.
    pub max_num: u32,
    /// Supported compression formats.
    pub supported_format: u32,
}

/// Software parsing capabilities.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct SwParsingCaps {
    /// Supported software parsing offloads.
    pub sw_parsing_offloads: u32,
    /// Supported QP types for software parsing.
    pub supported_qpts: u32,
}

/// Striding RQ capabilities.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct StridingRqCaps {
    /// Minimum log size of each stride in bytes.
    pub min_single_stride_log_num_of_bytes: u32,
    /// Maximum log size of each stride in bytes.
    pub max_single_stride_log_num_of_bytes: u32,
    /// Minimum log number of strides per WQE.
    pub min_single_wqe_log_num_of_strides: u32,
    /// Maximum log number of strides per WQE.
    pub max_single_wqe_log_num_of_strides: u32,
    /// Supported QP types for striding RQ.
    pub supported_qpts: u32,
}

/// Signature offload capabilities.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct SigCaps {
    /// Supported block sizes.
    pub block_size: u64,
    /// Supported block protection types.
    pub block_prot: u32,
    /// Supported T10-DIF background types.
    pub t10dif_bg: u16,
    /// Supported CRC types.
    pub crc_type: u16,
}

/// DCI streams capabilities.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct DciStreamsCaps {
    /// Maximum log number of concurrent streams that can be handled by HW.
    pub max_log_num_concurent: u8,
    /// Maximum DCI error stream channels supported per DCI before it moves to error state.
    pub max_log_num_errored: u8,
}

/// Crypto capabilities.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct CryptoCaps {
    /// Non-zero indicates self-test errors that may render crypto engines unusable.
    pub failed_selftests: u16,
    /// Supported crypto engines.
    pub crypto_engines: u8,
    /// Wrapped import method capabilities.
    pub wrapped_import_method: u8,
    /// Log of maximum number of DEKs.
    pub log_max_num_deks: u8,
    /// Padding.
    pub _reserved: [u8; 3],
    /// Crypto capability flags.
    pub flags: u32,
}

/// Register C0 for FDB matching.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct RegC0 {
    /// Value to match.
    pub value: u32,
    /// Mask for matching.
    pub mask: u32,
}

/// Out-of-order receive WRs capabilities.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct OooRecvWrsCaps {
    /// Maximum for RC QPs.
    pub max_rc: u32,
    /// Maximum for XRC QPs.
    pub max_xrc: u32,
    /// Maximum for DCT QPs.
    pub max_dct: u32,
    /// Maximum for UD QPs.
    pub max_ud: u32,
    /// Maximum for UC QPs.
    pub max_uc: u32,
}

/// mlx5-specific device attributes returned by `mlx5dv_query_device`.
///
/// This struct is layout-compatible with `mlx5_sys::mlx5dv_context`.
#[repr(C)]
#[derive(Debug, Clone)]
pub struct Mlx5DeviceAttr {
    /// Format version of internal hardware structures.
    pub version: u8,
    /// Device capability flags.
    pub flags: u64,
    /// Indicates which optional fields are valid.
    pub comp_mask: u64,
    /// CQE compression capabilities.
    pub cqe_comp_caps: CqeCompCaps,
    /// Software parsing capabilities.
    pub sw_parsing_caps: SwParsingCaps,
    /// Striding RQ capabilities.
    pub striding_rq_caps: StridingRqCaps,
    /// Tunnel offload capabilities.
    pub tunnel_offloads_caps: u32,
    /// Maximum blue-flame registers that can be dynamically allocated.
    pub max_dynamic_bfregs: u32,
    /// Maximum clock info update interval in nanoseconds.
    pub max_clock_info_update_nsec: u64,
    /// Flow action capability flags.
    pub flow_action_flags: u32,
    /// DC ODP capabilities.
    pub dc_odp_caps: u32,
    /// Pointer to HCA core clock (mapped memory location).
    pub hca_core_clock: *mut std::ffi::c_void,
    /// Number of LAG ports.
    pub num_lag_ports: u8,
    /// Signature offload capabilities.
    pub sig_caps: SigCaps,
    /// DCI streams capabilities.
    pub dci_streams_caps: DciStreamsCaps,
    /// Maximum length supported by DMA memcpy WR.
    pub max_wr_memcpy_length: usize,
    /// Crypto capabilities.
    pub crypto_caps: CryptoCaps,
    /// Maximum outstanding RDMA read/atomic per DC QP as requester.
    pub max_dc_rd_atom: u64,
    /// Maximum outstanding RDMA read/atomic per DC QP as responder.
    pub max_dc_init_rd_atom: u64,
    /// Value and mask to match local vport egress traffic in FDB.
    pub reg_c0: RegC0,
    /// Out-of-order receive WRs capabilities per QP type.
    pub ooo_recv_wrs_caps: OooRecvWrsCaps,
}

// =============================================================================
// GID and GRH Types for RoCE
// =============================================================================

/// GID (Global Identifier) - 128-bit address for RoCE.
///
/// In RoCE v2, the GID is derived from the IP address:
/// - For IPv4: Uses IPv4-mapped IPv6 format (::ffff:x.x.x.x)
/// - For IPv6: Uses the IPv6 address directly
///
/// # NOTE: RoCE support is untested (IB-only hardware environment)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Gid {
    /// Raw 128-bit GID value in network byte order
    pub raw: [u8; 16],
}

impl Gid {
    /// Create a new GID with all zeros.
    pub const fn zero() -> Self {
        Self { raw: [0u8; 16] }
    }

    /// Create a GID from raw bytes.
    pub const fn from_raw(raw: [u8; 16]) -> Self {
        Self { raw }
    }

    /// Create a GID from an IPv4 address (IPv4-mapped IPv6 format).
    ///
    /// The resulting GID is in the format: ::ffff:a.b.c.d
    /// where a.b.c.d is the IPv4 address.
    ///
    /// # NOTE: RoCE support is untested
    pub fn from_ipv4(ip: Ipv4Addr) -> Self {
        let octets = ip.octets();
        let raw = [
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // ::
            0x00, 0x00, 0xff, 0xff, // ffff
            octets[0], octets[1], octets[2], octets[3], // a.b.c.d
        ];
        Self { raw }
    }

    /// Create a GID from an IPv6 address.
    ///
    /// # NOTE: RoCE support is untested
    pub fn from_ipv6(ip: Ipv6Addr) -> Self {
        Self {
            raw: ip.octets(),
        }
    }

    /// Check if this GID is an IPv4-mapped address.
    pub fn is_ipv4_mapped(&self) -> bool {
        self.raw[0..10] == [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
            && self.raw[10] == 0xff
            && self.raw[11] == 0xff
    }

    /// Extract IPv4 address if this is an IPv4-mapped GID.
    pub fn to_ipv4(&self) -> Option<Ipv4Addr> {
        if self.is_ipv4_mapped() {
            Some(Ipv4Addr::new(
                self.raw[12],
                self.raw[13],
                self.raw[14],
                self.raw[15],
            ))
        } else {
            None
        }
    }

    /// Convert to IPv6 address.
    pub fn to_ipv6(&self) -> Ipv6Addr {
        Ipv6Addr::from(self.raw)
    }
}

impl From<Ipv4Addr> for Gid {
    fn from(ip: Ipv4Addr) -> Self {
        Self::from_ipv4(ip)
    }
}

impl From<Ipv6Addr> for Gid {
    fn from(ip: Ipv6Addr) -> Self {
        Self::from_ipv6(ip)
    }
}

impl From<[u8; 16]> for Gid {
    fn from(raw: [u8; 16]) -> Self {
        Self { raw }
    }
}

/// GRH (Global Route Header) attributes for RoCE addressing.
///
/// Contains the routing information needed for RoCE v2 communication.
/// In RoCE v2, the GRH fields are mapped to IP/UDP headers:
/// - dgid → Destination IP address
/// - sgid_index → Source GID table index (determines source IP)
/// - flow_label → IPv6 flow label (used for ECMP load balancing)
/// - traffic_class → IPv6 traffic class / IPv4 DSCP
/// - hop_limit → IPv6 hop limit / IPv4 TTL
///
/// # NOTE: RoCE support is untested (IB-only hardware environment)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GrhAttr {
    /// Destination GID (remote node's GID, derived from IP address)
    pub dgid: Gid,
    /// Source GID table index (local GID to use as source address)
    pub sgid_index: u8,
    /// Flow label for ECMP (Equal-Cost Multi-Path) routing.
    /// Only the lower 20 bits are used.
    pub flow_label: u32,
    /// Traffic class (QoS) - mapped to DSCP for IPv4, traffic class for IPv6
    pub traffic_class: u8,
    /// Hop limit (TTL) - number of router hops before packet is discarded
    pub hop_limit: u8,
}

impl Default for GrhAttr {
    fn default() -> Self {
        Self {
            dgid: Gid::zero(),
            sgid_index: 0,
            flow_label: 0,
            traffic_class: 0,
            hop_limit: 64, // Default TTL
        }
    }
}

impl GrhAttr {
    /// Create a new GRH attributes with the specified destination GID.
    ///
    /// Uses default values for other fields:
    /// - sgid_index: 0 (first GID in local table)
    /// - flow_label: 0 (no ECMP hashing)
    /// - traffic_class: 0 (default QoS)
    /// - hop_limit: 64 (standard TTL)
    ///
    /// # NOTE: RoCE support is untested
    pub fn new(dgid: Gid, sgid_index: u8) -> Self {
        Self {
            dgid,
            sgid_index,
            ..Default::default()
        }
    }

    /// Set the flow label for ECMP load balancing.
    ///
    /// The flow label is used by network switches to perform hash-based
    /// load balancing across multiple paths. Only the lower 20 bits are used.
    pub fn with_flow_label(mut self, flow_label: u32) -> Self {
        self.flow_label = flow_label & 0xFFFFF;
        self
    }

    /// Set the traffic class for QoS.
    pub fn with_traffic_class(mut self, traffic_class: u8) -> Self {
        self.traffic_class = traffic_class;
        self
    }

    /// Set the hop limit (TTL).
    pub fn with_hop_limit(mut self, hop_limit: u8) -> Self {
        self.hop_limit = hop_limit;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_device_attr_layout() {
        assert_eq!(
            std::mem::size_of::<DeviceAttr>(),
            std::mem::size_of::<mlx5_sys::ibv_device_attr>(),
            "DeviceAttr size mismatch"
        );
        assert_eq!(
            std::mem::align_of::<DeviceAttr>(),
            std::mem::align_of::<mlx5_sys::ibv_device_attr>(),
            "DeviceAttr alignment mismatch"
        );
    }

    #[test]
    fn test_port_attr_layout() {
        assert_eq!(
            std::mem::size_of::<PortAttr>(),
            std::mem::size_of::<mlx5_sys::ibv_port_attr>(),
            "PortAttr size mismatch"
        );
        assert_eq!(
            std::mem::align_of::<PortAttr>(),
            std::mem::align_of::<mlx5_sys::ibv_port_attr>(),
            "PortAttr alignment mismatch"
        );
    }

    #[test]
    fn test_mlx5_device_attr_layout() {
        assert_eq!(
            std::mem::size_of::<Mlx5DeviceAttr>(),
            std::mem::size_of::<mlx5_sys::mlx5dv_context>(),
            "Mlx5DeviceAttr size mismatch"
        );
        assert_eq!(
            std::mem::align_of::<Mlx5DeviceAttr>(),
            std::mem::align_of::<mlx5_sys::mlx5dv_context>(),
            "Mlx5DeviceAttr alignment mismatch"
        );
    }
}
