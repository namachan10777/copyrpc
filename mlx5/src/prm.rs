//! PRM (Programmable Reference Manual) command helpers.
//!
//! MLX5 uses PRM commands for creating and managing HW resources via DevX.
//! Commands are encoded as big-endian 32-bit dword arrays with bitfield packing.

use std::io;

// ============================================================================
// Bitfield helpers
// ============================================================================

/// Set a field (1-32 bits) in a PRM command buffer.
///
/// PRM layout: big-endian 32-bit dword array. Within each dword, bit 0 is MSB.
/// `bit_off` is the absolute bit offset from the start of the structure.
#[inline]
pub(crate) fn prm_set(buf: &mut [u8], bit_off: usize, bit_sz: usize, value: u32) {
    debug_assert!(bit_sz > 0 && bit_sz <= 32);
    debug_assert!(
        bit_off / 8 + 4 <= buf.len(),
        "prm_set out of bounds: off={:#x} sz={}",
        bit_off,
        bit_sz
    );
    let dw_idx = bit_off / 32;
    let dw_bit_off = 32 - bit_sz - (bit_off & 0x1f);
    let mask = if bit_sz == 32 {
        u32::MAX
    } else {
        (1u32 << bit_sz) - 1
    };
    let ptr = buf.as_mut_ptr() as *mut u32;
    unsafe {
        let old = u32::from_be(ptr.add(dw_idx).read_unaligned());
        let new = (old & !(mask << dw_bit_off)) | ((value & mask) << dw_bit_off);
        ptr.add(dw_idx).write_unaligned(new.to_be());
    }
}

/// Set a 64-bit field in a PRM command buffer.
#[inline]
pub(crate) fn prm_set64(buf: &mut [u8], bit_off: usize, value: u64) {
    prm_set(buf, bit_off, 32, (value >> 32) as u32);
    prm_set(buf, bit_off + 32, 32, value as u32);
}

/// Get a field (1-32 bits) from a PRM command buffer.
#[inline]
pub(crate) fn prm_get(buf: &[u8], bit_off: usize, bit_sz: usize) -> u32 {
    debug_assert!(bit_sz > 0 && bit_sz <= 32);
    let dw_idx = bit_off / 32;
    let dw_bit_off = 32 - bit_sz - (bit_off & 0x1f);
    let mask = if bit_sz == 32 {
        u32::MAX
    } else {
        (1u32 << bit_sz) - 1
    };
    let ptr = buf.as_ptr() as *const u32;
    unsafe { (u32::from_be(ptr.add(dw_idx).read_unaligned()) >> dw_bit_off) & mask }
}

/// Check PRM command result and return an error if it failed.
pub(crate) fn check_prm_result(ret: i32, out: &[u8], cmd_name: &str) -> io::Result<()> {
    if ret != 0 {
        let status = prm_get(out, 0x00, 8);
        let syndrome = prm_get(out, 0x20, 0x20);
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!(
                "{} failed: ret={}, status=0x{:x}, syndrome=0x{:x}, errno={}",
                cmd_name,
                ret,
                status,
                syndrome,
                io::Error::last_os_error()
            ),
        ));
    }
    Ok(())
}

// ============================================================================
// PRM command opcodes
// ============================================================================

pub(crate) const MLX5_CMD_OP_CREATE_CQ: u32 = 0x400;
pub(crate) const MLX5_CMD_OP_DESTROY_CQ: u32 = 0x401;
pub(crate) const MLX5_CMD_OP_CREATE_QP: u32 = 0x500;
pub(crate) const MLX5_CMD_OP_DESTROY_QP: u32 = 0x501;
pub(crate) const MLX5_CMD_OP_RST2INIT_QP: u32 = 0x502;
pub(crate) const MLX5_CMD_OP_INIT2RTR_QP: u32 = 0x503;
pub(crate) const MLX5_CMD_OP_RTR2RTS_QP: u32 = 0x504;
pub(crate) const MLX5_CMD_OP_2ERR_QP: u32 = 0x507;
pub(crate) const MLX5_CMD_OP_CREATE_SRQ: u32 = 0x700;
pub(crate) const MLX5_CMD_OP_DESTROY_SRQ: u32 = 0x701;

// ============================================================================
// create_qp_in / create_qp_out layout (bit offsets)
// ============================================================================

/// create_qp_in command header fields
pub(crate) const CREATE_QP_IN_OPCODE: (usize, usize) = (0x00, 0x10);
pub(crate) const CREATE_QP_IN_QPC_OFF: usize = 0xC0;
pub(crate) const CREATE_QP_IN_WQ_UMEM_OFFSET: usize = 0x800;
pub(crate) const CREATE_QP_IN_WQ_UMEM_ID: (usize, usize) = (0x840, 0x20);
pub(crate) const CREATE_QP_IN_WQ_UMEM_VALID: (usize, usize) = (0x860, 1);
pub(crate) const CREATE_QP_IN_PAS_OFF: usize = 0x880;
/// Base size of create_qp_in without PAS entries (in bytes)
pub(crate) const CREATE_QP_IN_BASE_SIZE: usize = CREATE_QP_IN_PAS_OFF / 8;

/// create_qp_out fields
pub(crate) const CREATE_QP_OUT_QPN: (usize, usize) = (0x48, 0x18);
pub(crate) const CREATE_QP_OUT_SIZE: usize = 0x80 / 8;

// ============================================================================
// modify_qp_in / modify_qp_out layout (shared by RST2INIT, INIT2RTR, RTR2RTS)
// ============================================================================

pub(crate) const MODIFY_QP_IN_OPCODE: (usize, usize) = (0x00, 0x10);
pub(crate) const MODIFY_QP_IN_QPN: (usize, usize) = (0x48, 0x18);
#[allow(dead_code)]
pub(crate) const MODIFY_QP_IN_OPT_PARAM_MASK: (usize, usize) = (0x80, 0x20);
pub(crate) const MODIFY_QP_IN_QPC_OFF: usize = 0xC0;
pub(crate) const MODIFY_QP_IN_SIZE: usize = 0x880 / 8;

pub(crate) const MODIFY_QP_OUT_SIZE: usize = 0x80 / 8;

// ============================================================================
// destroy_qp_in / destroy_qp_out layout
// ============================================================================

pub(crate) const DESTROY_QP_IN_OPCODE: (usize, usize) = (0x00, 0x10);
pub(crate) const DESTROY_QP_IN_QPN: (usize, usize) = (0x48, 0x18);
pub(crate) const DESTROY_QP_IN_SIZE: usize = 0x80 / 8;

pub(crate) const DESTROY_QP_OUT_SIZE: usize = 0x80 / 8;

// ============================================================================
// QPC field offsets (relative to QPC start within the command buffer)
//
// Derived from struct mlx5_ifc_qpc_bits in mlx5_ifc.h.
// Note: The ADS (Address Descriptor) in the actual firmware PRM is 0x160 bits,
// which is larger than the kernel header's 0x100 bits. This means all fields
// after the secondary ADS are shifted by +0xC0 compared to kernel offsets.
// ============================================================================

// Pre-ADS fields (same in kernel and firmware)
pub(crate) const QPC_ST: (usize, usize) = (0x08, 8);
pub(crate) const QPC_PM_STATE: (usize, usize) = (0x13, 2);
pub(crate) const QPC_PD: (usize, usize) = (0x28, 0x18);
pub(crate) const QPC_MTU: (usize, usize) = (0x40, 3);
pub(crate) const QPC_LOG_MSG_MAX: (usize, usize) = (0x43, 5);
pub(crate) const QPC_LOG_RQ_SIZE: (usize, usize) = (0x49, 4);
pub(crate) const QPC_LOG_RQ_STRIDE: (usize, usize) = (0x4D, 3);
pub(crate) const QPC_NO_SQ: (usize, usize) = (0x50, 1);
pub(crate) const QPC_LOG_SQ_SIZE: (usize, usize) = (0x51, 4);
pub(crate) const QPC_TS_FORMAT: (usize, usize) = (0x58, 2);
pub(crate) const QPC_UAR_PAGE: (usize, usize) = (0x68, 0x18);
pub(crate) const QPC_LOG_PAGE_SIZE: (usize, usize) = (0xA3, 5);
pub(crate) const QPC_REMOTE_QPN: (usize, usize) = (0xA8, 0x18);

// Primary / Secondary Address Path (ADS)
// ADS is 0x160 bits in the firmware PRM.
pub(crate) const QPC_PRI_ADS: usize = 0xC0;
#[allow(dead_code)]
pub(crate) const QPC_SEC_ADS: usize = 0xC0 + 0x160; // = 0x220

// Post-ADS fields (firmware offsets = kernel offsets + 0xC0)
pub(crate) const QPC_LOG_SRA_MAX: (usize, usize) = (0x388, 3);
pub(crate) const QPC_RETRY_COUNT: (usize, usize) = (0x38D, 3);
pub(crate) const QPC_RNR_RETRY: (usize, usize) = (0x390, 3);
pub(crate) const QPC_NEXT_SEND_PSN: (usize, usize) = (0x3C8, 0x18);
pub(crate) const QPC_CQN_SND: (usize, usize) = (0x3E8, 0x18);
pub(crate) const QPC_DETH_SQPN: (usize, usize) = (0x408, 0x18);
pub(crate) const QPC_LOG_RRA_MAX: (usize, usize) = (0x488, 3);
pub(crate) const QPC_ATOMIC_MODE: (usize, usize) = (0x48C, 4);
pub(crate) const QPC_RRE: (usize, usize) = (0x490, 1);
pub(crate) const QPC_RWE: (usize, usize) = (0x491, 1);
pub(crate) const QPC_RAE: (usize, usize) = (0x492, 1);
pub(crate) const QPC_PAGE_OFFSET: (usize, usize) = (0x494, 6);
pub(crate) const QPC_MIN_RNR_NAK: (usize, usize) = (0x4A3, 5);
pub(crate) const QPC_NEXT_RCV_PSN: (usize, usize) = (0x4A8, 0x18);
pub(crate) const QPC_CQN_RCV: (usize, usize) = (0x4E8, 0x18);
pub(crate) const QPC_DBR_ADDR: usize = 0x500;
pub(crate) const QPC_Q_KEY: (usize, usize) = (0x540, 0x20);
pub(crate) const QPC_RQ_TYPE: (usize, usize) = (0x565, 3);
pub(crate) const QPC_SRQN_RMPN_XRQN: (usize, usize) = (0x568, 0x18);
pub(crate) const QPC_DC_ACCESS_KEY: usize = 0x640;
pub(crate) const QPC_DBR_UMEM_VALID: (usize, usize) = (0x683, 1);
pub(crate) const QPC_DBR_UMEM_ID: (usize, usize) = (0x720, 0x20);

// QP service types
pub(crate) const QP_ST_RC: u32 = 0x00;
pub(crate) const QP_ST_UD: u32 = 0x03;
pub(crate) const QP_ST_DCR: u32 = 0x05;
pub(crate) const QP_ST_DCI: u32 = 0x06;

// ============================================================================
// ADS (Address Descriptor Segment) field offsets (relative to ADS start)
// ============================================================================

pub(crate) const ADS_PKEY_INDEX: (usize, usize) = (0x10, 0x10);
pub(crate) const ADS_GRH: (usize, usize) = (0x28, 1);
pub(crate) const ADS_RLID: (usize, usize) = (0x30, 0x10);
pub(crate) const ADS_ACK_TIMEOUT: (usize, usize) = (0x40, 5);
pub(crate) const ADS_SRC_ADDR_INDEX: (usize, usize) = (0x48, 8);
pub(crate) const ADS_HOP_LIMIT: (usize, usize) = (0x58, 8);
pub(crate) const ADS_TCLASS: (usize, usize) = (0x64, 8);
pub(crate) const ADS_FLOW_LABEL: (usize, usize) = (0x6C, 0x14);
pub(crate) const ADS_RGID_RIP: usize = 0x80; // 128-bit field
pub(crate) const ADS_SL: (usize, usize) = (0x124, 4);
pub(crate) const ADS_VHCA_PORT_NUM: (usize, usize) = (0x128, 8);

// ============================================================================
// create_cq_in / create_cq_out layout
// ============================================================================

pub(crate) const CREATE_CQ_IN_OPCODE: (usize, usize) = (0x00, 0x10);
pub(crate) const CREATE_CQ_IN_CQC_OFF: usize = 0x80;
pub(crate) const CREATE_CQ_IN_CQ_UMEM_OFFSET: usize = 0x280;
pub(crate) const CREATE_CQ_IN_CQ_UMEM_ID: (usize, usize) = (0x2C0, 0x20);
pub(crate) const CREATE_CQ_IN_CQ_UMEM_VALID: (usize, usize) = (0x2E0, 1);
pub(crate) const CREATE_CQ_IN_PAS_OFF: usize = 0x880;
/// Base size of create_cq_in without PAS entries (in bytes)
pub(crate) const CREATE_CQ_IN_BASE_SIZE: usize = CREATE_CQ_IN_PAS_OFF / 8;

pub(crate) const CREATE_CQ_OUT_CQN: (usize, usize) = (0x48, 0x18);
pub(crate) const CREATE_CQ_OUT_SIZE: usize = 0x80 / 8;

// ============================================================================
// destroy_cq_in / destroy_cq_out layout
// ============================================================================

pub(crate) const DESTROY_CQ_IN_OPCODE: (usize, usize) = (0x00, 0x10);
pub(crate) const DESTROY_CQ_IN_CQN: (usize, usize) = (0x48, 0x18);
pub(crate) const DESTROY_CQ_IN_SIZE: usize = 0x80 / 8;

pub(crate) const DESTROY_CQ_OUT_SIZE: usize = 0x80 / 8;

// ============================================================================
// CQC (CQ Context) field offsets (relative to CQC start)
//
// From struct mlx5_ifc_cqc_bits in mlx5_ifc.h.
// ============================================================================

pub(crate) const CQC_DBR_UMEM_VALID: (usize, usize) = (0x06, 1);
pub(crate) const CQC_DBR_UMEM_ID: (usize, usize) = (0x20, 0x20);
pub(crate) const CQC_CQE_SZ: (usize, usize) = (0x08, 3);
pub(crate) const CQC_LOG_CQ_SIZE: (usize, usize) = (0x63, 5);
pub(crate) const CQC_UAR_PAGE: (usize, usize) = (0x68, 0x18);
pub(crate) const CQC_C_EQN: (usize, usize) = (0xA0, 0x20);
pub(crate) const CQC_LOG_PAGE_SIZE: (usize, usize) = (0xC3, 5);
pub(crate) const CQC_DBR_ADDR: usize = 0x1C0;

// ============================================================================
// create_srq_in / create_srq_out layout
// ============================================================================

pub(crate) const CREATE_SRQ_IN_OPCODE: (usize, usize) = (0x00, 0x10);
pub(crate) const CREATE_SRQ_IN_SRQC_OFF: usize = 0x80;
pub(crate) const CREATE_SRQ_IN_PAS_OFF: usize = 0x880;
/// Base size of create_srq_in without PAS entries (in bytes)
pub(crate) const CREATE_SRQ_IN_BASE_SIZE: usize = CREATE_SRQ_IN_PAS_OFF / 8;

pub(crate) const CREATE_SRQ_OUT_SRQN: (usize, usize) = (0x48, 0x18);
pub(crate) const CREATE_SRQ_OUT_SIZE: usize = 0x80 / 8;

// ============================================================================
// destroy_srq_in / destroy_srq_out layout
// ============================================================================

pub(crate) const DESTROY_SRQ_IN_OPCODE: (usize, usize) = (0x00, 0x10);
pub(crate) const DESTROY_SRQ_IN_SRQN: (usize, usize) = (0x48, 0x18);
pub(crate) const DESTROY_SRQ_IN_SIZE: usize = 0x80 / 8;

pub(crate) const DESTROY_SRQ_OUT_SIZE: usize = 0x80 / 8;

// ============================================================================
// SRQC (SRQ Context) field offsets (relative to SRQC start)
//
// From struct mlx5_ifc_srqc_bits in mlx5_ifc.h.
// ============================================================================

pub(crate) const SRQC_LOG_SRQ_SIZE: (usize, usize) = (0x04, 4);
pub(crate) const SRQC_LOG_RQ_STRIDE: (usize, usize) = (0x25, 3);
pub(crate) const SRQC_CQN: (usize, usize) = (0x48, 0x18);
pub(crate) const SRQC_LOG_PAGE_SIZE: (usize, usize) = (0x82, 6);
pub(crate) const SRQC_PD: (usize, usize) = (0xC8, 0x18);
pub(crate) const SRQC_DBR_ADDR: usize = 0x140;
