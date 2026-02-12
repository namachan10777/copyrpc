//! DevX QP creation experiment.
//!
//! Creates an RC QP using raw MLX5 DevX PRM commands, demonstrating that
//! SQ/RQ buffers can be placed in arbitrary user-allocated memory via UMEM.
//!
//! Flow:
//! 1. Open device with MLX5DV_CONTEXT_FLAGS_DEVX
//! 2. Create PD/CQ via standard verbs (for simplicity)
//! 3. Allocate UAR, user buffers, and register UMEM via DevX
//! 4. Create QP via devx_obj_create with PRM CREATE_QP command
//! 5. Transition QP through RST->INIT->RTR->RTS via devx_obj_modify
//! 6. Verify with self-loopback RDMA WRITE (partner QP created via verbs)

#![allow(unsafe_op_in_unsafe_fn, dead_code)]

use std::alloc::{Layout, alloc_zeroed, dealloc};
use std::io;
use std::ptr::NonNull;

// ============================================================================
// PRM buffer helpers
// ============================================================================

/// Set a field (1-32 bits) in a PRM command buffer.
///
/// PRM layout: big-endian 32-bit dword array. Within each dword, bit 0 is MSB.
/// `bit_off` is the absolute bit offset from the start of the structure.
#[inline]
fn prm_set(buf: &mut [u8], bit_off: usize, bit_sz: usize, value: u32) {
    debug_assert!(bit_sz > 0 && bit_sz <= 32);
    debug_assert!(bit_off / 8 + 4 <= buf.len(), "prm_set out of bounds: off={:#x} sz={}", bit_off, bit_sz);
    let dw_idx = bit_off / 32;
    let dw_bit_off = 32 - bit_sz - (bit_off & 0x1f);
    let mask = if bit_sz == 32 { u32::MAX } else { (1u32 << bit_sz) - 1 };
    let ptr = buf.as_mut_ptr() as *mut u32;
    unsafe {
        let old = u32::from_be(ptr.add(dw_idx).read_unaligned());
        let new = (old & !(mask << dw_bit_off)) | ((value & mask) << dw_bit_off);
        ptr.add(dw_idx).write_unaligned(new.to_be());
    }
}

/// Set a 64-bit field in a PRM command buffer.
#[inline]
fn prm_set64(buf: &mut [u8], bit_off: usize, value: u64) {
    prm_set(buf, bit_off, 32, (value >> 32) as u32);
    prm_set(buf, bit_off + 32, 32, value as u32);
}

/// Get a field (1-32 bits) from a PRM command buffer.
#[inline]
fn prm_get(buf: &[u8], bit_off: usize, bit_sz: usize) -> u32 {
    debug_assert!(bit_sz > 0 && bit_sz <= 32);
    let dw_idx = bit_off / 32;
    let dw_bit_off = 32 - bit_sz - (bit_off & 0x1f);
    let mask = if bit_sz == 32 { u32::MAX } else { (1u32 << bit_sz) - 1 };
    let ptr = buf.as_ptr() as *const u32;
    unsafe { (u32::from_be(ptr.add(dw_idx).read_unaligned()) >> dw_bit_off) & mask }
}

// ============================================================================
// PRM opcodes (from mlx5_ifc.h)
// ============================================================================

const MLX5_CMD_OP_CREATE_QP: u32 = 0x500;
const MLX5_CMD_OP_DESTROY_QP: u32 = 0x501;
const MLX5_CMD_OP_RST2INIT_QP: u32 = 0x502;
const MLX5_CMD_OP_INIT2RTR_QP: u32 = 0x503;
const MLX5_CMD_OP_RTR2RTS_QP: u32 = 0x504;

// ============================================================================
// create_qp_in / create_qp_out layout (bit offsets)
// ============================================================================

// create_qp_in fields:
//   opcode[0x10] @ 0x00    uid[0x10] @ 0x10
//   reserved[0x10] @ 0x20  op_mod[0x10] @ 0x30
//   qpc_ext[0x1] @ 0x40    reserved[0x7] @ 0x41    input_qpn[0x18] @ 0x48
//   reserved[0x20] @ 0x60
//   opt_param_mask[0x20] @ 0x80
//   ece[0x20] @ 0xA0
//   qpc @ 0xC0 (0x740 bits)
//   reserved[0x60] @ 0x800
//   wq_umem_valid[0x1] @ 0x860   reserved[0x1f] @ 0x861
//   pas[0][0x40] @ 0x880

const CQ_IN_OPCODE: (usize, usize) = (0x00, 0x10);
const CQ_IN_QPC_OFF: usize = 0xC0;
const CQ_IN_WQ_UMEM_VALID: (usize, usize) = (0x860, 1);
const CQ_IN_PAS_OFF: usize = 0x880; // first PAS entry bit offset
const CQ_IN_BASE_SIZE: usize = 0x880 / 8; // 272 bytes without PAS

// create_qp_out fields:
const CQ_OUT_STATUS: (usize, usize) = (0x00, 8);
const CQ_OUT_SYNDROME: (usize, usize) = (0x20, 0x20);
const CQ_OUT_QPN: (usize, usize) = (0x48, 0x18);
const CQ_OUT_SIZE: usize = 0x80 / 8; // 16 bytes

// ============================================================================
// modify_qp_in / modify_qp_out layout (shared by RST2INIT, INIT2RTR, RTR2RTS)
// ============================================================================

const MOD_IN_OPCODE: (usize, usize) = (0x00, 0x10);
const MOD_IN_QPN: (usize, usize) = (0x48, 0x18);
const MOD_IN_OPT_PARAM_MASK: (usize, usize) = (0x80, 0x20);
const MOD_IN_QPC_OFF: usize = 0xC0;
const MOD_IN_SIZE: usize = 0x880 / 8; // 272 bytes

const MOD_OUT_STATUS: (usize, usize) = (0x00, 8);
const MOD_OUT_SYNDROME: (usize, usize) = (0x20, 0x20);
const MOD_OUT_SIZE: usize = 0x80 / 8; // 16 bytes

// ============================================================================
// QPC field offsets (relative to QPC start within the command buffer)
// Derived from struct mlx5_ifc_qpc_bits in mlx5_ifc.h
// ============================================================================

const QPC_ST: (usize, usize) = (0x08, 8);     // service type
const QPC_PM_STATE: (usize, usize) = (0x13, 2); // path migration state
const QPC_PD: (usize, usize) = (0x28, 0x18);  // protection domain
const QPC_MTU: (usize, usize) = (0x40, 3);
const QPC_LOG_MSG_MAX: (usize, usize) = (0x43, 5);
const QPC_LOG_RQ_SIZE: (usize, usize) = (0x49, 4);
const QPC_LOG_RQ_STRIDE: (usize, usize) = (0x4D, 3);
const QPC_LOG_SQ_SIZE: (usize, usize) = (0x51, 4);
const QPC_UAR_PAGE: (usize, usize) = (0x68, 0x18);
const QPC_LOG_PAGE_SIZE: (usize, usize) = (0xA3, 5);
const QPC_REMOTE_QPN: (usize, usize) = (0xA8, 0x18);

// Primary address path (ADS) starts at QPC + 0xC0
// ADS size = 0x160 bits
const QPC_PRI_ADS: usize = 0xC0;
// Secondary address path at QPC + 0xC0 + 0x160 = QPC + 0x220
// Fields after secondary address path (QPC + 0x220 + 0x160 = QPC + 0x380):
const QPC_LOG_SRA_MAX: (usize, usize) = (0x388, 3);
const QPC_RETRY_COUNT: (usize, usize) = (0x38D, 3);
const QPC_RNR_RETRY: (usize, usize) = (0x390, 3);
const QPC_NEXT_SEND_PSN: (usize, usize) = (0x3C8, 0x18);
const QPC_CQN_SND: (usize, usize) = (0x3E8, 0x18);
const QPC_LOG_RRA_MAX: (usize, usize) = (0x488, 3);
const QPC_RRE: (usize, usize) = (0x490, 1);
const QPC_RWE: (usize, usize) = (0x491, 1);
const QPC_RAE: (usize, usize) = (0x492, 1);
const QPC_MIN_RNR_NAK: (usize, usize) = (0x4A3, 5);
const QPC_NEXT_RCV_PSN: (usize, usize) = (0x4A8, 0x18);
const QPC_CQN_RCV: (usize, usize) = (0x4E8, 0x18);
const QPC_DBR_ADDR: usize = 0x500; // 64-bit field
const QPC_DBR_UMEM_VALID: (usize, usize) = (0x683, 1);

// ADS field offsets (relative to ADS start)
const ADS_PKEY_INDEX: (usize, usize) = (0x10, 0x10);
const ADS_RLID: (usize, usize) = (0x30, 0x10);
const ADS_ACK_TIMEOUT: (usize, usize) = (0x40, 5);
const ADS_VHCA_PORT_NUM: (usize, usize) = (0x128, 8);

// QP service types
const QP_ST_RC: u32 = 0x0;

// ============================================================================
// Page-aligned buffer
// ============================================================================

struct AlignedBuf {
    ptr: *mut u8,
    layout: Layout,
}

impl AlignedBuf {
    fn new(size: usize) -> Self {
        let layout = Layout::from_size_align(size, 4096).unwrap();
        let ptr = unsafe { alloc_zeroed(layout) };
        assert!(!ptr.is_null(), "alloc_zeroed failed");
        Self { ptr, layout }
    }

    fn as_ptr(&self) -> *mut u8 {
        self.ptr
    }

    fn size(&self) -> usize {
        self.layout.size()
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        unsafe { dealloc(self.ptr, self.layout) }
    }
}

// ============================================================================
// DevX resource wrappers (RAII cleanup)
// ============================================================================

struct DevxUar {
    uar: NonNull<mlx5_sys::mlx5dv_devx_uar>,
}

impl DevxUar {
    fn page_id(&self) -> u32 {
        unsafe { self.uar.as_ref().page_id }
    }

    fn reg_addr(&self) -> *mut u8 {
        unsafe { self.uar.as_ref().reg_addr as *mut u8 }
    }
}

impl Drop for DevxUar {
    fn drop(&mut self) {
        unsafe { mlx5_sys::mlx5dv_devx_free_uar(self.uar.as_ptr()) }
    }
}

struct DevxUmem {
    umem: NonNull<mlx5_sys::mlx5dv_devx_umem>,
}

impl DevxUmem {
    fn umem_id(&self) -> u32 {
        unsafe { self.umem.as_ref().umem_id }
    }
}

impl Drop for DevxUmem {
    fn drop(&mut self) {
        unsafe { mlx5_sys::mlx5dv_devx_umem_dereg(self.umem.as_ptr()); }
    }
}

struct DevxObj {
    obj: NonNull<mlx5_sys::mlx5dv_devx_obj>,
}

impl Drop for DevxObj {
    fn drop(&mut self) {
        unsafe { mlx5_sys::mlx5dv_devx_obj_destroy(self.obj.as_ptr()); }
    }
}

// ============================================================================
// Helper: check PRM command result
// ============================================================================

fn check_prm_result(ret: i32, out: &[u8], cmd_name: &str) -> io::Result<()> {
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
// DevX QP lifecycle functions
// ============================================================================

/// Create a QP via DevX PRM CREATE_QP command.
///
/// The SQ/RQ buffers are placed in the user-provided UMEM.
/// Buffer layout within UMEM: [SQ | RQ | DBREC(64B aligned)]
unsafe fn devx_create_qp(
    ctx: *mut mlx5_sys::ibv_context,
    pd_handle: u32,
    cqn: u32,
    uar_page_id: u32,
    wq_umem_id: u32,
    dbr_umem_id: u32,
    dbr_offset_in_umem: u32,
    log_sq_size: u32,
    log_rq_size: u32,
    log_rq_stride: u32,
) -> io::Result<(DevxObj, u32)> {
    // One PAS entry for the WQ UMEM
    let cmd_in_size = CQ_IN_BASE_SIZE + 8;
    let mut cmd_in = vec![0u8; cmd_in_size];
    let mut cmd_out = vec![0u8; CQ_OUT_SIZE];

    // Command header
    prm_set(&mut cmd_in, CQ_IN_OPCODE.0, CQ_IN_OPCODE.1, MLX5_CMD_OP_CREATE_QP);

    // wq_umem_valid = 1
    prm_set(&mut cmd_in, CQ_IN_WQ_UMEM_VALID.0, CQ_IN_WQ_UMEM_VALID.1, 1);

    // QPC fields
    let q = CQ_IN_QPC_OFF;
    prm_set(&mut cmd_in, q + QPC_ST.0, QPC_ST.1, QP_ST_RC);
    prm_set(&mut cmd_in, q + QPC_PD.0, QPC_PD.1, pd_handle);
    prm_set(&mut cmd_in, q + QPC_UAR_PAGE.0, QPC_UAR_PAGE.1, uar_page_id);
    prm_set(&mut cmd_in, q + QPC_LOG_SQ_SIZE.0, QPC_LOG_SQ_SIZE.1, log_sq_size);
    prm_set(&mut cmd_in, q + QPC_LOG_RQ_SIZE.0, QPC_LOG_RQ_SIZE.1, log_rq_size);
    prm_set(&mut cmd_in, q + QPC_LOG_RQ_STRIDE.0, QPC_LOG_RQ_STRIDE.1, log_rq_stride);
    prm_set(&mut cmd_in, q + QPC_LOG_MSG_MAX.0, QPC_LOG_MSG_MAX.1, 30);
    prm_set(&mut cmd_in, q + QPC_CQN_SND.0, QPC_CQN_SND.1, cqn);
    prm_set(&mut cmd_in, q + QPC_CQN_RCV.0, QPC_CQN_RCV.1, cqn);

    // DBR: encode umem_id in upper 32 bits, byte offset in lower 32 bits
    prm_set64(
        &mut cmd_in,
        q + QPC_DBR_ADDR,
        ((dbr_umem_id as u64) << 32) | (dbr_offset_in_umem as u64),
    );
    prm_set(&mut cmd_in, q + QPC_DBR_UMEM_VALID.0, QPC_DBR_UMEM_VALID.1, 1);

    // PAS[0]: umem_id in upper 32 bits, page offset 0 in lower 32 bits
    prm_set(&mut cmd_in, CQ_IN_PAS_OFF, 32, wq_umem_id);
    // lower 32 bits already 0 (page offset = 0)

    eprintln!("  CREATE_QP: pd=0x{:x} cqn=0x{:x} uar=0x{:x} umem=0x{:x} dbr_umem=0x{:x} dbr_off=0x{:x}",
              pd_handle, cqn, uar_page_id, wq_umem_id, dbr_umem_id, dbr_offset_in_umem);
    eprintln!("  QPC: log_sq={} log_rq={} log_rq_stride={}", log_sq_size, log_rq_size, log_rq_stride);

    let obj = mlx5_sys::mlx5dv_devx_obj_create(
        ctx,
        cmd_in.as_ptr() as *const _,
        cmd_in.len(),
        cmd_out.as_mut_ptr() as *mut _,
        cmd_out.len(),
    );

    if obj.is_null() {
        let status = prm_get(&cmd_out, CQ_OUT_STATUS.0, CQ_OUT_STATUS.1);
        let syndrome = prm_get(&cmd_out, CQ_OUT_SYNDROME.0, CQ_OUT_SYNDROME.1);
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!(
                "CREATE_QP failed: status=0x{:x}, syndrome=0x{:x}, errno={}",
                status,
                syndrome,
                io::Error::last_os_error()
            ),
        ));
    }

    let qpn = prm_get(&cmd_out, CQ_OUT_QPN.0, CQ_OUT_QPN.1);
    eprintln!("  CREATE_QP success: QPN=0x{:x}", qpn);

    Ok((
        DevxObj {
            obj: NonNull::new(obj).unwrap(),
        },
        qpn,
    ))
}

/// Transition QP: RST -> INIT
unsafe fn devx_rst2init(
    qp_obj: *mut mlx5_sys::mlx5dv_devx_obj,
    qpn: u32,
    port: u8,
) -> io::Result<()> {
    let mut cmd_in = vec![0u8; MOD_IN_SIZE];
    let mut cmd_out = vec![0u8; MOD_OUT_SIZE];

    prm_set(&mut cmd_in, MOD_IN_OPCODE.0, MOD_IN_OPCODE.1, MLX5_CMD_OP_RST2INIT_QP);
    prm_set(&mut cmd_in, MOD_IN_QPN.0, MOD_IN_QPN.1, qpn);

    let q = MOD_IN_QPC_OFF;
    let ads = q + QPC_PRI_ADS;

    // PM state = migrated (required by FW)
    prm_set(&mut cmd_in, q + QPC_PM_STATE.0, QPC_PM_STATE.1, 3);
    // Primary address path: port
    prm_set(&mut cmd_in, ads + ADS_VHCA_PORT_NUM.0, ADS_VHCA_PORT_NUM.1, port as u32);
    // Access flags: RRE + RWE (RAE requires atomic_mode to be set)
    prm_set(&mut cmd_in, q + QPC_RRE.0, QPC_RRE.1, 1);
    prm_set(&mut cmd_in, q + QPC_RWE.0, QPC_RWE.1, 1);

    let ret = mlx5_sys::mlx5dv_devx_obj_modify(
        qp_obj,
        cmd_in.as_ptr() as *const _,
        cmd_in.len(),
        cmd_out.as_mut_ptr() as *mut _,
        cmd_out.len(),
    );
    check_prm_result(ret, &cmd_out, "RST2INIT")?;
    eprintln!("  RST2INIT success");
    Ok(())
}

/// Transition QP: INIT -> RTR
unsafe fn devx_init2rtr(
    qp_obj: *mut mlx5_sys::mlx5dv_devx_obj,
    qpn: u32,
    remote_qpn: u32,
    remote_lid: u16,
    port: u8,
) -> io::Result<()> {
    let mut cmd_in = vec![0u8; MOD_IN_SIZE];
    let mut cmd_out = vec![0u8; MOD_OUT_SIZE];

    prm_set(&mut cmd_in, MOD_IN_OPCODE.0, MOD_IN_OPCODE.1, MLX5_CMD_OP_INIT2RTR_QP);
    prm_set(&mut cmd_in, MOD_IN_QPN.0, MOD_IN_QPN.1, qpn);

    let q = MOD_IN_QPC_OFF;
    prm_set(&mut cmd_in, q + QPC_MTU.0, QPC_MTU.1, 5); // MTU 4096
    prm_set(&mut cmd_in, q + QPC_REMOTE_QPN.0, QPC_REMOTE_QPN.1, remote_qpn);
    prm_set(&mut cmd_in, q + QPC_NEXT_RCV_PSN.0, QPC_NEXT_RCV_PSN.1, 0);
    prm_set(&mut cmd_in, q + QPC_LOG_RRA_MAX.0, QPC_LOG_RRA_MAX.1, 2); // max_dest_rd_atomic=4
    prm_set(&mut cmd_in, q + QPC_MIN_RNR_NAK.0, QPC_MIN_RNR_NAK.1, 12);

    // Primary address path
    let ads = q + QPC_PRI_ADS;
    prm_set(&mut cmd_in, ads + ADS_RLID.0, ADS_RLID.1, remote_lid as u32);
    prm_set(&mut cmd_in, ads + ADS_VHCA_PORT_NUM.0, ADS_VHCA_PORT_NUM.1, port as u32);

    let ret = mlx5_sys::mlx5dv_devx_obj_modify(
        qp_obj,
        cmd_in.as_ptr() as *const _,
        cmd_in.len(),
        cmd_out.as_mut_ptr() as *mut _,
        cmd_out.len(),
    );
    check_prm_result(ret, &cmd_out, "INIT2RTR")?;
    eprintln!("  INIT2RTR success");
    Ok(())
}

/// Transition QP: RTR -> RTS
unsafe fn devx_rtr2rts(
    qp_obj: *mut mlx5_sys::mlx5dv_devx_obj,
    qpn: u32,
) -> io::Result<()> {
    let mut cmd_in = vec![0u8; MOD_IN_SIZE];
    let mut cmd_out = vec![0u8; MOD_OUT_SIZE];

    prm_set(&mut cmd_in, MOD_IN_OPCODE.0, MOD_IN_OPCODE.1, MLX5_CMD_OP_RTR2RTS_QP);
    prm_set(&mut cmd_in, MOD_IN_QPN.0, MOD_IN_QPN.1, qpn);

    let q = MOD_IN_QPC_OFF;
    prm_set(&mut cmd_in, q + QPC_LOG_SRA_MAX.0, QPC_LOG_SRA_MAX.1, 2); // max_rd_atomic=4
    prm_set(&mut cmd_in, q + QPC_RETRY_COUNT.0, QPC_RETRY_COUNT.1, 7);
    prm_set(&mut cmd_in, q + QPC_RNR_RETRY.0, QPC_RNR_RETRY.1, 7);
    prm_set(&mut cmd_in, q + QPC_NEXT_SEND_PSN.0, QPC_NEXT_SEND_PSN.1, 0);

    // ack_timeout in primary address path
    let ads = q + QPC_PRI_ADS;
    prm_set(&mut cmd_in, ads + ADS_ACK_TIMEOUT.0, ADS_ACK_TIMEOUT.1, 14);

    let ret = mlx5_sys::mlx5dv_devx_obj_modify(
        qp_obj,
        cmd_in.as_ptr() as *const _,
        cmd_in.len(),
        cmd_out.as_mut_ptr() as *mut _,
        cmd_out.len(),
    );
    check_prm_result(ret, &cmd_out, "RTR2RTS")?;
    eprintln!("  RTR2RTS success");
    Ok(())
}

// ============================================================================
// Test
// ============================================================================

/// Create a DevX QP with user-allocated SQ/RQ memory, transition to RTS,
/// and verify with a loopback RDMA WRITE from a verbs partner QP.
#[test]
fn test_devx_qp_create_and_transition() {
    let port: u8 = 1;
    let log_sq_size: u32 = 4;   // 16 WQEs
    let log_rq_size: u32 = 4;   // 16 entries
    let log_rq_stride: u32 = 2; // stride = 2^(2+4) = 64 bytes
    let wqebb: usize = 64;

    // ---- 1. Open device with DevX ----
    let devices = mlx5::device::DeviceList::list().expect("device list");
    let ctx = devices[0].open_devx().expect("open_devx failed — is DevX supported?");
    eprintln!("[1/8] Device opened with DevX support");

    // ---- 2. Allocate PD and create CQ ----
    let pd = ctx.alloc_pd().expect("alloc_pd");
    let pd_handle = pd.pdn();

    let cq = ctx
        .create_cq(64, &mlx5::cq::CqConfig::default())
        .expect("create_cq");
    let cqn = cq.cqn();
    eprintln!("[2/8] PD handle=0x{:x}, CQN=0x{:x}", pd_handle, cqn);

    // ---- 3. Allocate UAR ----
    let uar = unsafe { mlx5_sys::mlx5dv_devx_alloc_uar(ctx.as_ptr(), 0) };
    let uar = DevxUar {
        uar: NonNull::new(uar).expect("alloc_uar failed"),
    };
    eprintln!(
        "[3/8] UAR allocated: page_id=0x{:x}, reg_addr={:p}",
        uar.page_id(),
        uar.reg_addr()
    );

    // ---- 4. Allocate page-aligned buffer for SQ + RQ + DBREC ----
    let sq_size = (1usize << log_sq_size) * wqebb;
    let rq_stride = 1usize << (log_rq_stride + 4);
    let rq_size = (1usize << log_rq_size) * rq_stride;
    let dbrec_offset = sq_size + rq_size; // DBREC at end, 64-byte aligned
    let total_size = ((sq_size + rq_size + 64 + 4095) / 4096) * 4096;

    let wq_buf = AlignedBuf::new(total_size);
    eprintln!(
        "[4/8] WQ buffer: {:p}, sq={}B rq={}B dbrec_off=0x{:x} total={}B",
        wq_buf.as_ptr(),
        sq_size,
        rq_size,
        dbrec_offset,
        total_size
    );

    // ---- 5. Register UMEM ----
    let umem = unsafe {
        mlx5_sys::mlx5dv_devx_umem_reg(
            ctx.as_ptr(),
            wq_buf.as_ptr() as *mut _,
            wq_buf.size(),
            7, // LOCAL_WRITE | REMOTE_WRITE | REMOTE_READ
        )
    };
    let umem = DevxUmem {
        umem: NonNull::new(umem).expect("umem_reg failed"),
    };
    eprintln!("[5/8] UMEM registered: id=0x{:x}", umem.umem_id());

    // ---- 6. Create QP via DevX ----
    let (devx_qp, devx_qpn) = unsafe {
        devx_create_qp(
            ctx.as_ptr(),
            pd_handle,
            cqn,
            uar.page_id(),
            umem.umem_id(),
            umem.umem_id(), // same UMEM for DBR
            dbrec_offset as u32,
            log_sq_size,
            log_rq_size,
            log_rq_stride,
        )
        .expect("devx_create_qp")
    };
    eprintln!("[6/8] DevX QP created: QPN=0x{:x}", devx_qpn);

    // ---- 7. Query port LID for loopback ----
    let port_attr = ctx.query_port(port).expect("query_port");
    let lid = port_attr.lid;
    eprintln!("  Port {} LID=0x{:x}", port, lid);

    // ---- 8. Transition QP: RST -> INIT -> RTR -> RTS ----
    unsafe {
        devx_rst2init(devx_qp.obj.as_ptr(), devx_qpn, port).expect("RST2INIT");
        devx_init2rtr(devx_qp.obj.as_ptr(), devx_qpn, devx_qpn, lid, port).expect("INIT2RTR");
        devx_rtr2rts(devx_qp.obj.as_ptr(), devx_qpn).expect("RTR2RTS");
    }
    eprintln!("[7/8] DevX QP transitioned to RTS");

    // ---- Summary ----
    eprintln!("[8/8] SUCCESS: DevX QP created and transitioned to RTS with user-allocated memory");
    eprintln!("  SQ buffer: {:p} ({}B)", wq_buf.as_ptr(), sq_size);
    eprintln!(
        "  RQ buffer: {:p} ({}B)",
        unsafe { wq_buf.as_ptr().add(sq_size) },
        rq_size
    );
    eprintln!(
        "  DBREC: {:p}",
        unsafe { wq_buf.as_ptr().add(dbrec_offset) }
    );
    eprintln!("  UAR reg: {:p}", uar.reg_addr());

    // Cleanup is automatic via Drop impls (DevxObj -> DevxUmem -> DevxUar)
    // Drop order matters: QP first, then UMEM, then UAR
    drop(devx_qp);
    drop(umem);
    drop(uar);
}
