//! Queue Pair (QP) management.
//!
//! Queue Pairs are the fundamental communication endpoints in RDMA.
//! This module provides RC (Reliable Connection) QP creation using mlx5dv_create_qp.

use std::cell::{Cell, RefCell};
use std::rc::{Rc, Weak};
use std::{io, mem::MaybeUninit, ptr::NonNull};

use crate::CompletionTarget;
use crate::cq::{CompletionQueue, Cqe};
use crate::device::Context;
use crate::pd::Pd;
use crate::srq::Srq;
use crate::transport::{IbRemoteQpInfo, RoCERemoteQpInfo};
use crate::types::GrhAttr;
use crate::wqe::{
    AtomicSeg, CtrlSeg, DataSeg, HasData, InlineHeader, Init, MaskedAtomicSeg32,
    MaskedAtomicSeg64, NeedsAtomic, NeedsData, NeedsRdma, NeedsRdmaThenAtomic, OrderedWqeTable,
    RdmaSeg, SubmissionError, WQEBB_SIZE, WqeFlags, WqeHandle, WqeOpcode, calc_wqebb_cnt,
    // Transport type tags
    InfiniBand, RoCE,
};

/// RC QP configuration.
#[derive(Debug, Clone)]
pub struct RcQpConfig {
    /// Maximum number of outstanding send WRs.
    pub max_send_wr: u32,
    /// Maximum number of outstanding receive WRs.
    pub max_recv_wr: u32,
    /// Maximum number of SGEs per send WR.
    pub max_send_sge: u32,
    /// Maximum number of SGEs per receive WR.
    pub max_recv_sge: u32,
    /// Maximum inline data size.
    pub max_inline_data: u32,
    /// Enable scatter-to-CQE for small received data.
    /// When enabled, received data ≤32 bytes is placed directly in CQE.
    pub enable_scatter_to_cqe: bool,
}

impl Default for RcQpConfig {
    fn default() -> Self {
        Self {
            max_send_wr: 256,
            max_recv_wr: 256,
            max_send_sge: 4,
            max_recv_sge: 4,
            max_inline_data: 64,
            enable_scatter_to_cqe: false,
        }
    }
}

/// QP state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QpState {
    Reset,
    Init,
    Rtr,
    Rts,
    Error,
}

/// Remote QP information for connection (InfiniBand).
///
/// This is an alias for [`IbRemoteQpInfo`] for backward compatibility.
/// New code should use [`IbRemoteQpInfo`] directly.
///
/// For RoCE, use [`crate::transport::RoCERemoteQpInfo`] instead.
pub type RemoteQpInfo = IbRemoteQpInfo;

/// QP internal info obtained from mlx5dv_init_obj.
#[derive(Debug)]
pub(crate) struct QpInfo {
    /// Doorbell record pointer.
    pub(crate) dbrec: *mut u32,
    /// Send Queue buffer pointer.
    pub(crate) sq_buf: *mut u8,
    /// Send Queue WQE count.
    pub(crate) sq_wqe_cnt: u32,
    /// Send Queue stride (bytes per WQE slot).
    #[allow(dead_code)]
    pub(crate) sq_stride: u32,
    /// Receive Queue buffer pointer.
    pub(crate) rq_buf: *mut u8,
    /// Receive Queue WQE count.
    pub(crate) rq_wqe_cnt: u32,
    /// Receive Queue stride.
    pub(crate) rq_stride: u32,
    /// BlueFlame register pointer.
    pub(crate) bf_reg: *mut u8,
    /// BlueFlame size.
    pub(crate) bf_size: u32,
    /// Send Queue Number.
    pub(crate) sqn: u32,
}

// =============================================================================
// Send Queue State
// =============================================================================

/// Send Queue state for direct WQE posting.
///
/// Generic over the table type `TableType`.
/// Uses interior mutability (Cell) so no RefCell wrapper is needed.
pub(crate) struct SendQueueState<Entry, TableType> {
    /// SQ buffer base address
    buf: *mut u8,
    /// Number of WQEBBs (64-byte blocks)
    wqe_cnt: u16,
    /// SQ number
    sqn: u32,
    /// Maximum inline data size (from QP config)
    max_inline_data: u32,
    /// Producer index (next WQE slot)
    pi: Cell<u16>,
    /// Consumer index (last completed WQE)
    ci: Cell<u16>,
    /// Last posted WQE pointer and size (for BlueFlame)
    last_wqe: Cell<Option<(*mut u8, usize)>>,
    /// Doorbell record pointer
    dbrec: *mut u32,
    /// BlueFlame register pointer
    bf_reg: *mut u8,
    /// BlueFlame size (64 or 0 if not available)
    bf_size: u32,
    /// Current BlueFlame offset (alternates between 0 and bf_size)
    bf_offset: Cell<u32>,
    /// WQE table for tracking in-flight operations.
    /// Uses interior mutability (Cell<Option<Entry>>) so no RefCell needed.
    table: TableType,
    /// Phantom for entry type
    _marker: std::marker::PhantomData<Entry>,
}

impl<Entry, TableType> SendQueueState<Entry, TableType> {
    #[inline]
    fn available(&self) -> u16 {
        self.wqe_cnt - self.pi.get().wrapping_sub(self.ci.get())
    }

    #[inline]
    fn max_inline_data(&self) -> u32 {
        self.max_inline_data
    }

    #[inline]
    fn get_wqe_ptr(&self, idx: u16) -> *mut u8 {
        let offset = ((idx & (self.wqe_cnt - 1)) as usize) * WQEBB_SIZE;
        unsafe { self.buf.add(offset) }
    }

    /// Returns the number of WQEBBs from current PI to the end of the ring buffer.
    ///
    /// This is used to check if a WQE would wrap around the ring boundary.
    #[inline]
    fn slots_to_end(&self) -> u16 {
        self.wqe_cnt - (self.pi.get() & (self.wqe_cnt - 1))
    }

    #[inline]
    fn advance_pi(&self, count: u16) {
        self.pi.set(self.pi.get().wrapping_add(count));
    }

    /// Post a NOP WQE to fill the remaining slots until the ring end.
    ///
    /// This is used when a variable-length WQE would wrap around the ring boundary.
    /// The NOP WQE consumes the remaining slots, allowing the next WQE to start
    /// at the ring beginning.
    ///
    /// # Arguments
    /// * `nop_wqebb_cnt` - Number of WQEBBs to consume with NOP (must be >= 1)
    ///
    /// # Safety
    /// Caller must ensure there are enough available slots for the NOP WQE.
    #[inline]
    unsafe fn post_nop(&self, nop_wqebb_cnt: u16) {
        debug_assert!(nop_wqebb_cnt >= 1, "NOP must be at least 1 WQEBB");
        debug_assert!(
            self.available() >= nop_wqebb_cnt,
            "Not enough slots for NOP"
        );

        let wqe_idx = self.pi.get();
        let wqe_ptr = self.get_wqe_ptr(wqe_idx);

        // Write NOP control segment
        // ds_count = nop_wqebb_cnt * 4 (each WQEBB = 4 data segments of 16 bytes)
        let ds_count = (nop_wqebb_cnt as u8) * 4;
        CtrlSeg::write(
            wqe_ptr,
            0, // opmod = 0 for NOP
            WqeOpcode::Nop as u8,
            wqe_idx,
            self.sqn,
            ds_count,
            0,
            0,
        );

        self.advance_pi(nop_wqebb_cnt);
        self.set_last_wqe(wqe_ptr, (nop_wqebb_cnt as usize) * WQEBB_SIZE);
    }

    #[inline]
    fn set_last_wqe(&self, ptr: *mut u8, size: usize) {
        self.last_wqe.set(Some((ptr, size)));
    }

    /// Ring the doorbell using regular doorbell write.
    #[inline]
    fn ring_doorbell(&self) {
        let Some((last_wqe_ptr, _)) = self.last_wqe.take() else {
            return;
        };

        mmio_flush_writes!();

        unsafe {
            std::ptr::write_volatile(self.dbrec.add(1), (self.pi.get() as u32).to_be());
        }

        udma_to_device_barrier!();

        self.ring_db(last_wqe_ptr);
    }

    #[inline]
    fn ring_db(&self, wqe_ptr: *mut u8) {
        let bf_offset = self.bf_offset.get();
        let bf = unsafe { self.bf_reg.add(bf_offset as usize) as *mut u64 };
        let ctrl = wqe_ptr as *const u64;
        unsafe {
            std::ptr::write_volatile(bf, *ctrl);
        }
        mmio_flush_writes!();
        self.bf_offset.set(bf_offset ^ self.bf_size);
    }
}

impl<Entry> SendQueueState<Entry, OrderedWqeTable<Entry>> {
    #[inline]
    fn process_completion(&self, wqe_idx: u16) -> Option<Entry> {
        let entry = self.table.take(wqe_idx)?;
        // ci_delta is the accumulated PI value at completion
        self.ci.set(entry.ci_delta);
        Some(entry.data)
    }
}

// =============================================================================
// Receive Queue State
// =============================================================================

/// Receive Queue state for direct WQE posting.
///
/// Generic over `Entry`, the entry type stored in the WQE table.
/// Unlike SQ, all RQ WQEs generate completions (all signaled),
/// so we use a simple table.
///
/// Uses `Cell<Option<Entry>>` for interior mutability, allowing safe access
/// without requiring `RefCell` or mutable borrows.
pub(crate) struct ReceiveQueueState<Entry> {
    /// RQ buffer base address
    buf: *mut u8,
    /// Number of WQE slots
    wqe_cnt: u32,
    /// Stride (bytes per WQE slot)
    stride: u32,
    /// Producer index (next WQE slot)
    pi: Cell<u16>,
    /// Consumer index (last completed WQE + 1)
    ci: Cell<u16>,
    /// Doorbell record pointer (dbrec[0] for RQ)
    dbrec: *mut u32,
    /// Entry table (all WQEs are signaled, uses Cell for interior mutability)
    table: Box<[Cell<Option<Entry>>]>,
}

/// MLX5 invalid lkey value used to mark end of SGE list.
const MLX5_INVALID_LKEY: u32 = 0x100;

impl<Entry> ReceiveQueueState<Entry> {
    #[inline]
    fn get_wqe_ptr(&self, idx: u16) -> *mut u8 {
        let offset = ((idx as u32) & (self.wqe_cnt - 1)) * self.stride;
        unsafe { self.buf.add(offset as usize) }
    }

    /// Process a receive completion.
    ///
    /// Returns the entry associated with the completed WQE.
    #[inline]
    fn process_completion(&self, wqe_idx: u16) -> Option<Entry> {
        self.ci.set(wqe_idx.wrapping_add(1));
        let idx = (wqe_idx as usize) & ((self.wqe_cnt - 1) as usize);
        self.table[idx].take()
    }

    /// Get the number of available WQE slots.
    #[inline]
    fn available(&self) -> u32 {
        self.wqe_cnt - (self.pi.get().wrapping_sub(self.ci.get()) as u32)
    }

    #[inline]
    fn ring_doorbell(&self) {
        mmio_flush_writes!();
        unsafe {
            std::ptr::write_volatile(self.dbrec, (self.pi.get() as u32).to_be());
        }
    }
}

// =============================================================================
// Receive Queue Type Markers
// =============================================================================

/// Marker type for QP with owned Receive Queue.
///
/// When a QP uses `OwnedRq`, it has its own dedicated receive queue and
/// the RQ-related methods (`post_recv`, `ring_rq_doorbell`, `blueflame_rq_batch`)
/// are available.
pub struct OwnedRq<RqEntry>(Option<ReceiveQueueState<RqEntry>>);

impl<RqEntry> OwnedRq<RqEntry> {
    pub(crate) fn new(rq: Option<ReceiveQueueState<RqEntry>>) -> Self {
        Self(rq)
    }

    pub(crate) fn as_ref(&self) -> Option<&ReceiveQueueState<RqEntry>> {
        self.0.as_ref()
    }
}

/// Marker type for QP with Shared Receive Queue (SRQ).
///
/// When a QP uses `SharedRq`, receive operations are handled through the SRQ
/// and the QP's own RQ-related methods are not available. Instead, use
/// `srq()` to access the underlying SRQ.
pub struct SharedRq<RqEntry>(Srq<RqEntry>);

impl<RqEntry> SharedRq<RqEntry> {
    pub(crate) fn new(srq: Srq<RqEntry>) -> Self {
        Self(srq)
    }

    /// Get a reference to the underlying SRQ.
    pub fn srq(&self) -> &Srq<RqEntry> {
        &self.0
    }
}

// =============================================================================
// WQE Builder
// =============================================================================

/// Zero-copy WQE builder for RC QP with type-state safety.
///
/// Writes segments directly to the SQ buffer without intermediate copies.
/// The `State` type parameter tracks the current builder state, ensuring
/// that only valid segment sequences can be constructed.
pub struct WqeBuilder<'a, Entry, TableType, State> {
    sq: &'a SendQueueState<Entry, TableType>,
    wqe_ptr: *mut u8,
    wqe_idx: u16,
    offset: usize,
    ds_count: u8,
    /// Whether SIGNALED flag is set
    signaled: bool,
    _state: std::marker::PhantomData<State>,
}

// =============================================================================
// WQE Builder Type-State Implementations
// =============================================================================

/// Helper to transition builder state.
impl<'a, Entry, TableType, State> WqeBuilder<'a, Entry, TableType, State> {
    #[inline]
    fn transition<NewState>(self) -> WqeBuilder<'a, Entry, TableType, NewState> {
        WqeBuilder {
            sq: self.sq,
            wqe_ptr: self.wqe_ptr,
            wqe_idx: self.wqe_idx,
            offset: self.offset,
            ds_count: self.ds_count,
            signaled: self.signaled,
            _state: std::marker::PhantomData,
        }
    }

    /// Write control segment (internal helper).
    #[inline]
    fn write_ctrl(&mut self, opcode: WqeOpcode, flags: WqeFlags, imm: u32) {
        let flags = if self.signaled {
            flags | WqeFlags::COMPLETION
        } else {
            flags
        };
        unsafe {
            CtrlSeg::write(
                self.wqe_ptr,
                0, // opmod = 0 for normal operations
                opcode as u8,
                self.wqe_idx,
                self.sq.sqn,
                0, // ds_count will be updated in finish
                flags.bits(),
                imm,
            );
        }
        self.offset = CtrlSeg::SIZE;
        self.ds_count = 1;
    }

    /// Write control segment with custom opmod (internal helper).
    #[inline]
    fn write_ctrl_with_opmod(&mut self, opcode: WqeOpcode, flags: WqeFlags, imm: u32, opmod: u8) {
        let fm_ce_se = if self.signaled {
            (flags | WqeFlags::COMPLETION | WqeFlags::FENCE).bits()
        } else {
            (flags | WqeFlags::FENCE).bits()
        };

        unsafe {
            CtrlSeg::write(
                self.wqe_ptr,
                opmod,
                opcode as u8,
                self.wqe_idx,
                self.sq.sqn,
                0, // ds_count will be updated in finish
                fm_ce_se,
                imm,
            );
        }
        self.offset = CtrlSeg::SIZE;
        self.ds_count = 1;
    }
}

/// Init state: control segment methods.
impl<'a, Entry, TableType> WqeBuilder<'a, Entry, TableType, Init> {
    // -------------------------------------------------------------------------
    // Send operations → NeedsData
    // -------------------------------------------------------------------------

    /// Write control segment for SEND operation.
    #[inline]
    pub fn ctrl_send(
        mut self,
        flags: WqeFlags,
    ) -> WqeBuilder<'a, Entry, TableType, NeedsData> {
        self.write_ctrl(WqeOpcode::Send, flags, 0);
        self.transition()
    }

    /// Write control segment for SEND with immediate data.
    #[inline]
    pub fn ctrl_send_imm(
        mut self,
        flags: WqeFlags,
        imm: u32,
    ) -> WqeBuilder<'a, Entry, TableType, NeedsData> {
        self.write_ctrl(WqeOpcode::SendImm, flags, imm);
        self.transition()
    }

    /// Write control segment for SEND with invalidate.
    #[inline]
    pub fn ctrl_send_inval(
        mut self,
        flags: WqeFlags,
        inv_rkey: u32,
    ) -> WqeBuilder<'a, Entry, TableType, NeedsData> {
        self.write_ctrl(WqeOpcode::SendInval, flags, inv_rkey);
        self.transition()
    }

    // -------------------------------------------------------------------------
    // RDMA operations → NeedsRdma
    // -------------------------------------------------------------------------

    /// Write control segment for RDMA WRITE operation.
    #[inline]
    pub fn ctrl_rdma_write(
        mut self,
        flags: WqeFlags,
    ) -> WqeBuilder<'a, Entry, TableType, NeedsRdma> {
        self.write_ctrl(WqeOpcode::RdmaWrite, flags, 0);
        self.transition()
    }

    /// Write control segment for RDMA WRITE with immediate data.
    #[inline]
    pub fn ctrl_rdma_write_imm(
        mut self,
        flags: WqeFlags,
        imm: u32,
    ) -> WqeBuilder<'a, Entry, TableType, NeedsRdma> {
        self.write_ctrl(WqeOpcode::RdmaWriteImm, flags, imm);
        self.transition()
    }

    /// Write control segment for RDMA READ operation.
    #[inline]
    pub fn ctrl_rdma_read(
        mut self,
        flags: WqeFlags,
    ) -> WqeBuilder<'a, Entry, TableType, NeedsRdma> {
        self.write_ctrl(WqeOpcode::RdmaRead, flags, 0);
        self.transition()
    }

    // -------------------------------------------------------------------------
    // Atomic operations → NeedsRdmaThenAtomic
    // -------------------------------------------------------------------------

    /// Write control segment for Atomic Compare-and-Swap operation.
    #[inline]
    pub fn ctrl_atomic_cas(
        mut self,
        flags: WqeFlags,
    ) -> WqeBuilder<'a, Entry, TableType, NeedsRdmaThenAtomic> {
        self.write_ctrl(WqeOpcode::AtomicCs, flags, 0);
        self.transition()
    }

    /// Write control segment for Atomic Fetch-and-Add operation.
    #[inline]
    pub fn ctrl_atomic_fa(
        mut self,
        flags: WqeFlags,
    ) -> WqeBuilder<'a, Entry, TableType, NeedsRdmaThenAtomic> {
        self.write_ctrl(WqeOpcode::AtomicFa, flags, 0);
        self.transition()
    }

    /// Write control segment for Masked Atomic Compare-and-Swap (32-bit).
    #[inline]
    pub fn ctrl_masked_atomic_cas32(
        mut self,
        flags: WqeFlags,
    ) -> WqeBuilder<'a, Entry, TableType, NeedsRdmaThenAtomic> {
        // opmod = 0x08 for 32-bit extended atomic
        self.write_ctrl_with_opmod(WqeOpcode::AtomicMaskedCs, flags, 0, 0x08);
        self.transition()
    }

    /// Write control segment for Masked Atomic Fetch-and-Add (32-bit).
    #[inline]
    pub fn ctrl_masked_atomic_fa32(
        mut self,
        flags: WqeFlags,
    ) -> WqeBuilder<'a, Entry, TableType, NeedsRdmaThenAtomic> {
        // opmod = 0x08 for 32-bit extended atomic
        self.write_ctrl_with_opmod(WqeOpcode::AtomicMaskedFa, flags, 0, 0x08);
        self.transition()
    }

    /// Write control segment for Masked Atomic Compare-and-Swap (64-bit).
    #[inline]
    pub fn ctrl_masked_atomic_cas64(
        mut self,
        flags: WqeFlags,
    ) -> WqeBuilder<'a, Entry, TableType, NeedsRdmaThenAtomic> {
        // opmod = 0x09 for 64-bit extended atomic
        self.write_ctrl_with_opmod(WqeOpcode::AtomicMaskedCs, flags, 0, 0x09);
        self.transition()
    }

    /// Write control segment for Masked Atomic Fetch-and-Add (64-bit).
    #[inline]
    pub fn ctrl_masked_atomic_fa64(
        mut self,
        flags: WqeFlags,
    ) -> WqeBuilder<'a, Entry, TableType, NeedsRdmaThenAtomic> {
        // opmod = 0x09 for 64-bit extended atomic
        self.write_ctrl_with_opmod(WqeOpcode::AtomicMaskedFa, flags, 0, 0x09);
        self.transition()
    }

    // -------------------------------------------------------------------------
    // NOP → HasData (finish immediately available)
    // -------------------------------------------------------------------------

    /// Write control segment for NOP operation.
    #[inline]
    pub fn ctrl_nop(mut self, flags: WqeFlags) -> WqeBuilder<'a, Entry, TableType, HasData> {
        self.write_ctrl(WqeOpcode::Nop, flags, 0);
        self.transition()
    }
}

/// NeedsRdma state: RDMA segment required.
impl<'a, Entry, TableType> WqeBuilder<'a, Entry, TableType, NeedsRdma> {
    /// Add an RDMA segment specifying remote address and rkey.
    #[inline]
    pub fn rdma(mut self, remote_addr: u64, rkey: u32) -> WqeBuilder<'a, Entry, TableType, NeedsData> {
        unsafe {
            RdmaSeg::write(self.wqe_ptr.add(self.offset), remote_addr, rkey);
        }
        self.offset += RdmaSeg::SIZE;
        self.ds_count += 1;
        self.transition()
    }
}

/// NeedsRdmaThenAtomic state: RDMA segment required, then atomic segment.
impl<'a, Entry, TableType> WqeBuilder<'a, Entry, TableType, NeedsRdmaThenAtomic> {
    /// Add an RDMA segment specifying remote address and rkey.
    #[inline]
    pub fn rdma(mut self, remote_addr: u64, rkey: u32) -> WqeBuilder<'a, Entry, TableType, NeedsAtomic> {
        unsafe {
            RdmaSeg::write(self.wqe_ptr.add(self.offset), remote_addr, rkey);
        }
        self.offset += RdmaSeg::SIZE;
        self.ds_count += 1;
        self.transition()
    }
}

/// NeedsAtomic state: atomic segment required.
impl<'a, Entry, TableType> WqeBuilder<'a, Entry, TableType, NeedsAtomic> {
    /// Add an atomic Compare-and-Swap segment.
    ///
    /// The CAS operation atomically compares the 8-byte value at `remote_addr`
    /// (specified in the preceding RDMA segment) with `compare`. If equal,
    /// replaces it with `swap`. The original value is written to the local
    /// buffer (specified in the following data segment).
    #[inline]
    pub fn atomic_cas(mut self, swap: u64, compare: u64) -> WqeBuilder<'a, Entry, TableType, NeedsData> {
        unsafe {
            AtomicSeg::write_cas(self.wqe_ptr.add(self.offset), swap, compare);
        }
        self.offset += AtomicSeg::SIZE;
        self.ds_count += 1;
        self.transition()
    }

    /// Add an atomic Fetch-and-Add segment.
    ///
    /// The FA operation atomically adds `add_value` to the 8-byte value at
    /// `remote_addr` (specified in the preceding RDMA segment). The original
    /// value is written to the local buffer (specified in the following data
    /// segment).
    #[inline]
    pub fn atomic_fa(mut self, add_value: u64) -> WqeBuilder<'a, Entry, TableType, NeedsData> {
        unsafe {
            AtomicSeg::write_fa(self.wqe_ptr.add(self.offset), add_value);
        }
        self.offset += AtomicSeg::SIZE;
        self.ds_count += 1;
        self.transition()
    }

    /// Masked Compare-and-Swap (64-bit, 2 segments).
    #[inline]
    pub fn masked_cas64(
        mut self,
        swap: u64,
        compare: u64,
        swap_mask: u64,
        compare_mask: u64,
    ) -> WqeBuilder<'a, Entry, TableType, NeedsData> {
        unsafe {
            let ptr1 = self.wqe_ptr.add(self.offset);
            let ptr2 = self
                .wqe_ptr
                .add(self.offset + MaskedAtomicSeg64::SIZE_CAS / 2);

            MaskedAtomicSeg64::write_cas(ptr1, ptr2, swap, compare, swap_mask, compare_mask);
        }
        self.offset += MaskedAtomicSeg64::SIZE_CAS;
        self.ds_count += 2;
        self.transition()
    }

    /// Masked Fetch-and-Add (64-bit, 1 segment).
    #[inline]
    pub fn masked_fa64(mut self, add: u64, field_mask: u64) -> WqeBuilder<'a, Entry, TableType, NeedsData> {
        unsafe {
            MaskedAtomicSeg64::write_fa(self.wqe_ptr.add(self.offset), add, field_mask);
        }
        self.offset += MaskedAtomicSeg64::SIZE_FA;
        self.ds_count += 1;
        self.transition()
    }

    /// Masked Compare-and-Swap (32-bit, 1 segment).
    #[inline]
    pub fn masked_cas32(
        mut self,
        swap: u32,
        compare: u32,
        swap_mask: u32,
        compare_mask: u32,
    ) -> WqeBuilder<'a, Entry, TableType, NeedsData> {
        unsafe {
            MaskedAtomicSeg32::write_cas(
                self.wqe_ptr.add(self.offset),
                swap,
                compare,
                swap_mask,
                compare_mask,
            );
        }
        self.offset += MaskedAtomicSeg32::SIZE;
        self.ds_count += 1;
        self.transition()
    }

    /// Masked Fetch-and-Add (32-bit, 1 segment).
    #[inline]
    pub fn masked_fa32(mut self, add: u32, field_mask: u32) -> WqeBuilder<'a, Entry, TableType, NeedsData> {
        unsafe {
            MaskedAtomicSeg32::write_fa(self.wqe_ptr.add(self.offset), add, field_mask);
        }
        self.offset += MaskedAtomicSeg32::SIZE;
        self.ds_count += 1;
        self.transition()
    }
}

/// NeedsData state: data segment (SGE or inline) required.
impl<'a, Entry, TableType> WqeBuilder<'a, Entry, TableType, NeedsData> {
    /// Add a data segment (SGE).
    #[inline]
    pub fn sge(mut self, addr: u64, len: u32, lkey: u32) -> WqeBuilder<'a, Entry, TableType, HasData> {
        unsafe {
            DataSeg::write(self.wqe_ptr.add(self.offset), len, lkey, addr);
        }
        self.offset += DataSeg::SIZE;
        self.ds_count += 1;
        self.transition()
    }

    /// Add inline data.
    #[inline]
    pub fn inline_data(mut self, data: &[u8]) -> WqeBuilder<'a, Entry, TableType, HasData> {
        let padded_size = unsafe {
            let ptr = self.wqe_ptr.add(self.offset);
            let size = InlineHeader::write(ptr, data.len() as u32);
            std::ptr::copy_nonoverlapping(data.as_ptr(), ptr.add(4), data.len());
            size
        };
        self.offset += padded_size;
        self.ds_count += (padded_size / 16) as u8;
        self.transition()
    }

    /// Get a mutable slice for inline data (zero-copy).
    ///
    /// Returns the builder and a slice that can be written to directly.
    #[inline]
    pub fn inline_slice(mut self, len: usize) -> (WqeBuilder<'a, Entry, TableType, HasData>, &'a mut [u8]) {
        let padded_size = unsafe {
            let ptr = self.wqe_ptr.add(self.offset);
            InlineHeader::write(ptr, len as u32)
        };
        let data_ptr = unsafe { self.wqe_ptr.add(self.offset + 4) };
        let slice = unsafe { std::slice::from_raw_parts_mut(data_ptr, len) };
        self.offset += padded_size;
        self.ds_count += (padded_size / 16) as u8;
        (self.transition(), slice)
    }
}

/// HasData state: additional SGEs and finish methods available.
impl<'a, Entry, TableType> WqeBuilder<'a, Entry, TableType, HasData> {
    /// Add another data segment (SGE).
    #[inline]
    pub fn sge(mut self, addr: u64, len: u32, lkey: u32) -> Self {
        unsafe {
            DataSeg::write(self.wqe_ptr.add(self.offset), len, lkey, addr);
        }
        self.offset += DataSeg::SIZE;
        self.ds_count += 1;
        self
    }
}

// =============================================================================
// RC QP
// =============================================================================

/// RC (Reliable Connection) Queue Pair with owned Receive Queue (InfiniBand).
///
/// Only signaled WQEs have entries stored in the WQE table.
///
/// Type parameters:
/// - `SqEntry`: Entry type stored in the SQ WQE table
/// - `RqEntry`: Entry type stored in the RQ WQE table
/// - `OnSqComplete`: SQ completion callback type `Fn(Cqe, SqEntry)`
/// - `OnRqComplete`: RQ completion callback type `Fn(Cqe, RqEntry)`
pub type RcQpIb<SqEntry, RqEntry, OnSqComplete, OnRqComplete> = RcQpInner<SqEntry, RqEntry, InfiniBand, OrderedWqeTable<SqEntry>, OwnedRq<RqEntry>, OnSqComplete, OnRqComplete>;

/// RC (Reliable Connection) Queue Pair with SRQ (InfiniBand).
///
/// Type parameters:
/// - `SqEntry`: Entry type stored in the SQ WQE table
/// - `RqEntry`: Entry type stored in the SRQ WQE table
/// - `OnSqComplete`: SQ completion callback type `Fn(Cqe, SqEntry)`
/// - `OnRqComplete`: RQ completion callback type `Fn(Cqe, RqEntry)`
pub type RcQpIbWithSrq<SqEntry, RqEntry, OnSqComplete, OnRqComplete> = RcQpInner<SqEntry, RqEntry, InfiniBand, OrderedWqeTable<SqEntry>, SharedRq<RqEntry>, OnSqComplete, OnRqComplete>;

/// RC (Reliable Connection) Queue Pair with owned Receive Queue (RoCE).
///
/// Type parameters:
/// - `SqEntry`: Entry type stored in the SQ WQE table
/// - `RqEntry`: Entry type stored in the RQ WQE table
/// - `OnSqComplete`: SQ completion callback type `Fn(Cqe, SqEntry)`
/// - `OnRqComplete`: RQ completion callback type `Fn(Cqe, RqEntry)`
pub type RcQpRoCE<SqEntry, RqEntry, OnSqComplete, OnRqComplete> = RcQpInner<SqEntry, RqEntry, RoCE, OrderedWqeTable<SqEntry>, OwnedRq<RqEntry>, OnSqComplete, OnRqComplete>;

/// RC (Reliable Connection) Queue Pair with SRQ (RoCE).
///
/// Type parameters:
/// - `SqEntry`: Entry type stored in the SQ WQE table
/// - `RqEntry`: Entry type stored in the SRQ WQE table
/// - `OnSqComplete`: SQ completion callback type `Fn(Cqe, SqEntry)`
/// - `OnRqComplete`: RQ completion callback type `Fn(Cqe, RqEntry)`
pub type RcQpRoCEWithSrq<SqEntry, RqEntry, OnSqComplete, OnRqComplete> = RcQpInner<SqEntry, RqEntry, RoCE, OrderedWqeTable<SqEntry>, SharedRq<RqEntry>, OnSqComplete, OnRqComplete>;

/// RC (Reliable Connection) Queue Pair (internal implementation).
///
/// Created using mlx5dv_create_qp for direct hardware access.
///
/// Type parameters:
/// - `SqEntry`: Entry type stored in the SQ WQE table
/// - `RqEntry`: Entry type stored in the RQ WQE table
/// - `Transport`: Transport type tag (`InfiniBand` or `RoCE`)
/// - `TableType`: WQE table behavior for the SQ (`OrderedWqeTable` or `UnorderedWqeTable`)
/// - `Rq`: Receive queue type (`OwnedRq<RqEntry>` or `SharedRq<RqEntry>`)
/// - `OnSqComplete`: SQ completion callback type
/// - `OnRqComplete`: RQ completion callback type
pub struct RcQpInner<SqEntry, RqEntry, Transport, TableType, Rq, OnSqComplete, OnRqComplete> {
    qp: NonNull<mlx5_sys::ibv_qp>,
    state: Cell<QpState>,
    /// Maximum inline data size (from QP config)
    max_inline_data: u32,
    sq: Option<SendQueueState<SqEntry, TableType>>,
    rq: Rq,
    sq_callback: OnSqComplete,
    rq_callback: OnRqComplete,
    /// Weak reference to the send CQ for unregistration on drop
    send_cq: Weak<CompletionQueue>,
    /// Weak reference to the recv CQ for unregistration on drop
    recv_cq: Weak<CompletionQueue>,
    /// Keep the PD alive while this QP exists.
    _pd: Pd,
    /// Phantom data for transport type and RqEntry
    _marker: std::marker::PhantomData<(Transport, RqEntry)>,
}

impl Context {
    /// Create an RC Queue Pair using mlx5dv_create_qp.
    ///
    /// Only signaled WQEs have entries stored in the WQE table.
    /// The callbacks are invoked for each completion with the CQE and
    /// the entry stored at WQE submission.
    ///
    /// # Arguments
    /// * `pd` - Protection Domain
    /// * `send_cq` - Completion Queue for send completions (wrapped in Rc for shared ownership)
    /// * `recv_cq` - Completion Queue for receive completions (wrapped in Rc for shared ownership)
    /// * `config` - QP configuration
    /// * `sq_callback` - SQ completion callback `Fn(Cqe, SqEntry)` called for each signaled SQ completion
    /// * `rq_callback` - RQ completion callback `Fn(Cqe, RqEntry)` called for each RQ completion
    ///
    /// # Errors
    /// Returns an error if the QP cannot be created.
    ///
    /// # Note
    /// The send_cq must have `init_direct_access()` called before this function.
    pub fn create_rc_qp<SqEntry, RqEntry, OnSqComplete, OnRqComplete>(
        &self,
        pd: &Pd,
        send_cq: &Rc<CompletionQueue>,
        recv_cq: &Rc<CompletionQueue>,
        config: &RcQpConfig,
        sq_callback: OnSqComplete,
        rq_callback: OnRqComplete,
    ) -> io::Result<Rc<RefCell<RcQpIb<SqEntry, RqEntry, OnSqComplete, OnRqComplete>>>>
    where
        SqEntry: 'static,
        RqEntry: 'static,
        OnSqComplete: Fn(Cqe, SqEntry) + 'static,
        OnRqComplete: Fn(Cqe, RqEntry) + 'static,
    {
        let qp = self.create_rc_qp_raw(pd, send_cq, recv_cq, config, sq_callback, rq_callback)?;
        let qp_rc = Rc::new(RefCell::new(qp));
        let qpn = qp_rc.borrow().qpn();

        // Register this QP with both CQs for completion dispatch
        send_cq.register_queue(qpn, Rc::downgrade(&qp_rc) as _);
        recv_cq.register_queue(qpn, Rc::downgrade(&qp_rc) as _);

        Ok(qp_rc)
    }

    fn create_rc_qp_raw<SqEntry, RqEntry, OnSqComplete, OnRqComplete>(
        &self,
        pd: &Pd,
        send_cq: &Rc<CompletionQueue>,
        recv_cq: &Rc<CompletionQueue>,
        config: &RcQpConfig,
        sq_callback: OnSqComplete,
        rq_callback: OnRqComplete,
    ) -> io::Result<RcQpIb<SqEntry, RqEntry, OnSqComplete, OnRqComplete>>
    where
        OnSqComplete: Fn(Cqe, SqEntry),
        OnRqComplete: Fn(Cqe, RqEntry),
    {
        unsafe {
            let mut qp_attr: mlx5_sys::ibv_qp_init_attr_ex = MaybeUninit::zeroed().assume_init();
            qp_attr.qp_type = mlx5_sys::ibv_qp_type_IBV_QPT_RC;
            qp_attr.send_cq = send_cq.as_ptr();
            qp_attr.recv_cq = recv_cq.as_ptr();
            qp_attr.cap.max_send_wr = config.max_send_wr;
            qp_attr.cap.max_recv_wr = config.max_recv_wr;
            qp_attr.cap.max_send_sge = config.max_send_sge;
            qp_attr.cap.max_recv_sge = config.max_recv_sge;
            qp_attr.cap.max_inline_data = config.max_inline_data;
            qp_attr.comp_mask = mlx5_sys::ibv_qp_init_attr_mask_IBV_QP_INIT_ATTR_PD;
            qp_attr.pd = pd.as_ptr();

            let mut mlx5_attr: mlx5_sys::mlx5dv_qp_init_attr = MaybeUninit::zeroed().assume_init();
            // Optionally disable scatter to CQE. When disabled, received data goes to the
            // receive buffer instead of being inlined in the CQE.
            if !config.enable_scatter_to_cqe {
                mlx5_attr.comp_mask =
                    mlx5_sys::mlx5dv_qp_init_attr_mask_MLX5DV_QP_INIT_ATTR_MASK_QP_CREATE_FLAGS
                        as u64;
                mlx5_attr.create_flags =
                    mlx5_sys::mlx5dv_qp_create_flags_MLX5DV_QP_CREATE_DISABLE_SCATTER_TO_CQE;
            }

            let qp = mlx5_sys::mlx5dv_create_qp(self.as_ptr(), &mut qp_attr, &mut mlx5_attr);
            let qp = NonNull::new(qp).ok_or_else(io::Error::last_os_error)?;

            let mut result = RcQpInner {
                qp,
                state: Cell::new(QpState::Reset),
                max_inline_data: config.max_inline_data,
                sq: None,
                rq: OwnedRq::new(None),
                sq_callback,
                rq_callback,
                send_cq: Rc::downgrade(send_cq),
                recv_cq: Rc::downgrade(recv_cq),
                _pd: pd.clone(),
                _marker: std::marker::PhantomData,
            };

            // Auto-initialize direct access
            RcQpIb::<SqEntry, RqEntry, OnSqComplete, OnRqComplete>::init_direct_access_internal(&mut result)?;

            Ok(result)
        }
    }

    /// Create an RC Queue Pair with SRQ using mlx5dv_create_qp.
    ///
    /// This variant uses a Shared Receive Queue (SRQ) for receive operations.
    /// The QP's own RQ-related methods (`post_recv`, `ring_rq_doorbell`, etc.)
    /// are not available. Instead, use the SRQ's methods for receive operations.
    ///
    /// # Arguments
    /// * `pd` - Protection Domain
    /// * `send_cq` - Completion Queue for send completions (wrapped in Rc for shared ownership)
    /// * `recv_cq` - Completion Queue for receive completions (wrapped in Rc for shared ownership)
    /// * `srq` - Shared Receive Queue
    /// * `config` - QP configuration (max_recv_wr and max_recv_sge are ignored)
    /// * `sq_callback` - SQ completion callback `Fn(Cqe, SqEntry)` called for each signaled SQ completion
    /// * `rq_callback` - RQ completion callback `Fn(Cqe, RqEntry)` called for each RQ completion
    ///
    /// # Errors
    /// Returns an error if the QP cannot be created.
    pub fn create_rc_qp_with_srq<SqEntry, RqEntry, OnSqComplete, OnRqComplete>(
        &self,
        pd: &Pd,
        send_cq: &Rc<CompletionQueue>,
        recv_cq: &Rc<CompletionQueue>,
        srq: &Srq<RqEntry>,
        config: &RcQpConfig,
        sq_callback: OnSqComplete,
        rq_callback: OnRqComplete,
    ) -> io::Result<Rc<RefCell<RcQpIbWithSrq<SqEntry, RqEntry, OnSqComplete, OnRqComplete>>>>
    where
        SqEntry: 'static,
        RqEntry: 'static,
        OnSqComplete: Fn(Cqe, SqEntry) + 'static,
        OnRqComplete: Fn(Cqe, RqEntry) + 'static,
    {
        let qp = self.create_rc_qp_with_srq_raw(pd, send_cq, recv_cq, srq, config, sq_callback, rq_callback)?;
        let qp_rc = Rc::new(RefCell::new(qp));
        let qpn = qp_rc.borrow().qpn();

        // Register this QP with both CQs for completion dispatch
        send_cq.register_queue(qpn, Rc::downgrade(&qp_rc) as _);
        recv_cq.register_queue(qpn, Rc::downgrade(&qp_rc) as _);

        Ok(qp_rc)
    }

    fn create_rc_qp_with_srq_raw<SqEntry, RqEntry, OnSqComplete, OnRqComplete>(
        &self,
        pd: &Pd,
        send_cq: &Rc<CompletionQueue>,
        recv_cq: &Rc<CompletionQueue>,
        srq: &Srq<RqEntry>,
        config: &RcQpConfig,
        sq_callback: OnSqComplete,
        rq_callback: OnRqComplete,
    ) -> io::Result<RcQpIbWithSrq<SqEntry, RqEntry, OnSqComplete, OnRqComplete>>
    where
        OnSqComplete: Fn(Cqe, SqEntry),
        OnRqComplete: Fn(Cqe, RqEntry),
    {
        unsafe {
            let mut qp_attr: mlx5_sys::ibv_qp_init_attr_ex = MaybeUninit::zeroed().assume_init();
            qp_attr.qp_type = mlx5_sys::ibv_qp_type_IBV_QPT_RC;
            qp_attr.send_cq = send_cq.as_ptr();
            qp_attr.recv_cq = recv_cq.as_ptr();
            qp_attr.srq = srq.as_ptr();
            qp_attr.cap.max_send_wr = config.max_send_wr;
            qp_attr.cap.max_recv_wr = 0; // RQ disabled when using SRQ
            qp_attr.cap.max_send_sge = config.max_send_sge;
            qp_attr.cap.max_recv_sge = 0; // RQ disabled when using SRQ
            qp_attr.cap.max_inline_data = config.max_inline_data;
            qp_attr.comp_mask = mlx5_sys::ibv_qp_init_attr_mask_IBV_QP_INIT_ATTR_PD;
            qp_attr.pd = pd.as_ptr();

            let mut mlx5_attr: mlx5_sys::mlx5dv_qp_init_attr = MaybeUninit::zeroed().assume_init();
            // Optionally disable scatter to CQE. When disabled, received data goes to the
            // receive buffer instead of being inlined in the CQE.
            if !config.enable_scatter_to_cqe {
                mlx5_attr.comp_mask =
                    mlx5_sys::mlx5dv_qp_init_attr_mask_MLX5DV_QP_INIT_ATTR_MASK_QP_CREATE_FLAGS
                        as u64;
                mlx5_attr.create_flags =
                    mlx5_sys::mlx5dv_qp_create_flags_MLX5DV_QP_CREATE_DISABLE_SCATTER_TO_CQE;
            }

            let qp = mlx5_sys::mlx5dv_create_qp(self.as_ptr(), &mut qp_attr, &mut mlx5_attr);
            let qp = NonNull::new(qp).ok_or_else(io::Error::last_os_error)?;

            let mut result = RcQpInner {
                qp,
                state: Cell::new(QpState::Reset),
                max_inline_data: config.max_inline_data,
                sq: None,
                rq: SharedRq::new(srq.clone()),
                sq_callback,
                rq_callback,
                send_cq: Rc::downgrade(send_cq),
                recv_cq: Rc::downgrade(recv_cq),
                _pd: pd.clone(),
                _marker: std::marker::PhantomData,
            };

            // Initialize SQ direct access
            RcQpIbWithSrq::<SqEntry, RqEntry, OnSqComplete, OnRqComplete>::init_direct_access_internal(&mut result)?;

            Ok(result)
        }
    }

    /// Create an RC Queue Pair for use with MonoCq (no internal callback).
    ///
    /// The callback is stored on the MonoCq side, not the QP. This enables
    /// the compiler to inline the callback when polling the MonoCq.
    ///
    /// Uses a single Entry type for both SQ and RQ completions.
    /// If you need different Entry types, use separate CQs for send and recv.
    ///
    /// # Arguments
    /// * `pd` - Protection Domain
    /// * `cq` - MonoCq for completions (can be same CQ for send and recv)
    /// * `config` - QP configuration
    ///
    /// # Errors
    /// Returns an error if the QP cannot be created.
    pub fn create_rc_qp_for_mono_cq<Entry, Q, F>(
        &self,
        pd: &Pd,
        cq: &crate::mono_cq::MonoCq<Q, F>,
        config: &RcQpConfig,
    ) -> io::Result<Rc<RefCell<RcQpForMonoCq<Entry>>>>
    where
        Q: crate::mono_cq::CompletionSource,
        F: Fn(Cqe, Q::Entry),
    {
        unsafe {
            let mut qp_attr: mlx5_sys::ibv_qp_init_attr_ex = MaybeUninit::zeroed().assume_init();
            qp_attr.qp_type = mlx5_sys::ibv_qp_type_IBV_QPT_RC;
            qp_attr.send_cq = cq.as_ptr();
            qp_attr.recv_cq = cq.as_ptr();
            qp_attr.cap.max_send_wr = config.max_send_wr;
            qp_attr.cap.max_recv_wr = config.max_recv_wr;
            qp_attr.cap.max_send_sge = config.max_send_sge;
            qp_attr.cap.max_recv_sge = config.max_recv_sge;
            qp_attr.cap.max_inline_data = config.max_inline_data;
            qp_attr.comp_mask = mlx5_sys::ibv_qp_init_attr_mask_IBV_QP_INIT_ATTR_PD;
            qp_attr.pd = pd.as_ptr();

            let mut mlx5_attr: mlx5_sys::mlx5dv_qp_init_attr = MaybeUninit::zeroed().assume_init();
            // Optionally disable scatter to CQE. When disabled, received data goes to the
            // receive buffer instead of being inlined in the CQE.
            if !config.enable_scatter_to_cqe {
                mlx5_attr.comp_mask =
                    mlx5_sys::mlx5dv_qp_init_attr_mask_MLX5DV_QP_INIT_ATTR_MASK_QP_CREATE_FLAGS
                        as u64;
                mlx5_attr.create_flags =
                    mlx5_sys::mlx5dv_qp_create_flags_MLX5DV_QP_CREATE_DISABLE_SCATTER_TO_CQE;
            }

            let qp = mlx5_sys::mlx5dv_create_qp(self.as_ptr(), &mut qp_attr, &mut mlx5_attr);
            let qp = NonNull::new(qp).ok_or_else(io::Error::last_os_error)?;

            // Query QP info for direct access initialization
            let mut dv_qp: MaybeUninit<mlx5_sys::mlx5dv_qp> = MaybeUninit::zeroed();
            let mut obj: MaybeUninit<mlx5_sys::mlx5dv_obj> = MaybeUninit::zeroed();

            let dv_qp_ptr = dv_qp.as_mut_ptr();
            (*dv_qp_ptr).comp_mask =
                mlx5_sys::mlx5dv_qp_comp_mask_MLX5DV_QP_MASK_RAW_QP_HANDLES as u64;

            let obj_ptr = obj.as_mut_ptr();
            (*obj_ptr).qp.in_ = qp.as_ptr();
            (*obj_ptr).qp.out = dv_qp_ptr;

            let ret =
                mlx5_sys::mlx5dv_init_obj(obj_ptr, mlx5_sys::mlx5dv_obj_type_MLX5DV_OBJ_QP as u64);
            if ret != 0 {
                // Destroy QP on failure
                mlx5_sys::ibv_destroy_qp(qp.as_ptr());
                return Err(io::Error::from_raw_os_error(-ret));
            }

            let dv_qp = dv_qp.assume_init();
            let qp_num = (*qp.as_ptr()).qp_num;
            let sqn = if dv_qp.sqn != 0 { dv_qp.sqn } else { qp_num };

            let sq_wqe_cnt = dv_qp.sq.wqe_cnt as u16;
            let rq_wqe_cnt = dv_qp.rq.wqe_cnt;

            Ok(Rc::new(RefCell::new(RcQpInner {
                qp,
                state: Cell::new(QpState::Reset),
                max_inline_data: config.max_inline_data,
                sq: Some(SendQueueState {
                    buf: dv_qp.sq.buf as *mut u8,
                    wqe_cnt: sq_wqe_cnt,
                    sqn,
                    max_inline_data: config.max_inline_data,
                    pi: Cell::new(0),
                    ci: Cell::new(0),
                    last_wqe: Cell::new(None),
                    dbrec: dv_qp.dbrec,
                    bf_reg: dv_qp.bf.reg as *mut u8,
                    bf_size: dv_qp.bf.size,
                    bf_offset: Cell::new(0),
                    table: OrderedWqeTable::new(sq_wqe_cnt),
                    _marker: std::marker::PhantomData,
                }),
                rq: OwnedRq::new(Some(ReceiveQueueState {
                    buf: dv_qp.rq.buf as *mut u8,
                    wqe_cnt: rq_wqe_cnt,
                    stride: dv_qp.rq.stride,
                    pi: Cell::new(0),
                    ci: Cell::new(0),
                    dbrec: dv_qp.dbrec,
                    table: (0..rq_wqe_cnt).map(|_| Cell::new(None)).collect(),
                })),
                sq_callback: (),
                rq_callback: (),
                // Empty weak references - MonoCq handles unregistration via Weak upgrade failure
                send_cq: Weak::new(),
                recv_cq: Weak::new(),
                _pd: pd.clone(),
                _marker: std::marker::PhantomData,
            })))
        }
    }
}

impl<SqEntry, RqEntry, Transport, TableType, Rq, OnSqComplete, OnRqComplete> Drop for RcQpInner<SqEntry, RqEntry, Transport, TableType, Rq, OnSqComplete, OnRqComplete> {
    fn drop(&mut self) {
        let qpn = self.qpn();
        // Unregister from both CQs before destroying QP
        if let Some(cq) = self.send_cq.upgrade() {
            cq.unregister_queue(qpn);
        }
        if let Some(cq) = self.recv_cq.upgrade() {
            cq.unregister_queue(qpn);
        }
        unsafe {
            mlx5_sys::ibv_destroy_qp(self.qp.as_ptr());
        }
    }
}

impl<SqEntry, RqEntry, Transport, TableType, Rq, OnSqComplete, OnRqComplete> RcQpInner<SqEntry, RqEntry, Transport, TableType, Rq, OnSqComplete, OnRqComplete> {
    /// Get the QP number.
    pub fn qpn(&self) -> u32 {
        unsafe { (*self.qp.as_ptr()).qp_num }
    }

    /// Get the current QP state.
    pub fn state(&self) -> QpState {
        self.state.get()
    }

    /// Get mlx5-specific QP information for direct WQE access.
    fn query_info(&self) -> io::Result<QpInfo> {
        unsafe {
            let mut dv_qp: MaybeUninit<mlx5_sys::mlx5dv_qp> = MaybeUninit::zeroed();
            let mut obj: MaybeUninit<mlx5_sys::mlx5dv_obj> = MaybeUninit::zeroed();

            let dv_qp_ptr = dv_qp.as_mut_ptr();
            (*dv_qp_ptr).comp_mask =
                mlx5_sys::mlx5dv_qp_comp_mask_MLX5DV_QP_MASK_RAW_QP_HANDLES as u64;

            let obj_ptr = obj.as_mut_ptr();
            (*obj_ptr).qp.in_ = self.qp.as_ptr();
            (*obj_ptr).qp.out = dv_qp_ptr;

            let ret =
                mlx5_sys::mlx5dv_init_obj(obj_ptr, mlx5_sys::mlx5dv_obj_type_MLX5DV_OBJ_QP as u64);
            if ret != 0 {
                return Err(io::Error::from_raw_os_error(-ret));
            }

            let dv_qp = dv_qp.assume_init();
            let qp_num = (*self.qp.as_ptr()).qp_num;
            let sqn = if dv_qp.sqn != 0 { dv_qp.sqn } else { qp_num };

            Ok(QpInfo {
                dbrec: dv_qp.dbrec,
                sq_buf: dv_qp.sq.buf as *mut u8,
                sq_wqe_cnt: dv_qp.sq.wqe_cnt,
                sq_stride: dv_qp.sq.stride,
                rq_buf: dv_qp.rq.buf as *mut u8,
                rq_wqe_cnt: dv_qp.rq.wqe_cnt,
                rq_stride: dv_qp.rq.stride,
                bf_reg: dv_qp.bf.reg as *mut u8,
                bf_size: dv_qp.bf.size,
                sqn,
            })
        }
    }

    /// Transition QP from RESET to INIT.
    pub fn modify_to_init(&mut self, port: u8, access_flags: u32) -> io::Result<()> {
        if self.state.get() != QpState::Reset {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "QP must be in RESET state",
            ));
        }

        unsafe {
            let mut attr: mlx5_sys::ibv_qp_attr = MaybeUninit::zeroed().assume_init();
            attr.qp_state = mlx5_sys::ibv_qp_state_IBV_QPS_INIT;
            attr.pkey_index = 0;
            attr.port_num = port;
            attr.qp_access_flags = access_flags;

            let mask = mlx5_sys::ibv_qp_attr_mask_IBV_QP_STATE
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_PKEY_INDEX
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_PORT
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_ACCESS_FLAGS;

            let ret = mlx5_sys::ibv_modify_qp(self.qp.as_ptr(), &mut attr, mask as i32);
            if ret != 0 {
                return Err(io::Error::from_raw_os_error(ret));
            }
        }

        self.state.set(QpState::Init);
        Ok(())
    }

    /// Transition QP from RTR to RTS (Ready to Send).
    pub fn modify_to_rts(&mut self, local_psn: u32, max_rd_atomic: u8) -> io::Result<()> {
        if self.state.get() != QpState::Rtr {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "QP must be in RTR state",
            ));
        }

        unsafe {
            let mut attr: mlx5_sys::ibv_qp_attr = MaybeUninit::zeroed().assume_init();
            attr.qp_state = mlx5_sys::ibv_qp_state_IBV_QPS_RTS;
            attr.timeout = 14;
            attr.retry_cnt = 7;
            attr.rnr_retry = 7;
            attr.sq_psn = local_psn;
            attr.max_rd_atomic = max_rd_atomic;

            let mask = mlx5_sys::ibv_qp_attr_mask_IBV_QP_STATE
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_TIMEOUT
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_RETRY_CNT
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_RNR_RETRY
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_SQ_PSN
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_MAX_QP_RD_ATOMIC;

            let ret = mlx5_sys::ibv_modify_qp(self.qp.as_ptr(), &mut attr, mask as i32);
            if ret != 0 {
                return Err(io::Error::from_raw_os_error(ret));
            }
        }

        self.state.set(QpState::Rts);
        Ok(())
    }

    /// Transition QP to ERROR state.
    ///
    /// This cleanly tears down the connection and flushes any pending work requests.
    /// Useful before destroying a QP to avoid crashes when the remote QP is already gone.
    pub fn modify_to_error(&self) -> io::Result<()> {
        unsafe {
            let mut attr: mlx5_sys::ibv_qp_attr = MaybeUninit::zeroed().assume_init();
            attr.qp_state = mlx5_sys::ibv_qp_state_IBV_QPS_ERR;

            let mask = mlx5_sys::ibv_qp_attr_mask_IBV_QP_STATE;

            let ret = mlx5_sys::ibv_modify_qp(self.qp.as_ptr(), &mut attr, mask as i32);
            if ret != 0 {
                return Err(io::Error::from_raw_os_error(ret));
            }
        }

        self.state.set(QpState::Error);
        Ok(())
    }

    fn sq(&self) -> io::Result<&SendQueueState<SqEntry, TableType>> {
        self.sq
            .as_ref()
            .ok_or_else(|| io::Error::other("direct access not initialized"))
    }

    /// Get the number of available WQE slots in the send queue.
    pub fn send_queue_available(&self) -> u16 {
        self.sq.as_ref().map(|sq| sq.available()).unwrap_or(0)
    }

    /// Ring the SQ doorbell to notify HCA of new WQEs.
    pub fn ring_sq_doorbell(&self) {
        if let Some(sq) = self.sq.as_ref() {
            sq.ring_doorbell();
        }
    }
}

/// InfiniBand transport-specific methods.
impl<SqEntry, RqEntry, TableType, Rq, OnSqComplete, OnRqComplete> RcQpInner<SqEntry, RqEntry, InfiniBand, TableType, Rq, OnSqComplete, OnRqComplete> {
    /// Transition QP from INIT to RTR (Ready to Receive).
    ///
    /// Uses LID-based addressing for InfiniBand fabric.
    pub fn modify_to_rtr(
        &mut self,
        remote: &RemoteQpInfo,
        port: u8,
        max_dest_rd_atomic: u8,
    ) -> io::Result<()> {
        if self.state.get() != QpState::Init {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "QP must be in INIT state",
            ));
        }

        unsafe {
            let mut attr: mlx5_sys::ibv_qp_attr = MaybeUninit::zeroed().assume_init();
            attr.qp_state = mlx5_sys::ibv_qp_state_IBV_QPS_RTR;
            attr.path_mtu = mlx5_sys::ibv_mtu_IBV_MTU_4096;
            attr.dest_qp_num = remote.qp_number;
            attr.rq_psn = remote.packet_sequence_number;
            attr.max_dest_rd_atomic = max_dest_rd_atomic;
            attr.min_rnr_timer = 12;
            attr.ah_attr.dlid = remote.local_identifier;
            attr.ah_attr.sl = 0;
            attr.ah_attr.src_path_bits = 0;
            attr.ah_attr.is_global = 0;
            attr.ah_attr.port_num = port;

            let mask = mlx5_sys::ibv_qp_attr_mask_IBV_QP_STATE
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_AV
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_PATH_MTU
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_DEST_QPN
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_RQ_PSN
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_MAX_DEST_RD_ATOMIC
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_MIN_RNR_TIMER;

            let ret = mlx5_sys::ibv_modify_qp(self.qp.as_ptr(), &mut attr, mask as i32);
            if ret != 0 {
                return Err(io::Error::from_raw_os_error(ret));
            }
        }

        self.state.set(QpState::Rtr);
        Ok(())
    }

    /// Connect to a remote QP.
    ///
    /// Transitions the QP through RESET -> INIT -> RTR -> RTS.
    pub fn connect(
        &mut self,
        remote: &RemoteQpInfo,
        port: u8,
        local_psn: u32,
        max_rd_atomic: u8,
        max_dest_rd_atomic: u8,
        access_flags: u32,
    ) -> io::Result<()> {
        self.modify_to_init(port, access_flags)?;
        self.modify_to_rtr(remote, port, max_dest_rd_atomic)?;
        self.modify_to_rts(local_psn, max_rd_atomic)?;
        Ok(())
    }
}

/// RoCE transport-specific methods.
impl<SqEntry, RqEntry, TableType, Rq, OnSqComplete, OnRqComplete> RcQpInner<SqEntry, RqEntry, RoCE, TableType, Rq, OnSqComplete, OnRqComplete> {
    /// Transition QP from INIT to RTR (Ready to Receive).
    ///
    /// Uses GID-based addressing for RoCE (RDMA over Converged Ethernet).
    ///
    /// # NOTE: RoCE support is untested (IB-only hardware environment)
    pub fn modify_to_rtr(
        &mut self,
        remote: &RoCERemoteQpInfo,
        port: u8,
        max_dest_rd_atomic: u8,
    ) -> io::Result<()> {
        if self.state.get() != QpState::Init {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "QP must be in INIT state",
            ));
        }

        unsafe {
            let mut attr: mlx5_sys::ibv_qp_attr = MaybeUninit::zeroed().assume_init();
            attr.qp_state = mlx5_sys::ibv_qp_state_IBV_QPS_RTR;
            attr.path_mtu = mlx5_sys::ibv_mtu_IBV_MTU_4096;
            attr.dest_qp_num = remote.qp_number;
            attr.rq_psn = remote.packet_sequence_number;
            attr.max_dest_rd_atomic = max_dest_rd_atomic;
            attr.min_rnr_timer = 12;

            // RoCE: use GRH (Global Route Header)
            attr.ah_attr.is_global = 1;
            attr.ah_attr.port_num = port;
            attr.ah_attr.dlid = 0; // Not used for RoCE

            // GRH configuration
            attr.ah_attr.grh.dgid.raw = remote.grh.dgid.raw;
            attr.ah_attr.grh.sgid_index = remote.grh.sgid_index;
            attr.ah_attr.grh.flow_label = remote.grh.flow_label;
            attr.ah_attr.grh.traffic_class = remote.grh.traffic_class;
            attr.ah_attr.grh.hop_limit = remote.grh.hop_limit;

            // Service level and source path bits
            attr.ah_attr.sl = 0;
            attr.ah_attr.src_path_bits = 0;

            let mask = mlx5_sys::ibv_qp_attr_mask_IBV_QP_STATE
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_AV
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_PATH_MTU
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_DEST_QPN
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_RQ_PSN
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_MAX_DEST_RD_ATOMIC
                | mlx5_sys::ibv_qp_attr_mask_IBV_QP_MIN_RNR_TIMER;

            let ret = mlx5_sys::ibv_modify_qp(self.qp.as_ptr(), &mut attr, mask as i32);
            if ret != 0 {
                return Err(io::Error::from_raw_os_error(ret));
            }
        }

        self.state.set(QpState::Rtr);
        Ok(())
    }

    /// Connect to a remote QP.
    ///
    /// Transitions the QP through RESET -> INIT -> RTR -> RTS.
    ///
    /// # NOTE: RoCE support is untested (IB-only hardware environment)
    pub fn connect(
        &mut self,
        remote: &RoCERemoteQpInfo,
        port: u8,
        local_psn: u32,
        max_rd_atomic: u8,
        max_dest_rd_atomic: u8,
        access_flags: u32,
    ) -> io::Result<()> {
        self.modify_to_init(port, access_flags)?;
        self.modify_to_rtr(remote, port, max_dest_rd_atomic)?;
        self.modify_to_rts(local_psn, max_rd_atomic)?;
        Ok(())
    }
}

impl<SqEntry, RqEntry, OnSqComplete, OnRqComplete> RcQpIb<SqEntry, RqEntry, OnSqComplete, OnRqComplete> {
    /// Initialize direct queue access (internal implementation).
    fn init_direct_access_internal(&mut self) -> io::Result<()> {
        if self.sq.is_some() {
            return Ok(()); // Already initialized
        }

        let info = self.query_info()?;
        let wqe_cnt = info.sq_wqe_cnt as u16;

        self.sq = Some(SendQueueState {
            buf: info.sq_buf,
            wqe_cnt,
            sqn: info.sqn,
            max_inline_data: self.max_inline_data,
            pi: Cell::new(0),
            ci: Cell::new(0),
            last_wqe: Cell::new(None),
            dbrec: info.dbrec,
            bf_reg: info.bf_reg,
            bf_size: info.bf_size,
            bf_offset: Cell::new(0),
            table: OrderedWqeTable::new(wqe_cnt),
            _marker: std::marker::PhantomData,
        });

        let rq_wqe_cnt = info.rq_wqe_cnt;
        self.rq = OwnedRq::new(Some(ReceiveQueueState {
            buf: info.rq_buf,
            wqe_cnt: rq_wqe_cnt,
            stride: info.rq_stride,
            pi: Cell::new(0),
            ci: Cell::new(0),
            dbrec: info.dbrec,
            table: (0..rq_wqe_cnt).map(|_| Cell::new(None)).collect(),
        }));

        Ok(())
    }
}

impl<SqEntry, RqEntry, OnSqComplete, OnRqComplete> RcQpIbWithSrq<SqEntry, RqEntry, OnSqComplete, OnRqComplete> {
    /// Initialize direct queue access for SRQ variant (SQ only).
    fn init_direct_access_internal(&mut self) -> io::Result<()> {
        if self.sq.is_some() {
            return Ok(()); // Already initialized
        }

        let info = self.query_info()?;
        let wqe_cnt = info.sq_wqe_cnt as u16;

        self.sq = Some(SendQueueState {
            buf: info.sq_buf,
            wqe_cnt,
            sqn: info.sqn,
            max_inline_data: self.max_inline_data,
            pi: Cell::new(0),
            ci: Cell::new(0),
            last_wqe: Cell::new(None),
            dbrec: info.dbrec,
            bf_reg: info.bf_reg,
            bf_size: info.bf_size,
            bf_offset: Cell::new(0),
            table: OrderedWqeTable::new(wqe_cnt),
            _marker: std::marker::PhantomData,
        });

        // Note: RQ is not initialized for SRQ variant - SRQ handles receives
        Ok(())
    }
}

// =============================================================================
// CompletionTarget impl for RcQp with OwnedRq
// =============================================================================

impl<SqEntry, RqEntry, OnSqComplete, OnRqComplete> CompletionTarget for RcQpIb<SqEntry, RqEntry, OnSqComplete, OnRqComplete>
where
    OnSqComplete: Fn(Cqe, SqEntry),
    OnRqComplete: Fn(Cqe, RqEntry),
{
    fn qpn(&self) -> u32 {
        RcQpInner::qpn(self)
    }

    fn dispatch_cqe(&self, cqe: Cqe) {
        if cqe.opcode.is_responder() {
            // RQ completion (responder)
            if let Some(rq) = self.rq.as_ref()
                && let Some(entry) = rq.process_completion(cqe.wqe_counter)
            {
                (self.rq_callback)(cqe, entry);
            }
        } else {
            // SQ completion (requester)
            if let Some(sq) = self.sq.as_ref()
                && let Some(entry) = sq.process_completion(cqe.wqe_counter)
            {
                (self.sq_callback)(cqe, entry);
            }
        }
    }
}

// =============================================================================
// CompletionTarget impl for RcQp with SharedRq (SRQ)
// =============================================================================

impl<SqEntry, RqEntry, OnSqComplete, OnRqComplete> CompletionTarget for RcQpIbWithSrq<SqEntry, RqEntry, OnSqComplete, OnRqComplete>
where
    OnSqComplete: Fn(Cqe, SqEntry),
    OnRqComplete: Fn(Cqe, RqEntry),
{
    fn qpn(&self) -> u32 {
        RcQpInner::qpn(self)
    }

    fn dispatch_cqe(&self, cqe: Cqe) {
        if cqe.opcode.is_responder() {
            // RQ completion via SRQ (responder)
            if let Some(entry) = self.rq.srq().process_recv_completion(cqe.wqe_counter)
            {
                (self.rq_callback)(cqe, entry);
            }
        } else {
            // SQ completion (requester)
            if let Some(sq) = self.sq.as_ref()
                && let Some(entry) = sq.process_completion(cqe.wqe_counter)
            {
                (self.sq_callback)(cqe, entry);
            }
        }
    }
}

// =============================================================================
// ReceiveQueue methods (OwnedRq)
// =============================================================================

impl<SqEntry, RqEntry, Transport, TableType, OnSqComplete, OnRqComplete> RcQpInner<SqEntry, RqEntry, Transport, TableType, OwnedRq<RqEntry>, OnSqComplete, OnRqComplete> {
    /// Ring the RQ doorbell to notify HCA of new WQEs.
    pub fn ring_rq_doorbell(&self) {
        if let Some(rq) = self.rq.as_ref() {
            rq.ring_doorbell();
        }
    }
}

// =============================================================================
// SRQ access methods (SharedRq)
// =============================================================================

impl<SqEntry, RqEntry, Transport, TableType, OnSqComplete, OnRqComplete> RcQpInner<SqEntry, RqEntry, Transport, TableType, SharedRq<RqEntry>, OnSqComplete, OnRqComplete> {
    /// Get a reference to the underlying SRQ.
    pub fn srq(&self) -> &Srq<RqEntry> {
        self.rq.srq()
    }
}

// =============================================================================
// Common methods
// =============================================================================

impl<SqEntry, RqEntry, Transport, TableType, Rq, OnSqComplete, OnRqComplete> RcQpInner<SqEntry, RqEntry, Transport, TableType, Rq, OnSqComplete, OnRqComplete> {
    /// Get the Send Queue Number (SQN).
    ///
    /// Returns None if direct access is not initialized.
    #[inline]
    pub fn sqn(&self) -> Option<u32> {
        self.sq.as_ref().map(|sq| sq.sqn)
    }
}

// =============================================================================
// Low-level SQ Access API for wqe_tx! macro
// =============================================================================

/// SQ slot information for direct WQE construction.
///
/// Contains the raw pointers and indices needed for constructing WQEs directly
/// without using the builder pattern.
#[derive(Debug, Clone, Copy)]
pub struct SqSlot {
    /// Pointer to the WQE buffer (64-byte aligned WQEBB).
    pub ptr: *mut u8,
    /// WQE index in the send queue.
    pub wqe_idx: u16,
    /// Send Queue Number.
    pub sqn: u32,
}


// =============================================================================
// CompletionSource impl for RcQp (for use with MonoCq)
// =============================================================================

use crate::mono_cq::CompletionSource;

impl<Entry> CompletionSource for RcQpInner<Entry, Entry, InfiniBand, OrderedWqeTable<Entry>, OwnedRq<Entry>, (), ()> {
    type Entry = Entry;

    fn qpn(&self) -> u32 {
        RcQpInner::qpn(self)
    }

    fn process_cqe(&self, cqe: Cqe) -> Option<Entry> {
        if cqe.opcode.is_responder() {
            // RQ completion
            self.rq.as_ref()?.process_completion(cqe.wqe_counter)
        } else {
            // SQ completion
            self.sq.as_ref()?.process_completion(cqe.wqe_counter)
        }
    }
}

// =============================================================================
// RcQp without callback for MonoCq
// =============================================================================

/// RcQp type for use with MonoCq (no internal callback).
///
/// Uses a single Entry type for both SQ and RQ completions.
/// If you need different Entry types for SQ and RQ, use separate CQs.
pub type RcQpForMonoCq<Entry> = RcQpInner<Entry, Entry, InfiniBand, OrderedWqeTable<Entry>, OwnedRq<Entry>, (), ()>;

// =============================================================================
// Simplified WQE Builder API
// =============================================================================

use crate::wqe::{
    // Data state marker (HasData is imported at file top)
    NoData,
    // Address Vector trait and types
    Av, NoAv,
    // Flags
    TxFlags,
};
use std::marker::PhantomData;

// =============================================================================
// Maximum WQEBB Calculation Functions
// =============================================================================

/// Calculate the padded inline data size (16-byte aligned).
///
/// inline_padded = ((4 + max_inline_data + 15) & !15)
#[inline]
fn calc_inline_padded(max_inline_data: u32) -> usize {
    ((4 + max_inline_data as usize) + 15) & !15
}

/// Calculate maximum WQEBB count for SEND operation.
///
/// Layout: ctrl(16) + AV + inline_padded
#[inline]
fn calc_max_wqebb_send(av_size: usize, max_inline_data: u32) -> u16 {
    let size = CtrlSeg::SIZE + av_size + calc_inline_padded(max_inline_data);
    calc_wqebb_cnt(size)
}

/// Calculate maximum WQEBB count for WRITE operation.
///
/// Layout: ctrl(16) + AV + rdma(16) + inline_padded
#[inline]
fn calc_max_wqebb_write(av_size: usize, max_inline_data: u32) -> u16 {
    let size = CtrlSeg::SIZE + av_size + RdmaSeg::SIZE + calc_inline_padded(max_inline_data);
    calc_wqebb_cnt(size)
}

/// Calculate maximum WQEBB count for READ operation.
///
/// Layout: ctrl(16) + AV + rdma(16) + sge(16)
#[inline]
fn calc_max_wqebb_read(av_size: usize) -> u16 {
    let size = CtrlSeg::SIZE + av_size + RdmaSeg::SIZE + DataSeg::SIZE;
    calc_wqebb_cnt(size)
}

/// Calculate maximum WQEBB count for Atomic operation.
///
/// Layout: ctrl(16) + AV + rdma(16) + atomic(16) + sge(16)
#[inline]
fn calc_max_wqebb_atomic(av_size: usize) -> u16 {
    let size = CtrlSeg::SIZE + av_size + RdmaSeg::SIZE + AtomicSeg::SIZE + DataSeg::SIZE;
    calc_wqebb_cnt(size)
}

/// Calculate maximum WQEBB count for NOP operation.
///
/// Layout: ctrl(16) only
#[inline]
fn calc_max_wqebb_nop() -> u16 {
    calc_wqebb_cnt(CtrlSeg::SIZE)
}

/// Internal WQE builder core that handles direct buffer writes.
struct WqeCore<'a, Entry> {
    sq: &'a SendQueueState<Entry, OrderedWqeTable<Entry>>,
    wqe_ptr: *mut u8,
    wqe_idx: u16,
    offset: usize,
    ds_count: u8,
    signaled: bool,
    entry: Option<Entry>,
}

impl<'a, Entry> WqeCore<'a, Entry> {
    #[inline]
    fn new(sq: &'a SendQueueState<Entry, OrderedWqeTable<Entry>>, entry: Option<Entry>) -> Self {
        let wqe_idx = sq.pi.get();
        let wqe_ptr = sq.get_wqe_ptr(wqe_idx);
        Self {
            sq,
            wqe_ptr,
            wqe_idx,
            offset: 0,
            ds_count: 0,
            signaled: entry.is_some(),
            entry,
        }
    }

    #[inline]
    fn available(&self) -> u16 {
        self.sq.available()
    }

    #[inline]
    fn max_inline_data(&self) -> u32 {
        self.sq.max_inline_data()
    }

    #[inline]
    fn write_ctrl(&mut self, opcode: WqeOpcode, flags: WqeFlags, imm: u32) {
        let flags = if self.signaled {
            flags | WqeFlags::COMPLETION
        } else {
            flags
        };
        unsafe {
            CtrlSeg::write(
                self.wqe_ptr,
                0,
                opcode as u8,
                self.wqe_idx,
                self.sq.sqn,
                0,
                flags.bits(),
                imm,
            );
        }
        self.offset = CtrlSeg::SIZE;
        self.ds_count = 1;
    }

    /// Write Address Vector segment using the Av trait.
    #[inline]
    fn write_av<A: Av>(&mut self, av: A) {
        unsafe {
            av.write_av(self.wqe_ptr.add(self.offset));
        }
        self.offset += A::SIZE;
        self.ds_count += A::DS_COUNT;
    }

    #[inline]
    fn write_rdma(&mut self, addr: u64, rkey: u32) {
        unsafe {
            RdmaSeg::write(self.wqe_ptr.add(self.offset), addr, rkey);
        }
        self.offset += RdmaSeg::SIZE;
        self.ds_count += 1;
    }

    #[inline]
    fn write_sge(&mut self, addr: u64, len: u32, lkey: u32) {
        unsafe {
            DataSeg::write(self.wqe_ptr.add(self.offset), len, lkey, addr);
        }
        self.offset += DataSeg::SIZE;
        self.ds_count += 1;
    }

    #[inline]
    fn write_inline(&mut self, data: &[u8]) {
        let padded_size = unsafe {
            let ptr = self.wqe_ptr.add(self.offset);
            let size = InlineHeader::write(ptr, data.len() as u32);
            std::ptr::copy_nonoverlapping(data.as_ptr(), ptr.add(4), data.len());
            size
        };
        self.offset += padded_size;
        self.ds_count += (padded_size / 16) as u8;
    }

    #[inline]
    fn write_atomic_cas(&mut self, swap: u64, compare: u64) {
        unsafe {
            AtomicSeg::write_cas(self.wqe_ptr.add(self.offset), swap, compare);
        }
        self.offset += AtomicSeg::SIZE;
        self.ds_count += 1;
    }

    #[inline]
    fn write_atomic_fa(&mut self, add_value: u64) {
        unsafe {
            AtomicSeg::write_fa(self.wqe_ptr.add(self.offset), add_value);
        }
        self.offset += AtomicSeg::SIZE;
        self.ds_count += 1;
    }

    #[inline]
    fn finish_internal(self) -> io::Result<WqeHandle> {
        let wqebb_cnt = calc_wqebb_cnt(self.offset);
        let slots_to_end = self.sq.slots_to_end();

        if wqebb_cnt > slots_to_end && slots_to_end < self.sq.wqe_cnt {
            return self.finish_with_wrap_around(wqebb_cnt, slots_to_end);
        }

        unsafe {
            CtrlSeg::update_ds_cnt(self.wqe_ptr, self.ds_count);
            // Set completion flag if signaled (may not have been set at write_ctrl time)
            if self.signaled {
                CtrlSeg::set_completion_flag(self.wqe_ptr);
            }
        }

        let wqe_idx = self.wqe_idx;
        self.sq.advance_pi(wqebb_cnt);
        self.sq.set_last_wqe(self.wqe_ptr, self.offset);

        if let Some(entry) = self.entry {
            self.sq.table.store(wqe_idx, entry, self.sq.pi.get());
        }

        Ok(WqeHandle {
            wqe_idx,
            size: self.offset,
        })
    }

    #[cold]
    fn finish_with_wrap_around(self, wqebb_cnt: u16, slots_to_end: u16) -> io::Result<WqeHandle> {
        let total_needed = slots_to_end + wqebb_cnt;
        if self.sq.available() < total_needed {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }

        let mut temp_buf = [0u8; 256];
        unsafe {
            std::ptr::copy_nonoverlapping(self.wqe_ptr, temp_buf.as_mut_ptr(), self.offset);
            self.sq.post_nop(slots_to_end);
        }

        let new_wqe_idx = self.sq.pi.get();
        let new_wqe_ptr = self.sq.get_wqe_ptr(new_wqe_idx);

        unsafe {
            std::ptr::copy_nonoverlapping(temp_buf.as_ptr(), new_wqe_ptr, self.offset);
            CtrlSeg::update_wqe_idx(new_wqe_ptr, new_wqe_idx);
            CtrlSeg::update_ds_cnt(new_wqe_ptr, self.ds_count);
            // Set completion flag if signaled (may not have been set at write_ctrl time)
            if self.signaled {
                CtrlSeg::set_completion_flag(new_wqe_ptr);
            }
        }

        self.sq.advance_pi(wqebb_cnt);
        self.sq.set_last_wqe(new_wqe_ptr, self.offset);

        if let Some(entry) = self.entry {
            self.sq.table.store(new_wqe_idx, entry, self.sq.pi.get());
        }

        Ok(WqeHandle {
            wqe_idx: new_wqe_idx,
            size: self.offset,
        })
    }
}

// =============================================================================
// Unified SQ WQE Entry Point
// =============================================================================

/// SQ WQE entry point for both IB and RoCE transport.
///
/// Provides verb methods that include all required parameters.
/// The `A` type parameter is the Address Vector type (NoAv for IB, &GrhAttr for RoCE).
#[must_use = "WQE builder must be finished"]
pub struct SqWqeEntryPoint<'a, Entry, A> {
    core: WqeCore<'a, Entry>,
    av: A,
}

impl<'a, Entry, A: Av> SqWqeEntryPoint<'a, Entry, A> {
    /// Create a new entry point with the given AV.
    #[inline]
    pub(crate) fn new(sq: &'a SendQueueState<Entry, OrderedWqeTable<Entry>>, av: A) -> Self {
        Self {
            core: WqeCore::new(sq, None),
            av,
        }
    }

    // -------------------------------------------------------------------------
    // SEND operations
    // -------------------------------------------------------------------------

    /// Start building a SEND WQE.
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the maximum
    /// possible WQE size (based on max_inline_data).
    #[inline]
    pub fn send(mut self, flags: TxFlags) -> io::Result<SendWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_send(A::SIZE, self.core.max_inline_data());
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        self.core.write_ctrl(WqeOpcode::Send, flags, 0);
        self.core.write_av(self.av);
        Ok(SendWqeBuilder { core: self.core, _data: PhantomData })
    }

    /// Start building a SEND with immediate data WQE.
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the maximum
    /// possible WQE size (based on max_inline_data).
    #[inline]
    pub fn send_imm(mut self, flags: TxFlags, imm: u32) -> io::Result<SendWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_send(A::SIZE, self.core.max_inline_data());
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        self.core.write_ctrl(WqeOpcode::SendImm, flags, imm);
        self.core.write_av(self.av);
        Ok(SendWqeBuilder { core: self.core, _data: PhantomData })
    }

    // -------------------------------------------------------------------------
    // WRITE operations (remote_addr, rkey required)
    // -------------------------------------------------------------------------

    /// Start building an RDMA WRITE WQE.
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the maximum
    /// possible WQE size (based on max_inline_data).
    #[inline]
    pub fn write(mut self, flags: TxFlags, remote_addr: u64, rkey: u32) -> io::Result<WriteWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_write(A::SIZE, self.core.max_inline_data());
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        self.core.write_ctrl(WqeOpcode::RdmaWrite, flags, 0);
        self.core.write_av(self.av);
        self.core.write_rdma(remote_addr, rkey);
        Ok(WriteWqeBuilder { core: self.core, _data: PhantomData })
    }

    /// Start building an RDMA WRITE with immediate data WQE.
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the maximum
    /// possible WQE size (based on max_inline_data).
    #[inline]
    pub fn write_imm(mut self, flags: TxFlags, remote_addr: u64, rkey: u32, imm: u32) -> io::Result<WriteWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_write(A::SIZE, self.core.max_inline_data());
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        self.core.write_ctrl(WqeOpcode::RdmaWriteImm, flags, imm);
        self.core.write_av(self.av);
        self.core.write_rdma(remote_addr, rkey);
        Ok(WriteWqeBuilder { core: self.core, _data: PhantomData })
    }

    // -------------------------------------------------------------------------
    // READ operations (remote_addr, rkey required)
    // -------------------------------------------------------------------------

    /// Start building an RDMA READ WQE.
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the WQE.
    #[inline]
    pub fn read(mut self, flags: TxFlags, remote_addr: u64, rkey: u32) -> io::Result<ReadWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_read(A::SIZE);
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        self.core.write_ctrl(WqeOpcode::RdmaRead, flags, 0);
        self.core.write_av(self.av);
        self.core.write_rdma(remote_addr, rkey);
        Ok(ReadWqeBuilder { core: self.core, _data: PhantomData })
    }

    // -------------------------------------------------------------------------
    // Atomic operations (remote_addr, rkey, and atomic params required)
    // -------------------------------------------------------------------------

    /// Start building an Atomic Compare-and-Swap WQE.
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the WQE.
    #[inline]
    pub fn cas(mut self, flags: TxFlags, remote_addr: u64, rkey: u32, swap: u64, compare: u64) -> io::Result<AtomicWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_atomic(A::SIZE);
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        self.core.write_ctrl(WqeOpcode::AtomicCs, flags, 0);
        self.core.write_av(self.av);
        self.core.write_rdma(remote_addr, rkey);
        self.core.write_atomic_cas(swap, compare);
        Ok(AtomicWqeBuilder { core: self.core, _data: PhantomData })
    }

    /// Start building an Atomic Fetch-and-Add WQE.
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the WQE.
    #[inline]
    pub fn fetch_add(mut self, flags: TxFlags, remote_addr: u64, rkey: u32, add_value: u64) -> io::Result<AtomicWqeBuilder<'a, Entry, NoData>> {
        let max_wqebb = calc_max_wqebb_atomic(A::SIZE);
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        self.core.write_ctrl(WqeOpcode::AtomicFa, flags, 0);
        self.core.write_av(self.av);
        self.core.write_rdma(remote_addr, rkey);
        self.core.write_atomic_fa(add_value);
        Ok(AtomicWqeBuilder { core: self.core, _data: PhantomData })
    }

    // -------------------------------------------------------------------------
    // NOP operation
    // -------------------------------------------------------------------------

    /// Start building a NOP WQE.
    ///
    /// Returns `Err(WouldBlock)` if the SQ doesn't have enough space for the WQE.
    #[inline]
    pub fn nop(mut self, flags: TxFlags) -> io::Result<NopWqeBuilder<'a, Entry>> {
        let max_wqebb = calc_max_wqebb_nop();
        if self.core.available() < max_wqebb {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        self.core.write_ctrl(WqeOpcode::Nop, flags, 0);
        Ok(NopWqeBuilder { core: self.core })
    }
}

// =============================================================================
// Send WQE Builder
// =============================================================================

/// SEND WQE builder.
///
/// Uses type-state pattern: `DataState` is `NoData` initially,
/// transitions to `HasData` after calling `sge()` or `inline()`.
/// `finish_*()` methods are only available in `HasData` state.
#[must_use = "WQE builder must be finished"]
pub struct SendWqeBuilder<'a, Entry, DataState> {
    core: WqeCore<'a, Entry>,
    _data: PhantomData<DataState>,
}

/// sge/inline: NoData state, transitions to HasData.
impl<'a, Entry> SendWqeBuilder<'a, Entry, NoData> {
    /// Add a scatter/gather entry.
    #[inline]
    pub fn sge(mut self, addr: u64, len: u32, lkey: u32) -> SendWqeBuilder<'a, Entry, HasData> {
        self.core.write_sge(addr, len, lkey);
        SendWqeBuilder { core: self.core, _data: PhantomData }
    }

    /// Add inline data.
    #[inline]
    pub fn inline(mut self, data: &[u8]) -> SendWqeBuilder<'a, Entry, HasData> {
        self.core.write_inline(data);
        SendWqeBuilder { core: self.core, _data: PhantomData }
    }
}

/// finish methods: only available in HasData state.
impl<'a, Entry> SendWqeBuilder<'a, Entry, HasData> {
    /// Add another scatter/gather entry.
    #[inline]
    pub fn sge(mut self, addr: u64, len: u32, lkey: u32) -> Self {
        self.core.write_sge(addr, len, lkey);
        self
    }

    /// Add more inline data.
    #[inline]
    pub fn inline(mut self, data: &[u8]) -> Self {
        self.core.write_inline(data);
        self
    }

    /// Finish the WQE construction (unsignaled).
    #[inline]
    pub fn finish_unsignaled(self) -> io::Result<WqeHandle> {
        self.core.finish_internal()
    }

    /// Finish the WQE construction with completion signaling.
    #[inline]
    pub fn finish_signaled(mut self, entry: Entry) -> io::Result<WqeHandle> {
        self.core.entry = Some(entry);
        self.core.signaled = true;
        self.core.finish_internal()
    }
}

// =============================================================================
// Write WQE Builder
// =============================================================================

/// RDMA WRITE WQE builder.
///
/// Uses type-state pattern: `DataState` is `NoData` initially,
/// transitions to `HasData` after calling `sge()` or `inline()`.
/// `finish_*()` methods are only available in `HasData` state.
#[must_use = "WQE builder must be finished"]
pub struct WriteWqeBuilder<'a, Entry, DataState> {
    core: WqeCore<'a, Entry>,
    _data: PhantomData<DataState>,
}

/// sge/inline: NoData state, transitions to HasData.
impl<'a, Entry> WriteWqeBuilder<'a, Entry, NoData> {
    /// Add a scatter/gather entry.
    #[inline]
    pub fn sge(mut self, addr: u64, len: u32, lkey: u32) -> WriteWqeBuilder<'a, Entry, HasData> {
        self.core.write_sge(addr, len, lkey);
        WriteWqeBuilder { core: self.core, _data: PhantomData }
    }

    /// Add inline data.
    #[inline]
    pub fn inline(mut self, data: &[u8]) -> WriteWqeBuilder<'a, Entry, HasData> {
        self.core.write_inline(data);
        WriteWqeBuilder { core: self.core, _data: PhantomData }
    }
}

/// finish methods: only available in HasData state.
impl<'a, Entry> WriteWqeBuilder<'a, Entry, HasData> {
    /// Add another scatter/gather entry.
    #[inline]
    pub fn sge(mut self, addr: u64, len: u32, lkey: u32) -> Self {
        self.core.write_sge(addr, len, lkey);
        self
    }

    /// Add more inline data.
    #[inline]
    pub fn inline(mut self, data: &[u8]) -> Self {
        self.core.write_inline(data);
        self
    }

    /// Finish the WQE construction (unsignaled).
    #[inline]
    pub fn finish_unsignaled(self) -> io::Result<WqeHandle> {
        self.core.finish_internal()
    }

    /// Finish the WQE construction with completion signaling.
    #[inline]
    pub fn finish_signaled(mut self, entry: Entry) -> io::Result<WqeHandle> {
        self.core.entry = Some(entry);
        self.core.signaled = true;
        self.core.finish_internal()
    }
}

// =============================================================================
// Read WQE Builder
// =============================================================================

/// RDMA READ WQE builder.
///
/// Uses type-state pattern: `DataState` is `NoData` initially,
/// transitions to `HasData` after calling `sge()`.
/// `finish_*()` methods are only available in `HasData` state.
/// Note: READ operations do not support inline data.
#[must_use = "WQE builder must be finished"]
pub struct ReadWqeBuilder<'a, Entry, DataState> {
    core: WqeCore<'a, Entry>,
    _data: PhantomData<DataState>,
}

/// buffer: NoData state, transitions to HasData.
impl<'a, Entry> ReadWqeBuilder<'a, Entry, NoData> {
    /// Set the buffer for the read result.
    /// Read operations only allow a single buffer.
    #[inline]
    pub fn buffer(mut self, addr: u64, len: u32, lkey: u32) -> ReadWqeBuilder<'a, Entry, HasData> {
        self.core.write_sge(addr, len, lkey);
        ReadWqeBuilder { core: self.core, _data: PhantomData }
    }
}

/// finish methods: only available in HasData state.
impl<'a, Entry> ReadWqeBuilder<'a, Entry, HasData> {
    /// Finish the WQE construction (unsignaled).
    #[inline]
    pub fn finish_unsignaled(self) -> io::Result<WqeHandle> {
        self.core.finish_internal()
    }

    /// Finish the WQE construction with completion signaling.
    #[inline]
    pub fn finish_signaled(mut self, entry: Entry) -> io::Result<WqeHandle> {
        self.core.entry = Some(entry);
        self.core.signaled = true;
        self.core.finish_internal()
    }
}

// =============================================================================
// Atomic WQE Builder (CAS / Fetch-Add)
// =============================================================================

/// Atomic operation WQE builder (CAS or Fetch-Add).
///
/// Uses type-state pattern: `DataState` is `NoData` initially,
/// transitions to `HasData` after calling `sge()`.
/// `finish_*()` methods are only available in `HasData` state.
/// Note: Atomic operations have fixed 8-byte data size.
#[must_use = "WQE builder must be finished"]
pub struct AtomicWqeBuilder<'a, Entry, DataState> {
    core: WqeCore<'a, Entry>,
    _data: PhantomData<DataState>,
}

/// buffer: NoData state, transitions to HasData.
impl<'a, Entry> AtomicWqeBuilder<'a, Entry, NoData> {
    /// Set the buffer for the atomic result.
    /// Atomic operations only allow a single 8-byte buffer.
    #[inline]
    pub fn buffer(mut self, addr: u64, len: u32, lkey: u32) -> AtomicWqeBuilder<'a, Entry, HasData> {
        self.core.write_sge(addr, len, lkey);
        AtomicWqeBuilder { core: self.core, _data: PhantomData }
    }
}

/// finish methods: only available in HasData state.
impl<'a, Entry> AtomicWqeBuilder<'a, Entry, HasData> {
    /// Finish the WQE construction (unsignaled).
    #[inline]
    pub fn finish_unsignaled(self) -> io::Result<WqeHandle> {
        self.core.finish_internal()
    }

    /// Finish the WQE construction with completion signaling.
    #[inline]
    pub fn finish_signaled(mut self, entry: Entry) -> io::Result<WqeHandle> {
        self.core.entry = Some(entry);
        self.core.signaled = true;
        self.core.finish_internal()
    }
}

// =============================================================================
// NOP WQE Builder
// =============================================================================

/// NOP WQE builder.
///
/// NOP operations have no data segment, so `finish_*()` methods are
/// available immediately without needing to call `sge()` or `inline()`.
#[must_use = "WQE builder must be finished"]
pub struct NopWqeBuilder<'a, Entry> {
    core: WqeCore<'a, Entry>,
}

impl<'a, Entry> NopWqeBuilder<'a, Entry> {
    /// Finish the WQE construction (unsignaled).
    #[inline]
    pub fn finish_unsignaled(self) -> io::Result<WqeHandle> {
        self.core.finish_internal()
    }

    /// Finish the WQE construction with completion signaling.
    #[inline]
    pub fn finish_signaled(mut self, entry: Entry) -> io::Result<WqeHandle> {
        self.core.entry = Some(entry);
        self.core.signaled = true;
        self.core.finish_internal()
    }
}

// =============================================================================
// QP Entry Points (OwnedRq - RQ methods)
// =============================================================================

impl<SqEntry, RqEntry, Transport, OnSqComplete, OnRqComplete> RcQpInner<SqEntry, RqEntry, Transport, OrderedWqeTable<SqEntry>, OwnedRq<RqEntry>, OnSqComplete, OnRqComplete> {
    /// Post a receive WQE with a single scatter/gather entry.
    ///
    /// # Arguments
    /// * `entry` - User entry to associate with this receive operation
    /// * `addr` - Address of the receive buffer
    /// * `len` - Length of the receive buffer
    /// * `lkey` - Local key for the memory region
    ///
    /// # Example
    /// ```ignore
    /// qp.post_recv(entry, addr, len, lkey)?;
    /// qp.ring_rq_doorbell();
    /// ```
    #[inline]
    pub fn post_recv(&self, entry: RqEntry, addr: u64, len: u32, lkey: u32) -> io::Result<()> {
        let rq = self
            .rq
            .as_ref()
            .ok_or_else(|| io::Error::other("direct access not initialized"))?;
        if rq.available() == 0 {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "RQ full"));
        }
        let wqe_idx = rq.pi.get();
        unsafe {
            let wqe_ptr = rq.get_wqe_ptr(wqe_idx);
            DataSeg::write(wqe_ptr, len, lkey, addr);

            if rq.stride > DataSeg::SIZE as u32 {
                let sentinel_ptr = wqe_ptr.add(DataSeg::SIZE);
                let ptr32 = sentinel_ptr as *mut u32;
                std::ptr::write_volatile(ptr32, 0u32);
                std::ptr::write_volatile(ptr32.add(1), MLX5_INVALID_LKEY.to_be());
            }
        }
        let idx = (wqe_idx as usize) & ((rq.wqe_cnt - 1) as usize);
        rq.table[idx].set(Some(entry));
        rq.pi.set(rq.pi.get().wrapping_add(1));
        Ok(())
    }

    /// Get a BlueFlame batch builder for low-latency RQ WQE submission.
    ///
    /// Multiple receive WQEs can be accumulated in the BlueFlame buffer (up to 256 bytes)
    /// and submitted together via a single BlueFlame doorbell.
    ///
    /// RQ WQE size is 16 bytes (single DataSeg), so up to 16 WQEs can fit in a batch.
    ///
    /// # Example
    /// ```ignore
    /// let mut batch = qp.blueflame_rq_batch()?;
    /// batch.post(entry1, addr1, len1, lkey1)?;
    /// batch.post(entry2, addr2, len2, lkey2)?;
    /// batch.finish();
    /// ```
    ///
    /// # Errors
    /// Returns `BlueflameNotAvailable` if BlueFlame is not supported on this device.
    #[inline]
    pub fn blueflame_rq_batch(&self) -> Result<RqBlueflameWqeBatch<'_, RqEntry>, SubmissionError> {
        let rq = self.rq.as_ref().ok_or(SubmissionError::RqFull)?;
        let sq = self.sq.as_ref().ok_or(SubmissionError::BlueflameNotAvailable)?;
        if sq.bf_size == 0 {
            return Err(SubmissionError::BlueflameNotAvailable);
        }
        Ok(RqBlueflameWqeBatch::new(rq, sq.bf_reg, sq.bf_size, &sq.bf_offset))
    }
}

// =============================================================================
// QP Entry Points (sq_wqe - IB)
// =============================================================================

impl<SqEntry, RqEntry, Rq, OnSqComplete, OnRqComplete> RcQpInner<SqEntry, RqEntry, InfiniBand, OrderedWqeTable<SqEntry>, Rq, OnSqComplete, OnRqComplete> {
    /// Get a SQ WQE builder for InfiniBand transport.
    ///
    /// # Example
    /// ```ignore
    /// qp.sq_wqe()?
    ///     .write(TxFlags::empty(), remote_addr, rkey)
    ///     .sge(local_addr, len, lkey)
    ///     .finish_signaled(entry)?;
    /// qp.ring_sq_doorbell();
    /// ```
    #[inline]
    pub fn sq_wqe(&self) -> io::Result<SqWqeEntryPoint<'_, SqEntry, NoAv>> {
        let sq = self.sq()?;
        if sq.available() == 0 {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        Ok(SqWqeEntryPoint::new(sq, NoAv))
    }
}

impl<SqEntry, RqEntry, Rq, OnSqComplete, OnRqComplete> RcQpInner<SqEntry, RqEntry, RoCE, OrderedWqeTable<SqEntry>, Rq, OnSqComplete, OnRqComplete> {
    /// Get a SQ WQE builder for RoCE transport.
    ///
    /// # Example
    /// ```ignore
    /// qp.sq_wqe(&grh)?
    ///     .write(TxFlags::empty(), remote_addr, rkey)
    ///     .sge(local_addr, len, lkey)
    ///     .finish_signaled(entry)?;
    /// qp.ring_sq_doorbell();
    /// ```
    #[inline]
    pub fn sq_wqe<'a>(&'a self, grh: &'a GrhAttr) -> io::Result<SqWqeEntryPoint<'a, SqEntry, &'a GrhAttr>> {
        let sq = self.sq()?;
        if sq.available() == 0 {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "SQ full"));
        }
        Ok(SqWqeEntryPoint::new(sq, grh))
    }
}

// =============================================================================
// RQ BlueFlame Batch Builder
// =============================================================================

/// BlueFlame buffer size in bytes (256B doorbell window).
const BLUEFLAME_BUFFER_SIZE: usize = 256;

/// RQ WQE size in bytes (single DataSeg).
const RQ_WQE_SIZE: usize = DataSeg::SIZE;

/// BlueFlame WQE batch builder for RC RQ.
///
/// Allows posting multiple receive WQEs efficiently using BlueFlame MMIO.
/// WQEs are accumulated in an internal buffer and submitted together
/// via BlueFlame doorbell when `finish()` is called.
///
/// RQ WQE size is 16 bytes (single DataSeg), so up to 16 WQEs can fit in a batch.
///
/// # Example
/// ```ignore
/// let mut batch = qp.blueflame_rq_batch()?;
/// batch.post(entry1, addr1, len1, lkey1)?;
/// batch.post(entry2, addr2, len2, lkey2)?;
/// batch.finish();
/// ```
pub struct RqBlueflameWqeBatch<'a, Entry> {
    rq: &'a ReceiveQueueState<Entry>,
    buffer: [u8; BLUEFLAME_BUFFER_SIZE],
    offset: usize,
    wqe_count: u16,
    bf_reg: *mut u8,
    bf_size: u32,
    bf_offset: &'a Cell<u32>,
}

impl<'a, Entry> RqBlueflameWqeBatch<'a, Entry> {
    /// Create a new RQ BlueFlame batch builder.
    fn new(
        rq: &'a ReceiveQueueState<Entry>,
        bf_reg: *mut u8,
        bf_size: u32,
        bf_offset: &'a Cell<u32>,
    ) -> Self {
        Self {
            rq,
            buffer: [0u8; BLUEFLAME_BUFFER_SIZE],
            offset: 0,
            wqe_count: 0,
            bf_reg,
            bf_size,
            bf_offset,
        }
    }

    /// Post a receive WQE to the batch.
    ///
    /// # Arguments
    /// * `entry` - User entry to associate with this receive operation
    /// * `addr` - Address of the receive buffer
    /// * `len` - Length of the receive buffer
    /// * `lkey` - Local key for the memory region
    ///
    /// # Errors
    /// Returns `RqFull` if the receive queue doesn't have enough space.
    /// Returns `BlueflameOverflow` if the batch buffer is full.
    #[inline]
    pub fn post(&mut self, entry: Entry, addr: u64, len: u32, lkey: u32) -> Result<(), SubmissionError> {
        if self.rq.available() <= self.wqe_count as u32 {
            return Err(SubmissionError::RqFull);
        }
        if self.offset + RQ_WQE_SIZE > BLUEFLAME_BUFFER_SIZE {
            return Err(SubmissionError::BlueflameOverflow);
        }

        // Write WQE to RQ buffer
        let wqe_idx = self.rq.pi.get().wrapping_add(self.wqe_count);
        unsafe {
            let wqe_ptr = self.rq.get_wqe_ptr(wqe_idx);
            DataSeg::write(wqe_ptr, len, lkey, addr);

            if self.rq.stride > DataSeg::SIZE as u32 {
                let sentinel_ptr = wqe_ptr.add(DataSeg::SIZE);
                let ptr32 = sentinel_ptr as *mut u32;
                std::ptr::write_volatile(ptr32, 0u32);
                std::ptr::write_volatile(ptr32.add(1), MLX5_INVALID_LKEY.to_be());
            }

            // Also write to batch buffer for BlueFlame copy
            DataSeg::write(self.buffer.as_mut_ptr().add(self.offset), len, lkey, addr);
        }

        // Store entry in table
        let idx = (wqe_idx as usize) & ((self.rq.wqe_cnt - 1) as usize);
        self.rq.table[idx].set(Some(entry));

        self.offset += RQ_WQE_SIZE;
        self.wqe_count += 1;
        Ok(())
    }

    /// Finish the batch and submit all WQEs via BlueFlame doorbell.
    ///
    /// This method updates the producer index, writes to the doorbell record,
    /// and copies the WQEs to the BlueFlame register.
    #[inline]
    pub fn finish(self) {
        if self.wqe_count == 0 {
            return; // No WQEs to submit
        }

        // Advance RQ producer index
        self.rq.pi.set(self.rq.pi.get().wrapping_add(self.wqe_count));

        mmio_flush_writes!();

        // Update doorbell record
        unsafe {
            std::ptr::write_volatile(self.rq.dbrec, (self.rq.pi.get() as u32).to_be());
        }

        udma_to_device_barrier!();

        // Copy buffer to BlueFlame register
        if self.bf_size > 0 {
            let bf_offset = self.bf_offset.get();
            let bf = unsafe { self.bf_reg.add(bf_offset as usize) };

            let mut src = self.buffer.as_ptr();
            let mut dst = bf;
            let mut remaining = self.offset;
            while remaining > 0 {
                unsafe {
                    mlx5_bf_copy!(dst, src);
                    src = src.add(WQEBB_SIZE);
                    dst = dst.add(WQEBB_SIZE);
                }
                remaining = remaining.saturating_sub(WQEBB_SIZE);
            }

            mmio_flush_writes!();
            self.bf_offset.set(bf_offset ^ self.bf_size);
        }
    }
}

// =============================================================================
// SQ BlueFlame Batch Builder
// =============================================================================

/// BlueFlame WQE batch builder for RC QP.
///
/// Allows building multiple WQEs that fit within the BlueFlame buffer (256 bytes).
/// Each WQE is copied to a contiguous buffer and submitted via BlueFlame doorbell
/// when `finish()` is called.
///
/// # Example
/// ```ignore
/// let mut bf = qp.blueflame_sq_wqe()?;
/// bf.wqe()?.send(TxFlags::empty()).inline(&data).finish()?;
/// bf.wqe()?.send(TxFlags::empty()).inline(&data).finish()?;
/// bf.finish();
/// ```
pub struct BlueflameWqeBatch<'a, Entry> {
    sq: &'a SendQueueState<Entry, OrderedWqeTable<Entry>>,
    buffer: [u8; BLUEFLAME_BUFFER_SIZE],
    offset: usize,
}

impl<'a, Entry> BlueflameWqeBatch<'a, Entry> {
    /// Create a new BlueFlame batch builder.
    fn new(sq: &'a SendQueueState<Entry, OrderedWqeTable<Entry>>) -> Self {
        Self {
            sq,
            buffer: [0u8; BLUEFLAME_BUFFER_SIZE],
            offset: 0,
        }
    }

    /// Get a WQE builder for the next WQE in the batch.
    ///
    /// # Errors
    /// Returns `SqFull` if the send queue doesn't have enough space.
    #[inline]
    pub fn wqe(&mut self) -> Result<BlueflameWqeEntryPoint<'_, 'a, Entry>, SubmissionError> {
        if self.sq.available() == 0 {
            return Err(SubmissionError::SqFull);
        }
        Ok(BlueflameWqeEntryPoint {
            batch: self,
        })
    }

    /// Finish the batch and submit all WQEs via BlueFlame doorbell.
    ///
    /// This method copies the accumulated WQEs to the BlueFlame register
    /// and rings the doorbell.
    #[inline]
    pub fn finish(self) {
        if self.offset == 0 {
            return; // No WQEs to submit
        }

        mmio_flush_writes!();

        unsafe {
            // Update doorbell record with new PI
            std::ptr::write_volatile(
                self.sq.dbrec.add(1),
                (self.sq.pi.get() as u32).to_be(),
            );
        }

        udma_to_device_barrier!();

        if self.sq.bf_size > 0 {
            let bf_offset = self.sq.bf_offset.get();
            let bf = unsafe { self.sq.bf_reg.add(bf_offset as usize) };

            // Copy buffer to BlueFlame register in 64-byte chunks
            let mut src = self.buffer.as_ptr();
            let mut dst = bf;
            let mut remaining = self.offset;
            while remaining > 0 {
                unsafe {
                    mlx5_bf_copy!(dst, src);
                    src = src.add(WQEBB_SIZE);
                    dst = dst.add(WQEBB_SIZE);
                }
                remaining = remaining.saturating_sub(WQEBB_SIZE);
            }

            mmio_flush_writes!();
            self.sq.bf_offset.set(bf_offset ^ self.sq.bf_size);
        }
    }
}

/// BlueFlame WQE entry point for a single WQE within a batch.
#[must_use = "WQE builder must be finished"]
pub struct BlueflameWqeEntryPoint<'b, 'a, Entry> {
    batch: &'b mut BlueflameWqeBatch<'a, Entry>,
}

impl<'b, 'a, Entry> BlueflameWqeEntryPoint<'b, 'a, Entry> {
    /// Start building a SEND WQE.
    #[inline]
    pub fn send(self, flags: TxFlags) -> Result<BlueflameWqeBuilder<'b, 'a, Entry, NoData>, SubmissionError> {
        let mut core = BlueflameWqeCore::new(self.batch)?;
        core.write_ctrl(WqeOpcode::Send, flags, 0);
        Ok(BlueflameWqeBuilder { core, _data: PhantomData })
    }

    /// Start building a SEND with immediate data WQE.
    #[inline]
    pub fn send_imm(self, flags: TxFlags, imm: u32) -> Result<BlueflameWqeBuilder<'b, 'a, Entry, NoData>, SubmissionError> {
        let mut core = BlueflameWqeCore::new(self.batch)?;
        core.write_ctrl(WqeOpcode::SendImm, flags, imm);
        Ok(BlueflameWqeBuilder { core, _data: PhantomData })
    }

    /// Start building an RDMA WRITE WQE.
    #[inline]
    pub fn write(self, flags: TxFlags, remote_addr: u64, rkey: u32) -> Result<BlueflameWqeBuilder<'b, 'a, Entry, NoData>, SubmissionError> {
        let mut core = BlueflameWqeCore::new(self.batch)?;
        core.write_ctrl(WqeOpcode::RdmaWrite, flags, 0);
        core.write_rdma(remote_addr, rkey)?;
        Ok(BlueflameWqeBuilder { core, _data: PhantomData })
    }

    /// Start building an RDMA WRITE with immediate data WQE.
    #[inline]
    pub fn write_imm(self, flags: TxFlags, remote_addr: u64, rkey: u32, imm: u32) -> Result<BlueflameWqeBuilder<'b, 'a, Entry, NoData>, SubmissionError> {
        let mut core = BlueflameWqeCore::new(self.batch)?;
        core.write_ctrl(WqeOpcode::RdmaWriteImm, flags, imm);
        core.write_rdma(remote_addr, rkey)?;
        Ok(BlueflameWqeBuilder { core, _data: PhantomData })
    }
}

/// Internal core for BlueFlame WQE construction.
struct BlueflameWqeCore<'b, 'a, Entry> {
    batch: &'b mut BlueflameWqeBatch<'a, Entry>,
    wqe_start: usize,
    offset: usize,
    ds_count: u8,
    signaled: bool,
}

impl<'b, 'a, Entry> BlueflameWqeCore<'b, 'a, Entry> {
    #[inline]
    fn new(batch: &'b mut BlueflameWqeBatch<'a, Entry>) -> Result<Self, SubmissionError> {
        // Ensure we have at least space for control segment
        if batch.offset + CtrlSeg::SIZE > BLUEFLAME_BUFFER_SIZE {
            return Err(SubmissionError::BlueflameOverflow);
        }
        Ok(Self {
            wqe_start: batch.offset,
            offset: batch.offset,
            batch,
            ds_count: 0,
            signaled: false,
        })
    }

    #[inline]
    fn remaining(&self) -> usize {
        BLUEFLAME_BUFFER_SIZE - self.offset
    }

    #[inline]
    fn write_ctrl(&mut self, opcode: WqeOpcode, flags: TxFlags, imm: u32) {
        let wqe_idx = self.batch.sq.pi.get();
        let flags = WqeFlags::from_bits_truncate(flags.bits());
        unsafe {
            CtrlSeg::write(
                self.batch.buffer.as_mut_ptr().add(self.offset),
                0,
                opcode as u8,
                wqe_idx,
                self.batch.sq.sqn,
                0,
                flags.bits(),
                imm,
            );
        }
        self.offset += CtrlSeg::SIZE;
        self.ds_count = 1;
    }

    #[inline]
    fn write_rdma(&mut self, addr: u64, rkey: u32) -> Result<(), SubmissionError> {
        if self.remaining() < RdmaSeg::SIZE {
            return Err(SubmissionError::BlueflameOverflow);
        }
        unsafe {
            RdmaSeg::write(self.batch.buffer.as_mut_ptr().add(self.offset), addr, rkey);
        }
        self.offset += RdmaSeg::SIZE;
        self.ds_count += 1;
        Ok(())
    }

    #[inline]
    fn write_sge(&mut self, addr: u64, len: u32, lkey: u32) -> Result<(), SubmissionError> {
        if self.remaining() < DataSeg::SIZE {
            return Err(SubmissionError::BlueflameOverflow);
        }
        unsafe {
            DataSeg::write(self.batch.buffer.as_mut_ptr().add(self.offset), len, lkey, addr);
        }
        self.offset += DataSeg::SIZE;
        self.ds_count += 1;
        Ok(())
    }

    #[inline]
    fn write_inline(&mut self, data: &[u8]) -> Result<(), SubmissionError> {
        let padded_size = ((4 + data.len()) + 15) & !15;
        if self.remaining() < padded_size {
            return Err(SubmissionError::BlueflameOverflow);
        }
        unsafe {
            let ptr = self.batch.buffer.as_mut_ptr().add(self.offset);
            InlineHeader::write(ptr, data.len() as u32);
            std::ptr::copy_nonoverlapping(data.as_ptr(), ptr.add(4), data.len());
        }
        self.offset += padded_size;
        self.ds_count += (padded_size / 16) as u8;
        Ok(())
    }

    #[inline]
    fn finish_internal(self, entry: Option<Entry>) -> Result<(), SubmissionError> {
        // Update ds_count in control segment
        unsafe {
            CtrlSeg::update_ds_cnt(
                self.batch.buffer.as_mut_ptr().add(self.wqe_start),
                self.ds_count,
            );
            if self.signaled || entry.is_some() {
                CtrlSeg::set_completion_flag(
                    self.batch.buffer.as_mut_ptr().add(self.wqe_start),
                );
            }
        }

        let wqe_size = self.offset - self.wqe_start;
        let wqebb_cnt = calc_wqebb_cnt(wqe_size);

        // Store entry if signaled
        let wqe_idx = self.batch.sq.pi.get();
        if let Some(entry) = entry {
            let ci_delta = wqe_idx.wrapping_add(wqebb_cnt);
            self.batch.sq.table.store(wqe_idx, entry, ci_delta);
        }

        // Advance batch state
        self.batch.offset = self.offset;
        self.batch.sq.advance_pi(wqebb_cnt);

        Ok(())
    }
}

/// BlueFlame WQE builder with type-state for data segments.
#[must_use = "WQE builder must be finished"]
pub struct BlueflameWqeBuilder<'b, 'a, Entry, DataState> {
    core: BlueflameWqeCore<'b, 'a, Entry>,
    _data: PhantomData<DataState>,
}

impl<'b, 'a, Entry> BlueflameWqeBuilder<'b, 'a, Entry, NoData> {
    /// Add a scatter/gather entry.
    #[inline]
    pub fn sge(mut self, addr: u64, len: u32, lkey: u32) -> Result<BlueflameWqeBuilder<'b, 'a, Entry, HasData>, SubmissionError> {
        self.core.write_sge(addr, len, lkey)?;
        Ok(BlueflameWqeBuilder { core: self.core, _data: PhantomData })
    }

    /// Add inline data.
    #[inline]
    pub fn inline(mut self, data: &[u8]) -> Result<BlueflameWqeBuilder<'b, 'a, Entry, HasData>, SubmissionError> {
        self.core.write_inline(data)?;
        Ok(BlueflameWqeBuilder { core: self.core, _data: PhantomData })
    }
}

impl<'b, 'a, Entry> BlueflameWqeBuilder<'b, 'a, Entry, HasData> {
    /// Add another scatter/gather entry.
    #[inline]
    pub fn sge(mut self, addr: u64, len: u32, lkey: u32) -> Result<Self, SubmissionError> {
        self.core.write_sge(addr, len, lkey)?;
        Ok(self)
    }

    /// Add more inline data.
    #[inline]
    pub fn inline(mut self, data: &[u8]) -> Result<Self, SubmissionError> {
        self.core.write_inline(data)?;
        Ok(self)
    }

    /// Finish the WQE construction (unsignaled).
    #[inline]
    pub fn finish(self) -> Result<(), SubmissionError> {
        self.core.finish_internal(None)
    }

    /// Finish the WQE construction with completion signaling.
    #[inline]
    pub fn finish_signaled(self, entry: Entry) -> Result<(), SubmissionError> {
        self.core.finish_internal(Some(entry))
    }
}

impl<SqEntry, RqEntry, Rq, OnSqComplete, OnRqComplete> RcQpInner<SqEntry, RqEntry, InfiniBand, OrderedWqeTable<SqEntry>, Rq, OnSqComplete, OnRqComplete> {
    /// Get a BlueFlame batch builder for low-latency WQE submission.
    ///
    /// Multiple WQEs can be accumulated in the BlueFlame buffer (up to 256 bytes)
    /// and submitted together via a single BlueFlame doorbell.
    ///
    /// # Example
    /// ```ignore
    /// let mut bf = qp.blueflame_sq_wqe()?;
    /// bf.wqe()?.send(TxFlags::empty()).inline(&data).finish()?;
    /// bf.wqe()?.send(TxFlags::empty()).inline(&data).finish()?;
    /// bf.finish();
    /// ```
    ///
    /// # Errors
    /// Returns `BlueflameNotAvailable` if BlueFlame is not supported on this device.
    #[inline]
    pub fn blueflame_sq_wqe(&self) -> Result<BlueflameWqeBatch<'_, SqEntry>, SubmissionError> {
        let sq = self.sq.as_ref().ok_or(SubmissionError::SqFull)?;
        if sq.bf_size == 0 {
            return Err(SubmissionError::BlueflameNotAvailable);
        }
        Ok(BlueflameWqeBatch::new(sq))
    }
}
