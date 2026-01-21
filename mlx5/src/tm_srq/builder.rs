//! TM-SRQ WQE Builders.
//!
//! This module contains the WQE builder types for TM-SRQ operations:
//! - `CmdQpWqeBuilder` - Builder for Command QP tag operations
//! - `RqWqeBuilder` - Builder for RQ unordered receive operations
//! - Trait-based API implementations for flexible WQE construction

use std::io;

use crate::wqe::{
    CTRL_SEG_SIZE, DATA_SEG_SIZE, HasData, Init, NeedsTmSeg, OrderedWqeTable, TmCmdWqeBuilder,
    TM_SEG_SIZE, TmTagAddWqeBuilder, TmTagDelWqeBuilder, WQEBB_SIZE, WqeFlags, WqeHandle, WqeOpcode,
    write_ctrl_seg, write_data_seg, write_tm_seg_add, write_tm_seg_del,
};

use super::{CmdQpState, TmSrqState};

// =============================================================================
// Command QP WQE Builder
// =============================================================================

/// WQE builder for Command QP tag operations.
///
/// Uses type-state pattern to enforce valid WQE construction at compile time:
/// - `Init` → `ctrl_tag_matching()` → `NeedsTmSeg`
/// - `NeedsTmSeg` → `tag_add()` or `tag_del()` → `HasData`
/// - `HasData` → `finish()` or `finish_with_blueflame()`
pub struct CmdQpWqeBuilder<'a, CmdEntry, CmdTableType, State> {
    pub(super) cmd_qp: &'a CmdQpState<CmdEntry, CmdTableType>,
    pub(super) wqe_ptr: *mut u8,
    pub(super) wqe_idx: u16,
    pub(super) offset: usize,
    pub(super) ds_count: u8,
    pub(super) entry: CmdEntry,
    /// Whether the TM operation requests a CQE on completion.
    pub(super) signaled: bool,
    pub(super) _state: std::marker::PhantomData<State>,
}

// =============================================================================
// Command QP WQE Builder - Init State
// =============================================================================

impl<'a, CmdEntry, CmdTableType> CmdQpWqeBuilder<'a, CmdEntry, CmdTableType, Init> {
    /// Write the control segment for tag matching operation.
    ///
    /// This transitions the builder to `NeedsTmSeg` state, requiring
    /// `tag_add()` or `tag_del()` before finishing.
    ///
    /// For TM operations, CQ_UPDATE (0x08) is automatically set.
    pub fn ctrl_tag_matching(
        mut self,
        opmod: u8,
    ) -> CmdQpWqeBuilder<'a, CmdEntry, CmdTableType, NeedsTmSeg> {
        // MLX5_WQE_CTRL_CQ_UPDATE = 0x08 - required for TM operations
        unsafe {
            write_ctrl_seg(
                self.wqe_ptr,
                opmod,
                WqeOpcode::TagMatching as u8,
                self.wqe_idx,
                self.cmd_qp.qpn,
                0,
                WqeFlags::COMPLETION,
                0,
            );
        }
        self.offset = 16; // CTRL_SEG_SIZE
        self.ds_count = 1;
        CmdQpWqeBuilder {
            cmd_qp: self.cmd_qp,
            wqe_ptr: self.wqe_ptr,
            wqe_idx: self.wqe_idx,
            offset: self.offset,
            ds_count: self.ds_count,
            entry: self.entry,
            signaled: self.signaled,
            _state: std::marker::PhantomData,
        }
    }
}

// =============================================================================
// Command QP WQE Builder - NeedsTmSeg State
// =============================================================================

impl<'a, CmdEntry, CmdTableType> CmdQpWqeBuilder<'a, CmdEntry, CmdTableType, NeedsTmSeg> {
    /// Add TAG_ADD segment with data segment.
    ///
    /// Transitions to `HasData` state, allowing `finish()` or `finish_with_blueflame()`.
    ///
    /// # Arguments
    /// * `index` - Tag index in the SRQ tag list
    /// * `tag` - Tag value to match against incoming messages
    /// * `addr` - Buffer address for received data
    /// * `len` - Buffer length
    /// * `lkey` - Local key for the memory region
    /// * `signaled` - Whether to request a CQE on completion
    pub fn tag_add(
        mut self,
        index: u16,
        tag: u64,
        addr: u64,
        len: u32,
        lkey: u32,
        signaled: bool,
    ) -> CmdQpWqeBuilder<'a, CmdEntry, CmdTableType, HasData> {
        unsafe {
            write_tm_seg_add(
                self.wqe_ptr.add(self.offset),
                index,
                index, // sw_cnt - same as index, per rdma-core
                tag,
                !0u64, // mask: all bits must match
                signaled,
            );
        }
        self.offset += TM_SEG_SIZE;
        self.ds_count += 2; // TmSeg = 32 bytes = 2 DS

        unsafe {
            write_data_seg(self.wqe_ptr.add(self.offset), len, lkey, addr);
        }
        self.offset += DATA_SEG_SIZE;
        self.ds_count += 1;

        CmdQpWqeBuilder {
            cmd_qp: self.cmd_qp,
            wqe_ptr: self.wqe_ptr,
            wqe_idx: self.wqe_idx,
            offset: self.offset,
            ds_count: self.ds_count,
            entry: self.entry,
            signaled,
            _state: std::marker::PhantomData,
        }
    }

    /// Add TAG_DEL segment.
    ///
    /// Transitions to `HasData` state, allowing `finish()` or `finish_with_blueflame()`.
    ///
    /// # Arguments
    /// * `index` - Tag index to remove
    /// * `signaled` - Whether to request a CQE on completion
    pub fn tag_del(
        mut self,
        index: u16,
        signaled: bool,
    ) -> CmdQpWqeBuilder<'a, CmdEntry, CmdTableType, HasData> {
        unsafe {
            write_tm_seg_del(self.wqe_ptr.add(self.offset), index, signaled);
        }
        self.offset += TM_SEG_SIZE;
        self.ds_count += 2; // TmSeg = 32 bytes = 2 DS

        CmdQpWqeBuilder {
            cmd_qp: self.cmd_qp,
            wqe_ptr: self.wqe_ptr,
            wqe_idx: self.wqe_idx,
            offset: self.offset,
            ds_count: self.ds_count,
            entry: self.entry,
            signaled,
            _state: std::marker::PhantomData,
        }
    }
}

// =============================================================================
// Command QP WQE Builder - HasData State (finish methods)
// =============================================================================

impl<'a, CmdEntry> CmdQpWqeBuilder<'a, CmdEntry, OrderedWqeTable<CmdEntry>, HasData> {
    /// Finish the WQE construction (sparse mode).
    ///
    /// The entry is stored only if signaled is true.
    /// The doorbell will be issued when `ring_cmd_doorbell()` is called.
    pub fn finish(self) -> WqeHandle {
        // Extract all fields before any moves
        let wqe_idx = self.wqe_idx;
        let wqe_ptr = self.wqe_ptr;
        let ds_count = self.ds_count;
        let offset = self.offset;
        let cmd_qp = self.cmd_qp;
        let signaled = self.signaled;
        let entry = self.entry;

        // For sparse tables, only store if signaled
        if signaled {
            // ci_delta is the accumulated PI value at completion
            let ci_delta = cmd_qp.pi.get().wrapping_add(1);
            cmd_qp.table.store(wqe_idx, entry, ci_delta);
        }

        // Inline finish_internal logic
        unsafe {
            std::ptr::write_volatile(wqe_ptr.add(7), ds_count);
        }
        cmd_qp.advance_pi(1);
        cmd_qp.set_last_wqe(wqe_ptr, offset);

        WqeHandle {
            wqe_idx,
            size: offset,
        }
    }

    /// Finish the WQE construction and immediately ring BlueFlame doorbell.
    pub fn finish_with_blueflame(self) -> WqeHandle {
        // Extract all fields before any moves
        let wqe_idx = self.wqe_idx;
        let wqe_ptr = self.wqe_ptr;
        let ds_count = self.ds_count;
        let offset = self.offset;
        let cmd_qp = self.cmd_qp;
        let signaled = self.signaled;
        let entry = self.entry;

        // For sparse tables, only store if signaled
        if signaled {
            let ci_delta = cmd_qp.pi.get().wrapping_add(1);
            cmd_qp.table.store(wqe_idx, entry, ci_delta);
        }

        // Inline finish_internal_with_blueflame logic
        unsafe {
            std::ptr::write_volatile(wqe_ptr.add(7), ds_count);
        }
        cmd_qp.advance_pi(1);
        cmd_qp.ring_blueflame(wqe_ptr);

        WqeHandle {
            wqe_idx,
            size: offset,
        }
    }
}

// =============================================================================
// RQ WQE Builder
// =============================================================================

/// WQE builder for RQ unordered receive operations.
///
/// Supports incremental WQEBB allocation for multi-segment WQEs.
pub struct RqWqeBuilder<'a, RecvEntry> {
    pub(super) srq_state: &'a TmSrqState<RecvEntry>,
    pub(super) wqe_ptr: *mut u8,
    pub(super) wqe_idx: u32,
    pub(super) wqebb_start: u16,
    pub(super) wqebb_count: u16,
    pub(super) offset: usize,
    pub(super) entry: RecvEntry,
}

impl<'a, RecvEntry> RqWqeBuilder<'a, RecvEntry> {
    /// Add a data segment to the WQE.
    ///
    /// Automatically extends WQEBB allocation if needed.
    pub fn data_seg(mut self, addr: u64, len: u32, lkey: u32) -> io::Result<Self> {
        let new_offset = self.offset + DATA_SEG_SIZE;
        let required_wqebbs = new_offset.div_ceil(WQEBB_SIZE) as u16;

        // Extend allocation if we need more WQEBBs
        while self.wqebb_count < required_wqebbs {
            if !self
                .srq_state
                .table
                .try_extend(self.wqebb_start, self.wqebb_count)
            {
                // Extension failed, rollback
                self.srq_state
                    .table
                    .release(self.wqebb_start, self.wqebb_count);
                return Err(io::Error::new(io::ErrorKind::WouldBlock, "RQ full"));
            }
            self.wqebb_count += 1;
        }

        unsafe {
            write_data_seg(self.wqe_ptr.add(self.offset), len, lkey, addr);
        }
        self.offset = new_offset;
        Ok(self)
    }

    /// Finish the WQE construction and post it to the RQ.
    ///
    /// The doorbell should be rung via `ring_srq_doorbell()` after posting.
    pub fn finish(self) {
        self.srq_state.table.store(
            self.wqe_idx as u16,
            self.wqebb_start,
            self.wqebb_count,
            self.entry,
        );
        self.srq_state
            .head
            .set(self.srq_state.head.get().wrapping_add(1));
    }
}

// =============================================================================
// Trait-Based WQE Builder API
// =============================================================================

/// TM-SRQ Command Queue entry point builder (internal).
#[must_use = "WQE builder must be finished"]
pub(crate) struct TmCmdEntryPointImpl<'a, CmdEntry> {
    pub(super) cmd_qp: &'a CmdQpState<CmdEntry, OrderedWqeTable<CmdEntry>>,
    pub(super) wqe_ptr: *mut u8,
    pub(super) wqe_idx: u16,
    pub(super) entry: CmdEntry,
}

impl<'a, CmdEntry> TmCmdWqeBuilder<'a, CmdEntry> for TmCmdEntryPointImpl<'a, CmdEntry> {
    #[inline]
    fn tag_add(self, index: u16, tag: u64) -> impl TmTagAddWqeBuilder<'a, CmdEntry> + 'a {
        // Write control segment with TM opcode
        unsafe {
            write_ctrl_seg(
                self.wqe_ptr,
                0,
                WqeOpcode::TagMatching as u8,
                self.wqe_idx,
                self.cmd_qp.qpn,
                0,
                WqeFlags::COMPLETION,
                0,
            );
        }

        TmTagAddWqeBuilderImpl {
            cmd_qp: self.cmd_qp,
            wqe_ptr: self.wqe_ptr,
            wqe_idx: self.wqe_idx,
            entry: self.entry,
            index,
            tag,
            has_data: false,
        }
    }

    #[inline]
    fn tag_del(self, index: u16) -> impl TmTagDelWqeBuilder<'a, CmdEntry> + 'a {
        // Write control segment with TM opcode
        unsafe {
            write_ctrl_seg(
                self.wqe_ptr,
                0,
                WqeOpcode::TagMatching as u8,
                self.wqe_idx,
                self.cmd_qp.qpn,
                0,
                WqeFlags::COMPLETION,
                0,
            );

            // Write TM segment for deletion
            write_tm_seg_del(self.wqe_ptr.add(CTRL_SEG_SIZE), index, false);
        }

        TmTagDelWqeBuilderImpl {
            cmd_qp: self.cmd_qp,
            wqe_ptr: self.wqe_ptr,
            wqe_idx: self.wqe_idx,
            entry: self.entry,
            index,
        }
    }
}

/// TM Tag Add WQE builder (internal).
#[must_use = "WQE builder must be finished"]
pub(crate) struct TmTagAddWqeBuilderImpl<'a, CmdEntry> {
    cmd_qp: &'a CmdQpState<CmdEntry, OrderedWqeTable<CmdEntry>>,
    wqe_ptr: *mut u8,
    wqe_idx: u16,
    #[allow(dead_code)]
    entry: CmdEntry, // Preserved for future signaled-at-construction pattern
    index: u16,
    tag: u64,
    has_data: bool,
}

impl<'a, CmdEntry> TmTagAddWqeBuilder<'a, CmdEntry> for TmTagAddWqeBuilderImpl<'a, CmdEntry> {
    #[inline]
    fn sge(mut self, addr: u64, len: u32, lkey: u32) -> Self {
        if !self.has_data {
            let offset = CTRL_SEG_SIZE;
            unsafe {
                // Write TM segment for add (not signaled initially)
                write_tm_seg_add(
                    self.wqe_ptr.add(offset),
                    self.index,
                    self.index,
                    self.tag,
                    !0u64,
                    false,
                );

                // Write data segment
                write_data_seg(self.wqe_ptr.add(offset + TM_SEG_SIZE), len, lkey, addr);
            }
            self.has_data = true;
        }
        // Note: TM Tag Add only supports one data segment
        self
    }

    #[inline]
    fn inline(self, _data: &[u8]) -> Self {
        // TM Tag Add doesn't support inline data
        self
    }

    #[inline]
    fn finish_unsignaled(self) -> WqeHandle {
        let ds_count = 1 + 2 + 1; // Ctrl(1) + TmSeg(2) + DataSeg(1)
        let offset = CTRL_SEG_SIZE + TM_SEG_SIZE + DATA_SEG_SIZE;

        unsafe {
            std::ptr::write_volatile(self.wqe_ptr.add(7), ds_count);
        }
        self.cmd_qp.advance_pi(1);
        self.cmd_qp.set_last_wqe(self.wqe_ptr, offset);

        WqeHandle {
            wqe_idx: self.wqe_idx,
            size: offset,
        }
    }

    #[inline]
    fn finish_signaled(self, entry: CmdEntry) -> WqeHandle {
        let ds_count = 1 + 2 + 1; // Ctrl(1) + TmSeg(2) + DataSeg(1)
        let offset = CTRL_SEG_SIZE + TM_SEG_SIZE + DATA_SEG_SIZE;

        // Re-write TM segment with signaled flag
        unsafe {
            write_tm_seg_add(
                self.wqe_ptr.add(CTRL_SEG_SIZE),
                self.index,
                self.index,
                self.tag,
                !0u64,
                true,
            );
        }
        let ci_delta = self.cmd_qp.pi.get().wrapping_add(1);
        self.cmd_qp.table.store(self.wqe_idx, entry, ci_delta);

        unsafe {
            std::ptr::write_volatile(self.wqe_ptr.add(7), ds_count);
        }
        self.cmd_qp.advance_pi(1);
        self.cmd_qp.set_last_wqe(self.wqe_ptr, offset);

        WqeHandle {
            wqe_idx: self.wqe_idx,
            size: offset,
        }
    }
}

/// TM Tag Del WQE builder (internal).
#[must_use = "WQE builder must be finished"]
pub(crate) struct TmTagDelWqeBuilderImpl<'a, CmdEntry> {
    cmd_qp: &'a CmdQpState<CmdEntry, OrderedWqeTable<CmdEntry>>,
    wqe_ptr: *mut u8,
    wqe_idx: u16,
    #[allow(dead_code)]
    entry: CmdEntry, // Preserved for future signaled-at-construction pattern
    index: u16,
}

impl<'a, CmdEntry> TmTagDelWqeBuilder<'a, CmdEntry> for TmTagDelWqeBuilderImpl<'a, CmdEntry> {
    #[inline]
    fn finish_unsignaled(self) -> WqeHandle {
        let ds_count = 1 + 2; // Ctrl(1) + TmSeg(2)
        let offset = CTRL_SEG_SIZE + TM_SEG_SIZE;

        unsafe {
            std::ptr::write_volatile(self.wqe_ptr.add(7), ds_count);
        }
        self.cmd_qp.advance_pi(1);
        self.cmd_qp.set_last_wqe(self.wqe_ptr, offset);

        WqeHandle {
            wqe_idx: self.wqe_idx,
            size: offset,
        }
    }

    #[inline]
    fn finish_signaled(self, entry: CmdEntry) -> WqeHandle {
        let ds_count = 1 + 2; // Ctrl(1) + TmSeg(2)
        let offset = CTRL_SEG_SIZE + TM_SEG_SIZE;

        // Re-write TM segment with signaled flag
        unsafe {
            write_tm_seg_del(self.wqe_ptr.add(CTRL_SEG_SIZE), self.index, true);
        }
        let ci_delta = self.cmd_qp.pi.get().wrapping_add(1);
        self.cmd_qp.table.store(self.wqe_idx, entry, ci_delta);

        unsafe {
            std::ptr::write_volatile(self.wqe_ptr.add(7), ds_count);
        }
        self.cmd_qp.advance_pi(1);
        self.cmd_qp.set_last_wqe(self.wqe_ptr, offset);

        WqeHandle {
            wqe_idx: self.wqe_idx,
            size: offset,
        }
    }
}
