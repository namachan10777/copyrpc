//! Wait-free MPSC using wCQ (Nikolaev, SPAA 2022 / wfring_cas2.h).
//!
//! Design:
//! - SCQD (dual rings): aq (allocated) + fq (free)
//! - Per-caller SPSC response rings (common::ResponseRing)
//!
//! Wait-free guarantee via fast-path-slow-path with announcement and helping:
//! - Fast path: CAS2-style enqueue/dequeue with patience limit
//! - Slow path: announce operation, other threads help via slow_inc protocol
//! - Each thread periodically helps one other thread's pending operation
//!
//! Entry encoding (128-bit pair):
//!   high 64: entry (cycle + flags), low 64: addon (for slow path tracking)
//! Counter values increment by 4 (bits 0-1 reserved for FIN/INC flags).
//! Cycle quantum = 4*n where n = 2*capacity.

use crate::common::{CachePadded, ResponseRing};
use crate::serial::Serial;
use crate::{CallError, MpscCaller, MpscChannel, MpscServer, ReplyToken};
use portable_atomic::{AtomicU128, Ordering as POrdering};
use std::cell::{Cell, UnsafeCell};
use std::mem::MaybeUninit;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

// ============================================================================
// Constants
// ============================================================================

const PATIENCE_ENQ: usize = 16;
const PATIENCE_DEQ: usize = 64;
const HELP_DELAY: usize = 16;

/// FIN flag in local head/tail counters (operation completed by helper)
const WFRING_FIN: u64 = 0x1;
/// Increment unit for local counters (low 1 bit is FIN flag)
const WFRING_INC: u64 = 0x2;

/// Sentinel: no active announcement
const EIDX_TERM: usize = 0;
/// Sentinel: dequeue operation announced
const EIDX_DEQ: usize = 1;

/// Sentinel for dequeue returning empty
const WF_EMPTY: u64 = u64::MAX;

// ============================================================================
// 128-bit pair helpers
// ============================================================================

#[inline(always)]
fn wf_pair(entry: u64, addon: u64) -> u128 {
    ((entry as u128) << 64) | (addon as u128)
}

#[inline(always)]
fn wf_entry(p: u128) -> u64 {
    (p >> 64) as u64
}

#[inline(always)]
fn wf_addon(p: u128) -> u64 {
    p as u64
}

#[inline(always)]
fn scmp(a: u64, b: u64) -> i64 {
    a.wrapping_sub(b) as i64
}

#[inline(always)]
fn threshold3(half: u64, n: u64) -> i64 {
    (half + n - 1) as i64
}

const LFRING_MIN: usize = 2; // log2(64/16) for 128-bit entries

#[inline(always)]
fn wf_raw_map(idx: u64, order: usize, n: u64) -> usize {
    let full_order = order;
    if full_order <= LFRING_MIN {
        return (idx & (n - 1)) as usize;
    }
    (((idx & (n - 1)) >> (full_order - LFRING_MIN)) | ((idx << LFRING_MIN) & (n - 1))) as usize
}

#[inline(always)]
fn wf_map(idx: u64, order: usize, n: u64) -> usize {
    wf_raw_map(idx, order + 1, n)
}

// ============================================================================
// Helpers for partial atomics on AtomicU128
// ============================================================================

/// fetch_add on the counter (high 64 bits) of an AtomicU128 pair.
fn fetch_add_entry(atom: &AtomicU128, delta: u64) -> u64 {
    loop {
        let old = atom.load(POrdering::Relaxed);
        let entry = wf_entry(old);
        let addon = wf_addon(old);
        let new = wf_pair(entry.wrapping_add(delta), addon);
        if atom
            .compare_exchange_weak(old, new, POrdering::AcqRel, POrdering::Acquire)
            .is_ok()
        {
            return entry;
        }
    }
}

/// Load only the counter (high 64 bits).
#[inline(always)]
fn load_entry(atom: &AtomicU128) -> u64 {
    wf_entry(atom.load(POrdering::Acquire))
}

/// CAS on entry portion, keeping addon. Returns Ok(old_entry) or Err(current_entry).
fn cas_entry_weak(atom: &AtomicU128, expected: &mut u64, desired: u64) -> bool {
    let full = atom.load(POrdering::Acquire);
    let entry = wf_entry(full);
    if entry != *expected {
        *expected = entry;
        return false;
    }
    let addon = wf_addon(full);
    let old_full = wf_pair(entry, addon);
    let new_full = wf_pair(desired, addon);
    match atom.compare_exchange_weak(old_full, new_full, POrdering::AcqRel, POrdering::Acquire) {
        Ok(_) => true,
        Err(current) => {
            *expected = wf_entry(current);
            false
        }
    }
}

/// fetch_or on entry portion only, returning old entry.
fn fetch_or_entry(atom: &AtomicU128, bits: u64) -> u64 {
    loop {
        let old = atom.load(POrdering::Acquire);
        let entry = wf_entry(old);
        let addon = wf_addon(old);
        let new = wf_pair(entry | bits, addon);
        if atom
            .compare_exchange_weak(old, new, POrdering::AcqRel, POrdering::Acquire)
            .is_ok()
        {
            return entry;
        }
    }
}

// ============================================================================
// Phase2 helping state
// ============================================================================

struct WfPhase2 {
    seq1: AtomicU64,
    /// Raw pointer to the local counter (AtomicU64) being helped.
    /// Safety: lifetime guaranteed by ring; access protected by seq protocol.
    local: UnsafeCell<*const AtomicU64>,
    cnt: UnsafeCell<u64>,
    seq2: AtomicU64,
}

// Safety: access to local/cnt is guarded by seq1/seq2 atomic protocol
unsafe impl Send for WfPhase2 {}
unsafe impl Sync for WfPhase2 {}

impl WfPhase2 {
    fn new() -> Self {
        Self {
            seq1: AtomicU64::new(1),
            local: UnsafeCell::new(std::ptr::null()),
            cnt: UnsafeCell::new(0),
            seq2: AtomicU64::new(0),
        }
    }
}

// ============================================================================
// Per-thread state
// ============================================================================

#[repr(C, align(128))]
struct WfState {
    /// Index of next state in circular list (immutable after setup)
    next_idx: AtomicUsize,
    /// Countdown to next help check (thread-local)
    next_check: Cell<usize>,
    /// Current thread index for round-robin helping (thread-local)
    curr_thread_idx: Cell<usize>,

    phase2: WfPhase2,

    seq1: AtomicU64,
    tail: AtomicU64,
    init_tail: Cell<u64>,
    head: AtomicU64,
    init_head: Cell<u64>,
    eidx: AtomicUsize,
    seq2: AtomicU64,
}

unsafe impl Send for WfState {}
unsafe impl Sync for WfState {}

impl WfState {
    fn new(self_idx: usize) -> Self {
        Self {
            next_idx: AtomicUsize::new(self_idx), // points to self initially
            next_check: Cell::new(HELP_DELAY),
            curr_thread_idx: Cell::new(self_idx),
            phase2: WfPhase2::new(),
            seq1: AtomicU64::new(1),
            tail: AtomicU64::new(0),
            init_tail: Cell::new(0),
            head: AtomicU64::new(0),
            init_head: Cell::new(0),
            eidx: AtomicUsize::new(EIDX_TERM),
            seq2: AtomicU64::new(0),
        }
    }
}

// ============================================================================
// Wait-free ring inner
// ============================================================================

struct WcqRingInner {
    /// Ring entries: 128-bit (entry, addon) pairs
    entries: Box<[AtomicU128]>,
    /// head: 128-bit (counter, phase2_addon)
    head: CachePadded<AtomicU128>,
    threshold: CachePadded<AtomicU64>,
    /// tail: 128-bit (counter, phase2_addon)
    tail: CachePadded<AtomicU128>,
    order: usize,
    half: u64,
    n: u64,
    /// Per-thread states
    states: Box<[WfState]>,
    num_threads: usize,
}

unsafe impl Send for WcqRingInner {}
unsafe impl Sync for WcqRingInner {}

impl WcqRingInner {
    fn new_empty(order: usize, num_threads: usize) -> Self {
        let half = 1u64 << order;
        let n = half * 2;

        let entries: Vec<AtomicU128> = (0..n)
            .map(|_| {
                AtomicU128::new(wf_pair(
                    u64::MAX,                         // (signed) -1
                    n.wrapping_neg().wrapping_sub(1), // -(n+1) as unsigned
                ))
            })
            .collect();

        let states: Vec<WfState> = (0..num_threads).map(WfState::new).collect();

        Self {
            entries: entries.into_boxed_slice(),
            head: CachePadded::new(AtomicU128::new(0)),
            threshold: CachePadded::new(AtomicU64::new(u64::MAX)), // -1
            tail: CachePadded::new(AtomicU128::new(0)),
            order,
            half,
            n,
            states: states.into_boxed_slice(),
            num_threads,
        }
    }

    fn new_full(order: usize, num_threads: usize) -> Self {
        let half = 1u64 << order;
        let n = half * 2;

        let entries: Vec<AtomicU128> = (0..n)
            .map(|_| AtomicU128::new(wf_pair(u64::MAX, n.wrapping_neg().wrapping_sub(1))))
            .collect();

        let states: Vec<WfState> = (0..num_threads).map(WfState::new).collect();

        let ring = Self {
            entries: entries.into_boxed_slice(),
            head: CachePadded::new(AtomicU128::new(0)),
            threshold: CachePadded::new(AtomicU64::new(threshold3(half, n) as u64)),
            tail: CachePadded::new(AtomicU128::new(wf_pair(half << 2, 0))),
            order,
            half,
            n,
            states: states.into_boxed_slice(),
            num_threads,
        };

        // Pre-fill indices 0..half-1
        for i in 0..half {
            let tidx = wf_map(i, order, n);
            let raw_idx = wf_raw_map(i, order, half) as u64;
            ring.entries[tidx].store(wf_pair(2 * n + n + raw_idx, u64::MAX), POrdering::Relaxed);
        }

        ring
    }

    /// Link all thread states into a circular list.
    fn link_states(&self) {
        if self.num_threads <= 1 {
            return;
        }
        for i in 0..self.num_threads {
            let next = (i + 1) % self.num_threads;
            self.states[i].next_idx.store(next, Ordering::Relaxed);
        }
    }

    // ------------------------------------------------------------------
    // Phase2 helping (load_global_help_phase2)
    // ------------------------------------------------------------------

    fn load_global_help_phase2(&self, global: &AtomicU128, mut gp: u128) -> u64 {
        loop {
            let addon = wf_addon(gp);
            if addon == 0 {
                break;
            }
            // addon is a phase2 state index + 1 (0 = no phase2)
            let state_idx = (addon - 1) as usize;
            if state_idx < self.num_threads {
                let phase2 = &self.states[state_idx].phase2;
                let seq = phase2.seq2.load(Ordering::Acquire);
                let local_ptr = unsafe { *phase2.local.get() };
                let cnt = unsafe { *phase2.cnt.get() };
                if phase2.seq1.load(Ordering::Acquire) == seq && !local_ptr.is_null() {
                    let local = unsafe { &*local_ptr };
                    let cnt_inc = cnt + WFRING_INC;
                    let _ =
                        local.compare_exchange(cnt_inc, cnt, Ordering::AcqRel, Ordering::Acquire);
                }
            }
            let entry = wf_entry(gp);
            let new_gp = wf_pair(entry, 0);
            match global.compare_exchange_weak(gp, new_gp, POrdering::AcqRel, POrdering::Acquire) {
                Ok(_) => return entry,
                Err(current) => gp = current,
            }
        }
        wf_entry(gp)
    }

    // ------------------------------------------------------------------
    // slow_inc: coordinated counter increment with helping
    // ------------------------------------------------------------------

    fn slow_inc(
        &self,
        global: &AtomicU128,
        local: &AtomicU64,
        prev: &mut u64,
        decrement_threshold: bool,
        phase2: &WfPhase2,
        state_idx: usize,
    ) -> bool {
        let mut gp = global.load(POrdering::Acquire);

        loop {
            if local.load(Ordering::Acquire) & WFRING_FIN != 0 {
                return false;
            }

            let cnt = self.load_global_help_phase2(global, gp);

            let new_val = cnt + WFRING_INC;
            match local.compare_exchange(*prev, new_val, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => {
                    *prev = new_val;
                }
                Err(current) => {
                    if current & WFRING_FIN != 0 {
                        return false;
                    }
                    if current & WFRING_INC == 0 {
                        // Another helper already updated; use current value
                        return true;
                    }
                    *prev = current;
                    // cnt = current - INC (revert the increment)
                    let cnt_adjusted = current - WFRING_INC;
                    // Continue with adjusted cnt
                    let seq = phase2.seq1.load(Ordering::Relaxed) + 1;
                    phase2.seq1.store(seq, Ordering::Relaxed);
                    unsafe {
                        *phase2.local.get() = local as *const AtomicU64;
                        *phase2.cnt.get() = cnt_adjusted;
                    }
                    phase2.seq2.store(seq, Ordering::Release);

                    gp = wf_pair(cnt_adjusted, 0);
                    let new_gp = wf_pair(cnt_adjusted + 1, (state_idx + 1) as u64);
                    match global.compare_exchange_weak(
                        gp,
                        new_gp,
                        POrdering::AcqRel,
                        POrdering::Acquire,
                    ) {
                        Ok(_) => {
                            if decrement_threshold {
                                self.threshold.fetch_sub(1, Ordering::AcqRel);
                            }
                            let cnt_inc = cnt_adjusted + WFRING_INC;
                            let _ = local.compare_exchange(
                                cnt_inc,
                                cnt_adjusted,
                                Ordering::AcqRel,
                                Ordering::Acquire,
                            );
                            let _ = global.compare_exchange_weak(
                                new_gp,
                                wf_pair(cnt_adjusted + 1, 0),
                                POrdering::AcqRel,
                                POrdering::Acquire,
                            );
                            *prev = cnt_adjusted;
                            return true;
                        }
                        Err(current_gp) => {
                            gp = current_gp;
                            continue;
                        }
                    }
                }
            }

            // Publish phase2 for this state
            let seq = phase2.seq1.load(Ordering::Relaxed) + 1;
            phase2.seq1.store(seq, Ordering::Relaxed);
            unsafe {
                *phase2.local.get() = local as *const AtomicU64;
                *phase2.cnt.get() = cnt;
            }
            phase2.seq2.store(seq, Ordering::Release);

            gp = wf_pair(cnt, 0);
            let new_gp = wf_pair(cnt + 1, (state_idx + 1) as u64);
            match global.compare_exchange_weak(gp, new_gp, POrdering::AcqRel, POrdering::Acquire) {
                Ok(_) => {
                    if decrement_threshold {
                        self.threshold.fetch_sub(1, Ordering::AcqRel);
                    }
                    let cnt_inc = cnt + WFRING_INC;
                    let _ =
                        local.compare_exchange(cnt_inc, cnt, Ordering::AcqRel, Ordering::Acquire);
                    let _ = global.compare_exchange_weak(
                        new_gp,
                        wf_pair(cnt + 1, 0),
                        POrdering::AcqRel,
                        POrdering::Acquire,
                    );
                    *prev = cnt;
                    return true;
                }
                Err(current_gp) => gp = current_gp,
            }
        }
    }

    // ------------------------------------------------------------------
    // Enqueue
    // ------------------------------------------------------------------

    fn do_enqueue_slow(
        &self,
        eidx: u64,
        seq: u64,
        mut tail: u64,
        nonempty: bool,
        state_idx: usize,
    ) {
        let n = self.n;
        let half = self.half;
        let four_n_mask = 4 * n - 1;
        let state = &self.states[state_idx];

        while self.slow_inc(
            &self.tail,
            &state.tail,
            &mut tail,
            false,
            &state.phase2,
            state_idx,
        ) {
            if state.seq1.load(Ordering::Acquire) != seq {
                break;
            }
            let tcycle = tail | four_n_mask;
            let tidx = wf_map(tail >> 2, self.order, n);
            let mut p = self.entries[tidx].load(POrdering::Acquire);

            loop {
                let entry = wf_entry(p);
                let note = wf_addon(p);
                let ecycle = entry | four_n_mask;

                if scmp(ecycle, tcycle) < 0 && scmp(note, tcycle) < 0 {
                    if ((entry | 0x1) == ecycle)
                        || (((entry | 0x1) == (ecycle ^ n))
                            && scmp(load_entry(&self.head), tail) <= 0)
                    {
                        if let Err(current) = self.entries[tidx].compare_exchange_weak(
                            p,
                            wf_pair(tcycle ^ eidx ^ n, note),
                            POrdering::AcqRel,
                            POrdering::Acquire,
                        ) {
                            p = current;
                            continue;
                        }

                        let mut entry_val = tcycle ^ eidx;
                        let fin_tail = tail + 0x1;
                        if state
                            .tail
                            .compare_exchange(tail, fin_tail, Ordering::AcqRel, Ordering::Acquire)
                            .is_ok()
                        {
                            // Finalize: toggle n bit
                            let desired = entry_val ^ n;
                            let _ = cas_entry_weak(&self.entries[tidx], &mut entry_val, desired);
                        }

                        if !nonempty {
                            let thresh = threshold3(half, n) as u64;
                            if self.threshold.load(Ordering::Relaxed) != thresh {
                                self.threshold.store(thresh, Ordering::Relaxed);
                            }
                        }
                        return;
                    } else if (entry | (2 * n + n)) == tcycle {
                        // Already produced
                        return;
                    } else {
                        // Skip entry: update addon to tcycle
                        match self.entries[tidx].compare_exchange_weak(
                            p,
                            wf_pair(entry, tcycle),
                            POrdering::AcqRel,
                            POrdering::Acquire,
                        ) {
                            Ok(_) => break,
                            Err(current) => {
                                p = current;
                                continue;
                            }
                        }
                    }
                }
                break;
            }
        }
    }

    fn enqueue_slow(&self, eidx: u64, tail: u64, nonempty: bool, state_idx: usize) {
        let state = &self.states[state_idx];
        let seq = state.seq1.load(Ordering::Relaxed);

        // Publish announcement
        state.tail.store(tail, Ordering::Relaxed);
        state.init_tail.set(tail);
        state.eidx.store(eidx as usize, Ordering::Relaxed);
        state.seq2.store(seq, Ordering::Release);

        self.do_enqueue_slow(eidx, seq, tail, nonempty, state_idx);

        // Clear announcement
        state.seq1.store(seq + 1, Ordering::Release);
        state.eidx.store(EIDX_TERM, Ordering::Release);
    }

    fn enqueue_help_thread(&self, nonempty: bool, helped_idx: usize) {
        let state = &self.states[helped_idx];
        let seq = state.seq2.load(Ordering::Acquire);
        let eidx = state.eidx.load(Ordering::Acquire);
        let tail = state.init_tail.get();

        if eidx <= EIDX_DEQ || state.seq1.load(Ordering::Acquire) != seq {
            return;
        }

        self.do_enqueue_slow(eidx as u64, seq, tail, nonempty, helped_idx);
    }

    pub fn enqueue(&self, tid: usize, index: u64, nonempty: bool) {
        let n = self.n;
        let half = self.half;
        let four_n_mask = 4 * n - 1;
        let state = &self.states[tid];

        let eidx = index ^ (n - 1);

        // Periodic helping
        let nc = state.next_check.get().wrapping_sub(1);
        state.next_check.set(nc);
        if nc == 0 {
            self.help(tid, nonempty);
        }

        let mut patience = PATIENCE_ENQ;
        loop {
            let tail = fetch_add_entry(&self.tail, 4);
            let tcycle = tail | four_n_mask;
            let tidx = wf_map(tail >> 2, self.order, n);
            let mut p = self.entries[tidx].load(POrdering::Acquire);
            let mut entry = wf_entry(p);

            loop {
                let ecycle = entry | four_n_mask;
                if scmp(ecycle, tcycle) < 0
                    && (((entry | 0x1) == ecycle)
                        || (((entry | 0x1) == (ecycle ^ (2 * n)))
                            && scmp(load_entry(&self.head), tail) <= 0))
                {
                    match self.entries[tidx].compare_exchange_weak(
                        p,
                        wf_pair(tcycle ^ eidx, wf_addon(p)),
                        POrdering::AcqRel,
                        POrdering::Acquire,
                    ) {
                        Ok(_) => {
                            if !nonempty {
                                let thresh = threshold3(half, n) as u64;
                                if self.threshold.load(Ordering::Relaxed) != thresh {
                                    self.threshold.store(thresh, Ordering::Relaxed);
                                }
                            }
                            return;
                        }
                        Err(current) => {
                            p = current;
                            entry = wf_entry(p);
                            continue;
                        }
                    }
                }
                break;
            }

            patience -= 1;
            if patience == 0 {
                self.enqueue_slow(eidx, tail, nonempty, tid);
                return;
            }
        }
    }

    // ------------------------------------------------------------------
    // Dequeue
    // ------------------------------------------------------------------

    fn catchup(&self, mut tail: u64, mut head: u64) {
        loop {
            match cas_entry_weak(&self.tail, &mut tail, head) {
                true => break,
                false => {
                    head = load_entry(&self.head);
                    tail = load_entry(&self.tail);
                    if scmp(tail, head) >= 0 {
                        break;
                    }
                }
            }
        }
    }

    fn lookup(&self, state_idx: usize, head_val: u64) {
        let state = &self.states[state_idx];
        let mut curr_idx = state.next_idx.load(Ordering::Relaxed);

        while curr_idx != state_idx {
            let curr = &self.states[curr_idx];
            let curr_tail = curr.tail.load(Ordering::Acquire);
            if (curr_tail & !0x3u64) == head_val {
                let _ = curr.tail.compare_exchange(
                    head_val,
                    head_val ^ 0x1,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                );
                return;
            }
            curr_idx = curr.next_idx.load(Ordering::Relaxed);
        }
    }

    fn do_dequeue_slow(&self, seq: u64, mut head: u64, nonempty: bool, state_idx: usize) {
        let n = self.n;
        let four_n_mask = 4 * n - 1;
        let state = &self.states[state_idx];

        while self.slow_inc(
            &self.head,
            &state.head,
            &mut head,
            !nonempty,
            &state.phase2,
            state_idx,
        ) {
            if state.seq1.load(Ordering::Acquire) != seq {
                break;
            }
            let hcycle = head | four_n_mask;
            let hidx = wf_map(head >> 2, self.order, n);
            let mut p = self.entries[hidx].load(POrdering::Acquire);

            loop {
                let entry = wf_entry(p);
                let note = wf_addon(p);
                let ecycle = entry | four_n_mask;

                if ecycle == hcycle && (entry & (n - 1)) != (n - 2) {
                    // Found item — finalize via CAS on local head
                    let _ = state.head.compare_exchange(
                        head,
                        head ^ 0x1,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    );
                    return;
                }

                if (entry | (2 * n) | 0x1) != ecycle {
                    if scmp(ecycle, hcycle) < 0 && scmp(note, hcycle) < 0 {
                        // Block enqueue at this entry
                        match self.entries[hidx].compare_exchange_weak(
                            p,
                            wf_pair(entry, hcycle),
                            POrdering::AcqRel,
                            POrdering::Acquire,
                        ) {
                            Err(current) => {
                                p = current;
                                continue;
                            }
                            Ok(_) => {
                                p = wf_pair(entry, hcycle);
                            }
                        }
                    }
                    let entry_new = entry & !(2 * n);
                    if entry == entry_new {
                        break;
                    }
                    match self.entries[hidx].compare_exchange_weak(
                        p,
                        wf_pair(entry_new, note),
                        POrdering::AcqRel,
                        POrdering::Acquire,
                    ) {
                        Ok(_) => break,
                        Err(current) => {
                            p = current;
                            continue;
                        }
                    }
                } else {
                    let entry_new = hcycle ^ ((!entry) & (2 * n)) ^ 0x1;
                    if scmp(ecycle, hcycle) >= 0 {
                        break;
                    }
                    match self.entries[hidx].compare_exchange_weak(
                        p,
                        wf_pair(entry_new, note),
                        POrdering::AcqRel,
                        POrdering::Acquire,
                    ) {
                        Ok(_) => break,
                        Err(current) => {
                            p = current;
                            continue;
                        }
                    }
                }
            }

            if !nonempty {
                let tail = load_entry(&self.tail);
                if scmp(tail, head + 4) <= 0 {
                    self.catchup(tail, head + 4);
                }
                if (self.threshold.load(Ordering::Relaxed) as i64) < 0 {
                    let _ = state.head.compare_exchange(
                        head,
                        head + WFRING_FIN,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    );
                }
            }
        }
    }

    fn dequeue_slow(&self, head: u64, nonempty: bool, state_idx: usize) -> u64 {
        let n = self.n;
        let four_n_mask = 4 * n - 1;
        let state = &self.states[state_idx];
        let seq = state.seq1.load(Ordering::Relaxed);

        // Publish announcement
        state.head.store(head, Ordering::Relaxed);
        state.init_head.set(head);
        state.eidx.store(EIDX_DEQ, Ordering::Relaxed);
        state.seq2.store(seq, Ordering::Release);

        self.do_dequeue_slow(seq, head, nonempty, state_idx);

        // Clear announcement
        state.seq1.store(seq + 1, Ordering::Release);
        state.eidx.store(EIDX_TERM, Ordering::Release);

        // Consume the element
        let head = state.head.load(Ordering::Acquire);
        let hcycle = head | four_n_mask;
        let hidx = wf_map(head >> 2, self.order, n);
        let entry = load_entry(&self.entries[hidx]);

        if nonempty || (entry | (2 * n + n)) == hcycle {
            if entry & n == 0 {
                self.lookup(state_idx, head);
            }
            fetch_or_entry(&self.entries[hidx], 2 * n - 1);
            return entry & (n - 1);
        }

        WF_EMPTY
    }

    fn dequeue_help_thread(&self, nonempty: bool, helped_idx: usize) {
        let state = &self.states[helped_idx];
        let seq = state.seq2.load(Ordering::Acquire);
        let eidx = state.eidx.load(Ordering::Acquire);
        let head = state.init_head.get();

        if eidx != EIDX_DEQ || state.seq1.load(Ordering::Acquire) != seq {
            return;
        }

        self.do_dequeue_slow(seq, head, nonempty, helped_idx);
    }

    pub fn dequeue(&self, tid: usize, nonempty: bool) -> Option<u64> {
        let n = self.n;
        let four_n_mask = 4 * n - 1;
        let state = &self.states[tid];

        if !nonempty && (self.threshold.load(Ordering::Relaxed) as i64) < 0 {
            return None;
        }

        // Periodic helping
        let nc = state.next_check.get().wrapping_sub(1);
        state.next_check.set(nc);
        if nc == 0 {
            self.help(tid, nonempty);
        }

        let mut patience = PATIENCE_DEQ;
        loop {
            let head = fetch_add_entry(&self.head, 4);
            let hcycle = head | four_n_mask;
            let hidx = wf_map(head >> 2, self.order, n);
            let mut entry = load_entry(&self.entries[hidx]);

            loop {
                let ecycle = entry | four_n_mask;
                if ecycle == hcycle {
                    // Help finalizing if needed
                    if entry & n == 0 {
                        self.lookup(tid, head);
                    }
                    fetch_or_entry(&self.entries[hidx], 2 * n - 1);
                    return Some(entry & (n - 1));
                }

                let entry_new;
                if (entry | (2 * n) | 0x1) != ecycle {
                    entry_new = entry & !(2 * n);
                    if entry == entry_new {
                        break;
                    }
                } else {
                    entry_new = hcycle ^ ((!entry) & (2 * n)) ^ 0x1;
                }

                if scmp(ecycle, hcycle) >= 0 {
                    break;
                }

                match cas_entry_weak(&self.entries[hidx], &mut entry, entry_new) {
                    true => break,
                    false => continue,
                }
            }

            if !nonempty {
                let tail = load_entry(&self.tail);
                if scmp(tail, head + 4) <= 0 {
                    self.catchup(tail, head + 4);
                    self.threshold.fetch_sub(1, Ordering::AcqRel);
                    return None;
                }
                if (self.threshold.fetch_sub(1, Ordering::AcqRel) as i64) <= 0 {
                    return None;
                }
            }

            patience -= 1;
            if patience == 0 {
                let result = self.dequeue_slow(head, nonempty, tid);
                return if result == WF_EMPTY {
                    None
                } else {
                    Some(result)
                };
            }
        }
    }

    // ------------------------------------------------------------------
    // Helping
    // ------------------------------------------------------------------

    fn help(&self, tid: usize, nonempty: bool) {
        let state = &self.states[tid];
        let curr_idx = state.curr_thread_idx.get();

        if curr_idx != tid {
            let eidx = self.states[curr_idx].eidx.load(Ordering::Acquire);
            if eidx != EIDX_TERM {
                if eidx != EIDX_DEQ {
                    self.enqueue_help_thread(nonempty, curr_idx);
                } else {
                    self.dequeue_help_thread(nonempty, curr_idx);
                }
            }
            let next = self.states[curr_idx].next_idx.load(Ordering::Relaxed);
            state.curr_thread_idx.set(next);
        } else {
            let next = state.next_idx.load(Ordering::Relaxed);
            state.curr_thread_idx.set(next);
        }

        state.next_check.set(HELP_DELAY);
    }
}

// ============================================================================
// Data Slot
// ============================================================================

#[repr(C, align(64))]
struct DataSlot<T> {
    caller_id: UnsafeCell<usize>,
    data: UnsafeCell<MaybeUninit<T>>,
}

// ============================================================================
// WCQ Request Ring (SCQD: aq + fq + data_slots)
// ============================================================================

struct WcqRequestRing<T> {
    aq: WcqRingInner,
    fq: WcqRingInner,
    data_slots: Box<[DataSlot<T>]>,
    sender_count: AtomicUsize,
    rx_alive: AtomicBool,
}

unsafe impl<T: Send> Send for WcqRequestRing<T> {}
unsafe impl<T: Send> Sync for WcqRequestRing<T> {}

impl<T> WcqRequestRing<T> {
    fn new(capacity: usize, num_senders: usize) -> Self {
        assert!(capacity.is_power_of_two());
        let order = capacity.trailing_zeros() as usize;
        let num_threads = num_senders + 1; // callers + server

        let aq = WcqRingInner::new_empty(order, num_threads);
        let fq = WcqRingInner::new_full(order, num_threads);
        aq.link_states();
        fq.link_states();

        let data_slots: Vec<DataSlot<T>> = (0..capacity)
            .map(|_| DataSlot {
                caller_id: UnsafeCell::new(0),
                data: UnsafeCell::new(MaybeUninit::uninit()),
            })
            .collect();

        Self {
            aq,
            fq,
            data_slots: data_slots.into_boxed_slice(),
            sender_count: AtomicUsize::new(num_senders),
            rx_alive: AtomicBool::new(true),
        }
    }

    /// caller_tid: 0..num_senders-1 for callers
    fn try_push(&self, caller_tid: usize, caller_id: usize, data: T) -> Result<u64, CallError<T>> {
        if !self.rx_alive.load(Ordering::Acquire) {
            return Err(CallError::Disconnected(data));
        }

        // Dequeue a free slot from fq
        let slot_idx = match self.fq.dequeue(caller_tid, false) {
            Some(idx) => idx as usize,
            None => return Err(CallError::Full(data)),
        };

        unsafe {
            *self.data_slots[slot_idx].caller_id.get() = caller_id;
            (*self.data_slots[slot_idx].data.get()).write(data);
        }

        // Enqueue into aq
        self.aq.enqueue(caller_tid, slot_idx as u64, false);
        Ok(slot_idx as u64)
    }

    /// server_tid: num_senders (last thread index)
    fn try_pop(&self, server_tid: usize) -> Option<(usize, T, u64)> {
        let slot_idx = self.aq.dequeue(server_tid, false)? as usize;

        let caller_id = unsafe { *self.data_slots[slot_idx].caller_id.get() };
        let data = unsafe { (*self.data_slots[slot_idx].data.get()).assume_init_read() };

        // Return slot to fq
        self.fq.enqueue(server_tid, slot_idx as u64, false);
        Some((caller_id, data, slot_idx as u64))
    }

    fn available(&self) -> usize {
        let head = load_entry(&self.aq.head);
        let tail = load_entry(&self.aq.tail);
        ((tail.wrapping_sub(head)) >> 2) as usize
    }

    fn disconnect_tx(&self) {
        self.sender_count.fetch_sub(1, Ordering::AcqRel);
    }

    fn disconnect_rx(&self) {
        self.rx_alive.store(false, Ordering::Release);
    }
}

// ============================================================================
// WcqCaller
// ============================================================================

pub struct WcqCaller<Req, Resp> {
    caller_id: usize,
    caller_tid: usize,
    req_ring: Arc<WcqRequestRing<Req>>,
    resp_ring: Arc<ResponseRing<Resp>>,
    send_count: u64,
    inflight: usize,
    max_inflight: usize,
    disconnected: bool,
}

impl<Req: Serial + Send, Resp: Serial + Send> MpscCaller<Req, Resp> for WcqCaller<Req, Resp> {
    fn call(&mut self, req: Req) -> Result<u64, CallError<Req>> {
        if self.inflight >= self.max_inflight {
            return Err(CallError::InflightExceeded(req));
        }

        self.req_ring
            .try_push(self.caller_tid, self.caller_id, req)?;

        let token = self.send_count;
        self.send_count += 1;
        self.inflight += 1;
        Ok(token)
    }

    fn sync(&mut self) {}

    fn try_recv_response(&mut self) -> Option<(u64, Resp)> {
        let result = self.resp_ring.try_pop();
        if result.is_some() {
            self.inflight = self.inflight.saturating_sub(1);
        }
        result
    }

    fn pending_count(&self) -> usize {
        self.inflight
    }
}

impl<Req, Resp> Drop for WcqCaller<Req, Resp> {
    fn drop(&mut self) {
        if !self.disconnected {
            self.disconnected = true;
            self.req_ring.disconnect_tx();
            self.resp_ring.disconnect_rx();
        }
    }
}

// ============================================================================
// WcqServer
// ============================================================================

pub struct WcqServer<Req, Resp> {
    server_tid: usize,
    req_ring: Arc<WcqRequestRing<Req>>,
    resp_rings: Vec<Arc<ResponseRing<Resp>>>,
    recv_counts: Vec<u64>,
    disconnected: bool,
}

pub struct WcqRecvRef<'a, Req, Resp> {
    server: &'a mut WcqServer<Req, Resp>,
    caller_id: usize,
    data: Req,
    slot_token: u64,
}

impl<'a, Req: Serial, Resp: Serial> WcqRecvRef<'a, Req, Resp> {
    pub fn data(&self) -> &Req {
        &self.data
    }
    pub fn caller_id(&self) -> usize {
        self.caller_id
    }
    pub fn reply(self, resp: Resp) {
        let token = ReplyToken {
            caller_id: self.caller_id,
            slot_token: self.slot_token,
        };
        self.server.reply(token, resp);
    }
    pub fn into_token(self) -> ReplyToken {
        ReplyToken {
            caller_id: self.caller_id,
            slot_token: self.slot_token,
        }
    }
}

impl<Req: Serial, Resp: Serial> crate::MpscRecvRef<Req> for WcqRecvRef<'_, Req, Resp> {
    fn caller_id(&self) -> usize {
        self.caller_id
    }
    fn data(&self) -> Req {
        self.data
    }
    fn into_token(self) -> crate::ReplyToken {
        crate::ReplyToken {
            caller_id: self.caller_id,
            slot_token: self.slot_token,
        }
    }
}

impl<Req: Serial, Resp: Serial> MpscServer<Req, Resp> for WcqServer<Req, Resp> {
    type RecvRef<'a>
        = WcqRecvRef<'a, Req, Resp>
    where
        Self: 'a;

    fn poll(&mut self) -> u32 {
        self.req_ring.available() as u32
    }

    fn try_recv(&mut self) -> Option<Self::RecvRef<'_>> {
        let (caller_id, data, _ring_token) = self.req_ring.try_pop(self.server_tid)?;
        let slot_token = self.recv_counts[caller_id];
        self.recv_counts[caller_id] += 1;
        Some(WcqRecvRef {
            server: self,
            caller_id,
            data,
            slot_token,
        })
    }

    fn reply(&mut self, token: ReplyToken, resp: Resp) {
        let resp_ring = &self.resp_rings[token.caller_id];
        while !resp_ring.try_push(token.slot_token, resp) {
            if !resp_ring.is_rx_alive() {
                return; // caller disconnected, discard response
            }
            std::hint::spin_loop();
        }
    }
}

impl<Req, Resp> Drop for WcqServer<Req, Resp> {
    fn drop(&mut self) {
        if !self.disconnected {
            self.disconnected = true;
            self.req_ring.disconnect_rx();
        }
    }
}

// ============================================================================
// WcqMpsc Factory
// ============================================================================

pub struct WcqMpsc;

impl MpscChannel for WcqMpsc {
    type Caller<Req: Serial + Send, Resp: Serial + Send> = WcqCaller<Req, Resp>;
    type Server<Req: Serial + Send, Resp: Serial + Send> = WcqServer<Req, Resp>;

    fn create<Req: Serial + Send, Resp: Serial + Send>(
        max_callers: usize,
        ring_depth: usize,
        max_inflight: usize,
    ) -> (Vec<Self::Caller<Req, Resp>>, Self::Server<Req, Resp>) {
        assert!(ring_depth.is_power_of_two());
        assert!(max_callers > 0);

        let req_ring = Arc::new(WcqRequestRing::new(ring_depth, max_callers));
        let resp_rings: Vec<_> = (0..max_callers)
            .map(|_| Arc::new(ResponseRing::new(ring_depth)))
            .collect();

        let callers = resp_rings
            .iter()
            .enumerate()
            .map(|(id, resp_ring)| WcqCaller {
                caller_id: id,
                caller_tid: id,
                req_ring: Arc::clone(&req_ring),
                resp_ring: Arc::clone(resp_ring),
                send_count: 0,
                inflight: 0,
                max_inflight,
                disconnected: false,
            })
            .collect();

        let server = WcqServer {
            server_tid: max_callers, // last thread index
            req_ring,
            resp_rings,
            recv_counts: vec![0u64; max_callers],
            disconnected: false,
        };

        (callers, server)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_single_caller() {
        let (mut callers, mut server) = WcqMpsc::create::<u64, u64>(1, 16, 16);
        let caller = &mut callers[0];

        let tok = caller.call(42).unwrap();
        assert_eq!(tok, 0);
        assert_eq!(caller.pending_count(), 1);

        assert!(server.poll() >= 1);
        let recv = server.try_recv().unwrap();
        assert_eq!(*recv.data(), 42);
        assert_eq!(recv.caller_id(), 0);
        recv.reply(84);

        let (resp_tok, resp) = caller.try_recv_response().unwrap();
        assert_eq!(resp_tok, tok);
        assert_eq!(resp, 84);
        assert_eq!(caller.pending_count(), 0);
    }

    #[test]
    fn multi_caller_concurrent() {
        let (mut callers, mut server) = WcqMpsc::create::<u64, u64>(4, 16, 8);

        for (i, caller) in callers.iter_mut().enumerate() {
            let val = (i as u64) * 100;
            caller.call(val).unwrap();
            caller.call(val + 1).unwrap();
        }

        let mut tokens = Vec::new();
        for _ in 0..8 {
            let recv = server.try_recv().unwrap();
            let caller_id = recv.caller_id();
            let val = *recv.data();
            let token = recv.into_token();
            tokens.push((caller_id, val, token));
        }

        for (_caller_id, val, token) in tokens {
            server.reply(token, val * 2);
        }

        for caller in callers.iter_mut() {
            let mut count = 0;
            while let Some((_, _resp)) = caller.try_recv_response() {
                count += 1;
            }
            assert_eq!(count, 2);
        }
    }

    #[test]
    fn ring_wraparound() {
        let (mut callers, mut server) = WcqMpsc::create::<u64, u64>(1, 8, 8);
        let caller = &mut callers[0];

        for round in 0u64..3 {
            for i in 0u64..8 {
                caller.call(round * 100 + i).unwrap();
            }
            for _ in 0u64..8 {
                let recv = server.try_recv().unwrap();
                let val = *recv.data();
                recv.reply(val * 2);
            }
            for _ in 0..8 {
                caller.try_recv_response().unwrap();
            }
        }
    }

    #[test]
    fn inflight_limit() {
        let (mut callers, _server) = WcqMpsc::create::<u64, u64>(1, 16, 4);
        let caller = &mut callers[0];
        for i in 0..4 {
            assert!(caller.call(i).is_ok());
        }
        match caller.call(100) {
            Err(CallError::InflightExceeded(val)) => assert_eq!(val, 100),
            _ => panic!("Expected InflightExceeded"),
        }
    }

    #[test]
    fn zero_copy_recv_ref() {
        let (mut callers, mut server) = WcqMpsc::create::<u64, u64>(1, 16, 16);
        callers[0].call(999).unwrap();
        let recv = server.try_recv().unwrap();
        assert_eq!(*recv.data(), 999);
        recv.reply(1000);
        let (_, resp) = callers[0].try_recv_response().unwrap();
        assert_eq!(resp, 1000);
    }

    #[test]
    fn deferred_reply() {
        let (mut callers, mut server) = WcqMpsc::create::<u64, u64>(2, 16, 8);
        callers[0].call(10).unwrap();
        callers[1].call(20).unwrap();

        let recv1 = server.try_recv().unwrap();
        let data1 = *recv1.data();
        let tok1 = recv1.into_token();
        let recv2 = server.try_recv().unwrap();
        let data2 = *recv2.data();
        let tok2 = recv2.into_token();

        server.reply(tok2, data2 * 2);
        server.reply(tok1, data1 * 2);

        let (_, resp0) = callers[0].try_recv_response().unwrap();
        let (_, resp1) = callers[1].try_recv_response().unwrap();
        assert_eq!(resp0, 20);
        assert_eq!(resp1, 40);
    }

    #[test]
    fn full_ring() {
        let (mut callers, _server) = WcqMpsc::create::<u64, u64>(1, 4, 4);
        let caller = &mut callers[0];
        for i in 0..4 {
            assert!(caller.call(i).is_ok());
        }
        match caller.call(100) {
            Err(CallError::InflightExceeded(val)) => assert_eq!(val, 100),
            _ => panic!("Expected InflightExceeded"),
        }
    }

    #[test]
    fn disconnect_detection() {
        let (mut callers, server) = WcqMpsc::create::<u64, u64>(1, 16, 16);
        drop(server);
        match callers[0].call(42) {
            Err(CallError::Disconnected(val)) => assert_eq!(val, 42),
            _ => panic!("Expected Disconnected"),
        }
    }

    #[test]
    fn stress_multi_caller() {
        let (callers, mut server) = WcqMpsc::create::<u64, u64>(8, 256, 32);
        const MSGS_PER_CALLER: usize = 100;

        let handles: Vec<_> = callers
            .into_iter()
            .enumerate()
            .map(|(id, mut caller)| {
                std::thread::spawn(move || {
                    let mut sent = 0;
                    while sent < MSGS_PER_CALLER {
                        let val = (id as u64) * 1000 + sent as u64;
                        match caller.call(val) {
                            Ok(_) => sent += 1,
                            Err(CallError::InflightExceeded(_) | CallError::Full(_)) => {
                                while caller.try_recv_response().is_some() {}
                                std::thread::yield_now();
                            }
                            Err(_) => std::thread::yield_now(),
                        }
                    }
                    while caller.pending_count() > 0 {
                        while caller.try_recv_response().is_some() {}
                        std::thread::yield_now();
                    }
                    id
                })
            })
            .collect();

        let mut processed = 0;
        let expected = 8 * MSGS_PER_CALLER;
        while processed < expected {
            if let Some(recv) = server.try_recv() {
                let val = *recv.data();
                recv.reply(val + 1);
                processed += 1;
            }
        }

        for handle in handles {
            handle.join().unwrap();
        }
        assert_eq!(processed, expected);
    }
}
