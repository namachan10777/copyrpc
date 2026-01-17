//! WQE construction macros for zero-overhead WQE building.
//!
//! These macros generate inline `write_volatile` calls at compile time,
//! eliminating function call overhead while maintaining flexible segment ordering.
//!
//! # Example
//!
//! ```ignore
//! // Basic RDMA WRITE IMM (48B = 1 WQEBB)
//! wqe_write!(ptr, sqn, wqe_idx,
//!     ctrl { opcode: RdmaWriteImm, fm_ce_se: 0x08, imm: 0x1234 },
//!     rdma { remote_addr: addr, rkey: key },
//!     sge { addr: local, len: 64, lkey: lk },
//! );
//!
//! // Multiple SGEs (64B = 1 WQEBB)
//! wqe_write!(ptr, sqn, wqe_idx,
//!     ctrl { opcode: RdmaWriteImm, fm_ce_se: 0x08, imm: 0 },
//!     rdma { remote_addr: addr, rkey: key },
//!     sge { addr: a1, len: 32, lkey: lk },
//!     sge { addr: a2, len: 32, lkey: lk },
//! );
//! ```

/// WQE construction macro with flexible segment ordering.
///
/// Generates `write_volatile` calls in segment order with automatic ds_cnt calculation.
///
/// # Safety
/// - `$ptr` must point to a valid WQEBB buffer (64-byte aligned)
/// - The buffer must be large enough for all segments
///
/// # Segments
/// - `ctrl { opcode: _, fm_ce_se: _ [, imm: _] }` - Control segment (16B)
/// - `rdma { remote_addr: _, rkey: _ }` - RDMA segment (16B)
/// - `sge { addr: _, len: _, lkey: _ }` - Data segment (16B)
/// - `inline_fixed16 { data: _ }` - Fixed 16B inline data (16B total)
/// - `atomic_cas { swap: _, compare: _ }` - Atomic CAS segment (16B)
/// - `atomic_fa { add_value: _ }` - Atomic Fetch-Add segment (16B)
#[macro_export]
macro_rules! wqe_write {
    // Entry point: count segments, then emit
    ($ptr:expr, $sqn:expr, $wqe_idx:expr, $($segs:tt)*) => {{
        let __ptr: *mut u8 = $ptr;
        let __sqn: u32 = $sqn;
        let __wqe_idx: u16 = $wqe_idx;
        let __ds_cnt: u8 = $crate::wqe_write!(@count $($segs)*);
        let mut __offset: usize = 0;
        $crate::wqe_write!(@emit __ptr, __sqn, __wqe_idx, __ds_cnt, __offset, $($segs)*)
    }};

    // =========================================================================
    // Segment counting (tt counting)
    // =========================================================================

    (@count) => { 0u8 };

    (@count ctrl { $($fields:tt)* } $(, $($rest:tt)*)?) => {
        1u8 + $crate::wqe_write!(@count $($($rest)*)?)
    };

    (@count rdma { $($fields:tt)* } $(, $($rest:tt)*)?) => {
        1u8 + $crate::wqe_write!(@count $($($rest)*)?)
    };

    (@count sge { $($fields:tt)* } $(, $($rest:tt)*)?) => {
        1u8 + $crate::wqe_write!(@count $($($rest)*)?)
    };

    (@count inline_fixed16 { $($fields:tt)* } $(, $($rest:tt)*)?) => {
        1u8 + $crate::wqe_write!(@count $($($rest)*)?)
    };

    (@count atomic_cas { $($fields:tt)* } $(, $($rest:tt)*)?) => {
        1u8 + $crate::wqe_write!(@count $($($rest)*)?)
    };

    (@count atomic_fa { $($fields:tt)* } $(, $($rest:tt)*)?) => {
        1u8 + $crate::wqe_write!(@count $($($rest)*)?)
    };

    // av (Address Vector for DC QP) - 48 bytes = 3 * 16-byte segments
    (@count av { $($fields:tt)* } $(, $($rest:tt)*)?) => {
        3u8 + $crate::wqe_write!(@count $($($rest)*)?)
    };

    // =========================================================================
    // Segment emission
    // =========================================================================

    // Base case: no more segments
    (@emit $ptr:ident, $sqn:ident, $wqe_idx:ident, $ds_cnt:ident, $offset:ident,) => {
        // Return total size for caller
        $offset
    };

    // ctrl with imm
    (@emit $ptr:ident, $sqn:ident, $wqe_idx:ident, $ds_cnt:ident, $offset:ident,
     ctrl { opcode: $op:expr, fm_ce_se: $fm:expr, imm: $imm:expr } $(, $($rest:tt)*)?) => {{
        #[allow(unused_unsafe)]
        unsafe {
            let p = $ptr.add($offset) as *mut u64;

            // DWORD0: [opmod:8][wqe_idx:16][opcode:8] in big-endian
            // DWORD1: [qpn:24][ds_cnt:8] in big-endian
            let opcode = $op as u8;
            let d0 = ((($wqe_idx as u32) << 8) | (opcode as u32)).to_be();
            let d1 = (($sqn << 8) | ($ds_cnt as u32)).to_be();
            std::ptr::write_volatile(p, (d0 as u64) | ((d1 as u64) << 32));

            // DWORD2: [sig:8][stream_hi:8][stream_lo:8][fm_ce_se:8]
            // DWORD3: imm (32-bit)
            let d2 = ($fm as u32).to_be();
            let d3 = ($imm as u32).to_be();
            std::ptr::write_volatile(p.add(1), (d2 as u64) | ((d3 as u64) << 32));
        }
        $offset += 16;
        $crate::wqe_write!(@emit $ptr, $sqn, $wqe_idx, $ds_cnt, $offset, $($($rest)*)?)
    }};

    // ctrl without imm
    (@emit $ptr:ident, $sqn:ident, $wqe_idx:ident, $ds_cnt:ident, $offset:ident,
     ctrl { opcode: $op:expr, fm_ce_se: $fm:expr } $(, $($rest:tt)*)?) => {{
        #[allow(unused_unsafe)]
        unsafe {
            let p = $ptr.add($offset) as *mut u64;

            let opcode = $op as u8;
            let d0 = ((($wqe_idx as u32) << 8) | (opcode as u32)).to_be();
            let d1 = (($sqn << 8) | ($ds_cnt as u32)).to_be();
            std::ptr::write_volatile(p, (d0 as u64) | ((d1 as u64) << 32));

            // DWORD2: fm_ce_se only, DWORD3: 0 (no imm)
            let d2 = ($fm as u32).to_be();
            std::ptr::write_volatile(p.add(1), d2 as u64);
        }
        $offset += 16;
        $crate::wqe_write!(@emit $ptr, $sqn, $wqe_idx, $ds_cnt, $offset, $($($rest)*)?)
    }};

    // rdma
    (@emit $ptr:ident, $sqn:ident, $wqe_idx:ident, $ds_cnt:ident, $offset:ident,
     rdma { remote_addr: $raddr:expr, rkey: $rkey:expr } $(, $($rest:tt)*)?) => {{
        #[allow(unused_unsafe)]
        unsafe {
            let p = $ptr.add($offset) as *mut u64;

            // DWORD0-1: remote_addr (8B, big-endian)
            std::ptr::write_volatile(p, ($raddr as u64).to_be());

            // DWORD2: rkey, DWORD3: reserved
            // [rkey:32][reserved:32] in big-endian
            std::ptr::write_volatile(p.add(1), (($rkey as u64) << 32).to_be());
        }
        $offset += 16;
        $crate::wqe_write!(@emit $ptr, $sqn, $wqe_idx, $ds_cnt, $offset, $($($rest)*)?)
    }};

    // sge (data segment)
    (@emit $ptr:ident, $sqn:ident, $wqe_idx:ident, $ds_cnt:ident, $offset:ident,
     sge { addr: $addr:expr, len: $len:expr, lkey: $lkey:expr } $(, $($rest:tt)*)?) => {{
        #[allow(unused_unsafe)]
        unsafe {
            let p = $ptr.add($offset) as *mut u64;

            // DWORD0: byte_count, DWORD1: lkey
            // [byte_count:32 BE][lkey:32 BE]
            let len_lkey = (($len as u64) << 32) | ($lkey as u64);
            std::ptr::write_volatile(p, len_lkey.to_be());

            // DWORD2-3: addr (8B, big-endian)
            std::ptr::write_volatile(p.add(1), ($addr as u64).to_be());
        }
        $offset += 16;
        $crate::wqe_write!(@emit $ptr, $sqn, $wqe_idx, $ds_cnt, $offset, $($($rest)*)?)
    }};

    // inline_fixed16 - fixed 16-byte inline segment
    // Layout: [inline_hdr:4][data:12] or padded to 16B boundary
    (@emit $ptr:ident, $sqn:ident, $wqe_idx:ident, $ds_cnt:ident, $offset:ident,
     inline_fixed16 { data: $data:expr } $(, $($rest:tt)*)?) => {{
        #[allow(unused_unsafe)]
        unsafe {
            let p = $ptr.add($offset) as *mut u32;
            let data_slice: &[u8] = $data;
            let byte_count = data_slice.len() as u32;
            debug_assert!(byte_count <= 12, "inline_fixed16 data must be <= 12 bytes");

            // Inline header: [1:1][0:31-byte_count] = 0x8000_0000 | byte_count
            let hdr = 0x8000_0000u32 | byte_count;
            std::ptr::write_volatile(p, hdr.to_be());

            // Copy data (up to 12 bytes after 4-byte header)
            let data_ptr = $ptr.add($offset + 4);
            std::ptr::copy_nonoverlapping(data_slice.as_ptr(), data_ptr, byte_count as usize);

            // Zero-pad remaining bytes to 16B boundary
            let pad_len = 12 - byte_count as usize;
            if pad_len > 0 {
                std::ptr::write_bytes(data_ptr.add(byte_count as usize), 0, pad_len);
            }
        }
        $offset += 16;
        $crate::wqe_write!(@emit $ptr, $sqn, $wqe_idx, $ds_cnt, $offset, $($($rest)*)?)
    }};

    // atomic_cas
    (@emit $ptr:ident, $sqn:ident, $wqe_idx:ident, $ds_cnt:ident, $offset:ident,
     atomic_cas { swap: $swap:expr, compare: $cmp:expr } $(, $($rest:tt)*)?) => {{
        #[allow(unused_unsafe)]
        unsafe {
            let p = $ptr.add($offset) as *mut u64;

            // DWORD0-1: swap value (8B, big-endian)
            std::ptr::write_volatile(p, ($swap as u64).to_be());

            // DWORD2-3: compare value (8B, big-endian)
            std::ptr::write_volatile(p.add(1), ($cmp as u64).to_be());
        }
        $offset += 16;
        $crate::wqe_write!(@emit $ptr, $sqn, $wqe_idx, $ds_cnt, $offset, $($($rest)*)?)
    }};

    // atomic_fa (fetch-and-add)
    (@emit $ptr:ident, $sqn:ident, $wqe_idx:ident, $ds_cnt:ident, $offset:ident,
     atomic_fa { add_value: $add:expr } $(, $($rest:tt)*)?) => {{
        #[allow(unused_unsafe)]
        unsafe {
            let p = $ptr.add($offset) as *mut u64;

            // DWORD0-1: add value (8B, big-endian)
            std::ptr::write_volatile(p, ($add as u64).to_be());

            // DWORD2-3: reserved (must be 0)
            std::ptr::write_volatile(p.add(1), 0u64);
        }
        $offset += 16;
        $crate::wqe_write!(@emit $ptr, $sqn, $wqe_idx, $ds_cnt, $offset, $($($rest)*)?)
    }};

    // av (Address Vector for DC QP) - 48 bytes
    // Layout:
    //   offset 0-7:   dc_key (8 bytes, big-endian)
    //   offset 8-11:  dqp_dct (4 bytes, 0x80000000 | dctn)
    //   offset 12-13: reserved (2 bytes)
    //   offset 14-15: dlid (2 bytes, big-endian)
    //   offset 16-47: reserved (32 bytes, zeros)
    (@emit $ptr:ident, $sqn:ident, $wqe_idx:ident, $ds_cnt:ident, $offset:ident,
     av { dc_key: $dc_key:expr, dctn: $dctn:expr, dlid: $dlid:expr } $(, $($rest:tt)*)?) => {{
        #[allow(unused_unsafe)]
        unsafe {
            let p = $ptr.add($offset);

            // dc_key (8 bytes, big-endian)
            let p64 = p as *mut u64;
            std::ptr::write_volatile(p64, ($dc_key as u64).to_be());

            // dqp_dct: 0x80000000 | (dctn & 0x00FFFFFF)
            let p32 = p.add(8) as *mut u32;
            let dqp_dct = 0x8000_0000u32 | (($dctn as u32) & 0x00FF_FFFF);
            std::ptr::write_volatile(p32, dqp_dct.to_be());

            // reserved (2 bytes at offset 12-13)
            std::ptr::write_volatile(p.add(12), 0u8);
            std::ptr::write_volatile(p.add(13), 0u8);

            // dlid (2 bytes, big-endian)
            let p16 = p.add(14) as *mut u16;
            std::ptr::write_volatile(p16, ($dlid as u16).to_be());

            // reserved (32 bytes, zeros)
            std::ptr::write_bytes(p.add(16), 0, 32);
        }
        $offset += 48;
        $crate::wqe_write!(@emit $ptr, $sqn, $wqe_idx, $ds_cnt, $offset, $($($rest)*)?)
    }};
}

/// Calculate the number of WQEBBs needed for a given byte size.
#[inline]
pub const fn wqebb_cnt(size: usize) -> u16 {
    ((size + 63) / 64) as u16
}

/// High-level WQE posting macro that hides internal details.
///
/// This macro handles slot allocation, WQE construction, PI advancement,
/// and entry storage automatically.
///
/// # Syntax
///
/// ```ignore
/// // Post WQE without doorbell (batch mode)
/// wqe_post!(qp, entry,
///     ctrl { opcode: RdmaWriteImm, fm_ce_se: 0x08, imm: 0x1234 },
///     rdma { remote_addr: addr, rkey: key },
///     sge { addr: local, len: 64, lkey: lk },
/// );
/// ```
///
/// # Returns
/// Returns `Option<u16>` - `Some(wqe_idx)` on success, `None` if SQ is full.
#[macro_export]
macro_rules! wqe_post {
    // Without doorbell (batch mode)
    ($qp:expr, $entry:expr, $($segs:tt)* ) => {{
        #[allow(unused_unsafe)]
        unsafe {
            if let Some(__slot) = $qp.__sq_ptr() {
                let __size = $crate::wqe_write!(__slot.ptr, __slot.sqn, __slot.wqe_idx, $($segs)*);
                let __wqebb = $crate::wqe::wqebb_cnt(__size);
                $qp.__sq_advance_pi(__wqebb);
                $qp.__sq_set_last_wqe(__slot.ptr, __size);
                $qp.__sq_store_entry(__slot.wqe_idx, $entry);
                Some(__slot.wqe_idx)
            } else {
                None
            }
        }
    }};
}

/// High-level WQE posting macro with doorbell.
///
/// Same as `wqe_post!` but rings the doorbell after posting.
#[macro_export]
macro_rules! wqe_post_doorbell {
    ($qp:expr, $entry:expr, $($segs:tt)* ) => {{
        #[allow(unused_unsafe)]
        unsafe {
            if let Some(__slot) = $qp.__sq_ptr() {
                let __size = $crate::wqe_write!(__slot.ptr, __slot.sqn, __slot.wqe_idx, $($segs)*);
                let __wqebb = $crate::wqe::wqebb_cnt(__size);
                $qp.__sq_advance_pi(__wqebb);
                $qp.__sq_set_last_wqe(__slot.ptr, __size);
                $qp.__sq_store_entry(__slot.wqe_idx, $entry);
                $qp.ring_sq_doorbell();
                Some(__slot.wqe_idx)
            } else {
                None
            }
        }
    }};
}

/// High-level WQE posting macro with BlueFlame.
///
/// Same as `wqe_post!` but uses BlueFlame for low-latency submission.
#[macro_export]
macro_rules! wqe_post_bf {
    ($qp:expr, $entry:expr, $($segs:tt)* ) => {{
        #[allow(unused_unsafe)]
        unsafe {
            if let Some(__slot) = $qp.__sq_ptr() {
                let __size = $crate::wqe_write!(__slot.ptr, __slot.sqn, __slot.wqe_idx, $($segs)*);
                let __wqebb = $crate::wqe::wqebb_cnt(__size);
                $qp.__sq_advance_pi(__wqebb);
                $qp.__sq_ring_blueflame(__slot.ptr);
                $qp.__sq_store_entry(__slot.wqe_idx, $entry);
                Some(__slot.wqe_idx)
            } else {
                None
            }
        }
    }};
}

/// Batch WQE posting - does NOT call `__sq_set_last_wqe`.
///
/// Use this for batching multiple WQEs. After the batch, call
/// `qp.__sq_set_last_wqe()` once with the last WQE's ptr/size,
/// then `qp.ring_sq_doorbell()`.
///
/// # Returns
/// Returns `Option<(u16, *mut u8, usize)>` - `Some((wqe_idx, ptr, size))` on success.
#[macro_export]
macro_rules! wqe_post_batch {
    ($qp:expr, $entry:expr, $($segs:tt)* ) => {{
        #[allow(unused_unsafe)]
        unsafe {
            if let Some(__slot) = $qp.__sq_ptr() {
                let __size = $crate::wqe_write!(__slot.ptr, __slot.sqn, __slot.wqe_idx, $($segs)*);
                let __wqebb = $crate::wqe::wqebb_cnt(__size);
                $qp.__sq_advance_pi(__wqebb);
                $qp.__sq_store_entry(__slot.wqe_idx, $entry);
                Some((__slot.wqe_idx, __slot.ptr, __size))
            } else {
                None
            }
        }
    }};
}

/// WQE transaction macro for posting one or more WQEs with optional doorbell.
///
/// # Syntax
///
/// ```ignore
/// // Single WQE, no doorbell (call ring_sq_doorbell() manually later)
/// wqe_tx!(qp,
///     [ctrl { ... }, rdma { ... }, sge { ... }]
/// );
///
/// // Single WQE with doorbell
/// wqe_tx!(qp,
///     [ctrl { ... }, rdma { ... }, sge { ... }],
///     doorbell
/// );
///
/// // Single WQE with BlueFlame (low latency)
/// wqe_tx!(qp,
///     [ctrl { ... }, rdma { ... }, sge { ... }],
///     blueflame
/// );
///
/// // Multiple WQEs with doorbell
/// wqe_tx!(qp,
///     [ctrl { ... }, rdma { ... }, sge { ... }],
///     [ctrl { ... }, rdma { ... }, sge { ... }],
///     doorbell
/// );
/// ```
///
/// # Safety
/// This macro uses unsafe operations internally. The QP must have direct access initialized.
///
/// # Returns
/// Returns `Option<()>` - `Some(())` on success, `None` if the SQ is full or not initialized.
#[macro_export]
macro_rules! wqe_tx {
    // Single WQE, no doorbell
    ($qp:expr, [$($segs:tt)*] $(,)?) => {{
        #[allow(unused_unsafe)]
        unsafe {
            let slot = $qp.__sq_ptr()?;
            let size = $crate::wqe_write!(slot.ptr, slot.sqn, slot.wqe_idx, $($segs)*);
            let wqebb = $crate::wqe::wqebb_cnt(size);
            $qp.__sq_advance_pi(wqebb);
            $qp.__sq_set_last_wqe(slot.ptr, size);
            Some(())
        }
    }};

    // Single WQE with doorbell
    ($qp:expr, [$($segs:tt)*], doorbell $(,)?) => {{
        #[allow(unused_unsafe)]
        unsafe {
            let slot = $qp.__sq_ptr()?;
            let size = $crate::wqe_write!(slot.ptr, slot.sqn, slot.wqe_idx, $($segs)*);
            let wqebb = $crate::wqe::wqebb_cnt(size);
            $qp.__sq_advance_pi(wqebb);
            $qp.__sq_set_last_wqe(slot.ptr, size);
            $qp.ring_sq_doorbell();
            Some(())
        }
    }};

    // Single WQE with blueflame
    ($qp:expr, [$($segs:tt)*], blueflame $(,)?) => {{
        #[allow(unused_unsafe)]
        unsafe {
            let slot = $qp.__sq_ptr()?;
            let size = $crate::wqe_write!(slot.ptr, slot.sqn, slot.wqe_idx, $($segs)*);
            let wqebb = $crate::wqe::wqebb_cnt(size);
            $qp.__sq_advance_pi(wqebb);
            $qp.__sq_ring_blueflame(slot.ptr);
            Some(())
        }
    }};

    // Multiple WQEs, no doorbell
    ($qp:expr, $([$($segs:tt)*]),+ $(,)?) => {{
        #[allow(unused_unsafe)]
        unsafe {
            let mut __last_ptr: *mut u8 = std::ptr::null_mut();
            let mut __last_size: usize = 0;

            $(
                let slot = $qp.__sq_ptr()?;
                let size = $crate::wqe_write!(slot.ptr, slot.sqn, slot.wqe_idx, $($segs)*);
                let wqebb = $crate::wqe::wqebb_cnt(size);
                $qp.__sq_advance_pi(wqebb);
                __last_ptr = slot.ptr;
                __last_size = size;
            )+

            $qp.__sq_set_last_wqe(__last_ptr, __last_size);
            Some(())
        }
    }};

    // Multiple WQEs with doorbell
    ($qp:expr, $([$($segs:tt)*]),+, doorbell $(,)?) => {{
        #[allow(unused_unsafe)]
        unsafe {
            let mut __last_ptr: *mut u8 = std::ptr::null_mut();
            let mut __last_size: usize = 0;

            $(
                let slot = $qp.__sq_ptr()?;
                let size = $crate::wqe_write!(slot.ptr, slot.sqn, slot.wqe_idx, $($segs)*);
                let wqebb = $crate::wqe::wqebb_cnt(size);
                $qp.__sq_advance_pi(wqebb);
                __last_ptr = slot.ptr;
                __last_size = size;
            )+

            $qp.__sq_set_last_wqe(__last_ptr, __last_size);
            $qp.ring_sq_doorbell();
            Some(())
        }
    }};

    // Multiple WQEs with blueflame
    // Note: BlueFlame only copies the first 64 bytes, so this only makes sense
    // when total WQE size fits in the BF buffer.
    ($qp:expr, $([$($segs:tt)*]),+, blueflame $(,)?) => {{
        #[allow(unused_unsafe)]
        unsafe {
            let mut __first_ptr: *mut u8 = std::ptr::null_mut();
            let mut __is_first = true;

            $(
                let slot = $qp.__sq_ptr()?;
                let size = $crate::wqe_write!(slot.ptr, slot.sqn, slot.wqe_idx, $($segs)*);
                let wqebb = $crate::wqe::wqebb_cnt(size);
                $qp.__sq_advance_pi(wqebb);

                if __is_first {
                    __first_ptr = slot.ptr;
                    __is_first = false;
                }
            )+

            $qp.__sq_ring_blueflame(__first_ptr);
            Some(())
        }
    }};
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wqe_write_ctrl_rdma_sge() {
        let mut buf = [0u8; 64];
        let ptr = buf.as_mut_ptr();
        let sqn = 0x123456u32;
        let wqe_idx = 0x0102u16;

        let size = wqe_write!(ptr, sqn, wqe_idx,
            ctrl { opcode: 0x09u8, fm_ce_se: 0x08, imm: 0xDEADBEEF_u32 },
            rdma { remote_addr: 0x1234_5678_9ABC_DEF0_u64, rkey: 0xAABBCCDD_u32 },
            sge { addr: 0xFEDC_BA98_7654_3210_u64, len: 0x1000_u32, lkey: 0x11223344_u32 },
        );

        assert_eq!(size, 48);

        // Verify control segment
        // DWORD0: opmod_idx_opcode = (wqe_idx << 8) | opcode = 0x00010209
        // In big-endian: 0x09020100
        assert_eq!(buf[0], 0x00); // opmod (upper byte)
        assert_eq!(buf[1], 0x01); // wqe_idx high
        assert_eq!(buf[2], 0x02); // wqe_idx low
        assert_eq!(buf[3], 0x09); // opcode

        // DWORD1: qpn_ds = (sqn << 8) | ds_cnt = (0x123456 << 8) | 3 = 0x12345603
        assert_eq!(buf[4], 0x12);
        assert_eq!(buf[5], 0x34);
        assert_eq!(buf[6], 0x56);
        assert_eq!(buf[7], 0x03); // ds_cnt = 3

        // DWORD2: fm_ce_se = 0x00000008 (big-endian)
        assert_eq!(buf[8], 0x00);
        assert_eq!(buf[9], 0x00);
        assert_eq!(buf[10], 0x00);
        assert_eq!(buf[11], 0x08);

        // DWORD3: imm = 0xDEADBEEF (big-endian)
        assert_eq!(buf[12], 0xDE);
        assert_eq!(buf[13], 0xAD);
        assert_eq!(buf[14], 0xBE);
        assert_eq!(buf[15], 0xEF);

        // Verify RDMA segment (offset 16)
        // remote_addr: 0x1234_5678_9ABC_DEF0 (big-endian)
        assert_eq!(buf[16], 0x12);
        assert_eq!(buf[17], 0x34);
        assert_eq!(buf[18], 0x56);
        assert_eq!(buf[19], 0x78);
        assert_eq!(buf[20], 0x9A);
        assert_eq!(buf[21], 0xBC);
        assert_eq!(buf[22], 0xDE);
        assert_eq!(buf[23], 0xF0);

        // rkey + reserved: ((rkey as u64) << 32).to_be()
        // = (0xAABBCCDD << 32).to_be() = 0xAABBCCDD_00000000 -> big-endian
        assert_eq!(buf[24], 0xAA);
        assert_eq!(buf[25], 0xBB);
        assert_eq!(buf[26], 0xCC);
        assert_eq!(buf[27], 0xDD);
        assert_eq!(buf[28], 0x00);
        assert_eq!(buf[29], 0x00);
        assert_eq!(buf[30], 0x00);
        assert_eq!(buf[31], 0x00);

        // Verify Data segment (offset 32)
        // len_lkey: ((len << 32) | lkey).to_be() = ((0x1000 << 32) | 0x11223344).to_be()
        // = 0x00001000_11223344 -> big-endian
        assert_eq!(buf[32], 0x00);
        assert_eq!(buf[33], 0x00);
        assert_eq!(buf[34], 0x10);
        assert_eq!(buf[35], 0x00);
        assert_eq!(buf[36], 0x11);
        assert_eq!(buf[37], 0x22);
        assert_eq!(buf[38], 0x33);
        assert_eq!(buf[39], 0x44);

        // addr: 0xFEDC_BA98_7654_3210 (big-endian)
        assert_eq!(buf[40], 0xFE);
        assert_eq!(buf[41], 0xDC);
        assert_eq!(buf[42], 0xBA);
        assert_eq!(buf[43], 0x98);
        assert_eq!(buf[44], 0x76);
        assert_eq!(buf[45], 0x54);
        assert_eq!(buf[46], 0x32);
        assert_eq!(buf[47], 0x10);
    }

    #[test]
    fn test_wqe_write_multiple_sge() {
        let mut buf = [0u8; 64];
        let ptr = buf.as_mut_ptr();
        let sqn = 0x100u32;
        let wqe_idx = 0x00u16;

        let size = wqe_write!(ptr, sqn, wqe_idx,
            ctrl { opcode: 0x09u8, fm_ce_se: 0x08, imm: 0 },
            rdma { remote_addr: 0x1000_u64, rkey: 0x2000_u32 },
            sge { addr: 0x3000_u64, len: 32_u32, lkey: 0x4000_u32 },
            sge { addr: 0x5000_u64, len: 32_u32, lkey: 0x4000_u32 },
        );

        assert_eq!(size, 64);

        // ds_cnt should be 4 (ctrl + rdma + 2 sge)
        assert_eq!(buf[7], 0x04);
    }

    #[test]
    fn test_wqe_write_ctrl_without_imm() {
        let mut buf = [0u8; 32];
        let ptr = buf.as_mut_ptr();
        let sqn = 0x100u32;
        let wqe_idx = 0x00u16;

        let size = wqe_write!(ptr, sqn, wqe_idx,
            ctrl { opcode: 0x0A_u8, fm_ce_se: 0x08 },
            sge { addr: 0x1000_u64, len: 64_u32, lkey: 0x2000_u32 },
        );

        assert_eq!(size, 32);

        // DWORD3 (imm) should be 0
        assert_eq!(buf[12], 0x00);
        assert_eq!(buf[13], 0x00);
        assert_eq!(buf[14], 0x00);
        assert_eq!(buf[15], 0x00);

        // ds_cnt should be 2 (ctrl + sge)
        assert_eq!(buf[7], 0x02);
    }

    #[test]
    fn test_wqe_write_atomic_cas() {
        let mut buf = [0u8; 64];
        let ptr = buf.as_mut_ptr();
        let sqn = 0x100u32;
        let wqe_idx = 0x00u16;

        let size = wqe_write!(ptr, sqn, wqe_idx,
            ctrl { opcode: 0x11_u8, fm_ce_se: 0x08 },
            rdma { remote_addr: 0x1000_u64, rkey: 0x2000_u32 },
            atomic_cas { swap: 0xAAAA_BBBB_CCCC_DDDD_u64, compare: 0x1111_2222_3333_4444_u64 },
            sge { addr: 0x3000_u64, len: 8_u32, lkey: 0x4000_u32 },
        );

        assert_eq!(size, 64);

        // Verify atomic segment at offset 32
        // swap value: 0xAAAA_BBBB_CCCC_DDDD (big-endian)
        assert_eq!(buf[32], 0xAA);
        assert_eq!(buf[33], 0xAA);
        assert_eq!(buf[34], 0xBB);
        assert_eq!(buf[35], 0xBB);
        assert_eq!(buf[36], 0xCC);
        assert_eq!(buf[37], 0xCC);
        assert_eq!(buf[38], 0xDD);
        assert_eq!(buf[39], 0xDD);

        // compare value: 0x1111_2222_3333_4444 (big-endian)
        assert_eq!(buf[40], 0x11);
        assert_eq!(buf[41], 0x11);
        assert_eq!(buf[42], 0x22);
        assert_eq!(buf[43], 0x22);
        assert_eq!(buf[44], 0x33);
        assert_eq!(buf[45], 0x33);
        assert_eq!(buf[46], 0x44);
        assert_eq!(buf[47], 0x44);
    }

    #[test]
    fn test_wqe_write_atomic_fa() {
        let mut buf = [0u8; 64];
        let ptr = buf.as_mut_ptr();
        let sqn = 0x100u32;
        let wqe_idx = 0x00u16;

        let size = wqe_write!(ptr, sqn, wqe_idx,
            ctrl { opcode: 0x12_u8, fm_ce_se: 0x08 },
            rdma { remote_addr: 0x1000_u64, rkey: 0x2000_u32 },
            atomic_fa { add_value: 0x0000_0000_0000_0001_u64 },
            sge { addr: 0x3000_u64, len: 8_u32, lkey: 0x4000_u32 },
        );

        assert_eq!(size, 64);

        // Verify atomic segment at offset 32
        // add value: 0x0000_0000_0000_0001 (big-endian)
        assert_eq!(buf[32], 0x00);
        assert_eq!(buf[33], 0x00);
        assert_eq!(buf[34], 0x00);
        assert_eq!(buf[35], 0x00);
        assert_eq!(buf[36], 0x00);
        assert_eq!(buf[37], 0x00);
        assert_eq!(buf[38], 0x00);
        assert_eq!(buf[39], 0x01);

        // reserved: 0
        assert_eq!(buf[40], 0x00);
        assert_eq!(buf[41], 0x00);
        assert_eq!(buf[42], 0x00);
        assert_eq!(buf[43], 0x00);
        assert_eq!(buf[44], 0x00);
        assert_eq!(buf[45], 0x00);
        assert_eq!(buf[46], 0x00);
        assert_eq!(buf[47], 0x00);
    }

    #[test]
    fn test_wqe_write_inline_fixed16() {
        let mut buf = [0u8; 32];
        let ptr = buf.as_mut_ptr();
        let sqn = 0x100u32;
        let wqe_idx = 0x00u16;

        let data = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08];
        let size = wqe_write!(ptr, sqn, wqe_idx,
            ctrl { opcode: 0x0A_u8, fm_ce_se: 0x08 },
            inline_fixed16 { data: &data },
        );

        assert_eq!(size, 32);

        // Verify inline header at offset 16
        // Header: 0x80000008 (inline flag + byte count = 8)
        assert_eq!(buf[16], 0x80);
        assert_eq!(buf[17], 0x00);
        assert_eq!(buf[18], 0x00);
        assert_eq!(buf[19], 0x08);

        // Verify data
        assert_eq!(&buf[20..28], &data);

        // Verify padding (zeros)
        assert_eq!(buf[28], 0x00);
        assert_eq!(buf[29], 0x00);
        assert_eq!(buf[30], 0x00);
        assert_eq!(buf[31], 0x00);
    }

    #[test]
    fn test_wqebb_cnt() {
        assert_eq!(wqebb_cnt(0), 0);
        assert_eq!(wqebb_cnt(1), 1);
        assert_eq!(wqebb_cnt(64), 1);
        assert_eq!(wqebb_cnt(65), 2);
        assert_eq!(wqebb_cnt(128), 2);
        assert_eq!(wqebb_cnt(129), 3);
    }

    #[test]
    fn test_wqe_write_av() {
        // DC QP WQE with AV: ctrl (16) + av (48) + sge (16) = 80 bytes
        let mut buf = [0u8; 80];
        let ptr = buf.as_mut_ptr();
        let sqn = 0x100u32;
        let wqe_idx = 0x00u16;

        let size = wqe_write!(ptr, sqn, wqe_idx,
            ctrl { opcode: 0x0A_u8, fm_ce_se: 0x08 },
            av { dc_key: 0x1234_5678_9ABC_DEF0_u64, dctn: 0x00ABCDEF_u32, dlid: 0x1234_u16 },
            sge { addr: 0x1000_u64, len: 64_u32, lkey: 0x2000_u32 },
        );

        assert_eq!(size, 80);

        // ds_cnt should be 5 (ctrl=1 + av=3 + sge=1)
        assert_eq!(buf[7], 0x05);

        // Verify AV at offset 16
        // dc_key: 0x1234_5678_9ABC_DEF0 (big-endian)
        assert_eq!(buf[16], 0x12);
        assert_eq!(buf[17], 0x34);
        assert_eq!(buf[18], 0x56);
        assert_eq!(buf[19], 0x78);
        assert_eq!(buf[20], 0x9A);
        assert_eq!(buf[21], 0xBC);
        assert_eq!(buf[22], 0xDE);
        assert_eq!(buf[23], 0xF0);

        // dqp_dct: 0x80ABCDEF (0x80000000 | 0x00ABCDEF) (big-endian)
        assert_eq!(buf[24], 0x80);
        assert_eq!(buf[25], 0xAB);
        assert_eq!(buf[26], 0xCD);
        assert_eq!(buf[27], 0xEF);

        // reserved (2 bytes)
        assert_eq!(buf[28], 0x00);
        assert_eq!(buf[29], 0x00);

        // dlid: 0x1234 (big-endian)
        assert_eq!(buf[30], 0x12);
        assert_eq!(buf[31], 0x34);

        // reserved (32 bytes, all zeros)
        for i in 32..64 {
            assert_eq!(buf[i], 0x00, "byte {} should be 0", i);
        }

        // Verify SGE at offset 64
        // len (0x40 = 64) and lkey (0x2000)
        assert_eq!(buf[64], 0x00);
        assert_eq!(buf[65], 0x00);
        assert_eq!(buf[66], 0x00);
        assert_eq!(buf[67], 0x40); // len = 64
        assert_eq!(buf[68], 0x00);
        assert_eq!(buf[69], 0x00);
        assert_eq!(buf[70], 0x20);
        assert_eq!(buf[71], 0x00); // lkey = 0x2000
    }
}
