#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(dead_code)]
#![allow(clippy::all)]

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

use std::mem::offset_of;
use std::ops::{Deref, DerefMut};

const __VERBS_ABI_IS_EXTENDED: u64 = u64::MAX;

#[inline]
unsafe fn verbs_get_ctx(ctx: *mut ibv_context) -> *mut verbs_context {
    if (*ctx).abi_compat != __VERBS_ABI_IS_EXTENDED as *mut _ {
        std::ptr::null_mut()
    } else {
        let offset = offset_of!(verbs_context, context);
        let ptr = ctx.byte_sub(offset);
        ptr as *mut _
    }
}

pub unsafe fn ibv_query_port_ex(
    context: *mut ibv_context,
    port: u8,
    port_attr: *mut ibv_port_attr,
) -> i32 {
    let vctx = verbs_get_ctx(context);

    if vctx.is_null() {
        ibv_query_port(context, port, port_attr as *mut _)
    } else if let Some(query_port_fn) = (*vctx).query_port {
        query_port_fn(context, port, port_attr, size_of::<ibv_port_attr>())
    } else {
        ibv_query_port(context, port, port_attr as *mut _)
    }
}

#[inline]
pub unsafe fn ibv_poll_cq_ex(cq: *mut ibv_cq, num_entries: i32, wc: *mut ibv_wc) -> i32 {
    if let Some(poll_cq_fn) = (*(*cq).context).ops.poll_cq {
        poll_cq_fn(cq, num_entries, wc)
    } else {
        panic!("poll_cq is not defined")
    }
}

#[inline]
pub unsafe fn ibv_post_recv_ex(
    qp: *mut ibv_qp,
    wr: *mut ibv_recv_wr,
    bad_wr: *mut *mut ibv_recv_wr,
) -> i32 {
    if let Some(post_recv_fn) = (*(*qp).context).ops.post_recv {
        post_recv_fn(qp, wr, bad_wr)
    } else {
        panic!("post_recv is not defined")
    }
}

#[inline]
pub unsafe fn ibv_post_srq_recv_ex(
    srq: *mut ibv_srq,
    wr: *mut ibv_recv_wr,
    bad_wr: *mut *mut ibv_recv_wr,
) -> i32 {
    if let Some(post_recv_fn) = (*(*srq).context).ops.post_srq_recv {
        post_recv_fn(srq, wr, bad_wr)
    } else {
        panic!("post_srq_recv is not defined")
    }
}

#[inline]
pub unsafe fn ibv_post_send_ex(
    qp: *mut ibv_qp,
    wr: *mut ibv_send_wr,
    bad_wr: *mut *mut ibv_send_wr,
) -> i32 {
    if let Some(post_send_fn) = (*(*qp).context).ops.post_send {
        post_send_fn(qp, wr, bad_wr)
    } else {
        panic!("post_send is not defined")
    }
}

#[inline]
pub unsafe fn ibv_create_srq_ex_ex(
    context: *mut ibv_context,
    srq_init_attr_ex: *mut ibv_srq_init_attr_ex,
) -> *mut ibv_srq {
    let vctx = verbs_get_ctx(context);

    if vctx.is_null() {
        std::ptr::null_mut()
    } else if let Some(create_srq_ex_fn) = (*vctx).create_srq_ex {
        create_srq_ex_fn(context, srq_init_attr_ex)
    } else {
        std::ptr::null_mut()
    }
}

#[inline]
pub unsafe fn ibv_post_srq_ops_ex(
    srq: *mut ibv_srq,
    wr: *mut ibv_ops_wr,
    bad_wr: *mut *mut ibv_ops_wr,
) -> ::std::os::raw::c_int {
    let vctx = verbs_get_ctx((*srq).context);

    if vctx.is_null() {
        -1
    } else if let Some(post_srq_ops_fn) = (*vctx).post_srq_ops {
        post_srq_ops_fn(srq, wr, bad_wr)
    } else {
        -1
    }
}

/// Inner packed struct for mlx5_wqe_ctrl_seg.
/// Rust doesn't support both packed and aligned attributes simultaneously,
/// so we use this inner struct with packed layout and wrap it in an aligned outer struct.
#[repr(C, packed)]
#[derive(Debug, Copy, Clone)]
pub struct mlx5_wqe_ctrl_seg_inner {
    pub opmod_idx_opcode: __be32,
    pub qpn_ds: __be32,
    pub signature: u8,
    pub dci_stream_channel_id: __be16,
    pub fm_ce_se: u8,
    pub imm: __be32,
}

/// mlx5_wqe_ctrl_seg with correct layout: packed fields with 4-byte alignment.
/// C definition: __attribute__((__packed__)) __attribute__((__aligned__(4)))
#[repr(C, align(4))]
#[derive(Debug, Copy, Clone)]
pub struct mlx5_wqe_ctrl_seg(pub mlx5_wqe_ctrl_seg_inner);

impl Deref for mlx5_wqe_ctrl_seg {
    type Target = mlx5_wqe_ctrl_seg_inner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for mlx5_wqe_ctrl_seg {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
