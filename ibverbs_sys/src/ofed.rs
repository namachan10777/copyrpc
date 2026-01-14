use std::mem::offset_of;

use crate::ibv_context;

const __VERBS_ABI_IS_EXTENDED: u64 = u64::MAX;

#[inline]
unsafe fn verbs_get_ctx(ctx: *mut ibv_context) -> *mut crate::verbs_context {
    if (*ctx).abi_compat != __VERBS_ABI_IS_EXTENDED as *mut _ {
        std::ptr::null_mut()
    } else {
        let offset = offset_of!(crate::verbs_context, context);
        let ptr = ctx.byte_sub(offset);
        ptr as *mut _
    }
}

pub unsafe fn ibv_query_port(
    context: *mut ibv_context,
    port: u8,
    port_attr: *mut crate::ibv_port_attr,
) -> i32 {
    let vctx = verbs_get_ctx(context);

    if vctx.is_null() {
        crate::ibv_query_port(context, port, port_attr as *mut _)
    } else if let Some(query_port_fn) = (*vctx).query_port {
        query_port_fn(context, port, port_attr, size_of::<crate::ibv_port_attr>())
    } else {
        crate::ibv_query_port(context, port, port_attr as *mut _)
    }
}

#[allow(clippy::missing_safety_doc)]
#[inline]
pub unsafe fn ibv_poll_cq(cq: *mut crate::ibv_cq, num_entries: i32, wc: *mut crate::ibv_wc) -> i32 {
    if let Some(poll_cq_fn) = (*(*cq).context).ops.poll_cq {
        poll_cq_fn(cq, num_entries, wc)
    } else {
        panic!("poll_cq is not defined")
    }
}

#[allow(clippy::missing_safety_doc)]
#[inline]
pub unsafe fn ibv_post_recv(
    qp: *mut crate::ibv_qp,
    wr: *mut crate::ibv_recv_wr,
    bad_wr: *mut *mut crate::ibv_recv_wr,
) -> i32 {
    if let Some(post_recv_fn) = (*(*qp).context).ops.post_recv {
        post_recv_fn(qp, wr, bad_wr)
    } else {
        panic!("poll_cq is not defined")
    }
}

#[allow(clippy::missing_safety_doc)]
#[inline]
pub unsafe fn ibv_post_srq_recv(
    srq: *mut crate::ibv_srq,
    wr: *mut crate::ibv_recv_wr,
    bad_wr: *mut *mut crate::ibv_recv_wr,
) -> i32 {
    if let Some(post_recv_fn) = (*(*srq).context).ops.post_srq_recv {
        post_recv_fn(srq, wr, bad_wr)
    } else {
        panic!("poll_cq is not defined")
    }
}

#[allow(clippy::missing_safety_doc)]
#[inline]
pub unsafe fn ibv_post_send(
    qp: *mut crate::ibv_qp,
    wr: *mut crate::ibv_send_wr,
    bad_wr: *mut *mut crate::ibv_send_wr,
) -> i32 {
    if let Some(post_send_fn) = (*(*qp).context).ops.post_send {
        post_send_fn(qp, wr, bad_wr)
    } else {
        panic!("poll_cq is not defined")
    }
}

/// Create an extended SRQ (including TM-SRQ).
///
/// # Safety
/// - `context` must be a valid ibv_context pointer
/// - `srq_init_attr_ex` must be a valid pointer to initialized struct
#[allow(clippy::missing_safety_doc)]
#[inline]
pub unsafe fn ibv_create_srq_ex(
    context: *mut crate::ibv_context,
    srq_init_attr_ex: *mut crate::ibv_srq_init_attr_ex,
) -> *mut crate::ibv_srq {
    let vctx = verbs_get_ctx(context);

    if vctx.is_null() {
        std::ptr::null_mut()
    } else if let Some(create_srq_ex_fn) = (*vctx).create_srq_ex {
        create_srq_ex_fn(context, srq_init_attr_ex)
    } else {
        std::ptr::null_mut()
    }
}

/// Post SRQ operations (for TM tag management).
///
/// # Safety
/// - `srq` must be a valid ibv_srq pointer (TM-SRQ)
/// - `wr` must be a valid pointer to ibv_ops_wr chain
/// - `bad_wr` must be a valid pointer to receive failed WR
#[allow(clippy::missing_safety_doc)]
#[inline]
pub unsafe fn ibv_post_srq_ops(
    srq: *mut crate::ibv_srq,
    wr: *mut crate::ibv_ops_wr,
    bad_wr: *mut *mut crate::ibv_ops_wr,
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
