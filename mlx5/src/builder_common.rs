//! Common builder utilities for CQ registration.
//!
//! This module provides shared functions used by QP, UD, and DCI builders
//! to register queues with Completion Queues.

use std::cell::RefCell;
use std::rc::{Rc, Weak};

use crate::CompletionTarget;
use crate::cq::Cq;
use crate::mono_cq::{CompletionSource, MonoCq};

// =============================================================================
// MaybeMonoCqRegister trait
// =============================================================================

/// Trait for optional MonoCq auto-registration in QP builders.
///
/// When a builder uses MonoCq, the builder stores the `Rc<MonoCq<Q>>`
/// via this trait's implementor. At `build()` time, `maybe_register()` is
/// called to register the newly-created QP with the MonoCq.
///
/// - `()` implements this as a no-op (used when the CQ is a normal `Cq`).
/// - `Rc<MonoCq<Q>>` implements the actual registration.
///
/// This is fully monomorphized — no dynamic dispatch, no heap allocation.
pub trait MaybeMonoCqRegister<Q> {
    fn maybe_register(&self, qp: &Rc<RefCell<Q>>);
}

impl<Q> MaybeMonoCqRegister<Q> for () {
    #[inline(always)]
    fn maybe_register(&self, _qp: &Rc<RefCell<Q>>) {}
}

impl<Q> MaybeMonoCqRegister<Q> for Rc<MonoCq<Q>>
where
    Q: CompletionSource + 'static,
{
    #[inline(always)]
    fn maybe_register(&self, qp: &Rc<RefCell<Q>>) {
        MonoCq::register(self, qp);
    }
}

/// Register a queue with its send and recv CQs.
///
/// This function handles the common pattern of registering a newly built QP
/// with its associated Completion Queues for completion dispatch.
///
/// # Arguments
/// * `qpn` - Queue Pair Number to register
/// * `send_cq_weak` - Weak reference to send CQ (if using normal Cq)
/// * `recv_cq_weak` - Weak reference to recv CQ (if using normal Cq)
/// * `qp_weak` - Weak reference to the QP being registered
///
/// When using MonoCq, the corresponding `*_cq_weak` should be `None`.
pub fn register_with_cqs<T: CompletionTarget + 'static>(
    qpn: u32,
    send_cq_weak: &Option<Weak<Cq>>,
    recv_cq_weak: &Option<Weak<Cq>>,
    qp_rc: &Rc<RefCell<T>>,
) {
    let qp_weak = Rc::downgrade(qp_rc) as Weak<RefCell<dyn CompletionTarget>>;

    if let Some(cq) = send_cq_weak.as_ref().and_then(|w| w.upgrade()) {
        cq.register_queue(qpn, qp_weak.clone());
    }
    if let Some(cq) = recv_cq_weak.as_ref().and_then(|w| w.upgrade()) {
        cq.register_queue(qpn, qp_weak);
    }
}

/// Register a queue with its send CQ only (for send-only queues like DCI).
///
/// # Arguments
/// * `qpn` - Queue Pair Number to register
/// * `send_cq_weak` - Weak reference to send CQ (if using normal Cq)
/// * `qp_weak` - Weak reference to the QP being registered
pub fn register_with_send_cq<T: CompletionTarget + 'static>(
    qpn: u32,
    send_cq_weak: &Option<Weak<Cq>>,
    qp_rc: &Rc<RefCell<T>>,
) {
    if let Some(cq) = send_cq_weak.as_ref().and_then(|w| w.upgrade()) {
        let qp_weak = Rc::downgrade(qp_rc) as Weak<RefCell<dyn CompletionTarget>>;
        cq.register_queue(qpn, qp_weak);
    }
}
