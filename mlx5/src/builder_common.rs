//! Common builder utilities for CQ registration.
//!
//! This module provides shared functions used by QP, UD, and DCI builders
//! to register queues with Completion Queues.

use std::cell::RefCell;
use std::rc::{Rc, Weak};

use crate::CompletionTarget;
use crate::cq::Cq;

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
