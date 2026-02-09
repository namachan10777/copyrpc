//! FastForward algorithm (PPoPP 2008) with boolean validity flags.
//!
//! SPSC design:
//! - Boolean validity flag per slot: true = has data, false = empty
//! - Sender: checks valid==false (slot empty), writes data, sets valid=true
//! - Receiver: checks valid==true, reads data, sets valid=false
//! - No shared atomic indices (validity flag is the only coordination)
//! - Volatile reads/writes with compiler_fence
//! - Token: monotonically increasing counter (send_count, recv_count)
//! - Immediate visibility (no sync needed, sync() is no-op)

use crate::common::{CachePadded, DisconnectState, Response};
use crate::serial::Serial;
use crate::{CallError, MpscCaller, MpscChannel, MpscServer, ReplyToken};
use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::ptr;
use std::sync::atomic::{compiler_fence, AtomicBool, Ordering};
use std::sync::Arc;

// ============================================================================
// SPSC Core
// ============================================================================

#[repr(C)]
struct Slot<T> {
    valid: UnsafeCell<bool>,
    _pad: [u8; 7],
    data: UnsafeCell<MaybeUninit<T>>,
}

impl<T> Slot<T> {
    fn new() -> Self {
        Self {
            valid: UnsafeCell::new(false),
            _pad: [0; 7],
            data: UnsafeCell::new(MaybeUninit::uninit()),
        }
    }

    unsafe fn is_valid(&self) -> bool {
        unsafe { ptr::read_volatile(self.valid.get()) }
    }

    unsafe fn set_valid(&self, v: bool) {
        unsafe { ptr::write_volatile(self.valid.get(), v) };
    }

    unsafe fn write_data(&self, data: T) {
        unsafe { (*self.data.get()).write(data) };
    }

    unsafe fn read_data(&self) -> T {
        unsafe { (*self.data.get()).assume_init_read() }
    }
}

unsafe impl<T: Send> Send for Slot<T> {}
unsafe impl<T: Send> Sync for Slot<T> {}

struct FastForwardSpscTx<T> {
    ring: Arc<Vec<Slot<T>>>,
    send_count: u64,
    _disconnect: Arc<DisconnectState>,
}

impl<T> FastForwardSpscTx<T> {
    fn try_send(&mut self, data: T) -> Result<u64, T> {
        let slot_idx = (self.send_count as usize) % self.ring.len();
        let slot = &self.ring[slot_idx];

        unsafe {
            if slot.is_valid() {
                return Err(data);
            }

            slot.write_data(data);
            compiler_fence(Ordering::Release);
            slot.set_valid(true);
        }

        let token = self.send_count;
        self.send_count += 1;
        Ok(token)
    }
}

struct FastForwardSpscRx<T> {
    ring: Arc<Vec<Slot<T>>>,
    recv_count: u64,
    _disconnect: Arc<DisconnectState>,
}

impl<T> FastForwardSpscRx<T> {
    fn try_recv(&mut self) -> Option<(u64, T)> {
        let slot_idx = (self.recv_count as usize) % self.ring.len();
        let slot = &self.ring[slot_idx];

        unsafe {
            if !slot.is_valid() {
                return None;
            }

            compiler_fence(Ordering::Acquire);
            let data = slot.read_data();
            slot.set_valid(false);

            let token = self.recv_count;
            self.recv_count += 1;
            Some((token, data))
        }
    }
}

fn fastforward_spsc<T>(ring_depth: usize) -> (FastForwardSpscTx<T>, FastForwardSpscRx<T>) {
    let ring = Arc::new((0..ring_depth).map(|_| Slot::new()).collect());
    let disconnect = Arc::new(DisconnectState {
        tx_alive: AtomicBool::new(true),
        rx_alive: AtomicBool::new(true),
    });

    let tx = FastForwardSpscTx {
        ring: Arc::clone(&ring),
        send_count: 0,
        _disconnect: Arc::clone(&disconnect),
    };

    let rx = FastForwardSpscRx {
        ring,
        recv_count: 0,
        _disconnect: disconnect,
    };

    (tx, rx)
}

// ============================================================================
// MPSC Wrapper
// ============================================================================

pub struct FastForwardCaller<Req: Serial, Resp: Serial> {
    call_tx: FastForwardSpscTx<Req>,
    resp_rx: FastForwardSpscRx<Response<Resp>>,
    send_count: u64,
    inflight: usize,
    max_inflight: usize,
}

impl<Req: Serial + Send, Resp: Serial + Send> MpscCaller<Req, Resp> for FastForwardCaller<Req, Resp> {
    fn call(&mut self, req: Req) -> Result<u64, CallError<Req>> {
        if self.inflight >= self.max_inflight {
            return Err(CallError::InflightExceeded(req));
        }

        match self.call_tx.try_send(req) {
            Ok(_slot_token) => {
                let token = self.send_count;
                self.send_count += 1;
                self.inflight += 1;
                Ok(token)
            }
            Err(req) => Err(CallError::Full(req)),
        }
    }

    fn sync(&mut self) {
        // No-op: FastForward has immediate visibility
    }

    fn try_recv_response(&mut self) -> Option<(u64, Resp)> {
        self.resp_rx.try_recv().map(|(_slot_token, resp)| {
            self.inflight -= 1;
            (resp.token, resp.data)
        })
    }

    fn pending_count(&self) -> usize {
        self.inflight
    }
}

unsafe impl<Req: Serial, Resp: Serial> Send for FastForwardCaller<Req, Resp> {}

struct PerCallerState<Req: Serial, Resp: Serial> {
    call_rx: FastForwardSpscRx<Req>,
    resp_tx: FastForwardSpscTx<Response<Resp>>,
    recv_count: u64,
}

pub struct FastForwardServer<Req: Serial, Resp: Serial> {
    callers: Vec<CachePadded<PerCallerState<Req, Resp>>>,
    available: Vec<usize>,
    scan_idx: usize,
}

impl<Req: Serial, Resp: Serial> FastForwardServer<Req, Resp> {
    fn new(callers: Vec<PerCallerState<Req, Resp>>) -> Self {
        Self {
            callers: callers.into_iter().map(CachePadded::new).collect(),
            available: Vec::new(),
            scan_idx: 0,
        }
    }
}

impl<Req: Serial + Send, Resp: Serial + Send> MpscServer<Req, Resp> for FastForwardServer<Req, Resp> {
    type RecvRef<'a> = FastForwardRecvRef<'a, Req, Resp> where Self: 'a;

    fn poll(&mut self) -> u32 {
        self.available.clear();
        let n = self.callers.len();
        for _ in 0..n {
            let caller_id = self.scan_idx;
            self.scan_idx = (self.scan_idx + 1) % n;

            let slot_idx = (self.callers[caller_id].recv_count as usize)
                % self.callers[caller_id].call_rx.ring.len();
            let slot = &self.callers[caller_id].call_rx.ring[slot_idx];

            unsafe {
                if slot.is_valid() {
                    self.available.push(caller_id);
                }
            }
        }
        self.available.len() as u32
    }

    fn try_recv(&mut self) -> Option<Self::RecvRef<'_>> {
        if self.available.is_empty() {
            return None;
        }

        let caller_id = self.available.swap_remove(0);
        let state = &mut self.callers[caller_id];

        let slot_idx = (state.recv_count as usize) % state.call_rx.ring.len();
        let slot = &state.call_rx.ring[slot_idx];

        unsafe {
            if !slot.is_valid() {
                return None;
            }
            compiler_fence(Ordering::Acquire);
        }

        Some(FastForwardRecvRef {
            server: self,
            caller_id,
            slot_idx,
            consumed: false,
        })
    }

    fn reply(&mut self, token: ReplyToken, resp: Resp) {
        let state = &mut self.callers[token.caller_id];
        let response = Response {
            token: token.slot_token,
            data: resp,
        };
        let _ = state.resp_tx.try_send(response);
    }
}

pub struct FastForwardRecvRef<'a, Req: Serial, Resp: Serial> {
    server: &'a mut FastForwardServer<Req, Resp>,
    caller_id: usize,
    slot_idx: usize,
    consumed: bool,
}

impl<'a, Req: Serial, Resp: Serial> FastForwardRecvRef<'a, Req, Resp> {
    pub fn data(&self) -> Req {
        let state = &self.server.callers[self.caller_id];
        let slot = &state.call_rx.ring[self.slot_idx];
        unsafe { slot.read_data() }
    }

    pub fn reply(mut self, resp: Resp) {
        let state = &mut self.server.callers[self.caller_id];
        let recv_count = state.recv_count;

        let slot = &state.call_rx.ring[self.slot_idx];
        unsafe {
            slot.set_valid(false);
        }
        state.recv_count += 1;

        let response = Response {
            token: recv_count,
            data: resp,
        };
        let _ = state.resp_tx.try_send(response);
        self.consumed = true;
    }

    pub fn into_token(mut self) -> ReplyToken {
        let state = &mut self.server.callers[self.caller_id];
        let recv_count = state.recv_count;

        let slot = &state.call_rx.ring[self.slot_idx];
        unsafe {
            slot.set_valid(false);
        }
        state.recv_count += 1;

        self.consumed = true;
        ReplyToken {
            caller_id: self.caller_id,
            slot_token: recv_count,
        }
    }

    pub fn caller_id(&self) -> usize {
        self.caller_id
    }
}

impl<Req: Serial, Resp: Serial> crate::MpscRecvRef<Req> for FastForwardRecvRef<'_, Req, Resp> {
    #[inline]
    fn caller_id(&self) -> usize {
        self.caller_id
    }

    #[inline]
    fn data(&self) -> Req {
        let state = &self.server.callers[self.caller_id];
        let slot = &state.call_rx.ring[self.slot_idx];
        unsafe { slot.read_data() }
    }

    #[inline]
    fn into_token(mut self) -> crate::ReplyToken {
        let state = &mut self.server.callers[self.caller_id];
        let recv_count = state.recv_count;

        let slot = &state.call_rx.ring[self.slot_idx];
        unsafe {
            slot.set_valid(false);
        }
        state.recv_count += 1;

        self.consumed = true;
        crate::ReplyToken {
            caller_id: self.caller_id,
            slot_token: recv_count,
        }
    }
}

impl<'a, Req: Serial, Resp: Serial> Drop for FastForwardRecvRef<'a, Req, Resp> {
    fn drop(&mut self) {
        if !self.consumed {
            panic!("FastForwardRecvRef dropped without reply() or into_token()");
        }
    }
}

// ============================================================================
// Factory
// ============================================================================

pub struct FastForwardMpsc;

impl MpscChannel for FastForwardMpsc {
    type Caller<Req: Serial + Send, Resp: Serial + Send> = FastForwardCaller<Req, Resp>;
    type Server<Req: Serial + Send, Resp: Serial + Send> = FastForwardServer<Req, Resp>;

    fn create<Req: Serial + Send, Resp: Serial + Send>(
        max_callers: usize,
        ring_depth: usize,
        max_inflight: usize,
    ) -> (Vec<Self::Caller<Req, Resp>>, Self::Server<Req, Resp>) {
        let mut callers = Vec::with_capacity(max_callers);
        let mut server_states = Vec::with_capacity(max_callers);

        for _ in 0..max_callers {
            let (call_tx, call_rx) = fastforward_spsc(ring_depth);
            let (resp_tx, resp_rx) = fastforward_spsc(ring_depth);

            callers.push(FastForwardCaller {
                call_tx,
                resp_rx,
                send_count: 0,
                inflight: 0,
                max_inflight,
            });

            server_states.push(PerCallerState {
                call_rx,
                resp_tx,
                recv_count: 0,
            });
        }

        let server = FastForwardServer::new(server_states);
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
    fn test_spsc_basic() {
        let (mut tx, mut rx) = fastforward_spsc::<u64>(4);

        assert_eq!(tx.try_send(42), Ok(0));
        assert_eq!(tx.try_send(100), Ok(1));

        assert_eq!(rx.try_recv(), Some((0, 42)));
        assert_eq!(rx.try_recv(), Some((1, 100)));
        assert_eq!(rx.try_recv(), None);
    }

    #[test]
    fn test_spsc_ring_wraparound() {
        let (mut tx, mut rx) = fastforward_spsc::<u64>(4);

        for i in 0..8 {
            assert_eq!(tx.try_send(i), Ok(i));
            assert_eq!(rx.try_recv(), Some((i, i)));
        }
    }

    #[test]
    fn test_spsc_full() {
        let (mut tx, mut rx) = fastforward_spsc::<u64>(2);

        assert_eq!(tx.try_send(1), Ok(0));
        assert_eq!(tx.try_send(2), Ok(1));
        assert_eq!(tx.try_send(3), Err(3)); // Full

        assert_eq!(rx.try_recv(), Some((0, 1)));
        assert_eq!(tx.try_send(3), Ok(2)); // Now succeeds
    }

    #[test]
    fn test_mpsc_basic() {
        let (mut callers, mut server) =
            FastForwardMpsc::create::<u64, u64>(2, 4, 4);

        assert_eq!(callers[0].call(10), Ok(0));
        assert_eq!(callers[1].call(20), Ok(0));

        assert_eq!(server.poll(), 2);

        let recv_ref = server.try_recv().unwrap();
        let req = recv_ref.data();
        let caller_id = recv_ref.caller_id();
        recv_ref.reply(req * 2);

        let recv_ref = server.try_recv().unwrap();
        let req = recv_ref.data();
        recv_ref.reply(req * 2);

        callers[caller_id].sync();

        assert_eq!(callers[0].try_recv_response(), Some((0, 20)));
        assert_eq!(callers[1].try_recv_response(), Some((0, 40)));
    }

    #[test]
    fn test_mpsc_multi_caller() {
        let (mut callers, mut server) =
            FastForwardMpsc::create::<u64, u64>(3, 4, 4);

        for (i, caller) in callers.iter_mut().enumerate() {
            assert_eq!(caller.call((i as u64 + 1) * 10), Ok(0));
        }

        assert_eq!(server.poll(), 3);

        for _ in 0..3 {
            if let Some(recv_ref) = server.try_recv() {
                let req = recv_ref.data();
                recv_ref.reply(req + 1);
            }
        }

        for caller in &mut callers {
            caller.sync();
        }

        assert_eq!(callers[0].try_recv_response(), Some((0, 11)));
        assert_eq!(callers[1].try_recv_response(), Some((0, 21)));
        assert_eq!(callers[2].try_recv_response(), Some((0, 31)));
    }

    #[test]
    fn test_mpsc_inflight_limit() {
        let (mut callers, mut _server) =
            FastForwardMpsc::create::<u64, u64>(1, 4, 2);

        assert_eq!(callers[0].call(1), Ok(0));
        assert_eq!(callers[0].call(2), Ok(1));
        assert!(matches!(callers[0].call(3), Err(CallError::InflightExceeded(3))));

        assert_eq!(callers[0].pending_count(), 2);
    }

    #[test]
    fn test_mpsc_ring_full() {
        let (mut callers, mut _server) =
            FastForwardMpsc::create::<u64, u64>(1, 2, 10);

        assert_eq!(callers[0].call(1), Ok(0));
        assert_eq!(callers[0].call(2), Ok(1));
        assert!(matches!(callers[0].call(3), Err(CallError::Full(3))));
    }

    #[test]
    fn test_mpsc_zero_copy_recv() {
        let (mut callers, mut server) =
            FastForwardMpsc::create::<u64, u64>(1, 4, 4);

        callers[0].call(42).unwrap();
        server.poll();

        let recv_ref = server.try_recv().unwrap();
        assert_eq!(recv_ref.data(), 42);
        assert_eq!(recv_ref.caller_id(), 0);
        recv_ref.reply(84);

        callers[0].sync();
        assert_eq!(callers[0].try_recv_response(), Some((0, 84)));
    }

    #[test]
    fn test_mpsc_deferred_reply() {
        let (mut callers, mut server) =
            FastForwardMpsc::create::<u64, u64>(2, 4, 4);

        callers[0].call(10).unwrap();
        callers[1].call(20).unwrap();

        assert_eq!(server.poll(), 2);

        let recv_ref1 = server.try_recv().unwrap();
        let token1 = recv_ref1.into_token();

        let recv_ref2 = server.try_recv().unwrap();
        let token2 = recv_ref2.into_token();

        // Reply out-of-order
        server.reply(token2, 200);
        server.reply(token1, 100);

        for caller in &mut callers {
            caller.sync();
        }

        assert_eq!(callers[0].try_recv_response(), Some((0, 100)));
        assert_eq!(callers[1].try_recv_response(), Some((0, 200)));
    }

    #[test]
    fn test_mpsc_ring_wraparound() {
        let (mut callers, mut server) =
            FastForwardMpsc::create::<u64, u64>(1, 4, 4);

        for i in 0..10 {
            callers[0].call(i).unwrap();
            server.poll();
            if let Some(recv_ref) = server.try_recv() {
                let req = recv_ref.data();
                recv_ref.reply(req * 2);
            }
            callers[0].sync();
            assert_eq!(callers[0].try_recv_response(), Some((i, i * 2)));
        }
    }

    #[test]
    #[should_panic(expected = "FastForwardRecvRef dropped without reply() or into_token()")]
    fn test_recv_ref_drop_panic() {
        let (mut callers, mut server) =
            FastForwardMpsc::create::<u64, u64>(1, 4, 4);

        callers[0].call(42).unwrap();
        server.poll();

        let _recv_ref = server.try_recv().unwrap();
        // Drop without reply or into_token
    }
}
