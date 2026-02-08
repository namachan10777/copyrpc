//! rtrb-based transport implementation.
//!
//! This transport uses the rtrb crate (wait-free, realtime-safe SPSC) for both
//! request and response channels.
//!
//! Token generation: uses monotonically increasing counters (0, 1, 2, ...).

use crate::serial::Serial;

use super::{Response, Transport, TransportEndpoint, TransportError};

/// A bidirectional endpoint using rtrb SPSC channels.
///
/// Each endpoint contains:
/// - Outgoing request channel (this endpoint calls, peer responds)
/// - Incoming response channel (peer responds to this endpoint's calls)
/// - Incoming request channel (peer calls, this endpoint responds)
/// - Outgoing response channel (this endpoint responds to peer's calls)
pub struct RtrbEndpoint<Req: Serial + Send, Resp: Serial + Send> {
    /// Channel for sending requests (this endpoint -> peer)
    call_tx: rtrb::Producer<Req>,
    /// Channel for receiving responses (peer -> this endpoint)
    resp_rx: rtrb::Consumer<Response<Resp>>,
    /// Channel for receiving requests (peer -> this endpoint)
    call_rx: rtrb::Consumer<Req>,
    /// Channel for sending responses (this endpoint -> peer)
    resp_tx: rtrb::Producer<Response<Resp>>,
    /// Monotonically increasing counter for call tokens
    send_count: u64,
    /// Monotonically increasing counter for recv tokens
    recv_count: u64,
    /// Capacity
    capacity: usize,
    /// Current number of inflight requests
    inflight: usize,
    /// Maximum allowed inflight requests
    max_inflight: usize,
}

impl<Req: Serial + Send, Resp: Serial + Send> TransportEndpoint<Req, Resp>
    for RtrbEndpoint<Req, Resp>
{
    #[inline]
    fn call(&mut self, req: Req) -> Result<u64, TransportError<Req>> {
        if self.inflight >= self.max_inflight {
            return Err(TransportError::InflightExceeded(req));
        }
        match self.call_tx.push(req) {
            Ok(()) => {
                let token = self.send_count;
                self.send_count += 1;
                self.inflight += 1;
                Ok(token)
            }
            Err(rtrb::PushError::Full(v)) => Err(TransportError::Full(v)),
        }
    }

    #[inline]
    fn sync(&mut self) {
        // No-op for rtrb: lock-free ring buffer provides immediate visibility
    }

    #[inline]
    fn try_recv_response(&mut self) -> Option<(u64, Resp)> {
        self.resp_rx.pop().ok().map(|resp| {
            self.inflight = self.inflight.saturating_sub(1);
            (resp.token, resp.data)
        })
    }

    #[inline]
    fn recv(&mut self) -> Option<(u64, Req)> {
        self.call_rx.pop().ok().map(|req| {
            let token = self.recv_count;
            self.recv_count += 1;
            (token, req)
        })
    }

    #[inline]
    fn reply(&mut self, token: u64, resp: Resp) -> Result<(), TransportError<Resp>> {
        let response = Response { token, data: resp };
        match self.resp_tx.push(response) {
            Ok(()) => Ok(()),
            Err(rtrb::PushError::Full(r)) => Err(TransportError::Full(r.data)),
        }
    }

    #[inline]
    fn capacity(&self) -> usize {
        self.capacity
    }

    #[inline]
    fn inflight(&self) -> usize {
        self.inflight
    }

    #[inline]
    fn max_inflight(&self) -> usize {
        self.max_inflight
    }
}

/// rtrb transport factory.
///
/// Uses the rtrb crate (wait-free, realtime-safe SPSC ring buffer).
/// Both request and response channels use the same implementation.
pub struct RtrbTransport;

impl Transport for RtrbTransport {
    type Endpoint<Req: Serial + Send, Resp: Serial + Send> = RtrbEndpoint<Req, Resp>;

    fn channel<Req: Serial + Send, Resp: Serial + Send>(
        capacity: usize,
        max_inflight: usize,
    ) -> (Self::Endpoint<Req, Resp>, Self::Endpoint<Req, Resp>) {
        // rtrb uses power-of-two internally
        let actual_capacity = capacity.next_power_of_two();

        // Endpoint A calls -> Endpoint B receives
        let (call_a_to_b_tx, call_a_to_b_rx) = rtrb::RingBuffer::new(actual_capacity);
        // Endpoint B responds -> Endpoint A receives response
        let (resp_b_to_a_tx, resp_b_to_a_rx) = rtrb::RingBuffer::new(actual_capacity);

        // Endpoint B calls -> Endpoint A receives
        let (call_b_to_a_tx, call_b_to_a_rx) = rtrb::RingBuffer::new(actual_capacity);
        // Endpoint A responds -> Endpoint B receives response
        let (resp_a_to_b_tx, resp_a_to_b_rx) = rtrb::RingBuffer::new(actual_capacity);

        (
            RtrbEndpoint {
                call_tx: call_a_to_b_tx,
                resp_rx: resp_b_to_a_rx,
                call_rx: call_b_to_a_rx,
                resp_tx: resp_a_to_b_tx,
                send_count: 0,
                recv_count: 0,
                capacity: actual_capacity,
                inflight: 0,
                max_inflight,
            },
            RtrbEndpoint {
                call_tx: call_b_to_a_tx,
                resp_rx: resp_a_to_b_rx,
                call_rx: call_a_to_b_rx,
                resp_tx: resp_b_to_a_tx,
                send_count: 0,
                recv_count: 0,
                capacity: actual_capacity,
                inflight: 0,
                max_inflight,
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_call_reply() {
        let (mut endpoint_a, mut endpoint_b) = RtrbTransport::channel::<u32, u32>(8, 8);

        // Endpoint A sends request to B
        let token0 = endpoint_a.call(100).unwrap();
        let token1 = endpoint_a.call(200).unwrap();

        assert_eq!(token0, 0);
        assert_eq!(token1, 1);

        // Endpoint B receives requests
        let (t0, req0) = endpoint_b.recv().unwrap();
        assert_eq!(t0, 0);
        assert_eq!(req0, 100);

        let (t1, req1) = endpoint_b.recv().unwrap();
        assert_eq!(t1, 1);
        assert_eq!(req1, 200);

        assert!(endpoint_b.recv().is_none());

        // Endpoint B sends responses
        endpoint_b.reply(t0, req0 + 1000).unwrap();
        endpoint_b.reply(t1, req1 + 1000).unwrap();

        // Endpoint A receives responses
        endpoint_a.sync();
        let (rt0, resp0) = endpoint_a.try_recv_response().unwrap();
        assert_eq!(rt0, 0);
        assert_eq!(resp0, 1100);

        let (rt1, resp1) = endpoint_a.try_recv_response().unwrap();
        assert_eq!(rt1, 1);
        assert_eq!(resp1, 1200);

        assert!(endpoint_a.try_recv_response().is_none());
    }

    #[test]
    fn test_bidirectional() {
        let (mut endpoint_a, mut endpoint_b) = RtrbTransport::channel::<u32, u32>(8, 8);

        // Both endpoints can call each other simultaneously
        let token_a = endpoint_a.call(100).unwrap();
        let token_b = endpoint_b.call(200).unwrap();

        // A receives B's call
        let (t, req) = endpoint_a.recv().unwrap();
        assert_eq!(req, 200);
        endpoint_a.reply(t, 201).unwrap();

        // B receives A's call
        let (t, req) = endpoint_b.recv().unwrap();
        assert_eq!(req, 100);
        endpoint_b.reply(t, 101).unwrap();

        // Both receive responses
        endpoint_a.sync();
        let (t, resp) = endpoint_a.try_recv_response().unwrap();
        assert_eq!(t, token_a);
        assert_eq!(resp, 101);

        endpoint_b.sync();
        let (t, resp) = endpoint_b.try_recv_response().unwrap();
        assert_eq!(t, token_b);
        assert_eq!(resp, 201);
    }

    #[test]
    fn test_capacity() {
        let (endpoint_a, _endpoint_b) = RtrbTransport::channel::<u32, u32>(8, 8);
        assert_eq!(endpoint_a.capacity(), 8);
    }

    #[test]
    fn test_inflight_limit() {
        let (mut endpoint_a, _endpoint_b) = RtrbTransport::channel::<u32, u32>(64, 4);

        // Can send up to max_inflight
        for i in 0..4 {
            assert!(endpoint_a.call(i).is_ok());
        }

        // Fifth should fail with InflightExceeded
        assert!(matches!(
            endpoint_a.call(4),
            Err(TransportError::InflightExceeded(4))
        ));
    }

    #[test]
    fn test_threaded() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;
        use std::thread;

        let (mut endpoint_a, mut endpoint_b) = RtrbTransport::channel::<u64, u64>(64, 64);

        let stop = Arc::new(AtomicBool::new(false));
        let stop2 = Arc::clone(&stop);

        let handle = thread::spawn(move || {
            let mut count = 0u64;
            while !stop2.load(Ordering::Relaxed) {
                if let Some((token, data)) = endpoint_b.recv() {
                    endpoint_b.reply(token, data + 1000).unwrap();
                    count += 1;
                }
            }
            // Drain remaining
            while let Some((token, data)) = endpoint_b.recv() {
                endpoint_b.reply(token, data + 1000).unwrap();
                count += 1;
            }
            count
        });

        let mut sent = 0u64;
        let mut received = 0u64;
        let iterations = 10000u64;

        while sent < iterations || received < sent {
            // Send some requests
            while sent < iterations && sent - received < 32 {
                if endpoint_a.call(sent).is_ok() {
                    sent += 1;
                } else {
                    break;
                }
            }

            // Receive responses
            endpoint_a.sync();
            while let Some((token, resp)) = endpoint_a.try_recv_response() {
                assert_eq!(token, received);
                assert_eq!(resp, received + 1000);
                received += 1;
            }
        }

        stop.store(true, Ordering::Relaxed);
        let callee_count = handle.join().unwrap();

        assert_eq!(received, iterations);
        assert_eq!(callee_count, iterations);
    }
}
