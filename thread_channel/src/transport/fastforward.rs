//! FastForward transport implementation.
//!
//! This transport uses the FastForward algorithm (PPoPP 2008) for both
//! request and response channels.
//!
//! Token generation: uses monotonically increasing counters (0, 1, 2, ...).

use crate::spsc;
use crate::spsc::Serial;

use super::{Response, Transport, TransportEndpoint, TransportError};

/// A bidirectional endpoint using FastForward SPSC channels.
///
/// Each endpoint contains:
/// - Outgoing request channel (this endpoint calls, peer responds)
/// - Incoming response channel (peer responds to this endpoint's calls)
/// - Incoming request channel (peer calls, this endpoint responds)
/// - Outgoing response channel (this endpoint responds to peer's calls)
pub struct FastForwardEndpoint<Req: Serial + Send, Resp: Serial + Send> {
    /// Channel for sending requests (this endpoint → peer)
    call_tx: spsc::Sender<Req>,
    /// Channel for receiving responses (peer → this endpoint)
    resp_rx: spsc::Receiver<Response<Resp>>,
    /// Channel for receiving requests (peer → this endpoint)
    call_rx: spsc::Receiver<Req>,
    /// Channel for sending responses (this endpoint → peer)
    resp_tx: spsc::Sender<Response<Resp>>,
    /// Monotonically increasing counter for call tokens
    send_count: u64,
    /// Monotonically increasing counter for recv tokens
    recv_count: u64,
    /// Capacity
    capacity: usize,
}

impl<Req: Serial + Send, Resp: Serial + Send> TransportEndpoint<Req, Resp>
    for FastForwardEndpoint<Req, Resp>
{
    #[inline]
    fn call(&mut self, req: Req) -> Result<u64, TransportError<Req>> {
        match self.call_tx.send(req) {
            Ok(()) => {
                let token = self.send_count;
                self.send_count += 1;
                Ok(token)
            }
            Err(spsc::SendError(v)) => Err(TransportError::Full(v)),
        }
    }

    #[inline]
    fn poll(&mut self) -> Option<(u64, Resp)> {
        self.resp_rx.recv().map(|resp| (resp.token, resp.data))
    }

    #[inline]
    fn recv(&mut self) -> Option<(u64, Req)> {
        self.call_rx.recv().map(|req| {
            let token = self.recv_count;
            self.recv_count += 1;
            (token, req)
        })
    }

    #[inline]
    fn reply(&mut self, token: u64, resp: Resp) -> Result<(), TransportError<Resp>> {
        let response = Response { token, data: resp };
        match self.resp_tx.send(response) {
            Ok(()) => Ok(()),
            Err(spsc::SendError(r)) => Err(TransportError::Full(r.data)),
        }
    }

    #[inline]
    fn capacity(&self) -> usize {
        self.capacity
    }
}

/// FastForward transport factory.
///
/// Uses the FastForward algorithm with validity flags for minimal cache line thrashing.
/// Both request and response channels use the same algorithm.
pub struct FastForwardTransport;

impl Transport for FastForwardTransport {
    type Endpoint<Req: Serial + Send, Resp: Serial + Send> = FastForwardEndpoint<Req, Resp>;

    fn channel<Req: Serial + Send, Resp: Serial + Send>(
        capacity: usize,
    ) -> (Self::Endpoint<Req, Resp>, Self::Endpoint<Req, Resp>) {
        let actual_capacity = capacity.next_power_of_two();

        // Endpoint A calls → Endpoint B receives
        let (call_a_to_b_tx, call_a_to_b_rx) = spsc::channel(capacity);
        // Endpoint B responds → Endpoint A receives response
        let (resp_b_to_a_tx, resp_b_to_a_rx) = spsc::channel(capacity);

        // Endpoint B calls → Endpoint A receives
        let (call_b_to_a_tx, call_b_to_a_rx) = spsc::channel(capacity);
        // Endpoint A responds → Endpoint B receives response
        let (resp_a_to_b_tx, resp_a_to_b_rx) = spsc::channel(capacity);

        (
            FastForwardEndpoint {
                call_tx: call_a_to_b_tx,
                resp_rx: resp_b_to_a_rx,
                call_rx: call_b_to_a_rx,
                resp_tx: resp_a_to_b_tx,
                send_count: 0,
                recv_count: 0,
                capacity: actual_capacity,
            },
            FastForwardEndpoint {
                call_tx: call_b_to_a_tx,
                resp_rx: resp_a_to_b_rx,
                call_rx: call_a_to_b_rx,
                resp_tx: resp_b_to_a_tx,
                send_count: 0,
                recv_count: 0,
                capacity: actual_capacity,
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_call_reply() {
        let (mut endpoint_a, mut endpoint_b) = FastForwardTransport::channel::<u32, u32>(8);

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
        let (rt0, resp0) = endpoint_a.poll().unwrap();
        assert_eq!(rt0, 0);
        assert_eq!(resp0, 1100);

        let (rt1, resp1) = endpoint_a.poll().unwrap();
        assert_eq!(rt1, 1);
        assert_eq!(resp1, 1200);

        assert!(endpoint_a.poll().is_none());
    }

    #[test]
    fn test_bidirectional() {
        let (mut endpoint_a, mut endpoint_b) = FastForwardTransport::channel::<u32, u32>(8);

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
        let (t, resp) = endpoint_a.poll().unwrap();
        assert_eq!(t, token_a);
        assert_eq!(resp, 101);

        let (t, resp) = endpoint_b.poll().unwrap();
        assert_eq!(t, token_b);
        assert_eq!(resp, 201);
    }

    #[test]
    fn test_capacity() {
        let (endpoint_a, _endpoint_b) = FastForwardTransport::channel::<u32, u32>(8);
        assert_eq!(endpoint_a.capacity(), 8);
    }

    #[test]
    fn test_monotonic_tokens() {
        let (mut endpoint_a, mut endpoint_b) = FastForwardTransport::channel::<u32, u32>(16);

        // Tokens should be monotonically increasing
        for i in 0..10 {
            let token = endpoint_a.call(i).unwrap();
            assert_eq!(token, i as u64);
        }

        for i in 0..10 {
            let (token, value) = endpoint_b.recv().unwrap();
            assert_eq!(token, i as u64);
            assert_eq!(value, i);
            endpoint_b.reply(token, value).unwrap();
        }

        // After wrap, tokens continue increasing
        for i in 10..20 {
            let token = endpoint_a.call(i).unwrap();
            assert_eq!(token, i as u64);
        }

        // Receive responses from first batch
        for i in 0..10 {
            let (token, resp) = endpoint_a.poll().unwrap();
            assert_eq!(token, i as u64);
            assert_eq!(resp, i);
        }
    }

    #[test]
    fn test_threaded() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;
        use std::thread;

        let (mut endpoint_a, mut endpoint_b) = FastForwardTransport::channel::<u64, u64>(64);

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
            while let Some((token, resp)) = endpoint_a.poll() {
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
