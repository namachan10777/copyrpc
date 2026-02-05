//! Onesided (producer-only write) transport implementation.
//!
//! This transport uses spsc_rpc for the request channel (producer-only write)
//! and spsc (FastForward) for the response channel.
//!
//! Token: uses slot_idx from spsc_rpc (0 to capacity-1, wraps around).
//!
//! Flow control: The slot_idx flows from caller → callee → caller.
//! When callee sends a response with slot_idx, caller knows that slot is consumed.

use crate::spsc;
use crate::spsc::Serial;
use crate::spsc_rpc;

use super::{Response, Transport, TransportEndpoint, TransportError};

/// A bidirectional endpoint using Onesided SPSC channels.
///
/// Each endpoint contains:
/// - Outgoing request channel using spsc_rpc (producer-only write)
/// - Incoming response channel using spsc (FastForward)
/// - Incoming request channel using spsc_rpc (producer-only write)
/// - Outgoing response channel using spsc (FastForward)
pub struct OnesidedEndpoint<Req: Serial + Send, Resp: Serial + Send> {
    /// Channel for sending requests (this endpoint → peer) using spsc_rpc
    call_tx: spsc_rpc::Sender<Req>,
    /// Channel for receiving responses (peer → this endpoint) using spsc
    resp_rx: spsc::Receiver<Response<Resp>>,
    /// Channel for receiving requests (peer → this endpoint) using spsc_rpc
    call_rx: spsc_rpc::Receiver<Req>,
    /// Channel for sending responses (this endpoint → peer) using spsc
    resp_tx: spsc::Sender<Response<Resp>>,
}

impl<Req: Serial + Send, Resp: Serial + Send> TransportEndpoint<Req, Resp>
    for OnesidedEndpoint<Req, Resp>
{
    #[inline]
    fn call(&mut self, req: Req) -> Result<u64, TransportError<Req>> {
        match self.call_tx.send(req) {
            Ok(slot_idx) => Ok(slot_idx as u64),
            Err(spsc_rpc::SendError(v)) => Err(TransportError::Full(v)),
        }
    }

    #[inline]
    fn poll(&mut self) -> Option<(u64, Resp)> {
        self.resp_rx.recv().map(|resp| (resp.token, resp.data))
    }

    #[inline]
    fn recv(&mut self) -> Option<(u64, Req)> {
        self.call_rx.recv().map(|(slot_idx, req)| (slot_idx as u64, req))
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
        self.call_tx.capacity()
    }
}

/// Onesided transport factory.
///
/// Uses spsc_rpc (producer-only write) for requests and spsc (FastForward)
/// for responses. This is the default transport for Flux, optimized for
/// RPC patterns where the producer never reads from the request channel.
pub struct OnesidedTransport;

impl Transport for OnesidedTransport {
    type Endpoint<Req: Serial + Send, Resp: Serial + Send> = OnesidedEndpoint<Req, Resp>;

    fn channel<Req: Serial + Send, Resp: Serial + Send>(
        capacity: usize,
    ) -> (Self::Endpoint<Req, Resp>, Self::Endpoint<Req, Resp>) {
        // Endpoint A calls → Endpoint B receives (spsc_rpc)
        let (call_a_to_b_tx, call_a_to_b_rx) = spsc_rpc::channel(capacity);
        // Endpoint B responds → Endpoint A receives response (spsc)
        let (resp_b_to_a_tx, resp_b_to_a_rx) = spsc::channel(capacity);

        // Endpoint B calls → Endpoint A receives (spsc_rpc)
        let (call_b_to_a_tx, call_b_to_a_rx) = spsc_rpc::channel(capacity);
        // Endpoint A responds → Endpoint B receives response (spsc)
        let (resp_a_to_b_tx, resp_a_to_b_rx) = spsc::channel(capacity);

        (
            OnesidedEndpoint {
                call_tx: call_a_to_b_tx,
                resp_rx: resp_b_to_a_rx,
                call_rx: call_b_to_a_rx,
                resp_tx: resp_a_to_b_tx,
            },
            OnesidedEndpoint {
                call_tx: call_b_to_a_tx,
                resp_rx: resp_a_to_b_rx,
                call_rx: call_a_to_b_rx,
                resp_tx: resp_b_to_a_tx,
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_call_reply() {
        let (mut endpoint_a, mut endpoint_b) = OnesidedTransport::channel::<u32, u32>(8);

        // Endpoint A sends request to B
        let token0 = endpoint_a.call(100).unwrap();
        let token1 = endpoint_a.call(200).unwrap();

        // Tokens are slot indices (0, 1, 2, ...)
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

        // Endpoint B sends responses with the same tokens
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
        let (mut endpoint_a, mut endpoint_b) = OnesidedTransport::channel::<u32, u32>(8);

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
        let (endpoint_a, _endpoint_b) = OnesidedTransport::channel::<u32, u32>(8);
        assert_eq!(endpoint_a.capacity(), 8);
    }

    #[test]
    fn test_wrap_around() {
        // Test that tokens (slot indices) wrap around correctly
        // spsc_rpc uses sentinel, so use capacity=8 and send batches of 4
        let (mut endpoint_a, mut endpoint_b) = OnesidedTransport::channel::<u32, u32>(8);

        for round in 0..3u32 {
            // Send 4 items
            for i in 0..4u32 {
                let token = endpoint_a.call(round * 4 + i).unwrap();
                // Token is slot_idx, wraps at capacity (8)
                let expected_token = (round * 4 + i) as u64 % 8;
                assert_eq!(token, expected_token, "round={}, i={}", round, i);
            }

            // Receive and reply
            for i in 0..4u32 {
                let (token, req) = endpoint_b.recv().unwrap();
                let expected_token = (round * 4 + i) as u64 % 8;
                assert_eq!(token, expected_token);
                assert_eq!(req, round * 4 + i);
                endpoint_b.reply(token, req + 1000).unwrap();
            }

            // Endpoint A receives responses
            for i in 0..4u32 {
                let (token, resp) = endpoint_a.poll().unwrap();
                let expected_token = (round * 4 + i) as u64 % 8;
                assert_eq!(token, expected_token);
                assert_eq!(resp, round * 4 + i + 1000);
            }
        }
    }

    #[test]
    fn test_threaded() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;
        use std::thread;

        let (mut endpoint_a, mut endpoint_b) = OnesidedTransport::channel::<u64, u64>(256);

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
        let inflight_max = 128u64;

        while sent < iterations || received < sent {
            // Send some requests (limited by inflight)
            while sent < iterations && sent - received < inflight_max {
                if endpoint_a.call(sent).is_ok() {
                    sent += 1;
                } else {
                    break;
                }
            }

            // Receive responses
            while let Some((_token, resp)) = endpoint_a.poll() {
                // Note: responses may not be in order due to batching
                assert!(resp >= 1000);
                received += 1;
            }
        }

        stop.store(true, Ordering::Relaxed);
        let callee_count = handle.join().unwrap();

        assert_eq!(received, iterations);
        assert_eq!(callee_count, iterations);
    }
}
