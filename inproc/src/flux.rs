//! SPSC-based n-to-n communication with callback-based response handling.
//!
//! Each node has one MPSC server and N-1 callers, providing zero-contention
//! communication at the cost of O(N^2) memory usage.
//!
//! This module is generic over the MPSC channel implementation:
//! - `OnesidedMpsc` (default): Producer-only write pattern
//! - `FastForwardMpsc`: FastForward algorithm with validity flags
//! - `LamportMpsc`: Batched index synchronization
//!
//! API:
//! - `call(to, payload, user_data)` sends a request
//! - `poll()` processes messages (responses invoke callback)
//! - `try_recv()` returns received requests via `RecvHandle`

use std::marker::PhantomData;

use mempc::CallerWithUserData;
use mempc::{MpscChannel, MpscRecvRef, MpscServer, OnesidedMpsc, ReplyToken, Serial};

use crate::SendError;

/// Handle for a received request that allows replying.
pub struct RecvHandle<'a, T: Serial + Send, U, F: FnMut(U, T), M: MpscChannel = OnesidedMpsc> {
    flux: &'a mut Flux<T, U, F, M>,
    from: usize,
    token: ReplyToken,
    data: T,
}

impl<'a, T: Serial + Send, U, F: FnMut(U, T), M: MpscChannel> RecvHandle<'a, T, U, F, M> {
    /// Returns the sender's node ID.
    #[inline]
    pub fn from(&self) -> usize {
        self.from
    }

    /// Returns a copy of the request data.
    #[inline]
    pub fn data(&self) -> T {
        self.data
    }

    /// Sends a reply to the request, consuming the handle.
    #[inline]
    pub fn reply(self, value: T) {
        self.flux.server.reply(self.token, value);
    }
}

/// A node in a Flux network with callback-based response handling.
///
/// Generic over the MPSC channel implementation.
pub struct Flux<T: Serial + Send, U, F: FnMut(U, T), M: MpscChannel = OnesidedMpsc> {
    id: usize,
    num_nodes: usize,
    server: M::Server<T, T>,
    callers: Vec<CallerWithUserData<M::Caller<T, T>, T, T, U, F>>,
    /// peer_ids[i] = peer node ID for callers[i]
    peer_ids: Vec<usize>,
    _marker: PhantomData<M>,
}

impl<T: Serial + Send, U, F: FnMut(U, T), M: MpscChannel> Flux<T, U, F, M> {
    /// Returns this node's ID.
    #[inline]
    pub fn id(&self) -> usize {
        self.id
    }

    /// Returns the number of peers (excluding self).
    #[inline]
    pub fn num_peers(&self) -> usize {
        self.callers.len()
    }

    /// Converts peer ID to caller index in O(1).
    #[inline]
    fn peer_to_caller_index(&self, peer: usize) -> Option<usize> {
        if peer >= self.num_nodes || peer == self.id {
            return None;
        }
        Some(if peer < self.id { peer } else { peer - 1 })
    }

    /// Sends a call to the specified peer.
    #[inline]
    pub fn call(&mut self, to: usize, value: T, user_data: U) -> Result<(), SendError<T>> {
        let caller_idx = match self.peer_to_caller_index(to) {
            Some(idx) => idx,
            None => return Err(SendError::InvalidPeer(value)),
        };

        self.callers[caller_idx]
            .call(value, user_data)
            .map_err(|e| match e {
                mempc::CallError::Full(v) | mempc::CallError::InflightExceeded(v) => {
                    SendError::Full(v)
                }
                mempc::CallError::Disconnected(v) => SendError::Disconnected(v),
            })
    }

    /// Polls for messages from peers.
    ///
    /// - Requests: available for retrieval via `try_recv()`
    /// - Responses: invokes callback (handled by CallerWithUserData)
    #[inline]
    pub fn poll(&mut self) {
        // Poll server for incoming requests
        self.server.poll();

        // Poll all callers for responses (invokes callback)
        for caller in self.callers.iter_mut() {
            caller.poll();
        }
    }

    /// Tries to receive the next request from the server.
    #[inline]
    pub fn try_recv(&mut self) -> Option<RecvHandle<'_, T, U, F, M>> {
        let recv_ref = self.server.try_recv()?;

        // Copy data (T: Serial = Copy) and extract token
        let caller_id = recv_ref.caller_id();
        let data = recv_ref.data();
        let token = recv_ref.into_token();

        // Map caller_id to peer_id
        let from = self.peer_ids[caller_id];

        Some(RecvHandle {
            flux: self,
            from,
            token,
            data,
        })
    }

    /// Returns the number of pending calls awaiting responses.
    #[inline]
    pub fn pending_count(&self) -> usize {
        self.callers.iter().map(|c| c.pending_count()).sum()
    }

    /// Pops the next received request without holding a borrow on Flux.
    ///
    /// Returns `(from_peer, token, data)`. Use [`reply`] to send a response later.
    /// Unlike [`try_recv`], this does not create a `RecvHandle`, so the caller
    /// can hold multiple pending requests and reply asynchronously.
    #[inline]
    pub fn try_recv_raw(&mut self) -> Option<(usize, ReplyToken, T)> {
        let recv_ref = self.server.try_recv()?;

        let caller_id = recv_ref.caller_id();
        let data = recv_ref.data();
        let token = recv_ref.into_token();

        let from = self.peer_ids[caller_id];

        Some((from, token, data))
    }

    /// Replies to a previously received request identified by token.
    ///
    /// This is the deferred counterpart to [`RecvHandle::reply`].
    /// The `token` parameter contains the caller information, so no `to` parameter is needed.
    #[inline]
    pub fn reply(&mut self, token: ReplyToken, value: T) {
        self.server.reply(token, value);
    }
}

/// Creates a Flux network with `n` nodes using the default MPSC channel (OnesidedMpsc).
///
/// # Arguments
/// * `n` - Number of nodes
/// * `capacity` - Channel buffer capacity (will be rounded to power of 2)
/// * `inflight_max` - Maximum inflight requests per channel (must be < capacity)
/// * `on_response` - Callback invoked when responses arrive
///
/// # Panics
/// Panics if `n` is 0, `capacity` is 0, or `inflight_max >= capacity`.
pub fn create_flux<T, U, F>(
    n: usize,
    capacity: usize,
    inflight_max: usize,
    on_response: F,
) -> Vec<Flux<T, U, F, OnesidedMpsc>>
where
    T: Serial + Send,
    U: Send,
    F: FnMut(U, T) + Clone,
{
    create_flux_with::<T, U, F, OnesidedMpsc>(n, capacity, inflight_max, on_response)
}

/// Creates a Flux network with `n` nodes using the specified MPSC channel.
///
/// # Arguments
/// * `n` - Number of nodes
/// * `capacity` - Channel buffer capacity (will be rounded to power of 2)
/// * `inflight_max` - Maximum inflight requests per channel (must be < capacity)
/// * `on_response` - Callback invoked when responses arrive
///
/// # Panics
/// Panics if `n` is 0, `capacity` is 0, or `inflight_max >= capacity`.
pub fn create_flux_with<T, U, F, M>(
    n: usize,
    capacity: usize,
    inflight_max: usize,
    on_response: F,
) -> Vec<Flux<T, U, F, M>>
where
    T: Serial + Send,
    U: Send,
    F: FnMut(U, T) + Clone,
    M: MpscChannel,
{
    assert!(n > 0, "must have at least one node");
    assert!(capacity > 0, "capacity must be greater than 0");

    // Actual capacity after power-of-2 rounding
    let actual_capacity = capacity.next_power_of_two();
    assert!(
        inflight_max < actual_capacity,
        "inflight_max must be less than capacity"
    );

    // Create N MPSC channel groups (each group has n-1 callers and 1 server)
    let mut groups: Vec<(Vec<Option<M::Caller<T, T>>>, Option<M::Server<T, T>>)> = (0..n)
        .map(|_| {
            let (callers, server) = M::create::<T, T>(n - 1, capacity, inflight_max);
            let callers_opt: Vec<Option<M::Caller<T, T>>> = callers.into_iter().map(Some).collect();
            (callers_opt, Some(server))
        })
        .collect();

    // Build nodes
    let mut nodes = Vec::with_capacity(n);

    #[allow(clippy::needless_range_loop)]
    for i in 0..n {
        // Take server for node i
        let server = groups[i].1.take().unwrap();

        // Collect callers from other nodes' groups
        let mut callers_raw = Vec::with_capacity(n - 1);
        let mut peer_ids = Vec::with_capacity(n - 1);

        for j in 0..n {
            if j == i {
                continue;
            }

            // Node i gets a caller from node j's group
            // The caller index in node j's group is the relative position of i among j's peers
            let caller_idx = if i < j { i } else { i - 1 };
            let caller = groups[j].0[caller_idx].take().unwrap();

            callers_raw.push(caller);
            peer_ids.push(j);
        }

        // Wrap each caller with CallerWithUserData
        let callers: Vec<CallerWithUserData<M::Caller<T, T>, T, T, U, F>> = callers_raw
            .into_iter()
            .map(|caller| CallerWithUserData::new(caller, actual_capacity, on_response.clone()))
            .collect();

        nodes.push(Flux {
            id: i,
            num_nodes: n,
            server,
            callers,
            peer_ids,
            _marker: PhantomData,
        });
    }

    nodes
}

#[cfg(test)]
mod tests {
    use super::*;
    use mempc::{FastForwardMpsc, LamportMpsc};

    #[test]
    fn test_create_flux() {
        let nodes: Vec<Flux<u32, (), _, OnesidedMpsc>> = create_flux(3, 16, 8, |_, _| {});
        assert_eq!(nodes.len(), 3);
        assert_eq!(nodes[0].num_peers(), 2);
        assert_eq!(nodes[1].num_peers(), 2);
        assert_eq!(nodes[2].num_peers(), 2);
    }

    #[test]
    fn test_call_reply() {
        use std::cell::RefCell;
        use std::rc::Rc;

        let responses: Rc<RefCell<Vec<(u32, u32)>>> = Rc::new(RefCell::new(Vec::new()));
        let responses_clone = Rc::clone(&responses);

        let mut nodes: Vec<Flux<u32, u32, _, OnesidedMpsc>> =
            create_flux(2, 16, 8, move |user_data, data| {
                responses_clone.borrow_mut().push((user_data, data));
            });

        // Node 0 calls node 1 with user_data=100
        nodes[0].call(1, 42, 100).unwrap();
        nodes[0].poll();

        // Node 1 receives and replies
        nodes[1].poll();
        let handle = nodes[1].try_recv().unwrap();
        assert_eq!(handle.from(), 0);
        assert_eq!(handle.data(), 42);
        handle.reply(43);
        nodes[1].poll();

        // Node 0 receives response (callback is invoked)
        nodes[0].poll();

        let responses = responses.borrow();
        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0], (100, 43));
    }

    #[test]
    fn test_poll_round_robin() {
        let mut nodes: Vec<Flux<u32, (), _, OnesidedMpsc>> = create_flux(3, 16, 8, |_, _| {});

        // Node 1 and 2 both send to node 0
        nodes[1].call(0, 100, ()).unwrap();
        nodes[1].poll();
        nodes[2].call(0, 200, ()).unwrap();
        nodes[2].poll();

        nodes[0].poll();

        let mut received = Vec::new();
        while let Some(handle) = nodes[0].try_recv() {
            received.push((handle.from(), handle.data()));
            handle.reply(0);
        }

        assert_eq!(received.len(), 2);
        assert!(received.contains(&(1, 100)));
        assert!(received.contains(&(2, 200)));
    }

    #[test]
    fn test_invalid_peer() {
        let mut nodes: Vec<Flux<u32, (), _, OnesidedMpsc>> = create_flux(2, 16, 8, |_, _| {});

        assert!(matches!(
            nodes[0].call(5, 42, ()),
            Err(SendError::InvalidPeer(42))
        ));
    }

    #[test]
    fn test_inflight_limit() {
        let mut nodes: Vec<Flux<u32, (), _, OnesidedMpsc>> = create_flux(2, 16, 4, |_, _| {});

        // Send up to inflight_max
        for i in 0..4 {
            assert!(nodes[0].call(1, i, ()).is_ok());
        }

        // Next call should fail (inflight limit reached)
        assert!(matches!(nodes[0].call(1, 4, ()), Err(SendError::Full(4))));

        // Publish calls so peer can see them
        nodes[0].poll();

        // Process responses to free up slots
        nodes[1].poll();
        while let Some(handle) = nodes[1].try_recv() {
            let data = handle.data();
            handle.reply(data);
        }
        // Publish replies
        nodes[1].poll();

        // Receive responses
        nodes[0].poll();

        // Now we can send again
        assert!(nodes[0].call(1, 5, ()).is_ok());
    }

    #[test]
    fn test_threaded() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::thread;

        let completed_count = Arc::new(AtomicU64::new(0));

        let nodes: Vec<Flux<u64, Arc<AtomicU64>, _, OnesidedMpsc>> = {
            let completed = Arc::clone(&completed_count);
            create_flux(4, 1024, 256, move |_, _| {
                completed.fetch_add(1, Ordering::Relaxed);
            })
        };

        let mut handles = Vec::new();

        for mut node in nodes {
            handles.push(thread::spawn(move || {
                let id = node.id();
                let n = node.num_peers() + 1;

                // Send to all peers
                for peer in 0..n {
                    if peer != id {
                        for _ in 0..100 {
                            loop {
                                match node.call(
                                    peer,
                                    (id * 1000) as u64,
                                    Arc::new(AtomicU64::new(0)),
                                ) {
                                    Ok(_) => break,
                                    Err(SendError::Full(_)) => {
                                        node.poll();
                                        std::hint::spin_loop();
                                    }
                                    Err(e) => panic!("send error: {:?}", e),
                                }
                            }
                        }
                    }
                }
                node.poll();

                // Receive from all peers and reply
                let mut request_count = 0;
                let expected = (n - 1) * 100;
                while request_count < expected {
                    node.poll();
                    while let Some(handle) = node.try_recv() {
                        let data = handle.data();
                        handle.reply(data);
                        request_count += 1;
                    }
                }

                request_count
            }));
        }

        for h in handles {
            assert_eq!(h.join().unwrap(), 300);
        }
    }

    #[test]
    fn test_threaded_all_to_all_call() {
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::sync::{Arc, Barrier};

        let n = 2;
        let iterations = 100u64;
        let capacity = 1024;
        let inflight_max = (capacity / 4).max(1);

        let global_response_count = Arc::new(AtomicU64::new(0));
        let global_count_clone = Arc::clone(&global_response_count);

        let nodes: Vec<Flux<u64, (), _, OnesidedMpsc>> =
            create_flux(n, capacity, inflight_max, move |_: (), _: u64| {
                global_count_clone.fetch_add(1, Ordering::Relaxed);
            });
        let end_barrier = Arc::new(Barrier::new(n));
        let expected_total = n as u64 * iterations * ((n - 1) as u64);
        let global_response_count_clone = Arc::clone(&global_response_count);

        std::thread::scope(|s| {
            let handles: Vec<_> = nodes
                .into_iter()
                .map(|mut node| {
                    let end_barrier = Arc::clone(&end_barrier);
                    let global_count = Arc::clone(&global_response_count_clone);
                    s.spawn(move || {
                        let id = node.id();
                        let num_peers = node.num_peers();
                        let peers: Vec<usize> = (0..n).filter(|&p| p != id).collect();

                        let mut sent_per_peer = vec![0u64; n];
                        let expected_requests = iterations * (num_peers as u64);
                        let mut sent_replies = 0u64;
                        let total_sent_target = iterations * (num_peers as u64);

                        let mut total_sent = 0u64;
                        while total_sent < total_sent_target || sent_replies < expected_requests {
                            for &peer in &peers {
                                if sent_per_peer[peer] < iterations
                                    && node.call(peer, sent_per_peer[peer], ()).is_ok()
                                {
                                    sent_per_peer[peer] += 1;
                                    total_sent += 1;
                                }
                            }

                            node.poll();

                            while let Some(handle) = node.try_recv() {
                                let data = handle.data();
                                handle.reply(data);
                                sent_replies += 1;
                            }
                        }

                        // Wait for all responses to be processed globally
                        while global_count.load(Ordering::Relaxed) < expected_total {
                            node.poll();
                            while let Some(handle) = node.try_recv() {
                                let data = handle.data();
                                handle.reply(data);
                            }
                            std::hint::spin_loop();
                        }

                        end_barrier.wait();
                        sent_replies
                    })
                })
                .collect();

            for h in handles {
                let result = h.join().unwrap();
                assert_eq!(result, iterations * ((n - 1) as u64));
            }
        });

        let total_responses = global_response_count.load(Ordering::Relaxed);
        assert_eq!(total_responses, expected_total);
    }

    #[test]
    fn test_ring_wrap() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicU64, Ordering};

        let response_count = Arc::new(AtomicU64::new(0));
        let response_count_clone = Arc::clone(&response_count);

        let mut nodes: Vec<Flux<u64, (), _, OnesidedMpsc>> =
            create_flux(2, 8, 4, move |_: (), _: u64| {
                response_count_clone.fetch_add(1, Ordering::Relaxed);
            });

        // Send many iterations to test ring buffer wrap
        for iteration in 0..24u64 {
            // Node 0 calls node 1 (up to inflight limit)
            while nodes[0].pending_count() < 4 {
                nodes[0].call(1, iteration, ()).unwrap();
            }

            // Node 1 processes all requests and replies
            nodes[1].poll();
            while let Some(handle) = nodes[1].try_recv() {
                let data = handle.data();
                handle.reply(data + 1000);
            }

            // Node 0 receives all responses
            nodes[0].poll();
        }

        assert!(response_count.load(Ordering::Relaxed) >= 24);
    }

    #[test]
    fn test_try_recv_raw_and_reply() {
        use std::cell::RefCell;
        use std::rc::Rc;

        let responses: Rc<RefCell<Vec<(u32, u32)>>> = Rc::new(RefCell::new(Vec::new()));
        let responses_clone = Rc::clone(&responses);

        let mut nodes: Vec<Flux<u32, u32, _, OnesidedMpsc>> =
            create_flux(2, 16, 8, move |user_data, data| {
                responses_clone.borrow_mut().push((user_data, data));
            });

        // Node 0 calls node 1 with user_data=100
        nodes[0].call(1, 42, 100).unwrap();
        nodes[0].poll();

        // Node 1 receives via try_recv_raw and replies via reply()
        nodes[1].poll();
        let (from, token, data) = nodes[1].try_recv_raw().unwrap();
        assert_eq!(from, 0);
        assert_eq!(data, 42);

        // Can do other work between recv_raw and reply
        assert!(nodes[1].try_recv_raw().is_none());

        // Reply using the stored token (no 'to' parameter needed)
        nodes[1].reply(token, 43);
        nodes[1].poll();

        // Node 0 receives response
        nodes[0].poll();

        let responses = responses.borrow();
        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0], (100, 43));
    }

    #[test]
    fn test_multiple_deferred_replies() {
        use std::cell::RefCell;
        use std::rc::Rc;

        let responses: Rc<RefCell<Vec<(u32, u32)>>> = Rc::new(RefCell::new(Vec::new()));
        let responses_clone = Rc::clone(&responses);

        let mut nodes: Vec<Flux<u32, u32, _, OnesidedMpsc>> =
            create_flux(3, 16, 8, move |user_data, data| {
                responses_clone.borrow_mut().push((user_data, data));
            });

        // Node 1 and 2 both call node 0
        nodes[1].call(0, 100, 10).unwrap();
        nodes[1].poll();
        nodes[2].call(0, 200, 20).unwrap();
        nodes[2].poll();

        // Node 0 receives both via try_recv_raw (no borrow conflict)
        nodes[0].poll();
        let req1 = nodes[0].try_recv_raw().unwrap();
        let req2 = nodes[0].try_recv_raw().unwrap();

        // Reply in reverse order (no 'to' parameter needed)
        nodes[0].reply(req2.1, req2.2 + 1);
        nodes[0].reply(req1.1, req1.2 + 1);
        nodes[0].poll();

        // Both callers receive responses
        nodes[1].poll();
        nodes[2].poll();

        let responses = responses.borrow();
        assert_eq!(responses.len(), 2);
    }

    // Tests for different MPSC channel implementations

    #[test]
    fn test_fastforward_mpsc() {
        use std::cell::RefCell;
        use std::rc::Rc;

        let responses: Rc<RefCell<Vec<(u32, u32)>>> = Rc::new(RefCell::new(Vec::new()));
        let responses_clone = Rc::clone(&responses);

        let mut nodes: Vec<Flux<u32, u32, _, FastForwardMpsc>> =
            create_flux_with(2, 16, 8, move |user_data, data| {
                responses_clone.borrow_mut().push((user_data, data));
            });

        nodes[0].call(1, 42, 100).unwrap();
        nodes[0].poll();

        nodes[1].poll();
        let handle = nodes[1].try_recv().unwrap();
        assert_eq!(handle.from(), 0);
        assert_eq!(handle.data(), 42);
        handle.reply(43);
        nodes[1].poll();

        nodes[0].poll();

        let responses = responses.borrow();
        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0], (100, 43));
    }

    #[test]
    fn test_lamport_mpsc() {
        use std::cell::RefCell;
        use std::rc::Rc;

        let responses: Rc<RefCell<Vec<(u32, u32)>>> = Rc::new(RefCell::new(Vec::new()));
        let responses_clone = Rc::clone(&responses);

        let mut nodes: Vec<Flux<u32, u32, _, LamportMpsc>> =
            create_flux_with(2, 16, 8, move |user_data, data| {
                responses_clone.borrow_mut().push((user_data, data));
            });

        nodes[0].call(1, 42, 100).unwrap();
        nodes[0].poll();

        nodes[1].poll();
        let handle = nodes[1].try_recv().unwrap();
        assert_eq!(handle.from(), 0);
        assert_eq!(handle.data(), 42);
        handle.reply(43);
        nodes[1].poll();

        nodes[0].poll();

        let responses = responses.borrow();
        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0], (100, 43));
    }
}
