//! SPSC-based n-to-n communication with callback-based response handling.
//!
//! Each node pair has dedicated SPSC channels, providing zero-contention
//! communication at the cost of O(N^2) memory usage.
//!
//! This module is generic over the transport implementation:
//! - `OnesidedTransport` (default): Producer-only write pattern
//! - `FastForwardTransport`: FastForward algorithm with validity flags
//! - `LamportTransport`: Batched index synchronization
//!
//! API:
//! - `call(to, payload, user_data)` sends a request
//! - `poll()` processes messages (responses invoke callback)
//! - `try_recv()` returns received requests via `RecvHandle`

use std::collections::VecDeque;
use std::marker::PhantomData;

use crate::serial::Serial;
use crate::transport::{OnesidedTransport, Transport, TransportEndpoint, TransportError};
use crate::SendError;

/// A bidirectional channel to a single peer.
///
/// Uses a single TransportEndpoint for all RPC communication.
struct FluxChannel<T: Serial + Send, Tr: Transport> {
    peer_id: usize,
    /// Bidirectional endpoint for this peer
    endpoint: Tr::Endpoint<T, T>,
    /// Current inflight count for this channel
    inflight_count: usize,
}

/// A received request, stored in the internal queue.
struct RecvRequest<T> {
    from: usize,
    token: u64,
    data: T,
}

/// Handle for a received request that allows replying.
pub struct RecvHandle<'a, T: Serial + Send, U, F: FnMut(U, T), Tr: Transport = OnesidedTransport> {
    flux: &'a mut Flux<T, U, F, Tr>,
    from: usize,
    token: u64,
    data: T,
}

impl<'a, T: Serial + Send, U, F: FnMut(U, T), Tr: Transport> RecvHandle<'a, T, U, F, Tr> {
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

    /// Sends a reply to the request, consuming the handle on success.
    #[inline]
    pub fn reply(self, value: T) -> Result<(), (Self, SendError<T>)> {
        let channel_idx = match self.flux.peer_to_channel_index(self.from) {
            Some(idx) => idx,
            None => return Err((self, SendError::InvalidPeer(value))),
        };

        match self.flux.channels[channel_idx].endpoint.reply(self.token, value) {
            Ok(()) => Ok(()),
            Err(TransportError::Full(v)) | Err(TransportError::InflightExceeded(v)) => Err((
                Self {
                    flux: self.flux,
                    from: self.from,
                    token: self.token,
                    data: self.data,
                },
                SendError::Full(v),
            )),
            Err(TransportError::Disconnected(v)) => Err((
                Self {
                    flux: self.flux,
                    from: self.from,
                    token: self.token,
                    data: self.data,
                },
                SendError::Disconnected(v),
            )),
        }
    }

    /// Tries to send a reply. On failure, requeues the request for later retry.
    #[inline]
    pub fn reply_or_requeue(self, value: T) -> bool {
        let channel_idx = match self.flux.peer_to_channel_index(self.from) {
            Some(idx) => idx,
            None => {
                self.flux.recv_queue.push_front(RecvRequest {
                    from: self.from,
                    token: self.token,
                    data: self.data,
                });
                return false;
            }
        };

        match self.flux.channels[channel_idx].endpoint.reply(self.token, value) {
            Ok(()) => true,
            Err(
                TransportError::Full(_)
                | TransportError::Disconnected(_)
                | TransportError::InflightExceeded(_),
            ) => {
                self.flux.recv_queue.push_front(RecvRequest {
                    from: self.from,
                    token: self.token,
                    data: self.data,
                });
                false
            }
        }
    }
}

/// A node in a Flux network with callback-based response handling.
///
/// Generic over the transport implementation.
pub struct Flux<T: Serial + Send, U, F: FnMut(U, T), Tr: Transport = OnesidedTransport> {
    id: usize,
    num_nodes: usize,
    channels: Vec<FluxChannel<T, Tr>>,
    /// Round-robin index for poll
    recv_index: usize,
    /// Pending calls: maps token % capacity to user_data (fixed-size array per channel)
    /// Outer vec is per-channel, inner vec is per-slot
    pending_calls: Vec<Vec<Option<U>>>,
    /// Response callback
    on_response: F,
    /// Queue of received requests
    recv_queue: VecDeque<RecvRequest<T>>,
    /// Maximum inflight requests per channel
    inflight_max: usize,
    /// Capacity per channel (for token % capacity mapping)
    channel_capacity: usize,
    _marker: PhantomData<Tr>,
}

impl<T: Serial + Send, U, F: FnMut(U, T), Tr: Transport> Flux<T, U, F, Tr> {
    /// Returns this node's ID.
    #[inline]
    pub fn id(&self) -> usize {
        self.id
    }

    /// Returns the number of peers (excluding self).
    #[inline]
    pub fn num_peers(&self) -> usize {
        self.channels.len()
    }

    /// Converts peer ID to channel index in O(1).
    #[inline]
    fn peer_to_channel_index(&self, peer: usize) -> Option<usize> {
        if peer >= self.num_nodes || peer == self.id {
            return None;
        }
        Some(if peer < self.id { peer } else { peer - 1 })
    }

    /// Sends a call to the specified peer.
    #[inline]
    pub fn call(&mut self, to: usize, value: T, user_data: U) -> Result<(), SendError<T>> {
        let channel_idx = match self.peer_to_channel_index(to) {
            Some(idx) => idx,
            None => return Err(SendError::InvalidPeer(value)),
        };

        let channel = &mut self.channels[channel_idx];

        // Check inflight limit
        if channel.inflight_count >= self.inflight_max {
            return Err(SendError::Full(value));
        }

        match channel.endpoint.call(value) {
            Ok(token) => {
                channel.inflight_count += 1;
                // Store user_data for this token (using token % capacity for indexing)
                let slot = (token as usize) % self.channel_capacity;
                self.pending_calls[channel_idx][slot] = Some(user_data);
                Ok(())
            }
            Err(TransportError::Full(v) | TransportError::InflightExceeded(v)) => {
                Err(SendError::Full(v))
            }
            Err(TransportError::Disconnected(v)) => Err(SendError::Disconnected(v)),
        }
    }

    /// Polls for messages from peers.
    ///
    /// - Requests: queued for retrieval via `try_recv()`
    /// - Responses: invokes callback
    #[inline]
    pub fn poll(&mut self) {
        let n = self.channels.len();
        if n == 0 {
            return;
        }

        for _ in 0..n {
            let idx = self.recv_index;
            self.recv_index = (self.recv_index + 1) % n;

            let channel = &mut self.channels[idx];
            let peer_id = channel.peer_id;

            // Sync once per channel (for Lamport, this does 4 atomic ops)
            channel.endpoint.sync();

            // Receive requests (from peer's calls)
            while let Some((token, data)) = channel.endpoint.recv() {
                self.recv_queue.push_back(RecvRequest {
                    from: peer_id,
                    token,
                    data,
                });
            }

            // Receive responses (to our calls) - no sync needed, already done above
            while let Some((token, data)) = channel.endpoint.try_recv_response() {
                // Decrement inflight count
                channel.inflight_count -= 1;

                // Get user_data and invoke callback
                let slot = (token as usize) % self.channel_capacity;
                if let Some(user_data) = self.pending_calls[idx][slot].take() {
                    (self.on_response)(user_data, data);
                }
            }
        }
    }

    /// Tries to receive the next request from the queue.
    #[inline]
    pub fn try_recv(&mut self) -> Option<RecvHandle<'_, T, U, F, Tr>> {
        let req = self.recv_queue.pop_front()?;
        Some(RecvHandle {
            flux: self,
            from: req.from,
            token: req.token,
            data: req.data,
        })
    }

    /// Returns the number of pending calls awaiting responses.
    #[inline]
    pub fn pending_count(&self) -> usize {
        self.channels.iter().map(|c| c.inflight_count).sum()
    }

    /// Pops the next received request without holding a borrow on Flux.
    ///
    /// Returns `(from_peer, token, data)`. Use [`reply`] to send a response later.
    /// Unlike [`try_recv`], this does not create a `RecvHandle`, so the caller
    /// can hold multiple pending requests and reply asynchronously.
    #[inline]
    pub fn try_recv_raw(&mut self) -> Option<(usize, u64, T)> {
        let req = self.recv_queue.pop_front()?;
        Some((req.from, req.token, req.data))
    }

    /// Replies to a previously received request identified by peer ID and token.
    ///
    /// This is the deferred counterpart to [`RecvHandle::reply`].
    #[inline]
    pub fn reply(&mut self, to: usize, token: u64, value: T) -> Result<(), SendError<T>> {
        let channel_idx = match self.peer_to_channel_index(to) {
            Some(idx) => idx,
            None => return Err(SendError::InvalidPeer(value)),
        };

        self.channels[channel_idx]
            .endpoint
            .reply(token, value)
            .map_err(|e| match e {
                TransportError::Full(v) | TransportError::InflightExceeded(v) => {
                    SendError::Full(v)
                }
                TransportError::Disconnected(v) => SendError::Disconnected(v),
            })
    }
}

/// Creates a Flux network with `n` nodes using the default transport (OnesidedTransport).
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
) -> Vec<Flux<T, U, F, OnesidedTransport>>
where
    T: Serial + Send,
    U: Send,
    F: FnMut(U, T) + Clone,
{
    create_flux_with_transport::<T, U, F, OnesidedTransport>(n, capacity, inflight_max, on_response)
}

/// Creates a Flux network with `n` nodes using the specified transport.
///
/// # Arguments
/// * `n` - Number of nodes
/// * `capacity` - Channel buffer capacity (will be rounded to power of 2)
/// * `inflight_max` - Maximum inflight requests per channel (must be < capacity)
/// * `on_response` - Callback invoked when responses arrive
///
/// # Panics
/// Panics if `n` is 0, `capacity` is 0, or `inflight_max >= capacity`.
pub fn create_flux_with_transport<T, U, F, Tr>(
    n: usize,
    capacity: usize,
    inflight_max: usize,
    on_response: F,
) -> Vec<Flux<T, U, F, Tr>>
where
    T: Serial + Send,
    U: Send,
    F: FnMut(U, T) + Clone,
    Tr: Transport,
{
    assert!(n > 0, "must have at least one node");
    assert!(capacity > 0, "capacity must be greater than 0");

    // Actual capacity after power-of-2 rounding
    let actual_capacity = capacity.next_power_of_two();
    assert!(
        inflight_max < actual_capacity,
        "inflight_max must be less than capacity"
    );

    // Store endpoints separately for each (i, j) directed pair
    // channel_endpoints[i][j] = Some(endpoint) means:
    //   - endpoint: node i uses this to communicate with node j
    let mut channel_endpoints: Vec<Vec<Option<Tr::Endpoint<T, T>>>> =
        (0..n).map(|_| (0..n).map(|_| None).collect()).collect();

    #[allow(clippy::needless_range_loop)]
    for i in 0..n {
        for j in (i + 1)..n {
            // Create bidirectional channel between i and j
            let (endpoint_i, endpoint_j) = Tr::channel(capacity, inflight_max);

            // Node i's endpoint for peer j
            channel_endpoints[i][j] = Some(endpoint_i);
            // Node j's endpoint for peer i
            channel_endpoints[j][i] = Some(endpoint_j);
        }
    }

    // Build nodes
    let mut nodes = Vec::with_capacity(n);

    #[allow(clippy::needless_range_loop)]
    for i in 0..n {
        let mut channels = Vec::with_capacity(n - 1);
        let mut pending_calls = Vec::with_capacity(n - 1);

        for j in 0..n {
            if i == j {
                continue;
            }

            let endpoint = channel_endpoints[i][j].take().unwrap();

            channels.push(FluxChannel {
                peer_id: j,
                endpoint,
                inflight_count: 0,
            });
            // Fixed-size array for pending calls per channel
            pending_calls.push((0..actual_capacity).map(|_| None).collect());
        }

        nodes.push(Flux {
            id: i,
            num_nodes: n,
            channels,
            recv_index: 0,
            pending_calls,
            on_response: on_response.clone(),
            recv_queue: VecDeque::with_capacity(capacity),
            inflight_max,
            channel_capacity: actual_capacity,
            _marker: PhantomData,
        });
    }

    nodes
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::{FastForwardTransport, LamportTransport};

    #[test]
    fn test_create_flux() {
        let nodes: Vec<Flux<u32, (), _, OnesidedTransport>> = create_flux(3, 16, 8, |_, _| {});
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

        let mut nodes: Vec<Flux<u32, u32, _, OnesidedTransport>> =
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
        assert!(handle.reply(43).is_ok());
        nodes[1].poll();

        // Node 0 receives response (callback is invoked)
        nodes[0].poll();

        let responses = responses.borrow();
        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0], (100, 43));
    }

    #[test]
    fn test_poll_round_robin() {
        let mut nodes: Vec<Flux<u32, (), _, OnesidedTransport>> = create_flux(3, 16, 8, |_, _| {});

        // Node 1 and 2 both send to node 0
        nodes[1].call(0, 100, ()).unwrap();
        nodes[1].poll();
        nodes[2].call(0, 200, ()).unwrap();
        nodes[2].poll();

        nodes[0].poll();

        let mut received = Vec::new();
        while let Some(handle) = nodes[0].try_recv() {
            received.push((handle.from(), handle.data()));
        }

        assert_eq!(received.len(), 2);
        assert!(received.contains(&(1, 100)));
        assert!(received.contains(&(2, 200)));
    }

    #[test]
    fn test_invalid_peer() {
        let mut nodes: Vec<Flux<u32, (), _, OnesidedTransport>> = create_flux(2, 16, 8, |_, _| {});

        assert!(matches!(
            nodes[0].call(5, 42, ()),
            Err(SendError::InvalidPeer(42))
        ));
    }

    #[test]
    fn test_inflight_limit() {
        let mut nodes: Vec<Flux<u32, (), _, OnesidedTransport>> = create_flux(2, 16, 4, |_, _| {});

        // Send up to inflight_max
        for i in 0..4 {
            assert!(nodes[0].call(1, i, ()).is_ok());
        }

        // Next call should fail (inflight limit reached)
        assert!(matches!(
            nodes[0].call(1, 4, ()),
            Err(SendError::Full(4))
        ));

        // Publish calls so peer can see them
        nodes[0].poll();

        // Process responses to free up slots
        nodes[1].poll();
        while let Some(handle) = nodes[1].try_recv() {
            let data = handle.data();
            assert!(handle.reply(data).is_ok());
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
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::sync::Arc;
        use std::thread;

        let completed_count = Arc::new(AtomicU64::new(0));

        let nodes: Vec<Flux<u64, Arc<AtomicU64>, _, OnesidedTransport>> = {
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
                                match node.call(peer, (id * 1000) as u64, Arc::new(AtomicU64::new(0)))
                                {
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
                        handle.reply(data).ok();
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

        let nodes: Vec<Flux<u64, (), _, OnesidedTransport>> =
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
                                    && node.call(peer, sent_per_peer[peer], ()).is_ok() {
                                        sent_per_peer[peer] += 1;
                                        total_sent += 1;
                                    }
                            }

                            node.poll();

                            while let Some(handle) = node.try_recv() {
                                let data = handle.data();
                                if handle.reply(data).is_ok() {
                                    sent_replies += 1;
                                }
                            }
                        }

                        // Wait for all responses to be processed globally
                        while global_count.load(Ordering::Relaxed) < expected_total {
                            node.poll();
                            while let Some(handle) = node.try_recv() {
                                let data = handle.data();
                                handle.reply(data).ok();
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
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::sync::Arc;

        let response_count = Arc::new(AtomicU64::new(0));
        let response_count_clone = Arc::clone(&response_count);

        let mut nodes: Vec<Flux<u64, (), _, OnesidedTransport>> =
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
                assert!(handle.reply(data + 1000).is_ok());
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

        let mut nodes: Vec<Flux<u32, u32, _, OnesidedTransport>> =
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

        // Reply using the stored token
        nodes[1].reply(from, token, 43).unwrap();
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

        let mut nodes: Vec<Flux<u32, u32, _, OnesidedTransport>> =
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

        // Reply in reverse order
        nodes[0].reply(req2.0, req2.1, req2.2 + 1).unwrap();
        nodes[0].reply(req1.0, req1.1, req1.2 + 1).unwrap();
        nodes[0].poll();

        // Both callers receive responses
        nodes[1].poll();
        nodes[2].poll();

        let responses = responses.borrow();
        assert_eq!(responses.len(), 2);
    }

    // Tests for different transport implementations

    #[test]
    fn test_fastforward_transport() {
        use std::cell::RefCell;
        use std::rc::Rc;

        let responses: Rc<RefCell<Vec<(u32, u32)>>> = Rc::new(RefCell::new(Vec::new()));
        let responses_clone = Rc::clone(&responses);

        let mut nodes: Vec<Flux<u32, u32, _, FastForwardTransport>> =
            create_flux_with_transport(2, 16, 8, move |user_data, data| {
                responses_clone.borrow_mut().push((user_data, data));
            });

        nodes[0].call(1, 42, 100).unwrap();
        nodes[0].poll();

        nodes[1].poll();
        let handle = nodes[1].try_recv().unwrap();
        assert_eq!(handle.from(), 0);
        assert_eq!(handle.data(), 42);
        assert!(handle.reply(43).is_ok());
        nodes[1].poll();

        nodes[0].poll();

        let responses = responses.borrow();
        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0], (100, 43));
    }

    #[test]
    fn test_lamport_transport() {
        use std::cell::RefCell;
        use std::rc::Rc;

        let responses: Rc<RefCell<Vec<(u32, u32)>>> = Rc::new(RefCell::new(Vec::new()));
        let responses_clone = Rc::clone(&responses);

        let mut nodes: Vec<Flux<u32, u32, _, LamportTransport>> =
            create_flux_with_transport(2, 16, 8, move |user_data, data| {
                responses_clone.borrow_mut().push((user_data, data));
            });

        nodes[0].call(1, 42, 100).unwrap();
        nodes[0].poll();

        nodes[1].poll();
        let handle = nodes[1].try_recv().unwrap();
        assert_eq!(handle.from(), 0);
        assert_eq!(handle.data(), 42);
        assert!(handle.reply(43).is_ok());
        nodes[1].poll();

        nodes[0].poll();

        let responses = responses.borrow();
        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0], (100, 43));
    }
}
