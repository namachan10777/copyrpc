//! SPSC-based n-to-n communication with callback-based response handling.
//!
//! Each node pair has dedicated SPSC channels, providing zero-contention
//! communication at the cost of O(N^2) memory usage.
//!
//! This module uses producer-only write SPSC for requests:
//! - Request channel: consumer is read-only (producer frees slots on response)
//! - Response channel: standard SPSC
//!
//! API:
//! - `call(to, payload, user_data)` sends a request
//! - `poll()` processes messages (responses invoke callback)
//! - `try_recv()` returns received requests via `RecvHandle`

use std::collections::VecDeque;

use crate::spsc::{self, Serial};
use crate::spsc_rpc;
use crate::SendError;

/// Response message containing slot index for freeing.
#[derive(Clone, Copy)]
#[repr(C)]
struct Response<T: Serial> {
    slot_idx: usize,
    data: T,
}

unsafe impl<T: Serial> Serial for Response<T> {}

/// A bidirectional channel to a single peer.
///
/// Uses producer-only write SPSC for requests (consumer is read-only),
/// and standard SPSC for responses.
struct FluxChannel<T: Serial> {
    peer_id: usize,
    /// Request sender (producer-only write)
    req_tx: spsc_rpc::Sender<T>,
    /// Request receiver (read-only)
    req_rx: spsc_rpc::Receiver<T>,
    /// Response sender (standard SPSC)
    resp_tx: spsc::Sender<Response<T>>,
    /// Response receiver (standard SPSC)
    resp_rx: spsc::Receiver<Response<T>>,
}

/// A received request, stored in the internal queue.
struct RecvRequest<T> {
    from: usize,
    slot_idx: usize,
    data: T,
}

/// Handle for a received request that allows replying.
pub struct RecvHandle<'a, T: Serial, U, F: FnMut(&mut U, T)> {
    flux: &'a mut Flux<T, U, F>,
    from: usize,
    slot_idx: usize,
    data: T,
}

impl<'a, T: Serial + Send, U, F: FnMut(&mut U, T)> RecvHandle<'a, T, U, F> {
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

        let resp = Response {
            slot_idx: self.slot_idx,
            data: value,
        };

        match self.flux.channels[channel_idx].resp_tx.send(resp) {
            Ok(()) => Ok(()),
            Err(spsc::SendError(r)) => Err((
                Self {
                    flux: self.flux,
                    from: self.from,
                    slot_idx: self.slot_idx,
                    data: self.data,
                },
                SendError::Full(r.data),
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
                    slot_idx: self.slot_idx,
                    data: self.data,
                });
                return false;
            }
        };

        let resp = Response {
            slot_idx: self.slot_idx,
            data: value,
        };

        match self.flux.channels[channel_idx].resp_tx.send(resp) {
            Ok(()) => true,
            Err(spsc::SendError(_)) => {
                self.flux.recv_queue.push_front(RecvRequest {
                    from: self.from,
                    slot_idx: self.slot_idx,
                    data: self.data,
                });
                false
            }
        }
    }
}

/// A node in a Flux network with callback-based response handling.
///
/// Uses producer-only write SPSC for requests, eliminating cache line
/// bouncing on the consumer side.
pub struct Flux<T: Serial, U, F: FnMut(&mut U, T)> {
    id: usize,
    num_nodes: usize,
    channels: Vec<FluxChannel<T>>,
    /// Round-robin index for poll
    recv_index: usize,
    /// Pending calls: maps slot_idx to user_data (fixed-size array per channel)
    /// Outer vec is per-channel, inner vec is per-slot
    pending_calls: Vec<Vec<Option<U>>>,
    /// Response callback
    on_response: F,
    /// Queue of received requests
    recv_queue: VecDeque<RecvRequest<T>>,
}

impl<T: Serial + Send, U, F: FnMut(&mut U, T)> Flux<T, U, F> {
    /// Returns this node's ID.
    pub fn id(&self) -> usize {
        self.id
    }

    /// Returns the number of peers (excluding self).
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

        match self.channels[channel_idx].req_tx.send(value) {
            Ok(slot_idx) => {
                // Store user_data for this slot
                self.pending_calls[channel_idx][slot_idx] = Some(user_data);
                Ok(())
            }
            Err(spsc_rpc::SendError(v)) => Err(SendError::Full(v)),
        }
    }

    /// Polls for messages from peers.
    ///
    /// - Requests: queued for retrieval via `try_recv()`
    /// - Responses: invokes callback and frees the request slot
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

            // Receive requests (read-only from consumer perspective)
            while let Some((slot_idx, data)) = channel.req_rx.recv() {
                self.recv_queue.push_back(RecvRequest {
                    from: peer_id,
                    slot_idx,
                    data,
                });
            }

            // Receive responses and free slots
            while let Some(resp) = channel.resp_rx.recv() {
                // Free the request slot (producer writes valid=false)
                channel.req_tx.free_slot(resp.slot_idx);

                // Get user_data and invoke callback
                if let Some(mut user_data) = self.pending_calls[idx][resp.slot_idx].take() {
                    (self.on_response)(&mut user_data, resp.data);
                }
            }
        }
    }

    /// Tries to receive the next request from the queue.
    #[inline]
    pub fn try_recv(&mut self) -> Option<RecvHandle<'_, T, U, F>> {
        let req = self.recv_queue.pop_front()?;
        Some(RecvHandle {
            flux: self,
            from: req.from,
            slot_idx: req.slot_idx,
            data: req.data,
        })
    }

    /// Returns the number of pending calls awaiting responses.
    #[inline]
    pub fn pending_count(&self) -> usize {
        self.pending_calls
            .iter()
            .flat_map(|v| v.iter())
            .filter(|o| o.is_some())
            .count()
    }
}

/// Creates a Flux network with `n` nodes.
///
/// # Panics
/// Panics if `n` is 0 or `capacity` is 0.
pub fn create_flux<T, U, F>(n: usize, capacity: usize, on_response: F) -> Vec<Flux<T, U, F>>
where
    T: Serial + Send,
    U: Send,
    F: FnMut(&mut U, T) + Clone,
{
    assert!(n > 0, "must have at least one node");
    assert!(capacity > 0, "capacity must be greater than 0");

    // Actual capacity after power-of-2 rounding
    let actual_capacity = capacity.next_power_of_two();

    // Create all channel pairs
    // For nodes i and j (i < j), we create:
    // - Request channel i -> j (spsc_rpc)
    // - Request channel j -> i (spsc_rpc)
    // - Response channel i -> j (spsc)
    // - Response channel j -> i (spsc)
    struct ChannelPair<T: Serial> {
        req_tx: spsc_rpc::Sender<T>,
        req_rx: spsc_rpc::Receiver<T>,
        resp_tx: spsc::Sender<Response<T>>,
        resp_rx: spsc::Receiver<Response<T>>,
    }

    let mut channel_pairs: Vec<Vec<Option<ChannelPair<T>>>> =
        (0..n).map(|_| (0..n).map(|_| None).collect()).collect();

    for i in 0..n {
        for j in (i + 1)..n {
            // Channels from i to j
            let (req_tx_i_j, req_rx_i_j) = spsc_rpc::channel(capacity);
            let (resp_tx_i_j, resp_rx_i_j) = spsc::channel(capacity);

            // Channels from j to i
            let (req_tx_j_i, req_rx_j_i) = spsc_rpc::channel(capacity);
            let (resp_tx_j_i, resp_rx_j_i) = spsc::channel(capacity);

            // i's view: send requests to j, receive responses from j
            channel_pairs[i][j] = Some(ChannelPair {
                req_tx: req_tx_i_j,
                req_rx: req_rx_j_i,
                resp_tx: resp_tx_j_i,
                resp_rx: resp_rx_i_j,
            });

            // j's view: send requests to i, receive responses from i
            channel_pairs[j][i] = Some(ChannelPair {
                req_tx: req_tx_j_i,
                req_rx: req_rx_i_j,
                resp_tx: resp_tx_i_j,
                resp_rx: resp_rx_j_i,
            });
        }
    }

    // Build nodes
    let mut nodes = Vec::with_capacity(n);

    for i in 0..n {
        let mut channels = Vec::with_capacity(n - 1);
        let mut pending_calls = Vec::with_capacity(n - 1);

        for j in 0..n {
            if i == j {
                continue;
            }

            if let Some(pair) = channel_pairs[i][j].take() {
                channels.push(FluxChannel {
                    peer_id: j,
                    req_tx: pair.req_tx,
                    req_rx: pair.req_rx,
                    resp_tx: pair.resp_tx,
                    resp_rx: pair.resp_rx,
                });
                // Fixed-size array for pending calls per channel
                pending_calls.push((0..actual_capacity).map(|_| None).collect());
            }
        }

        nodes.push(Flux {
            id: i,
            num_nodes: n,
            channels,
            recv_index: 0,
            pending_calls,
            on_response: on_response.clone(),
            recv_queue: VecDeque::with_capacity(capacity),
        });
    }

    nodes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_flux() {
        let nodes: Vec<Flux<u32, (), _>> = create_flux(3, 16, |_, _| {});
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

        let mut nodes: Vec<Flux<u32, u32, _>> = create_flux(2, 16, move |user_data, data| {
            responses_clone.borrow_mut().push((*user_data, data));
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
        let mut nodes: Vec<Flux<u32, (), _>> = create_flux(3, 16, |_, _| {});

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
        let mut nodes: Vec<Flux<u32, (), _>> = create_flux(2, 16, |_, _| {});

        assert!(matches!(
            nodes[0].call(5, 42, ()),
            Err(SendError::InvalidPeer(42))
        ));
    }

    #[test]
    fn test_threaded() {
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::sync::Arc;
        use std::thread;

        let completed_count = Arc::new(AtomicU64::new(0));

        let nodes: Vec<Flux<u64, Arc<AtomicU64>, _>> = {
            let completed = Arc::clone(&completed_count);
            create_flux(4, 1024, move |_, _| {
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
        let max_in_flight = (capacity / 4).max(1) as u64;

        let global_response_count = Arc::new(AtomicU64::new(0));
        let global_count_clone = Arc::clone(&global_response_count);

        let nodes: Vec<Flux<u64, (), _>> =
            create_flux(n, capacity, move |_: &mut (), _: u64| {
                global_count_clone.fetch_add(1, Ordering::Relaxed);
            });
        let end_barrier = Arc::new(Barrier::new(n));

        std::thread::scope(|s| {
            let handles: Vec<_> = nodes
                .into_iter()
                .enumerate()
                .map(|(_, mut node)| {
                    let end_barrier = Arc::clone(&end_barrier);
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
                                if sent_per_peer[peer] < iterations {
                                    let in_flight = sent_per_peer[peer]
                                        .saturating_sub(sent_per_peer[peer].min(max_in_flight));
                                    if in_flight < max_in_flight {
                                        if node.call(peer, sent_per_peer[peer], ()).is_ok() {
                                            sent_per_peer[peer] += 1;
                                            total_sent += 1;
                                        }
                                    }
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

                        for _ in 0..10 {
                            node.poll();
                            while node.try_recv().is_some() {}
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
        assert_eq!(total_responses, n as u64 * iterations * ((n - 1) as u64));
    }
}
