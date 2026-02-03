//! SPSC-based n-to-n communication with callback-based response handling.
//!
//! Each node pair has a dedicated SPSC channel, providing zero-contention
//! communication at the cost of O(N^2) memory usage.
//!
//! This module uses a copyrpc-style API:
//! - `call(to, payload, user_data)` writes a request (user_data tracked internally)
//! - `poll()` processes messages (auto-flush, responses invoke callback)
//! - `try_recv()` returns received requests via `RecvHandle`
//! - Response handling is done via callbacks, not manual req_num matching

use std::collections::VecDeque;

use slab::Slab;

use crate::spsc::{self, Serial};
use crate::SendError;

/// Message kind for internal protocol.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
enum MessageKind {
    Request = 1,
    Response = 2,
}

/// Internal message wrapper that includes protocol information.
#[derive(Clone, Copy)]
#[repr(C)]
struct Message<T: Serial> {
    kind: MessageKind,
    req_num: u64,
    data: T,
}

// SAFETY: Message<T> is Copy if T is Copy (which Serial implies), and has no padding issues
// since all fields are themselves Serial-compatible types.
unsafe impl<T: Serial> Serial for Message<T> {}

/// A bidirectional channel to a single peer (internal).
struct FluxChannel<T: Serial> {
    peer_id: usize,
    tx: spsc::Sender<Message<T>>,
    rx: spsc::Receiver<Message<T>>,
}

/// A received request, stored in the internal queue.
struct RecvRequest<T> {
    from: usize,
    req_num: u64,
    data: T,
}

/// Handle for a received request that allows replying.
///
/// This handle hides the internal `req_num` from users, providing a cleaner API.
pub struct RecvHandle<'a, T: Serial, U, F: FnMut(&mut U, T)> {
    flux: &'a mut Flux<T, U, F>,
    from: usize,
    req_num: u64,
    data: T,
}

impl<'a, T: Serial + Send, U, F: FnMut(&mut U, T)> RecvHandle<'a, T, U, F> {
    /// Returns the sender's node ID.
    pub fn from(&self) -> usize {
        self.from
    }

    /// Returns a copy of the request data.
    pub fn data(&self) -> T {
        self.data
    }

    /// Sends a reply to the request, consuming the handle on success.
    ///
    /// On failure, returns the handle along with the error so it can be retried.
    /// The reply is written to the buffer but not flushed. The next `poll()` call
    /// will flush all pending writes.
    pub fn reply(self, value: T) -> Result<(), (Self, SendError<T>)> {
        let msg = Message {
            kind: MessageKind::Response,
            req_num: self.req_num,
            data: value,
        };
        match self
            .flux
            .channels
            .iter_mut()
            .find(|c| c.peer_id == self.from)
        {
            Some(ch) => match ch.tx.write(msg) {
                Ok(()) => Ok(()),
                Err(spsc::SendError(m)) => Err((
                    Self {
                        flux: self.flux,
                        from: self.from,
                        req_num: self.req_num,
                        data: self.data,
                    },
                    SendError::Full(m.data),
                )),
            },
            None => Err((self, SendError::InvalidPeer(value))),
        }
    }

    /// Tries to send a reply. On failure, requeues the request for later retry.
    ///
    /// Returns `true` if the reply was sent successfully, `false` if it was requeued.
    /// Use this when you want to process multiple requests without blocking on any single one.
    pub fn reply_or_requeue(self, value: T) -> bool {
        let msg = Message {
            kind: MessageKind::Response,
            req_num: self.req_num,
            data: value,
        };
        match self
            .flux
            .channels
            .iter_mut()
            .find(|c| c.peer_id == self.from)
        {
            Some(ch) => match ch.tx.write(msg) {
                Ok(()) => true,
                Err(spsc::SendError(_)) => {
                    // Requeue the request at the front
                    self.flux.recv_queue.push_front(RecvRequest {
                        from: self.from,
                        req_num: self.req_num,
                        data: self.data,
                    });
                    false
                }
            },
            None => {
                // Invalid peer - requeue anyway to avoid data loss
                self.flux.recv_queue.push_front(RecvRequest {
                    from: self.from,
                    req_num: self.req_num,
                    data: self.data,
                });
                false
            }
        }
    }
}

/// A node in a Flux network with callback-based response handling.
///
/// Each node can send to and receive from any other node through dedicated
/// SPSC channels. Responses are handled via callbacks, eliminating manual
/// req_num management.
///
/// ## API Overview
///
/// - `call(to, value, user_data)` - Write a request to peer (user_data for callback)
/// - `poll()` - Process messages (auto-flush, responses invoke callback)
/// - `try_recv()` - Get next received request as `RecvHandle`
/// - `flush()` - Explicitly flush pending writes (poll does this automatically)
pub struct Flux<T: Serial, U, F: FnMut(&mut U, T)> {
    id: usize,
    channels: Vec<FluxChannel<T>>,
    /// Round-robin index for poll
    recv_index: usize,
    /// Next request number for call()
    next_req_num: u64,
    /// Pending calls: maps call_id (internal req_num) to user_data
    pending_calls: Slab<U>,
    /// Response callback (called for each response received)
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

    /// Writes a call to the specified peer without making it visible yet.
    ///
    /// The `user_data` is stored internally and passed to the response callback
    /// when the response arrives.
    ///
    /// The message is written to the slot but not flushed. Call `flush()` or `poll()`
    /// to make all written messages visible to peers.
    pub fn call(&mut self, to: usize, value: T, user_data: U) -> Result<(), SendError<T>> {
        let call_id = self.pending_calls.insert(user_data);
        let req_num = call_id as u64;

        let msg = Message {
            kind: MessageKind::Request,
            req_num,
            data: value,
        };

        match self.channels.iter_mut().find(|c| c.peer_id == to) {
            Some(ch) => match ch.tx.write(msg) {
                Ok(()) => {
                    self.next_req_num = self.next_req_num.wrapping_add(1);
                    Ok(())
                }
                Err(spsc::SendError(m)) => {
                    // Remove the user_data we just inserted
                    self.pending_calls.remove(call_id);
                    Err(SendError::Full(m.data))
                }
            },
            None => {
                // Remove the user_data we just inserted
                self.pending_calls.remove(call_id);
                Err(SendError::InvalidPeer(value))
            }
        }
    }

    /// Flushes all pending writes to all peers.
    ///
    /// This makes all previously written messages visible to their respective peers.
    /// Called automatically by `poll()`.
    fn flush(&mut self) {
        for ch in &mut self.channels {
            ch.tx.flush();
        }
    }

    /// Polls for messages from peers, processing them appropriately.
    ///
    /// - Automatically flushes pending writes
    /// - For responses: invokes the callback with user_data and response data
    /// - For requests: queues them for retrieval via `try_recv()`
    ///
    /// Note: SPSC's `poll()` internally syncs when the local view is empty,
    /// so a single pass over all channels is sufficient.
    pub fn poll(&mut self) {
        // Auto-flush before receiving
        self.flush();

        let n = self.channels.len();
        if n == 0 {
            return;
        }

        // Single pass: poll() auto-syncs when local view is empty
        for _ in 0..n {
            let idx = self.recv_index;
            self.recv_index = (self.recv_index + 1) % n;

            while let Some(msg) = self.channels[idx].rx.poll() {
                let peer_id = self.channels[idx].peer_id;
                match msg.kind {
                    MessageKind::Request => {
                        self.recv_queue.push_back(RecvRequest {
                            from: peer_id,
                            req_num: msg.req_num,
                            data: msg.data,
                        });
                    }
                    MessageKind::Response => {
                        let call_id = msg.req_num as usize;
                        if let Some(mut user_data) = self.pending_calls.try_remove(call_id) {
                            (self.on_response)(&mut user_data, msg.data);
                        }
                    }
                }
            }
        }
    }

    /// Tries to receive the next request from the queue.
    ///
    /// Returns a `RecvHandle` that can be used to read the data and send a reply.
    /// Returns `None` if no requests are queued (call `poll()` to receive more).
    pub fn try_recv(&mut self) -> Option<RecvHandle<'_, T, U, F>> {
        let req = self.recv_queue.pop_front()?;
        Some(RecvHandle {
            flux: self,
            from: req.from,
            req_num: req.req_num,
            data: req.data,
        })
    }
}

/// Creates a Flux network with `n` nodes.
///
/// Returns a vector of `Flux` nodes, each capable of communicating with
/// all other nodes through dedicated SPSC channels.
///
/// The `on_response` callback is invoked for each response received, with
/// the user_data that was passed to the corresponding `call()` and the
/// response data.
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

    // Create all channel pairs
    // For nodes i and j (i < j), we create two SPSC channels:
    // - i -> j
    // - j -> i
    let mut channel_pairs: Vec<Vec<Option<(spsc::Sender<Message<T>>, spsc::Receiver<Message<T>>)>>> =
        (0..n).map(|_| (0..n).map(|_| None).collect()).collect();

    for i in 0..n {
        for j in (i + 1)..n {
            // Channel from i to j
            let (tx_i_j, rx_i_j) = spsc::channel(capacity);
            // Channel from j to i
            let (tx_j_i, rx_j_i) = spsc::channel(capacity);

            channel_pairs[i][j] = Some((tx_i_j, rx_j_i)); // i sends to j, i receives from j
            channel_pairs[j][i] = Some((tx_j_i, rx_i_j)); // j sends to i, j receives from i
        }
    }

    // Build nodes
    let mut nodes = Vec::with_capacity(n);

    for i in 0..n {
        let mut channels = Vec::with_capacity(n - 1);

        for j in 0..n {
            if i == j {
                continue;
            }

            if let Some((tx, rx)) = channel_pairs[i][j].take() {
                channels.push(FluxChannel {
                    peer_id: j,
                    tx,
                    rx,
                });
            }
        }

        nodes.push(Flux {
            id: i,
            channels,
            recv_index: 0,
            next_req_num: 0,
            pending_calls: Slab::new(),
            on_response: on_response.clone(),
            recv_queue: VecDeque::new(),
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
        nodes[0].poll(); // flush

        // Node 1 receives and replies
        nodes[1].poll();
        let handle = nodes[1].try_recv().unwrap();
        assert_eq!(handle.from(), 0);
        assert_eq!(handle.data(), 42);
        assert!(handle.reply(43).is_ok());
        nodes[1].poll(); // flush reply

        // Node 0 receives response (callback is invoked)
        nodes[0].poll();

        let responses = responses.borrow();
        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0], (100, 43)); // user_data=100, response=43
    }

    #[test]
    fn test_poll_round_robin() {
        let mut nodes: Vec<Flux<u32, (), _>> = create_flux(3, 16, |_, _| {});

        // Node 1 and 2 both send to node 0
        nodes[1].call(0, 100, ()).unwrap();
        nodes[1].poll(); // flush
        nodes[2].call(0, 200, ()).unwrap();
        nodes[2].poll(); // flush

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
                node.poll(); // final flush

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
            assert_eq!(h.join().unwrap(), 300); // 3 peers * 100 messages
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

        // Global response counter shared by all callbacks
        let global_response_count = Arc::new(AtomicU64::new(0));
        let global_count_clone = Arc::clone(&global_response_count);

        // Each node's callback increments the global counter
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

                        // Send all calls first
                        let mut total_sent = 0u64;
                        while total_sent < total_sent_target || sent_replies < expected_requests {
                            // Try to send calls to all peers (with in-flight limit)
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

                            // Process received messages
                            node.poll();

                            while let Some(handle) = node.try_recv() {
                                let data = handle.data();
                                if handle.reply(data).is_ok() {
                                    sent_replies += 1;
                                }
                            }
                        }

                        // Final flush and poll to ensure all responses are processed
                        for _ in 0..10 {
                            node.poll();
                            while node.try_recv().is_some() {}
                        }

                        // Wait for all threads to complete before dropping node
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

        // Verify total responses received via callback
        let total_responses = global_response_count.load(Ordering::Relaxed);
        assert_eq!(total_responses, n as u64 * iterations * ((n - 1) as u64));
    }
}
