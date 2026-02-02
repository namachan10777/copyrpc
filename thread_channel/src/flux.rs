//! SPSC-based n-to-n communication.
//!
//! Each node pair has a dedicated SPSC channel, providing zero-contention
//! communication at the cost of O(N^2) memory usage.
//!
//! This module uses a batch-oriented API:
//! - `call()` / `reply()` write to the buffer without flushing
//! - `flush()` makes all written messages visible to peers
//! - `poll()` receives messages (auto-syncs when empty)
//! - `sync()` explicitly synchronizes with all peers

use crate::spsc::{self, Serial};
use crate::{ReceivedMessage, SendError};

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

/// A node in a Flux network.
///
/// Each node can send to and receive from any other node through dedicated
/// SPSC channels.
///
/// ## API Overview
///
/// - `call(to, value)` - Write a request to peer (returns request number)
/// - `reply(to, req_num, value)` - Write a reply to peer
/// - `flush()` - Make all written messages visible to peers
/// - `poll()` - Receive a message (auto-syncs when empty)
/// - `sync()` - Explicitly synchronize with all peers
pub struct Flux<T: Serial> {
    id: usize,
    channels: Vec<FluxChannel<T>>,
    /// Round-robin index for poll
    recv_index: usize,
    /// Next request number for call()
    next_req_num: u64,
}

impl<T: Serial + Send> Flux<T> {
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
    /// The message is written to the slot but not flushed. Call `flush()` to make
    /// all written messages visible to peers.
    ///
    /// Returns the request number on success.
    pub fn call(&mut self, to: usize, value: T) -> Result<u64, SendError<T>> {
        let req_num = self.next_req_num;
        let msg = Message {
            kind: MessageKind::Request,
            req_num,
            data: value,
        };
        match self.channels.iter_mut().find(|c| c.peer_id == to) {
            Some(ch) => match ch.tx.write(msg) {
                Ok(()) => {
                    self.next_req_num = self.next_req_num.wrapping_add(1);
                    Ok(req_num)
                }
                Err(spsc::SendError(m)) => Err(SendError::Full(m.data)),
            },
            None => Err(SendError::InvalidPeer(value)),
        }
    }

    /// Writes a reply to the specified peer without making it visible yet.
    ///
    /// Call `flush()` to make all written messages visible to peers.
    pub fn reply(&mut self, to: usize, req_num: u64, value: T) -> Result<(), SendError<T>> {
        let msg = Message {
            kind: MessageKind::Response,
            req_num,
            data: value,
        };
        match self.channels.iter_mut().find(|c| c.peer_id == to) {
            Some(ch) => ch.tx.write(msg).map_err(|spsc::SendError(m)| SendError::Full(m.data)),
            None => Err(SendError::InvalidPeer(value)),
        }
    }

    /// Flushes all pending writes to all peers.
    ///
    /// This makes all previously written messages visible to their respective peers.
    pub fn flush(&mut self) {
        for ch in &mut self.channels {
            ch.tx.flush();
        }
    }

    /// Synchronizes with all peers.
    ///
    /// This loads each sender's tail (to see new data) and stores our head
    /// (to notify senders of consumed slots).
    pub fn sync(&mut self) {
        for ch in &mut self.channels {
            ch.rx.sync();
        }
    }

    /// Polls for a message from any peer (round-robin).
    ///
    /// Automatically syncs when all channels appear empty, so you don't need
    /// to call `sync()` before `poll()`.
    ///
    /// Returns the peer ID and the received message if data is available.
    pub fn poll(&mut self) -> Option<(usize, ReceivedMessage<T>)> {
        let n = self.channels.len();
        if n == 0 {
            return None;
        }

        // First pass: check local views
        for _ in 0..n {
            let idx = self.recv_index;
            self.recv_index += 1;
            if self.recv_index >= n {
                self.recv_index = 0;
            }

            if let Some(msg) = self.channels[idx].rx.poll() {
                let peer_id = self.channels[idx].peer_id;
                let received = match msg.kind {
                    MessageKind::Request => ReceivedMessage::Request {
                        req_num: msg.req_num,
                        data: msg.data,
                    },
                    MessageKind::Response => ReceivedMessage::Response {
                        req_num: msg.req_num,
                        data: msg.data,
                    },
                };
                return Some((peer_id, received));
            }
        }

        // All channels empty in local view, sync and try again
        self.sync();

        for _ in 0..n {
            let idx = self.recv_index;
            self.recv_index += 1;
            if self.recv_index >= n {
                self.recv_index = 0;
            }

            if let Some(msg) = self.channels[idx].rx.poll() {
                let peer_id = self.channels[idx].peer_id;
                let received = match msg.kind {
                    MessageKind::Request => ReceivedMessage::Request {
                        req_num: msg.req_num,
                        data: msg.data,
                    },
                    MessageKind::Response => ReceivedMessage::Response {
                        req_num: msg.req_num,
                        data: msg.data,
                    },
                };
                return Some((peer_id, received));
            }
        }

        None
    }
}

/// Creates a Flux network with `n` nodes.
///
/// Returns a vector of `Flux` nodes, each capable of communicating with
/// all other nodes through dedicated SPSC channels.
///
/// # Panics
/// Panics if `n` is 0 or `capacity` is 0.
pub fn create_flux<T: Serial + Send>(n: usize, capacity: usize) -> Vec<Flux<T>> {
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
        });
    }

    nodes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_flux() {
        let nodes: Vec<Flux<u32>> = create_flux(3, 16);
        assert_eq!(nodes.len(), 3);
        assert_eq!(nodes[0].num_peers(), 2);
        assert_eq!(nodes[1].num_peers(), 2);
        assert_eq!(nodes[2].num_peers(), 2);
    }

    #[test]
    fn test_call_reply() {
        let mut nodes: Vec<Flux<u32>> = create_flux(2, 16);

        let req_num = nodes[0].call(1, 42).unwrap();
        nodes[0].flush();
        assert_eq!(req_num, 0);

        let (from, msg) = nodes[1].poll().unwrap();
        assert_eq!(from, 0);
        match msg {
            ReceivedMessage::Request { req_num: r, data } => {
                assert_eq!(r, 0);
                assert_eq!(data, 42);
                nodes[1].reply(0, r, data + 1).unwrap();
                nodes[1].flush();
            }
            _ => panic!("expected Request"),
        }

        let (from, msg) = nodes[0].poll().unwrap();
        assert_eq!(from, 1);
        match msg {
            ReceivedMessage::Response { req_num: r, data } => {
                assert_eq!(r, 0);
                assert_eq!(data, 43);
            }
            _ => panic!("expected Response"),
        }
    }

    #[test]
    fn test_poll_round_robin() {
        let mut nodes: Vec<Flux<u32>> = create_flux(3, 16);

        // Node 1 and 2 both send to node 0
        nodes[1].call(0, 100).unwrap();
        nodes[1].flush();
        nodes[2].call(0, 200).unwrap();
        nodes[2].flush();

        let mut received = Vec::new();
        while let Some((from, ReceivedMessage::Request { data, .. })) = nodes[0].poll() {
            received.push((from, data));
            if received.len() >= 2 {
                break;
            }
        }

        assert_eq!(received.len(), 2);
        assert!(received.contains(&(1, 100)));
        assert!(received.contains(&(2, 200)));
    }

    #[test]
    fn test_invalid_peer() {
        let mut nodes: Vec<Flux<u32>> = create_flux(2, 16);

        assert!(matches!(
            nodes[0].call(5, 42),
            Err(SendError::InvalidPeer(42))
        ));
    }

    #[test]
    fn test_threaded() {
        use std::thread;

        let nodes: Vec<Flux<u64>> = create_flux(4, 1024);
        let mut handles = Vec::new();

        for mut node in nodes {
            handles.push(thread::spawn(move || {
                let id = node.id();
                let n = node.num_peers() + 1;

                // Send to all peers
                for peer in 0..n {
                    if peer != id {
                        for i in 0..100 {
                            loop {
                                match node.call(peer, (id * 1000 + i) as u64) {
                                    Ok(_) => break,
                                    Err(SendError::Full(_)) => {
                                        node.flush();
                                        std::hint::spin_loop();
                                    }
                                    Err(e) => panic!("send error: {:?}", e),
                                }
                            }
                        }
                    }
                }
                node.flush();

                // Receive from all peers
                let mut count = 0;
                let expected = (n - 1) * 100;
                while count < expected {
                    if node.poll().is_some() {
                        count += 1;
                    }
                }

                count
            }));
        }

        for h in handles {
            assert_eq!(h.join().unwrap(), 300); // 3 peers * 100 messages
        }
    }

    #[test]
    fn test_threaded_all_to_all_call() {
        use std::sync::{Arc, Barrier};

        let n = 2;
        let iterations = 100u64;
        let capacity = 1024;
        let max_in_flight = (capacity / 4).max(1) as u64;

        let nodes: Vec<Flux<u64>> = create_flux(n, capacity);
        let end_barrier = Arc::new(Barrier::new(n));

        std::thread::scope(|s| {
            let handles: Vec<_> = nodes
                .into_iter()
                .map(|mut node| {
                    let end_barrier = Arc::clone(&end_barrier);
                    s.spawn(move || {
                        let id = node.id();
                        let num_peers = node.num_peers();
                        let peers: Vec<usize> = (0..n).filter(|&p| p != id).collect();

                        let mut sent_per_peer = vec![0u64; n];
                        let mut completed_per_peer = vec![0u64; n];
                        let mut completed = 0u64;
                        let expected_responses = iterations * (num_peers as u64);
                        let expected_requests = iterations * (num_peers as u64);
                        let mut pending_replies: Vec<(usize, u64, u64)> = Vec::new();
                        let mut sent_replies = 0u64;

                        // Loop until:
                        // 1. All responses received (completed == expected_responses)
                        // 2. All incoming requests processed (sent_replies == expected_requests)
                        while completed < expected_responses || (sent_replies + pending_replies.len() as u64) < expected_requests {
                            // Try to flush pending replies first
                            let old_pending = pending_replies.len();
                            pending_replies.retain(|&(to, req_num, data)| {
                                node.reply(to, req_num, data).is_err()
                            });
                            sent_replies += (old_pending - pending_replies.len()) as u64;

                            // Try to send calls to all peers (with in-flight limit)
                            for &peer in &peers {
                                if sent_per_peer[peer] < iterations {
                                    let in_flight = sent_per_peer[peer] - completed_per_peer[peer];
                                    if in_flight < max_in_flight {
                                        if node.call(peer, sent_per_peer[peer]).is_ok() {
                                            sent_per_peer[peer] += 1;
                                        }
                                    }
                                }
                            }
                            node.flush();

                            // Process received messages
                            while let Some((from, msg)) = node.poll() {
                                match msg {
                                    ReceivedMessage::Request { req_num, data } => {
                                        if node.reply(from, req_num, data).is_ok() {
                                            sent_replies += 1;
                                        } else {
                                            pending_replies.push((from, req_num, data));
                                        }
                                    }
                                    ReceivedMessage::Response { .. } => {
                                        completed_per_peer[from] += 1;
                                        completed += 1;
                                    }
                                    ReceivedMessage::Notify(_) => {}
                                }
                            }
                            node.flush();
                        }

                        // Wait for all threads to complete before dropping node
                        end_barrier.wait();
                        completed
                    })
                })
                .collect();

            for h in handles {
                let result = h.join().unwrap();
                assert_eq!(result, iterations * ((n - 1) as u64));
            }
        });
    }
}
