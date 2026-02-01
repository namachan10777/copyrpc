//! MPSC-based n-to-n communication.
//!
//! Each node has a single MPSC receive queue shared by all senders.
//! This uses O(N) channels but has potential contention when multiple
//! nodes send to the same target.

use crate::mpsc::{self, MpscReceiver, MpscSender};
use crate::{RecvError, SendError};

/// A message tagged with the sender's ID (internal).
struct TaggedMessage<T> {
    from: usize,
    data: T,
}

/// A node in a Mesh network.
///
/// Each node has a single receive queue that all other nodes send to.
pub struct Mesh<T> {
    id: usize,
    num_nodes: usize,
    rx: MpscReceiver<TaggedMessage<T>>,
    txs: Vec<Option<MpscSender<TaggedMessage<T>>>>,
}

impl<T: Send> Mesh<T> {
    /// Returns this node's ID.
    pub fn id(&self) -> usize {
        self.id
    }

    /// Returns the number of peers (excluding self).
    pub fn num_peers(&self) -> usize {
        self.num_nodes - 1
    }

    /// Sends a value to a specific peer.
    pub fn send(&self, to: usize, value: T) -> Result<(), SendError<T>> {
        if to >= self.num_nodes || to == self.id {
            return Err(SendError::InvalidPeer(value));
        }

        match &self.txs[to] {
            Some(tx) => tx
                .send(TaggedMessage {
                    from: self.id,
                    data: value,
                })
                .map_err(|mpsc::SendError(msg)| SendError::Disconnected(msg.data)),
            None => Err(SendError::InvalidPeer(value)),
        }
    }

    /// Attempts to receive a value from any peer.
    ///
    /// Returns the sender's ID and the value if successful.
    pub fn try_recv(&mut self) -> Result<(usize, T), RecvError> {
        match self.rx.try_recv() {
            Ok(msg) => Ok((msg.from, msg.data)),
            Err(mpsc::TryRecvError::Empty) => Err(RecvError::Empty),
            Err(mpsc::TryRecvError::Disconnected) => Err(RecvError::Disconnected),
        }
    }

    /// Receives a value from any peer, blocking until one is available.
    pub fn recv(&mut self) -> Result<(usize, T), RecvError> {
        match self.rx.recv() {
            Ok(msg) => Ok((msg.from, msg.data)),
            Err(mpsc::TryRecvError::Empty) => Err(RecvError::Empty),
            Err(mpsc::TryRecvError::Disconnected) => Err(RecvError::Disconnected),
        }
    }
}

/// Creates a Mesh network with `n` nodes.
///
/// Returns a vector of `Mesh` nodes, each capable of communicating with
/// all other nodes through a single shared MPSC receive queue.
///
/// # Panics
/// Panics if `n` is 0.
pub fn create_mesh<T: Send>(n: usize) -> Vec<Mesh<T>> {
    assert!(n > 0, "must have at least one node");

    // Create receivers and collect senders
    let mut receivers = Vec::with_capacity(n);
    let mut all_senders: Vec<Vec<Option<MpscSender<TaggedMessage<T>>>>> =
        (0..n).map(|_| (0..n).map(|_| None).collect()).collect();

    for i in 0..n {
        let (tx, rx) = mpsc::channel();
        receivers.push(rx);

        // All other nodes get a clone of this sender
        for j in 0..n {
            if i != j {
                all_senders[j][i] = Some(tx.clone());
            }
        }
    }

    // Build nodes
    receivers
        .into_iter()
        .enumerate()
        .zip(all_senders)
        .map(|((id, rx), txs)| Mesh {
            id,
            num_nodes: n,
            rx,
            txs,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_mesh() {
        let nodes: Vec<Mesh<u32>> = create_mesh(3);
        assert_eq!(nodes.len(), 3);
        assert_eq!(nodes[0].num_peers(), 2);
        assert_eq!(nodes[1].num_peers(), 2);
        assert_eq!(nodes[2].num_peers(), 2);
    }

    #[test]
    fn test_send_recv() {
        let mut nodes: Vec<Mesh<u32>> = create_mesh(2);

        nodes[0].send(1, 42).unwrap();
        assert_eq!(nodes[1].try_recv().unwrap(), (0, 42));

        nodes[1].send(0, 123).unwrap();
        assert_eq!(nodes[0].try_recv().unwrap(), (1, 123));
    }

    #[test]
    fn test_recv_from_multiple() {
        let mut nodes: Vec<Mesh<u32>> = create_mesh(3);

        // Nodes 1 and 2 both send to node 0
        nodes[1].send(0, 100).unwrap();
        nodes[2].send(0, 200).unwrap();

        let mut received = Vec::new();
        for _ in 0..2 {
            match nodes[0].try_recv() {
                Ok((from, val)) => received.push((from, val)),
                Err(_) => break,
            }
        }

        assert_eq!(received.len(), 2);
        assert!(received.contains(&(1, 100)));
        assert!(received.contains(&(2, 200)));
    }

    #[test]
    fn test_invalid_peer() {
        let nodes: Vec<Mesh<u32>> = create_mesh(2);

        assert!(matches!(
            nodes[0].send(5, 42),
            Err(SendError::InvalidPeer(42))
        ));
        assert!(matches!(
            nodes[0].send(0, 42),
            Err(SendError::InvalidPeer(42))
        )); // Can't send to self
    }

    #[test]
    fn test_threaded() {
        use std::thread;

        let nodes: Vec<Mesh<u64>> = create_mesh(4);
        let mut handles = Vec::new();

        for mut node in nodes {
            handles.push(thread::spawn(move || {
                let id = node.id();
                let n = node.num_peers() + 1;

                // Send to all peers
                for peer in 0..n {
                    if peer != id {
                        for i in 0..100 {
                            node.send(peer, (id * 1000 + i) as u64).unwrap();
                        }
                    }
                }

                // Receive from all peers
                let mut count = 0;
                let expected = (n - 1) * 100;
                while count < expected {
                    match node.try_recv() {
                        Ok(_) => count += 1,
                        Err(RecvError::Empty) => std::hint::spin_loop(),
                        Err(RecvError::Disconnected) => break,
                    }
                }

                count
            }));
        }

        for h in handles {
            assert_eq!(h.join().unwrap(), 300); // 3 peers * 100 messages
        }
    }
}
