//! SPSC-based n-to-n communication.
//!
//! Each node pair has a dedicated SPSC channel, providing zero-contention
//! communication at the cost of O(N^2) memory usage.

use crate::spsc::{self, Serial, TryRecvError};
use crate::{RecvError, SendError};

/// A bidirectional channel to a single peer (internal).
struct FluxChannel<T: Serial> {
    peer_id: usize,
    tx: spsc::Sender<T>,
    rx: spsc::Receiver<T>,
}

impl<T: Serial> FluxChannel<T> {
    fn try_send(&mut self, value: T) -> Result<(), SendError<T>> {
        match self.tx.try_send(value) {
            Ok(None) => Ok(()),
            Ok(Some(v)) => Err(SendError::Full(v)),
            Err(spsc::SendError(v)) => Err(SendError::Disconnected(v)),
        }
    }

    fn send(&mut self, value: T) -> Result<(), SendError<T>> {
        self.tx
            .send(value)
            .map_err(|spsc::SendError(v)| SendError::Disconnected(v))
    }

    fn try_recv(&mut self) -> Result<T, TryRecvError> {
        self.rx.try_recv()
    }
}

/// A node in a Flux network.
///
/// Each node can send to and receive from any other node through dedicated
/// SPSC channels.
pub struct Flux<T: Serial> {
    id: usize,
    channels: Vec<FluxChannel<T>>,
    /// Round-robin index for try_recv
    recv_index: usize,
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

    /// Attempts to send a value to a specific peer.
    pub fn try_send(&mut self, to: usize, value: T) -> Result<(), SendError<T>> {
        match self.channels.iter_mut().find(|c| c.peer_id == to) {
            Some(ch) => ch.try_send(value),
            None => Err(SendError::InvalidPeer(value)),
        }
    }

    /// Sends a value to a specific peer, blocking until space is available.
    pub fn send(&mut self, to: usize, value: T) -> Result<(), SendError<T>> {
        match self.channels.iter_mut().find(|c| c.peer_id == to) {
            Some(ch) => ch.send(value),
            None => Err(SendError::InvalidPeer(value)),
        }
    }

    /// Attempts to receive a value from any peer (round-robin).
    ///
    /// Returns the peer ID and the value if successful.
    pub fn try_recv(&mut self) -> Result<(usize, T), RecvError> {
        let n = self.channels.len();
        if n == 0 {
            return Err(RecvError::Empty);
        }

        for _ in 0..n {
            let idx = self.recv_index;
            // Avoid expensive div instruction by using conditional branch
            self.recv_index += 1;
            if self.recv_index >= n {
                self.recv_index = 0;
            }

            match self.channels[idx].try_recv() {
                Ok(v) => return Ok((self.channels[idx].peer_id, v)),
                Err(TryRecvError::Empty) => continue,
                Err(TryRecvError::Disconnected) => continue,
            }
        }

        Err(RecvError::Empty)
    }

    /// Receives a value from any peer (round-robin), blocking until one is available.
    ///
    /// Returns the peer ID and the value if successful.
    pub fn recv(&mut self) -> Result<(usize, T), RecvError> {
        loop {
            match self.try_recv() {
                Ok(v) => return Ok(v),
                Err(RecvError::Empty) => std::hint::spin_loop(),
                Err(e) => return Err(e),
            }
        }
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
    let mut channel_pairs: Vec<Vec<Option<(spsc::Sender<T>, spsc::Receiver<T>)>>> =
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
    fn test_send_recv() {
        let mut nodes: Vec<Flux<u32>> = create_flux(2, 16);

        nodes[0].send(1, 42).unwrap();
        let (from, val) = nodes[1].recv().unwrap();
        assert_eq!(from, 0);
        assert_eq!(val, 42);

        nodes[1].send(0, 123).unwrap();
        let (from, val) = nodes[0].recv().unwrap();
        assert_eq!(from, 1);
        assert_eq!(val, 123);
    }

    #[test]
    fn test_try_recv_round_robin() {
        let mut nodes: Vec<Flux<u32>> = create_flux(3, 16);

        // Node 1 and 2 both send to node 0
        nodes[1].send(0, 100).unwrap();
        nodes[2].send(0, 200).unwrap();

        let mut received = Vec::new();
        while let Ok((from, val)) = nodes[0].try_recv() {
            received.push((from, val));
        }

        assert_eq!(received.len(), 2);
        assert!(received.contains(&(1, 100)));
        assert!(received.contains(&(2, 200)));
    }

    #[test]
    fn test_invalid_peer() {
        let mut nodes: Vec<Flux<u32>> = create_flux(2, 16);

        assert!(matches!(
            nodes[0].try_send(5, 42),
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
                        Err(RecvError::Disconnected) => panic!("disconnected"),
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
