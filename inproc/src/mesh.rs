//! MPSC-based n-to-n communication.
//!
//! Each node has a single MPSC receive queue shared by all senders.
//! This uses O(N) channels but has potential contention when multiple
//! nodes send to the same target.
//!
//! The `Mesh` struct is generic over the MPSC channel implementation:
//! - `StdMpsc`: default, uses `std::sync::mpsc`
//! - `CrossbeamMpsc` (feature `crossbeam`): uses `crossbeam-channel`

use std::marker::PhantomData;

use crate::mpsc::{
    MpscChannel, MpscChannelReceiver, MpscChannelSender, SendError, StdMpsc, TryRecvError,
};
use crate::serial::Serial;
use crate::{ReceivedMessage, RecvError, SendError as CrateSendError};

/// Sender matrix: `all_senders[j][i]` holds node j's sender to node i's receive queue.
type SenderMatrix<T, M> = Vec<Vec<Option<<M as MpscChannel>::Sender<TaggedMessage<T>>>>>;

/// Default Mesh type with no-op callback for responses.
pub type DefaultMesh<T> = Mesh<T, (), fn((), T), StdMpsc>;

#[cfg(feature = "crossbeam")]
pub use crate::mpsc::CrossbeamMpsc;

/// Message kind for internal protocol.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
enum MessageKind {
    Notify = 0,
    Request = 1,
    Response = 2,
}

/// A message tagged with the sender's ID (internal).
struct TaggedMessage<T> {
    from: usize,
    kind: MessageKind,
    req_num: u64,
    data: T,
}

/// A node in a Mesh network.
///
/// Each node has a single receive queue that all other nodes send to.
///
/// The `M` type parameter selects the MPSC channel implementation.
/// Use [`create_mesh`] for the default `StdMpsc` implementation, or
/// [`create_mesh_with`] to select a different implementation.
pub struct Mesh<T: Send, U = (), F: FnMut(U, T) = fn((), T), M: MpscChannel = StdMpsc> {
    id: usize,
    num_nodes: usize,
    rx: M::Receiver<TaggedMessage<T>>,
    txs: Vec<Option<M::Sender<TaggedMessage<T>>>>,
    next_req_num: u64,
    pending_calls: Vec<Option<U>>,
    pending_capacity: usize,
    on_response: F,
    _marker: PhantomData<M>,
}

impl<T: Serial + Send, U, F: FnMut(U, T), M: MpscChannel> Mesh<T, U, F, M> {
    /// Returns this node's ID.
    pub fn id(&self) -> usize {
        self.id
    }

    /// Returns the number of peers (excluding self).
    pub fn num_peers(&self) -> usize {
        self.num_nodes - 1
    }

    /// Sends a one-way notification to a specific peer.
    pub fn notify(&self, to: usize, value: T) -> Result<(), CrateSendError<T>> {
        if to >= self.num_nodes || to == self.id {
            return Err(CrateSendError::InvalidPeer(value));
        }

        match &self.txs[to] {
            Some(tx) => tx
                .send(TaggedMessage {
                    from: self.id,
                    kind: MessageKind::Notify,
                    req_num: 0,
                    data: value,
                })
                .map_err(|SendError(msg)| CrateSendError::Disconnected(msg.data)),
            None => Err(CrateSendError::InvalidPeer(value)),
        }
    }

    /// Sends a request to a specific peer.
    ///
    /// Returns the request number on success, which can be used to match the response.
    pub fn call(&mut self, to: usize, value: T, user_data: U) -> Result<u64, CrateSendError<T>> {
        if to >= self.num_nodes || to == self.id {
            return Err(CrateSendError::InvalidPeer(value));
        }

        let req_num = self.next_req_num;
        match &self.txs[to] {
            Some(tx) => match tx.send(TaggedMessage {
                from: self.id,
                kind: MessageKind::Request,
                req_num,
                data: value,
            }) {
                Ok(()) => {
                    self.next_req_num = self.next_req_num.wrapping_add(1);
                    let slot = (req_num as usize) % self.pending_capacity;
                    self.pending_calls[slot] = Some(user_data);
                    Ok(req_num)
                }
                Err(SendError(msg)) => Err(CrateSendError::Disconnected(msg.data)),
            },
            None => Err(CrateSendError::InvalidPeer(value)),
        }
    }

    /// Sends a reply to a previous request.
    pub fn reply(&self, to: usize, req_num: u64, value: T) -> Result<(), CrateSendError<T>> {
        if to >= self.num_nodes || to == self.id {
            return Err(CrateSendError::InvalidPeer(value));
        }

        match &self.txs[to] {
            Some(tx) => tx
                .send(TaggedMessage {
                    from: self.id,
                    kind: MessageKind::Response,
                    req_num,
                    data: value,
                })
                .map_err(|SendError(msg)| CrateSendError::Disconnected(msg.data)),
            None => Err(CrateSendError::InvalidPeer(value)),
        }
    }

    /// Attempts to receive a message from any peer.
    ///
    /// Returns the sender's ID and the received message if successful.
    pub fn try_recv(&mut self) -> Result<(usize, ReceivedMessage<T>), RecvError> {
        match self.rx.try_recv() {
            Ok(msg) => {
                let received = match msg.kind {
                    MessageKind::Notify => ReceivedMessage::Notify(msg.data),
                    MessageKind::Request => ReceivedMessage::Request {
                        req_num: msg.req_num,
                        data: msg.data,
                    },
                    MessageKind::Response => {
                        let slot = (msg.req_num as usize) % self.pending_capacity;
                        if let Some(user_data) = self.pending_calls[slot].take() {
                            (self.on_response)(user_data, msg.data);
                        }
                        ReceivedMessage::Response {
                            req_num: msg.req_num,
                            data: msg.data,
                        }
                    }
                };
                Ok((msg.from, received))
            }
            Err(TryRecvError::Empty) => Err(RecvError::Empty),
            Err(TryRecvError::Disconnected) => Err(RecvError::Disconnected),
        }
    }

    /// Receives a message from any peer, blocking until one is available.
    pub fn recv(&mut self) -> Result<(usize, ReceivedMessage<T>), RecvError> {
        match self.rx.recv() {
            Ok(msg) => {
                let received = match msg.kind {
                    MessageKind::Notify => ReceivedMessage::Notify(msg.data),
                    MessageKind::Request => ReceivedMessage::Request {
                        req_num: msg.req_num,
                        data: msg.data,
                    },
                    MessageKind::Response => {
                        let slot = (msg.req_num as usize) % self.pending_capacity;
                        if let Some(user_data) = self.pending_calls[slot].take() {
                            (self.on_response)(user_data, msg.data);
                        }
                        ReceivedMessage::Response {
                            req_num: msg.req_num,
                            data: msg.data,
                        }
                    }
                };
                Ok((msg.from, received))
            }
            Err(TryRecvError::Empty) => Err(RecvError::Empty),
            Err(TryRecvError::Disconnected) => Err(RecvError::Disconnected),
        }
    }
}

/// Creates a Mesh network with `n` nodes using the default `StdMpsc` channel.
///
/// Returns a vector of `Mesh` nodes, each capable of communicating with
/// all other nodes through a single shared MPSC receive queue.
///
/// # Panics
/// Panics if `n` is 0.
#[allow(clippy::type_complexity)]
pub fn create_mesh<T: Serial + Send>(n: usize) -> Vec<Mesh<T, (), fn((), T), StdMpsc>> {
    create_mesh_with::<T, (), fn((), T), StdMpsc>(n, |(), _| {})
}

/// Creates a Mesh network with `n` nodes using a custom MPSC channel implementation.
///
/// Returns a vector of `Mesh` nodes, each capable of communicating with
/// all other nodes through a single shared MPSC receive queue.
///
/// # Panics
/// Panics if `n` is 0.
pub fn create_mesh_with<T: Serial + Send, U, F: FnMut(U, T) + Clone, M: MpscChannel>(n: usize, on_response: F) -> Vec<Mesh<T, U, F, M>> {
    assert!(n > 0, "must have at least one node");

    // Create receivers and collect senders
    let mut receivers = Vec::with_capacity(n);
    let mut all_senders: SenderMatrix<T, M> =
        (0..n).map(|_| (0..n).map(|_| None).collect()).collect();

    #[allow(clippy::needless_range_loop)]
    for i in 0..n {
        let (tx, rx) = M::channel();
        receivers.push(rx);

        // All other nodes get a clone of this sender
        for j in 0..n {
            if i != j {
                all_senders[j][i] = Some(tx.clone());
            }
        }
    }

    // Build nodes
    let pending_capacity = 1024usize.next_power_of_two();
    receivers
        .into_iter()
        .enumerate()
        .zip(all_senders)
        .map(|((id, rx), txs)| Mesh {
            id,
            num_nodes: n,
            rx,
            txs,
            next_req_num: 0,
            pending_calls: (0..pending_capacity).map(|_| None).collect(),
            pending_capacity,
            on_response: on_response.clone(),
            _marker: PhantomData,
        })
        .collect()
}

// ============================================================================
// Backward compatibility: specialized implementation for StdMpsc
// ============================================================================

impl<T: Serial + Send> Mesh<T, (), fn((), T), StdMpsc> {
    /// Creates a Mesh network with `n` nodes using the default channel.
    ///
    /// This is a convenience method equivalent to `create_mesh(n)`.
    #[allow(dead_code)]
    fn new(n: usize) -> Vec<Self> {
        create_mesh(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_mesh() {
        let nodes = create_mesh::<u32>(3);
        assert_eq!(nodes.len(), 3);
        assert_eq!(nodes[0].num_peers(), 2);
        assert_eq!(nodes[1].num_peers(), 2);
        assert_eq!(nodes[2].num_peers(), 2);
    }

    #[test]
    fn test_notify_recv() {
        let mut nodes = create_mesh::<u32>(2);

        nodes[0].notify(1, 42).unwrap();
        assert_eq!(
            nodes[1].try_recv().unwrap(),
            (0, ReceivedMessage::Notify(42))
        );

        nodes[1].notify(0, 123).unwrap();
        assert_eq!(
            nodes[0].try_recv().unwrap(),
            (1, ReceivedMessage::Notify(123))
        );
    }

    #[test]
    fn test_call_reply() {
        let mut nodes = create_mesh::<u32>(2);

        let req_num = nodes[0].call(1, 42, ()).unwrap();
        assert_eq!(req_num, 0);

        let (from, msg) = nodes[1].recv().unwrap();
        assert_eq!(from, 0);
        match msg {
            ReceivedMessage::Request { req_num: r, data } => {
                assert_eq!(r, 0);
                assert_eq!(data, 42);
                nodes[1].reply(0, r, data + 1).unwrap();
            }
            _ => panic!("expected Request"),
        }

        let (from, msg) = nodes[0].recv().unwrap();
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
    fn test_recv_from_multiple() {
        let mut nodes = create_mesh::<u32>(3);

        // Nodes 1 and 2 both send to node 0
        nodes[1].notify(0, 100).unwrap();
        nodes[2].notify(0, 200).unwrap();

        let mut received = Vec::new();
        for _ in 0..2 {
            match nodes[0].try_recv() {
                Ok((from, ReceivedMessage::Notify(val))) => received.push((from, val)),
                _ => break,
            }
        }

        assert_eq!(received.len(), 2);
        assert!(received.contains(&(1, 100)));
        assert!(received.contains(&(2, 200)));
    }

    #[test]
    fn test_invalid_peer() {
        let nodes = create_mesh::<u32>(2);

        assert!(matches!(
            nodes[0].notify(5, 42),
            Err(CrateSendError::InvalidPeer(42))
        ));
        assert!(matches!(
            nodes[0].notify(0, 42),
            Err(CrateSendError::InvalidPeer(42))
        )); // Can't send to self
    }

    #[test]
    fn test_threaded() {
        use std::thread;

        let nodes = create_mesh::<u64>(4);
        let mut handles = Vec::new();

        for mut node in nodes {
            handles.push(thread::spawn(move || {
                let id = node.id();
                let n = node.num_peers() + 1;

                // Send to all peers
                for peer in 0..n {
                    if peer != id {
                        for i in 0..100 {
                            node.notify(peer, (id * 1000 + i) as u64).unwrap();
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

    #[cfg(feature = "crossbeam")]
    #[test]
    fn test_crossbeam_mesh() {
        let mut nodes = create_mesh_with::<u32, (), fn((), u32), CrossbeamMpsc>(2, |(), _| {});

        nodes[0].notify(1, 42).unwrap();
        assert_eq!(
            nodes[1].try_recv().unwrap(),
            (0, ReceivedMessage::Notify(42))
        );

        nodes[1].notify(0, 123).unwrap();
        assert_eq!(
            nodes[0].try_recv().unwrap(),
            (1, ReceivedMessage::Notify(123))
        );
    }

    #[cfg(feature = "crossbeam")]
    #[test]
    fn test_crossbeam_mesh_threaded() {
        use std::thread;

        let nodes = create_mesh_with::<u64, (), fn((), u64), CrossbeamMpsc>(4, |(), _| {});
        let mut handles = Vec::new();

        for mut node in nodes {
            handles.push(thread::spawn(move || {
                let id = node.id();
                let n = node.num_peers() + 1;

                // Send to all peers
                for peer in 0..n {
                    if peer != id {
                        for i in 0..100 {
                            node.notify(peer, (id * 1000 + i) as u64).unwrap();
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

    #[test]
    fn test_callback_on_response() {
        use std::sync::{Arc, Mutex};

        let callback_data = Arc::new(Mutex::new(Vec::new()));
        let callback_data_clone = callback_data.clone();

        let mut nodes = create_mesh_with::<u32, u32, _, StdMpsc>(2, move |user_data, response| {
            callback_data_clone.lock().unwrap().push((user_data, response));
        });

        // Send request with user_data = 100
        let req_num = nodes[0].call(1, 42, 100).unwrap();
        assert_eq!(req_num, 0);

        // Server receives and replies
        let (from, msg) = nodes[1].recv().unwrap();
        assert_eq!(from, 0);
        match msg {
            ReceivedMessage::Request { req_num: r, data } => {
                assert_eq!(r, 0);
                assert_eq!(data, 42);
                nodes[1].reply(0, r, data + 1).unwrap();
            }
            _ => panic!("expected Request"),
        }

        // Client receives response - callback should be invoked
        let (from, msg) = nodes[0].recv().unwrap();
        assert_eq!(from, 1);
        match msg {
            ReceivedMessage::Response { req_num: r, data } => {
                assert_eq!(r, 0);
                assert_eq!(data, 43);
            }
            _ => panic!("expected Response"),
        }

        // Verify callback was invoked with correct data
        let callback_results = callback_data.lock().unwrap();
        assert_eq!(callback_results.len(), 1);
        assert_eq!(callback_results[0], (100, 43));
    }
}
