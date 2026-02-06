//! Multi-producer single-consumer channel.
//!
//! This module provides:
//! - `MpscSender` / `MpscReceiver`: concrete wrapper around `std::sync::mpsc`
//! - `MpscChannelSender` / `MpscChannelReceiver` traits: abstract MPSC interface
//! - `MpscChannel` trait: factory for creating MPSC channels
//! - `StdMpsc`: default implementation using `std::sync::mpsc`
//!
//! Optional implementations:
//! - `OmangoMpsc` (feature: `omango`): uses omango MPMC as MPSC
//! - `CrossbeamMpsc` (feature: `crossbeam`): uses crossbeam-channel

use std::sync::mpsc;

// ============================================================================
// Abstract MPSC Traits
// ============================================================================

/// Abstract MPSC channel sender trait.
pub trait MpscChannelSender<T>: Clone + Send {
    /// Sends a value on this channel.
    fn send(&self, value: T) -> Result<(), SendError<T>>;
}

/// Abstract MPSC channel receiver trait.
pub trait MpscChannelReceiver<T>: Send {
    /// Attempts to receive a value without blocking.
    fn try_recv(&self) -> Result<T, TryRecvError>;

    /// Receives a value, blocking until one is available.
    fn recv(&self) -> Result<T, TryRecvError>;
}

/// Factory trait for creating MPSC channels.
pub trait MpscChannel: 'static + Send + Sync {
    /// The sender type for this channel.
    type Sender<T: Send>: MpscChannelSender<T>;
    /// The receiver type for this channel.
    type Receiver<T: Send>: MpscChannelReceiver<T>;

    /// Creates a new MPSC channel.
    fn channel<T: Send>() -> (Self::Sender<T>, Self::Receiver<T>);
}

// ============================================================================
// StdMpsc Implementation (default)
// ============================================================================

/// Default MPSC implementation using `std::sync::mpsc`.
pub struct StdMpsc;

impl MpscChannel for StdMpsc {
    type Sender<T: Send> = MpscSender<T>;
    type Receiver<T: Send> = MpscReceiver<T>;

    fn channel<T: Send>() -> (Self::Sender<T>, Self::Receiver<T>) {
        channel()
    }
}

// ============================================================================
// OmangoMpsc Implementation (optional)
// ============================================================================

// Note: omango MPMC does not support try_recv on unbounded queues,
// so we don't provide OmangoMpsc. Use CrossbeamMpsc or StdMpsc instead.

// ============================================================================
// CrossbeamMpsc Implementation (optional)
// ============================================================================

#[cfg(feature = "crossbeam")]
mod crossbeam_impl {
    use super::*;
    use crossbeam_channel::{Receiver as CReceiver, Sender as CSender};

    /// MPSC implementation using crossbeam-channel.
    pub struct CrossbeamMpsc;

    impl MpscChannel for CrossbeamMpsc {
        type Sender<T: Send> = CrossbeamSender<T>;
        type Receiver<T: Send> = CrossbeamReceiver<T>;

        fn channel<T: Send>() -> (Self::Sender<T>, Self::Receiver<T>) {
            let (tx, rx) = crossbeam_channel::unbounded();
            (
                CrossbeamSender { inner: tx },
                CrossbeamReceiver { inner: rx },
            )
        }
    }

    /// Sender wrapper for crossbeam-channel.
    pub struct CrossbeamSender<T> {
        inner: CSender<T>,
    }

    impl<T> Clone for CrossbeamSender<T> {
        fn clone(&self) -> Self {
            Self {
                inner: self.inner.clone(),
            }
        }
    }

    impl<T: Send> MpscChannelSender<T> for CrossbeamSender<T> {
        fn send(&self, value: T) -> Result<(), SendError<T>> {
            self.inner.send(value).map_err(|e| SendError(e.0))
        }
    }

    /// Receiver wrapper for crossbeam-channel.
    pub struct CrossbeamReceiver<T> {
        inner: CReceiver<T>,
    }

    impl<T: Send> MpscChannelReceiver<T> for CrossbeamReceiver<T> {
        fn try_recv(&self) -> Result<T, TryRecvError> {
            self.inner.try_recv().map_err(|e| match e {
                crossbeam_channel::TryRecvError::Empty => TryRecvError::Empty,
                crossbeam_channel::TryRecvError::Disconnected => TryRecvError::Disconnected,
            })
        }

        fn recv(&self) -> Result<T, TryRecvError> {
            self.inner.recv().map_err(|_| TryRecvError::Disconnected)
        }
    }
}

#[cfg(feature = "crossbeam")]
pub use crossbeam_impl::CrossbeamMpsc;

// ============================================================================
// Concrete std::sync::mpsc Implementation
// ============================================================================

/// Error returned when sending fails because the receiver has disconnected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SendError<T>(pub T);

impl<T> std::fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "sending on a disconnected channel")
    }
}

impl<T: std::fmt::Debug> std::error::Error for SendError<T> {}

impl<T> From<mpsc::SendError<T>> for SendError<T> {
    fn from(err: mpsc::SendError<T>) -> Self {
        SendError(err.0)
    }
}

/// Error returned when receiving fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TryRecvError {
    /// The channel is empty.
    Empty,
    /// All senders have disconnected.
    Disconnected,
}

impl std::fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TryRecvError::Empty => write!(f, "receiving on an empty channel"),
            TryRecvError::Disconnected => write!(f, "receiving on a disconnected channel"),
        }
    }
}

impl std::error::Error for TryRecvError {}

impl From<mpsc::TryRecvError> for TryRecvError {
    fn from(err: mpsc::TryRecvError) -> Self {
        match err {
            mpsc::TryRecvError::Empty => TryRecvError::Empty,
            mpsc::TryRecvError::Disconnected => TryRecvError::Disconnected,
        }
    }
}

/// The sending half of an MPSC channel. Can be cloned to create multiple senders.
pub struct MpscSender<T> {
    inner: mpsc::Sender<T>,
}

impl<T> Clone for MpscSender<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T> MpscSender<T> {
    /// Sends a value on this channel.
    ///
    /// Returns `Err` if the receiver has disconnected.
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        self.inner.send(value).map_err(|e| SendError(e.0))
    }
}

impl<T: Send> MpscChannelSender<T> for MpscSender<T> {
    fn send(&self, value: T) -> Result<(), SendError<T>> {
        MpscSender::send(self, value)
    }
}

/// The receiving half of an MPSC channel.
pub struct MpscReceiver<T> {
    inner: mpsc::Receiver<T>,
}

impl<T> MpscReceiver<T> {
    /// Attempts to receive a value from this channel without blocking.
    ///
    /// Returns `Err(TryRecvError::Empty)` if the channel is empty.
    /// Returns `Err(TryRecvError::Disconnected)` if all senders have disconnected.
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        self.inner.try_recv().map_err(|e| e.into())
    }

    /// Receives a value, blocking until one is available.
    ///
    /// Returns `Err` if all senders have disconnected.
    pub fn recv(&self) -> Result<T, TryRecvError> {
        self.inner.recv().map_err(|_| TryRecvError::Disconnected)
    }
}

impl<T: Send> MpscChannelReceiver<T> for MpscReceiver<T> {
    fn try_recv(&self) -> Result<T, TryRecvError> {
        MpscReceiver::try_recv(self)
    }

    fn recv(&self) -> Result<T, TryRecvError> {
        MpscReceiver::recv(self)
    }
}

/// Creates a new MPSC channel.
///
/// Note: Unlike the bounded SPSC channel, this is an unbounded channel.
pub fn channel<T>() -> (MpscSender<T>, MpscReceiver<T>) {
    let (tx, rx) = mpsc::channel();
    (MpscSender { inner: tx }, MpscReceiver { inner: rx })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    /// Generic test for MpscChannel implementations.
    fn test_mpsc_channel_impl<M: MpscChannel>() {
        let (tx, rx) = M::channel::<u64>();
        let num_senders = 4;
        let msgs_per_sender = 100;

        let handles: Vec<_> = (0..num_senders)
            .map(|i| {
                let tx = tx.clone();
                thread::spawn(move || {
                    for j in 0..msgs_per_sender {
                        tx.send(i * msgs_per_sender + j).unwrap();
                    }
                })
            })
            .collect();

        drop(tx);

        let mut received = Vec::new();
        loop {
            match rx.try_recv() {
                Ok(v) => received.push(v),
                Err(TryRecvError::Empty) => {
                    if handles.iter().all(|h| h.is_finished()) {
                        while let Ok(v) = rx.try_recv() {
                            received.push(v);
                        }
                        break;
                    }
                    std::hint::spin_loop();
                }
                Err(TryRecvError::Disconnected) => break,
            }
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(received.len(), (num_senders * msgs_per_sender) as usize);
    }

    #[test]
    fn test_std_mpsc_generic() {
        test_mpsc_channel_impl::<StdMpsc>();
    }

    #[cfg(feature = "crossbeam")]
    #[test]
    fn test_crossbeam_mpsc_generic() {
        test_mpsc_channel_impl::<CrossbeamMpsc>();
    }

    #[test]
    fn test_send_recv() {
        let (tx, rx) = channel::<u32>();

        tx.send(1).unwrap();
        tx.send(2).unwrap();

        assert_eq!(rx.try_recv().unwrap(), 1);
        assert_eq!(rx.try_recv().unwrap(), 2);
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));
    }

    #[test]
    fn test_multiple_senders() {
        let (tx, rx) = channel::<u32>();
        let tx2 = tx.clone();

        tx.send(1).unwrap();
        tx2.send(2).unwrap();

        let mut values = vec![rx.try_recv().unwrap(), rx.try_recv().unwrap()];
        values.sort();
        assert_eq!(values, vec![1, 2]);
    }

    #[test]
    fn test_sender_disconnect() {
        let (tx, rx) = channel::<u32>();

        drop(tx);

        assert!(matches!(rx.try_recv(), Err(TryRecvError::Disconnected)));
    }

    #[test]
    fn test_receiver_disconnect() {
        let (tx, rx) = channel::<u32>();

        drop(rx);

        assert!(matches!(tx.send(1), Err(SendError(1))));
    }

    #[test]
    fn test_threaded_multiple_producers() {
        let (tx, rx) = channel::<u64>();
        let num_senders = 4;
        let msgs_per_sender = 1000;

        let handles: Vec<_> = (0..num_senders)
            .map(|i| {
                let tx = tx.clone();
                thread::spawn(move || {
                    for j in 0..msgs_per_sender {
                        tx.send(i * msgs_per_sender + j).unwrap();
                    }
                })
            })
            .collect();

        drop(tx); // Drop original sender

        let mut received = Vec::new();
        loop {
            match rx.try_recv() {
                Ok(v) => received.push(v),
                Err(TryRecvError::Empty) => {
                    if handles.iter().all(|h| h.is_finished()) {
                        // Try one more time after all threads finished
                        while let Ok(v) = rx.try_recv() {
                            received.push(v);
                        }
                        break;
                    }
                    std::hint::spin_loop();
                }
                Err(TryRecvError::Disconnected) => break,
            }
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(received.len(), (num_senders * msgs_per_sender) as usize);
    }
}
