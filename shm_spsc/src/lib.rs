//! Process-to-process SPSC communication over shared memory.
//!
//! This crate provides a dual-lane SPSC channel for inter-process communication
//! using `/dev/shm`. The design follows a server-client model where:
//!
//! - The server creates the shared memory and waits for a client to connect.
//! - The client connects to existing shared memory.
//! - Both sides can send and receive through dedicated lanes.
//!
//! # Example
//!
//! ```ignore
//! // Server process
//! let server = unsafe { Server::<u64>::create("/my_channel", 1024)? };
//! server.send(42)?;
//! let response = server.recv()?;
//!
//! // Client process
//! let client = unsafe { Client::<u64>::connect("/my_channel", 1024)? };
//! let value = client.recv()?;
//! client.send(value + 1)?;
//! ```

pub mod shm;

use shm::SharedMemory;
use std::cell::UnsafeCell;
use std::io;
use std::marker::PhantomData;
use std::mem::{ManuallyDrop, MaybeUninit};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

/// Marker trait for types that can be safely transmitted through the channel.
///
/// # Safety
/// Types implementing this trait must be `Copy` and have a stable memory layout
/// suitable for inter-process communication.
pub unsafe trait Serial: Copy {}

// Implement Serial for common types
unsafe impl Serial for u8 {}
unsafe impl Serial for u16 {}
unsafe impl Serial for u32 {}
unsafe impl Serial for u64 {}
unsafe impl Serial for u128 {}
unsafe impl Serial for usize {}
unsafe impl Serial for i8 {}
unsafe impl Serial for i16 {}
unsafe impl Serial for i32 {}
unsafe impl Serial for i64 {}
unsafe impl Serial for i128 {}
unsafe impl Serial for isize {}
unsafe impl Serial for f32 {}
unsafe impl Serial for f64 {}
unsafe impl Serial for bool {}
unsafe impl<T: Copy, const N: usize> Serial for [T; N] {}

/// Error returned when sending fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendError<T> {
    /// The channel is full.
    Full(T),
    /// The peer has disconnected.
    Disconnected(T),
}

impl<T> std::fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SendError::Full(_) => write!(f, "channel is full"),
            SendError::Disconnected(_) => write!(f, "peer has disconnected"),
        }
    }
}

impl<T: std::fmt::Debug> std::error::Error for SendError<T> {}

/// Error returned when receiving fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecvError {
    /// The channel is empty.
    Empty,
    /// The peer has disconnected.
    Disconnected,
}

impl std::fmt::Display for RecvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RecvError::Empty => write!(f, "channel is empty"),
            RecvError::Disconnected => write!(f, "peer has disconnected"),
        }
    }
}

impl std::error::Error for RecvError {}

/// Magic number to identify valid shared memory layout.
const MAGIC: u64 = 0x5350_5343_4348_414E; // "SPSCHAN"

/// Version of the shared memory layout.
const VERSION: u32 = 1;

/// Header stored at the beginning of shared memory.
#[repr(C)]
struct Header {
    magic: u64,
    version: u32,
    capacity: u32,
    slot_size: u32,
    _padding: u32,
    /// Server -> Client lane
    s2c_head: AtomicUsize,
    s2c_tail: AtomicUsize,
    /// Client -> Server lane
    c2s_head: AtomicUsize,
    c2s_tail: AtomicUsize,
    /// Connection state
    server_alive: AtomicBool,
    client_alive: AtomicBool,
    client_connected: AtomicBool,
}

/// Calculate the total size needed for shared memory.
fn calc_shm_size<T>(capacity: usize) -> usize {
    let header_size = std::mem::size_of::<Header>();
    let slot_size = std::mem::size_of::<T>();
    // Two lanes: S2C and C2S
    let lane_size = slot_size * capacity;
    header_size + lane_size * 2
}

/// A lane (one direction of communication).
struct Lane<T> {
    slots: *mut UnsafeCell<ManuallyDrop<MaybeUninit<T>>>,
    capacity: usize,
    head: *const AtomicUsize,
    tail: *const AtomicUsize,
    /// Cached values for reduced atomic reads
    head_cache: usize,
    tail_cache: usize,
    /// Am I the sender or receiver on this lane?
    is_sender: bool,
}

impl<T: Serial> Lane<T> {
    unsafe fn new(
        slots: *mut u8,
        capacity: usize,
        head: *const AtomicUsize,
        tail: *const AtomicUsize,
        is_sender: bool,
    ) -> Self {
        Self {
            slots: slots as *mut UnsafeCell<ManuallyDrop<MaybeUninit<T>>>,
            capacity,
            head,
            tail,
            head_cache: 0,
            tail_cache: 0,
            is_sender,
        }
    }

    fn try_send(&mut self, value: T) -> Result<(), T> {
        debug_assert!(self.is_sender);

        let tail = unsafe { (*self.tail).load(Ordering::Relaxed) };
        let next_tail = (tail + 1) % self.capacity;

        // Check if we have room
        if next_tail == self.head_cache {
            self.head_cache = unsafe { (*self.head).load(Ordering::Acquire) };
            if next_tail == self.head_cache {
                return Err(value);
            }
        }

        // Write the value
        unsafe {
            let slot = &*self.slots.add(tail);
            std::ptr::write_volatile((*slot.get()).as_mut_ptr(), value);
        }

        // Update tail
        unsafe { (*self.tail).store(next_tail, Ordering::Release) };

        Ok(())
    }

    fn try_recv(&mut self) -> Result<T, ()> {
        debug_assert!(!self.is_sender);

        let head = unsafe { (*self.head).load(Ordering::Relaxed) };

        // Check if we have data
        if head == self.tail_cache {
            self.tail_cache = unsafe { (*self.tail).load(Ordering::Acquire) };
            if head == self.tail_cache {
                return Err(());
            }
        }

        // Read the value
        let value = unsafe {
            let slot = &*self.slots.add(head);
            std::ptr::read_volatile((*slot.get()).as_ptr())
        };

        // Update head
        let next_head = (head + 1) % self.capacity;
        unsafe { (*self.head).store(next_head, Ordering::Release) };

        Ok(value)
    }
}

/// Server side of the shared memory channel.
///
/// The server creates the shared memory and owns its lifecycle.
pub struct Server<T: Serial> {
    _shm: SharedMemory,
    header: *mut Header,
    tx: Lane<T>,
    rx: Lane<T>,
    _marker: PhantomData<T>,
}

unsafe impl<T: Serial + Send> Send for Server<T> {}

impl<T: Serial> Server<T> {
    /// Creates a new server and shared memory region.
    ///
    /// # Safety
    /// The caller must ensure that `path` is a valid shared memory path
    /// and that no other process is using the same path.
    pub unsafe fn create<P: AsRef<Path>>(path: P, capacity: usize) -> io::Result<Self> {
        assert!(capacity > 1, "capacity must be greater than 1");

        let size = calc_shm_size::<T>(capacity);
        let shm = unsafe { SharedMemory::create(path, size)? };
        let base = shm.as_ptr();

        // Initialize header
        let header = base as *mut Header;
        unsafe {
            std::ptr::write(
                header,
                Header {
                    magic: MAGIC,
                    version: VERSION,
                    capacity: capacity as u32,
                    slot_size: std::mem::size_of::<T>() as u32,
                    _padding: 0,
                    s2c_head: AtomicUsize::new(0),
                    s2c_tail: AtomicUsize::new(0),
                    c2s_head: AtomicUsize::new(0),
                    c2s_tail: AtomicUsize::new(0),
                    server_alive: AtomicBool::new(true),
                    client_alive: AtomicBool::new(false),
                    client_connected: AtomicBool::new(false),
                },
            );
        }

        let header_size = std::mem::size_of::<Header>();
        let lane_size = std::mem::size_of::<T>() * capacity;

        // S2C lane (server sends, client receives)
        let s2c_slots = unsafe { base.add(header_size) };
        let tx = unsafe {
            Lane::new(
                s2c_slots,
                capacity,
                &(*header).s2c_head,
                &(*header).s2c_tail,
                true, // sender
            )
        };

        // C2S lane (client sends, server receives)
        let c2s_slots = unsafe { base.add(header_size + lane_size) };
        let rx = unsafe {
            Lane::new(
                c2s_slots,
                capacity,
                &(*header).c2s_head,
                &(*header).c2s_tail,
                false, // receiver
            )
        };

        Ok(Self {
            _shm: shm,
            header,
            tx,
            rx,
            _marker: PhantomData,
        })
    }

    /// Returns true if a client is connected.
    pub fn is_connected(&self) -> bool {
        unsafe { (*self.header).client_connected.load(Ordering::Acquire) }
    }

    /// Returns true if the client has disconnected (was connected but is now gone).
    pub fn is_disconnected(&self) -> bool {
        unsafe {
            (*self.header).client_connected.load(Ordering::Acquire)
                && !(*self.header).client_alive.load(Ordering::Acquire)
        }
    }

    /// Attempts to send a value to the client.
    pub fn try_send(&mut self, value: T) -> Result<(), SendError<T>> {
        if self.is_disconnected() {
            return Err(SendError::Disconnected(value));
        }
        self.tx.try_send(value).map_err(SendError::Full)
    }

    /// Sends a value, blocking until space is available.
    pub fn send(&mut self, mut value: T) -> Result<(), SendError<T>> {
        loop {
            match self.try_send(value) {
                Ok(()) => return Ok(()),
                Err(SendError::Full(v)) => {
                    value = v;
                    std::hint::spin_loop();
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Attempts to receive a value from the client.
    pub fn try_recv(&mut self) -> Result<T, RecvError> {
        match self.rx.try_recv() {
            Ok(v) => Ok(v),
            Err(()) => {
                if self.is_disconnected() {
                    Err(RecvError::Disconnected)
                } else {
                    Err(RecvError::Empty)
                }
            }
        }
    }

    /// Receives a value, blocking until one is available.
    pub fn recv(&mut self) -> Result<T, RecvError> {
        loop {
            match self.try_recv() {
                Ok(v) => return Ok(v),
                Err(RecvError::Empty) => std::hint::spin_loop(),
                Err(e) => return Err(e),
            }
        }
    }
}

impl<T: Serial> Drop for Server<T> {
    fn drop(&mut self) {
        unsafe {
            (*self.header).server_alive.store(false, Ordering::Release);
        }
    }
}

/// Client side of the shared memory channel.
pub struct Client<T: Serial> {
    _shm: SharedMemory,
    header: *mut Header,
    tx: Lane<T>,
    rx: Lane<T>,
    _marker: PhantomData<T>,
}

unsafe impl<T: Serial + Send> Send for Client<T> {}

impl<T: Serial> Client<T> {
    /// Connects to an existing shared memory region.
    ///
    /// # Safety
    /// The caller must ensure that a server has created the shared memory
    /// with the same type `T` and at least `capacity` slots.
    pub unsafe fn connect<P: AsRef<Path>>(path: P, capacity: usize) -> io::Result<Self> {
        let size = calc_shm_size::<T>(capacity);
        let shm = unsafe { SharedMemory::open(path, size)? };
        let base = shm.as_ptr();

        let header = base as *mut Header;

        // Verify header
        unsafe {
            if (*header).magic != MAGIC {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid magic number",
                ));
            }
            if (*header).version != VERSION {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "version mismatch",
                ));
            }
            if (*header).capacity != capacity as u32 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "capacity mismatch",
                ));
            }
            if (*header).slot_size != std::mem::size_of::<T>() as u32 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "slot size mismatch",
                ));
            }
            if !(*header).server_alive.load(Ordering::Acquire) {
                return Err(io::Error::new(
                    io::ErrorKind::ConnectionRefused,
                    "server not alive",
                ));
            }
        }

        let header_size = std::mem::size_of::<Header>();
        let lane_size = std::mem::size_of::<T>() * capacity;

        // S2C lane (server sends, client receives)
        let s2c_slots = unsafe { base.add(header_size) };
        let rx = unsafe {
            Lane::new(
                s2c_slots,
                capacity,
                &(*header).s2c_head,
                &(*header).s2c_tail,
                false, // receiver
            )
        };

        // C2S lane (client sends, server receives)
        let c2s_slots = unsafe { base.add(header_size + lane_size) };
        let tx = unsafe {
            Lane::new(
                c2s_slots,
                capacity,
                &(*header).c2s_head,
                &(*header).c2s_tail,
                true, // sender
            )
        };

        // Mark as connected
        unsafe {
            (*header).client_alive.store(true, Ordering::Release);
            (*header).client_connected.store(true, Ordering::Release);
        }

        Ok(Self {
            _shm: shm,
            header,
            tx,
            rx,
            _marker: PhantomData,
        })
    }

    /// Returns true if the server has disconnected.
    pub fn is_disconnected(&self) -> bool {
        unsafe { !(*self.header).server_alive.load(Ordering::Acquire) }
    }

    /// Attempts to send a value to the server.
    pub fn try_send(&mut self, value: T) -> Result<(), SendError<T>> {
        if self.is_disconnected() {
            return Err(SendError::Disconnected(value));
        }
        self.tx.try_send(value).map_err(SendError::Full)
    }

    /// Sends a value, blocking until space is available.
    pub fn send(&mut self, mut value: T) -> Result<(), SendError<T>> {
        loop {
            match self.try_send(value) {
                Ok(()) => return Ok(()),
                Err(SendError::Full(v)) => {
                    value = v;
                    std::hint::spin_loop();
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Attempts to receive a value from the server.
    pub fn try_recv(&mut self) -> Result<T, RecvError> {
        match self.rx.try_recv() {
            Ok(v) => Ok(v),
            Err(()) => {
                if self.is_disconnected() {
                    Err(RecvError::Disconnected)
                } else {
                    Err(RecvError::Empty)
                }
            }
        }
    }

    /// Receives a value, blocking until one is available.
    pub fn recv(&mut self) -> Result<T, RecvError> {
        loop {
            match self.try_recv() {
                Ok(v) => return Ok(v),
                Err(RecvError::Empty) => std::hint::spin_loop(),
                Err(e) => return Err(e),
            }
        }
    }
}

impl<T: Serial> Drop for Client<T> {
    fn drop(&mut self) {
        unsafe {
            (*self.header).client_alive.store(false, Ordering::Release);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_server_client() {
        let name = format!("/shm_spsc_test_{}", std::process::id());

        unsafe {
            let mut server = Server::<u64>::create(&name, 64).unwrap();
            assert!(!server.is_connected());

            let mut client = Client::<u64>::connect(&name, 64).unwrap();
            assert!(server.is_connected());

            // Server -> Client
            server.send(42).unwrap();
            assert_eq!(client.recv().unwrap(), 42);

            // Client -> Server
            client.send(123).unwrap();
            assert_eq!(server.recv().unwrap(), 123);
        }
    }

    #[test]
    fn test_disconnect_detection() {
        let name = format!("/shm_spsc_disc_{}", std::process::id());

        unsafe {
            let server = Server::<u64>::create(&name, 64).unwrap();

            {
                let _client = Client::<u64>::connect(&name, 64).unwrap();
                assert!(server.is_connected());
            }

            // Client dropped
            assert!(server.is_disconnected());
        }
    }
}
