mod shm;

use shm::SharedMemory;
use std::io;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicU64, Ordering};

const CACHE_LINE: usize = 64;
const MAGIC: u64 = 0x444C_4752_5043_5631; // "DLGRPCV1"
const VERSION: u32 = 1;

// Layout offsets (all cache-line aligned)
const HEADER_OFFSET: usize = 0;
const HEADER_SIZE: usize = 128;
const RING_CONTROL_OFFSET: usize = HEADER_SIZE;
const RING_CONTROL_SIZE: usize = 128; // head on CL0, tail on CL1
const RING_SLOTS_OFFSET: usize = RING_CONTROL_OFFSET + RING_CONTROL_SIZE;

#[derive(Debug)]
pub enum CallError {
    ServerDisconnected,
    QueueFull,
}

pub struct RecvEntry<Req: Copy> {
    pub client_id: u32,
    pub resp_slot_idx: u32,
    pub payload: Req,
}

// --- Layout helpers ---

const fn align_up(val: usize, align: usize) -> usize {
    (val + align - 1) & !(align - 1)
}

fn req_slot_size<Req: Copy>() -> usize {
    align_up(16 + std::mem::size_of::<Req>(), CACHE_LINE)
}

fn resp_slot_size<Resp: Copy>() -> usize {
    align_up(8 + std::mem::size_of::<Resp>(), CACHE_LINE)
}

fn resp_area_offset<Req: Copy>(ring_depth: u32) -> usize {
    RING_SLOTS_OFFSET + req_slot_size::<Req>() * ring_depth as usize
}

fn total_shm_size<Req: Copy, Resp: Copy>(
    max_clients: u32,
    ring_depth: u32,
    resp_depth: u32,
) -> usize {
    resp_area_offset::<Req>(ring_depth)
        + max_clients as usize * resp_depth as usize * resp_slot_size::<Resp>()
}

// --- Header layout (at offset 0) ---
// [0..8]   magic: u64
// [8..12]  version: u32
// [12..16] max_clients: u32
// [16..20] ring_depth: u32
// [20..24] resp_depth: u32
// [24..28] next_client_id: AtomicU32
// [28..29] server_alive: AtomicBool

unsafe fn header_magic(base: *const u8) -> *const u64 {
    unsafe { base.add(HEADER_OFFSET).cast() }
}
unsafe fn header_version(base: *const u8) -> *const u32 {
    unsafe { base.add(HEADER_OFFSET + 8).cast() }
}
unsafe fn header_max_clients(base: *const u8) -> *const u32 {
    unsafe { base.add(HEADER_OFFSET + 12).cast() }
}
unsafe fn header_ring_depth(base: *const u8) -> *const u32 {
    unsafe { base.add(HEADER_OFFSET + 16).cast() }
}
unsafe fn header_resp_depth(base: *const u8) -> *const u32 {
    unsafe { base.add(HEADER_OFFSET + 20).cast() }
}
unsafe fn header_next_client_id(base: *const u8) -> *const AtomicU32 {
    unsafe { base.add(HEADER_OFFSET + 24).cast() }
}
unsafe fn header_server_alive(base: *const u8) -> *const AtomicBool {
    unsafe { base.add(HEADER_OFFSET + 28).cast() }
}

// --- Ring control (at RING_CONTROL_OFFSET) ---
// CL 0: head (AtomicU64)
// CL 1: tail (AtomicU64)

unsafe fn ring_head(base: *const u8) -> *const AtomicU64 {
    unsafe { base.add(RING_CONTROL_OFFSET).cast() }
}
unsafe fn ring_tail(base: *const u8) -> *const AtomicU64 {
    unsafe { base.add(RING_CONTROL_OFFSET + CACHE_LINE).cast() }
}

// --- Ring slot layout (each slot is CL-aligned) ---
// [0]     committed: AtomicU8
// [1..4]  _pad
// [4..8]  client_id: u32
// [8..12] resp_slot_idx: u32
// [12..16] _pad2
// [16..]  payload: Req

unsafe fn ring_slot_ptr<Req: Copy>(base: *const u8, ring_depth: u32, pos: u64) -> *mut u8 {
    unsafe {
        let idx = (pos & (ring_depth as u64 - 1)) as usize;
        base.add(RING_SLOTS_OFFSET + idx * req_slot_size::<Req>()) as *mut u8
    }
}

unsafe fn slot_committed(slot: *const u8) -> &'static AtomicU8 {
    unsafe { &*(slot as *const AtomicU8) }
}

unsafe fn slot_client_id(slot: *const u8) -> *mut u32 {
    unsafe { slot.add(4) as *mut u32 }
}

unsafe fn slot_resp_slot_idx(slot: *const u8) -> *mut u32 {
    unsafe { slot.add(8) as *mut u32 }
}

unsafe fn slot_payload<Req: Copy>(slot: *const u8) -> *mut Req {
    unsafe { slot.add(16) as *mut Req }
}

// --- Response slot layout (each slot is CL-aligned) ---
// [0]    valid: AtomicU8
// [1..8] _pad
// [8..]  payload: Resp

unsafe fn resp_slot_ptr<Req: Copy, Resp: Copy>(
    base: *const u8,
    ring_depth: u32,
    resp_depth: u32,
    client_id: u32,
    slot_idx: u32,
) -> *mut u8 {
    unsafe {
        let offset = resp_area_offset::<Req>(ring_depth)
            + (client_id as usize * resp_depth as usize + slot_idx as usize)
                * resp_slot_size::<Resp>();
        base.add(offset) as *mut u8
    }
}

unsafe fn resp_valid(slot: *const u8) -> &'static AtomicU8 {
    unsafe { &*(slot as *const AtomicU8) }
}

unsafe fn resp_payload<Resp: Copy>(slot: *const u8) -> *mut Resp {
    unsafe { slot.add(8) as *mut Resp }
}

// ============================================================
// Server
// ============================================================

pub struct Server<Req: Copy + Send, Resp: Copy + Send> {
    _shm: SharedMemory,
    base: *mut u8,
    ring_depth: u32,
    resp_depth: u32,
    cursor: u64,
    cached_head: u64,
    _phantom: PhantomData<(Req, Resp)>,
}

unsafe impl<Req: Copy + Send, Resp: Copy + Send> Send for Server<Req, Resp> {}

impl<Req: Copy + Send, Resp: Copy + Send> Server<Req, Resp> {
    /// Create a new delegation ring in shared memory.
    ///
    /// # Safety
    /// `ring_depth` and `resp_depth` must be powers of 2.
    pub unsafe fn create<P: AsRef<Path>>(
        path: P,
        max_clients: u32,
        ring_depth: u32,
        resp_depth: u32,
    ) -> io::Result<Self> {
        assert!(ring_depth.is_power_of_two());
        assert!(resp_depth.is_power_of_two());

        let size = total_shm_size::<Req, Resp>(max_clients, ring_depth, resp_depth);
        let shm = unsafe { SharedMemory::create(path, size)? };
        let base = shm.as_ptr();

        // Zero-init (mmap of new shm is already zeroed, but be explicit)
        unsafe { std::ptr::write_bytes(base, 0, size) };

        // Write header
        unsafe {
            std::ptr::write_volatile(header_magic(base) as *mut u64, MAGIC);
            std::ptr::write_volatile(header_version(base) as *mut u32, VERSION);
            std::ptr::write_volatile(header_max_clients(base) as *mut u32, max_clients);
            std::ptr::write_volatile(header_ring_depth(base) as *mut u32, ring_depth);
            std::ptr::write_volatile(header_resp_depth(base) as *mut u32, resp_depth);
            (*header_next_client_id(base)).store(0, Ordering::Release);
            (*header_server_alive(base)).store(true, Ordering::Release);
            (*ring_head(base)).store(0, Ordering::Release);
            (*ring_tail(base)).store(0, Ordering::Release);
        }

        Ok(Self {
            _shm: shm,
            base,
            ring_depth,
            resp_depth,
            cursor: 0,
            cached_head: 0,
            _phantom: PhantomData,
        })
    }

    /// Snapshot the head and count committed entries from cursor.
    pub fn poll(&mut self) -> u32 {
        let head = unsafe { (*ring_head(self.base)).load(Ordering::Acquire) };
        self.cached_head = head;

        let mut count = 0u32;
        let mut pos = self.cursor;
        while pos < head {
            let slot = unsafe { ring_slot_ptr::<Req>(self.base, self.ring_depth, pos) };
            let committed = unsafe { slot_committed(slot) }.load(Ordering::Acquire);
            if committed == 0 {
                break; // hole: client claimed but not yet committed
            }
            count += 1;
            pos += 1;
        }
        count
    }

    /// Receive the next committed entry.
    pub fn recv(&mut self) -> Option<RecvEntry<Req>> {
        if self.cursor >= self.cached_head {
            return None;
        }
        let slot = unsafe { ring_slot_ptr::<Req>(self.base, self.ring_depth, self.cursor) };
        let committed = unsafe { slot_committed(slot) }.load(Ordering::Acquire);
        if committed == 0 {
            return None;
        }

        let client_id = unsafe { std::ptr::read_volatile(slot_client_id(slot)) };
        let resp_slot_idx = unsafe { std::ptr::read_volatile(slot_resp_slot_idx(slot)) };
        let payload = unsafe { std::ptr::read_volatile(slot_payload::<Req>(slot)) };

        // Clear committed flag for reuse
        unsafe { slot_committed(slot) }.store(0, Ordering::Release);
        self.cursor += 1;

        Some(RecvEntry {
            client_id,
            resp_slot_idx,
            payload,
        })
    }

    /// Advance the tail to the current cursor position, unblocking clients.
    pub fn advance_tail(&mut self) {
        unsafe { (*ring_tail(self.base)).store(self.cursor, Ordering::Release) };
    }

    /// Write a response directly to a client's response slot.
    pub fn reply(&self, client_id: u32, resp_slot_idx: u32, resp: Resp) {
        let slot = unsafe {
            resp_slot_ptr::<Req, Resp>(
                self.base,
                self.ring_depth,
                self.resp_depth,
                client_id,
                resp_slot_idx,
            )
        };
        unsafe {
            std::ptr::write_volatile(resp_payload::<Resp>(slot), resp);
            std::sync::atomic::fence(Ordering::Release);
            resp_valid(slot).store(1, Ordering::Release);
        }
    }
}

impl<Req: Copy + Send, Resp: Copy + Send> Drop for Server<Req, Resp> {
    fn drop(&mut self) {
        unsafe {
            (*header_server_alive(self.base)).store(false, Ordering::Release);
        }
    }
}

// ============================================================
// Client
// ============================================================

pub struct Client<Req: Copy + Send, Resp: Copy + Send> {
    _shm: SharedMemory,
    base: *mut u8,
    client_id: u32,
    ring_depth: u32,
    resp_depth: u32,
    resp_mask: u32,
    next_resp_slot: u32,
    in_flight: u32,
    _phantom: PhantomData<(Req, Resp)>,
}

unsafe impl<Req: Copy + Send, Resp: Copy + Send> Send for Client<Req, Resp> {}

impl<Req: Copy + Send, Resp: Copy + Send> Client<Req, Resp> {
    /// Connect to an existing delegation ring.
    ///
    /// # Safety
    /// The shared memory must exist and have been created by `Server::create`
    /// with matching `Req` and `Resp` types.
    pub unsafe fn connect<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        // First open with minimal size to read the header
        let header_shm = unsafe { SharedMemory::open(&path, RING_SLOTS_OFFSET)? };
        let base = header_shm.as_ptr();

        let magic = unsafe { std::ptr::read_volatile(header_magic(base)) };
        if magic != MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid magic number",
            ));
        }

        let max_clients = unsafe { std::ptr::read_volatile(header_max_clients(base)) };
        let ring_depth = unsafe { std::ptr::read_volatile(header_ring_depth(base)) };
        let resp_depth = unsafe { std::ptr::read_volatile(header_resp_depth(base)) };

        // Allocate client ID
        let client_id = unsafe { (*header_next_client_id(base)).fetch_add(1, Ordering::AcqRel) };
        if client_id >= max_clients {
            return Err(io::Error::other("max clients exceeded"));
        }

        // Drop the small mapping
        drop(header_shm);

        // Re-open with full size
        let full_size = total_shm_size::<Req, Resp>(max_clients, ring_depth, resp_depth);
        let shm = unsafe { SharedMemory::open(path, full_size)? };
        let base = shm.as_ptr();

        Ok(Self {
            _shm: shm,
            base,
            client_id,
            ring_depth,
            resp_depth,
            resp_mask: resp_depth - 1,
            next_resp_slot: 0,
            in_flight: 0,
            _phantom: PhantomData,
        })
    }

    /// Submit a request to the shared ring.
    /// Returns the response slot index used for this request.
    pub fn call(&mut self, req: Req) -> Result<u32, CallError> {
        // Check server alive
        let alive = unsafe { (*header_server_alive(self.base)).load(Ordering::Acquire) };
        if !alive {
            return Err(CallError::ServerDisconnected);
        }

        // Allocate response slot (round-robin)
        let resp_slot_idx = self.next_resp_slot;
        self.next_resp_slot = (self.next_resp_slot + 1) & self.resp_mask;

        // Claim a ring position
        let pos = unsafe { (*ring_head(self.base)).fetch_add(1, Ordering::Relaxed) };

        // Tail-based flow control: spin if ring is full
        loop {
            let tail = unsafe { (*ring_tail(self.base)).load(Ordering::Acquire) };
            if pos.wrapping_sub(tail) < self.ring_depth as u64 {
                break;
            }
            std::hint::spin_loop();
        }

        // 2-phase commit: write data, then set committed flag
        let slot = unsafe { ring_slot_ptr::<Req>(self.base, self.ring_depth, pos) };
        unsafe {
            std::ptr::write_volatile(slot_client_id(slot), self.client_id);
            std::ptr::write_volatile(slot_resp_slot_idx(slot), resp_slot_idx);
            std::ptr::write_volatile(slot_payload::<Req>(slot), req);
            std::sync::atomic::fence(Ordering::Release);
            slot_committed(slot).store(1, Ordering::Release);
        }

        self.in_flight += 1;
        Ok(resp_slot_idx)
    }

    /// Poll all response slots for completed responses.
    /// Calls `on_response` for each valid response with `(resp_slot_idx, resp)`.
    /// Returns the count of valid responses consumed.
    pub fn poll(&mut self, mut on_response: impl FnMut(u32, Resp)) -> u32 {
        let mut count = 0u32;
        for i in 0..self.resp_depth {
            let slot = unsafe {
                resp_slot_ptr::<Req, Resp>(
                    self.base,
                    self.ring_depth,
                    self.resp_depth,
                    self.client_id,
                    i,
                )
            };
            let valid = unsafe { resp_valid(slot) }.load(Ordering::Acquire);
            if valid != 0 {
                let resp = unsafe { std::ptr::read_volatile(resp_payload::<Resp>(slot)) };
                unsafe { resp_valid(slot) }.store(0, Ordering::Release);
                self.in_flight -= 1;
                on_response(i, resp);
                count += 1;
            }
        }
        count
    }

    pub fn in_flight(&self) -> u32 {
        self.in_flight
    }

    pub fn client_id(&self) -> u32 {
        self.client_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[repr(C)]
    #[derive(Clone, Copy, Debug, PartialEq)]
    struct TestReq {
        key: u64,
        value: u64,
    }

    #[repr(C)]
    #[derive(Clone, Copy, Debug, PartialEq)]
    struct TestResp {
        result: u64,
    }

    fn unique_name(suffix: &str) -> String {
        format!("/deleg_test_{}_{}", std::process::id(), suffix)
    }

    #[test]
    fn test_single_client_call_reply() {
        let name = unique_name("single");
        unsafe {
            let mut server = Server::<TestReq, TestResp>::create(&name, 4, 16, 4).unwrap();
            let mut client = Client::<TestReq, TestResp>::connect(&name).unwrap();

            // Client sends a request
            let slot_idx = client
                .call(TestReq {
                    key: 42,
                    value: 100,
                })
                .unwrap();

            // Server receives it
            let count = server.poll();
            assert_eq!(count, 1);
            let entry = server.recv().unwrap();
            assert_eq!(entry.payload.key, 42);
            assert_eq!(entry.payload.value, 100);
            assert_eq!(entry.client_id, 0);
            assert_eq!(entry.resp_slot_idx, slot_idx);
            server.advance_tail();

            // Server replies
            server.reply(
                entry.client_id,
                entry.resp_slot_idx,
                TestResp { result: 200 },
            );

            // Client receives reply
            let mut responses = Vec::new();
            let n = client.poll(|idx, resp| responses.push((idx, resp)));
            assert_eq!(n, 1);
            assert_eq!(responses[0].0, slot_idx);
            assert_eq!(responses[0].1.result, 200);
            assert_eq!(client.in_flight(), 0);
        }
    }

    #[test]
    fn test_multi_client() {
        let name = unique_name("multi");
        unsafe {
            let mut server = Server::<TestReq, TestResp>::create(&name, 4, 16, 4).unwrap();

            let mut clients: Vec<Client<TestReq, TestResp>> =
                (0..3).map(|_| Client::connect(&name).unwrap()).collect();

            // Each client sends a request
            for (i, client) in clients.iter_mut().enumerate() {
                client
                    .call(TestReq {
                        key: i as u64,
                        value: i as u64 * 10,
                    })
                    .unwrap();
            }

            // Server receives all 3
            let count = server.poll();
            assert_eq!(count, 3);

            for _ in 0..3 {
                let entry = server.recv().unwrap();
                server.reply(
                    entry.client_id,
                    entry.resp_slot_idx,
                    TestResp {
                        result: entry.payload.value + 1,
                    },
                );
            }
            server.advance_tail();

            // Each client receives its response
            for (i, client) in clients.iter_mut().enumerate() {
                let n = client.poll(|_, resp| {
                    assert_eq!(resp.result, i as u64 * 10 + 1);
                });
                assert_eq!(n, 1);
            }
        }
    }

    #[test]
    fn test_ring_full_backpressure() {
        let name = unique_name("backpressure");
        unsafe {
            let mut server = Server::<TestReq, TestResp>::create(&name, 1, 4, 4).unwrap();
            let mut client = Client::<TestReq, TestResp>::connect(&name).unwrap();

            // Fill the ring (4 slots)
            for i in 0..4 {
                client.call(TestReq { key: i, value: 0 }).unwrap();
            }

            // Next call would block (tail-based), so we drain first
            let count = server.poll();
            assert_eq!(count, 4);
            for _ in 0..4 {
                let entry = server.recv().unwrap();
                server.reply(entry.client_id, entry.resp_slot_idx, TestResp { result: 0 });
            }
            server.advance_tail();

            // Drain responses
            client.poll(|_, _| {});

            // Now we can send again
            client.call(TestReq { key: 99, value: 0 }).unwrap();

            let count = server.poll();
            assert_eq!(count, 1);
            let entry = server.recv().unwrap();
            assert_eq!(entry.payload.key, 99);
            server.advance_tail();
        }
    }

    #[test]
    fn test_multi_threaded() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        let name = unique_name("mt");
        let num_clients = 4u32;
        let msgs_per_client = 100u32;

        unsafe {
            let mut server =
                Server::<TestReq, TestResp>::create(&name, num_clients, 64, 8).unwrap();

            let barrier = Arc::new(Barrier::new(num_clients as usize + 1));

            let client_handles: Vec<_> = (0..num_clients)
                .map(|_| {
                    let name = name.clone();
                    let b = barrier.clone();
                    thread::spawn(move || {
                        let mut client = Client::<TestReq, TestResp>::connect(&name).unwrap();
                        b.wait();

                        let mut completed = 0u32;
                        let mut sent = 0u32;

                        while completed < msgs_per_client {
                            // Send as many as possible
                            while sent < msgs_per_client && client.in_flight() < 8 {
                                client
                                    .call(TestReq {
                                        key: sent as u64,
                                        value: 0,
                                    })
                                    .unwrap();
                                sent += 1;
                            }
                            // Poll responses
                            let n = client.poll(|_, _| {});
                            completed += n;
                        }
                        assert_eq!(completed, msgs_per_client);
                    })
                })
                .collect();

            barrier.wait();

            // Server loop
            let total = num_clients * msgs_per_client;
            let mut processed = 0u32;
            while processed < total {
                server.poll();
                while let Some(entry) = server.recv() {
                    server.reply(entry.client_id, entry.resp_slot_idx, TestResp { result: 0 });
                    processed += 1;
                }
                server.advance_tail();
            }

            for h in client_handles {
                h.join().unwrap();
            }
        }
    }
}
