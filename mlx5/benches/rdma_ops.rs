//! RDMA operations benchmark.
//!
//! Benchmarks:
//! 1. 32B inline send (throughput) - MElem/s
//! 2. 32B BlueFlame inline send (throughput + latency) - MElem/s
//! 3. 4KiB WRITE (no relaxed ordering) - Byte/s
//! 4. 4KiB WRITE (relaxed ordering) - Byte/s
//! 5. 4KiB READ (no relaxed ordering) - Byte/s
//! 6. 4KiB READ (relaxed ordering) - Byte/s
//!
//! Run with:
//! ```bash
//! cargo bench --bench rdma_ops
//! ```

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};

use mlx5::cq::{CqConfig, Cqe};
use mlx5::device::{Context, DeviceList};
use mlx5::emit_wqe;
use mlx5::mono_cq::MonoCqRc;
use mlx5::pd::{AccessFlags, MemoryRegion, Pd};
use mlx5::qp::{RcQpConfig, RcQpForMonoCq};
use mlx5::transport::IbRemoteQpInfo;
use mlx5::wqe::WqeFlags;

// =============================================================================
// Constants
// =============================================================================

// Send/Recv benchmark constants
const SEND_RECV_QUEUE_DEPTH: usize = 1024;
const SEND_RECV_SIGNAL_INTERVAL: usize = 4; // 1/4 signaled (like previous)
// RQ matches queue depth (like previous implementation)
const SEND_RECV_RQ_SIZE: usize = SEND_RECV_QUEUE_DEPTH;
const SEND_RECV_BUFFER_SIZE: usize = SEND_RECV_QUEUE_DEPTH * 64; // 64 bytes per entry (like send_recv_small.rs)

// RDMA WRITE/READ benchmark constants
const RDMA_QUEUE_DEPTH: usize = 1024;
const RDMA_SIGNAL_INTERVAL: usize = 4; // 1/4 signaled
const RDMA_MSG_SIZE: usize = 1024 * 1024; // 1MiB per WQE
const RDMA_BUFFER_SIZE: usize = 1024 * 1024; // 1MiB total (WQEs write to same buffer)

// Common constants
const SMALL_MSG_SIZE: usize = 32;
const PAGE_SIZE: usize = 4096;

// =============================================================================
// Aligned Buffer
// =============================================================================

struct AlignedBuffer {
    ptr: *mut u8,
    size: usize,
}

impl AlignedBuffer {
    fn new(size: usize) -> Self {
        let aligned_size = (size + PAGE_SIZE - 1) & !(PAGE_SIZE - 1);
        let ptr = unsafe {
            let mut ptr: *mut std::ffi::c_void = std::ptr::null_mut();
            let ret = libc::posix_memalign(&mut ptr, PAGE_SIZE, aligned_size);
            if ret != 0 {
                panic!("posix_memalign failed: {}", ret);
            }
            std::ptr::write_bytes(ptr as *mut u8, 0, aligned_size);
            ptr as *mut u8
        };
        Self {
            ptr,
            size: aligned_size,
        }
    }

    fn as_ptr(&self) -> *mut u8 {
        self.ptr
    }

    fn addr(&self) -> u64 {
        self.ptr as u64
    }

    fn size(&self) -> usize {
        self.size
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        unsafe {
            libc::free(self.ptr as *mut std::ffi::c_void);
        }
    }
}

// =============================================================================
// Full Access Flags
// =============================================================================

fn full_access() -> AccessFlags {
    AccessFlags::LOCAL_WRITE
        | AccessFlags::REMOTE_WRITE
        | AccessFlags::REMOTE_READ
        | AccessFlags::REMOTE_ATOMIC
}

fn full_access_relaxed() -> AccessFlags {
    full_access() | AccessFlags::RELAXED_ORDERING
}

// =============================================================================
// Connection Info
// =============================================================================

#[derive(Clone)]
struct ConnectionInfo {
    qpn: u32,
    lid: u16,
}

// =============================================================================
// Shared CQE State for Send/Recv
// =============================================================================

#[derive(Clone)]
struct SharedCqeState {
    rx_count: Rc<Cell<usize>>,
}

impl SharedCqeState {
    fn new() -> Self {
        Self {
            rx_count: Rc::new(Cell::new(0)),
        }
    }

    fn reset(&self) {
        self.rx_count.set(0);
    }

    fn push(&self, _cqe: &Cqe) {
        self.rx_count.set(self.rx_count.get() + 1);
    }
}

// =============================================================================
// Server Handle
// =============================================================================

struct ServerHandle {
    stop_flag: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl ServerHandle {
    fn stop(&mut self) {
        self.stop_flag.store(true, Ordering::SeqCst);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        self.stop();
    }
}

// =============================================================================
// Endpoint State for Send/Recv benchmarks
// =============================================================================

struct SendRecvEndpoint<SF, RF>
where
    SF: Fn(Cqe, u64),
    RF: Fn(Cqe, u64),
{
    qp: Rc<RefCell<RcQpForMonoCq<u64>>>,
    send_cq: Rc<MonoCqRc<u64, SF>>,
    recv_cq: Rc<MonoCqRc<u64, RF>>,
    shared_state: SharedCqeState,
    _send_mr: MemoryRegion,
    recv_mr: MemoryRegion,
    _send_buf: AlignedBuffer,
    recv_buf: AlignedBuffer,
    // RQ replenish tracking (for latency benchmark)
    rq_next_idx: usize,
}

struct SendRecvBenchmarkSetup<SF, RF>
where
    SF: Fn(Cqe, u64),
    RF: Fn(Cqe, u64),
{
    client: SendRecvEndpoint<SF, RF>,
    _server_handle: ServerHandle,
    _pd: Rc<Pd>,
    _ctx: Context,
}

// =============================================================================
// Endpoint State for RDMA Read/Write benchmarks (loopback)
// =============================================================================

struct RdmaLoopbackEndpoint<SF, RF>
where
    SF: Fn(Cqe, u64),
    RF: Fn(Cqe, u64),
{
    qp: Rc<RefCell<RcQpForMonoCq<u64>>>,
    send_cq: Rc<MonoCqRc<u64, SF>>,
    _recv_cq: Rc<MonoCqRc<u64, RF>>,
    local_mr: MemoryRegion,
    remote_mr: MemoryRegion,
    local_buf: AlignedBuffer,
    remote_buf: AlignedBuffer,
}

struct RdmaLoopbackSetup<SF, RF>
where
    SF: Fn(Cqe, u64),
    RF: Fn(Cqe, u64),
{
    endpoint: RdmaLoopbackEndpoint<SF, RF>,
    _pd: Rc<Pd>,
    _ctx: Context,
}

// =============================================================================
// Device/QP Setup Functions
// =============================================================================

fn open_mlx5_device() -> Option<Context> {
    let device_list = DeviceList::list().ok()?;
    for device in device_list.iter() {
        if let Ok(ctx) = device.open() {
            return Some(ctx);
        }
    }
    None
}

fn setup_send_recv_benchmark() -> Option<SendRecvBenchmarkSetup<impl Fn(Cqe, u64), impl Fn(Cqe, u64)>> {
    let ctx = open_mlx5_device()?;
    let port = 1u8;
    let port_attr = ctx.query_port(port).ok()?;
    let pd = Rc::new(ctx.alloc_pd().ok()?);

    let shared_state = SharedCqeState::new();
    let shared_state_for_recv = shared_state.clone();

    let send_callback = move |_cqe: Cqe, _entry: u64| {};
    let recv_callback = move |cqe: Cqe, _entry: u64| {
        if cqe.opcode.is_responder() && cqe.syndrome == 0 {
            shared_state_for_recv.push(&cqe);
        }
    };

    let send_cq = Rc::new(ctx.create_mono_cq(SEND_RECV_QUEUE_DEPTH as i32, send_callback, &CqConfig::default()).ok()?);
    let recv_cq = Rc::new(ctx.create_mono_cq(SEND_RECV_QUEUE_DEPTH as i32, recv_callback, &CqConfig::default()).ok()?);

    let config = RcQpConfig {
        max_send_wr: SEND_RECV_QUEUE_DEPTH as u32,
        max_recv_wr: SEND_RECV_RQ_SIZE as u32,
        max_send_sge: 1,
        max_recv_sge: 1,
        max_inline_data: 256,
        enable_scatter_to_cqe: false,
    };

    let qp = ctx
        .rc_qp_builder::<u64, u64>(&pd, &config)
        .sq_mono_cq(&send_cq)
        .rq_mono_cq(&recv_cq)
        .build()
        .ok()?;

    send_cq.register(&qp);
    recv_cq.register(&qp);

    let send_buf = AlignedBuffer::new(SEND_RECV_BUFFER_SIZE);
    let recv_buf = AlignedBuffer::new(SEND_RECV_BUFFER_SIZE);

    let send_mr = unsafe { pd.register(send_buf.as_ptr(), send_buf.size(), full_access()) }.ok()?;
    let recv_mr = unsafe { pd.register(recv_buf.as_ptr(), recv_buf.size(), full_access()) }.ok()?;

    let client_info = ConnectionInfo {
        qpn: qp.borrow().qpn(),
        lid: port_attr.lid,
    };

    let (server_info_tx, server_info_rx): (Sender<ConnectionInfo>, Receiver<ConnectionInfo>) =
        mpsc::channel();
    let (client_info_tx, client_info_rx): (Sender<ConnectionInfo>, Receiver<ConnectionInfo>) =
        mpsc::channel();
    let server_ready = Arc::new(AtomicU32::new(0));
    let server_ready_clone = server_ready.clone();

    let stop_flag = Arc::new(AtomicBool::new(false));
    let server_stop = stop_flag.clone();

    let handle = thread::spawn(move || {
        server_thread_main(
            server_info_tx,
            client_info_rx,
            server_ready_clone,
            server_stop,
        );
    });

    client_info_tx.send(client_info).ok()?;
    let server_info = server_info_rx.recv().ok()?;

    while server_ready.load(Ordering::Acquire) == 0 {
        std::hint::spin_loop();
    }

    let server_remote = IbRemoteQpInfo {
        qp_number: server_info.qpn,
        packet_sequence_number: 0,
        local_identifier: server_info.lid,
    };

    let access = full_access().bits();
    qp.borrow_mut()
        .connect(&server_remote, port, 0, 4, 4, access)
        .ok()?;

    // Pre-post all receives
    for i in 0..SEND_RECV_RQ_SIZE {
        let offset = (i * 64) as u64;
        qp.borrow()
            .post_recv(i as u64, recv_buf.addr() + offset, 64, recv_mr.lkey())
            .unwrap();
    }
    qp.borrow().ring_rq_doorbell();

    let client = SendRecvEndpoint {
        qp,
        send_cq,
        recv_cq,
        shared_state,
        _send_mr: send_mr,
        recv_mr,
        _send_buf: send_buf,
        recv_buf,
        rq_next_idx: 0,
    };

    let server_handle = ServerHandle {
        stop_flag,
        handle: Some(handle),
    };

    Some(SendRecvBenchmarkSetup {
        client,
        _server_handle: server_handle,
        _pd: pd,
        _ctx: ctx,
    })
}

fn setup_rdma_loopback_benchmark(relaxed: bool) -> Option<RdmaLoopbackSetup<impl Fn(Cqe, u64), impl Fn(Cqe, u64)>> {
    let ctx = open_mlx5_device()?;
    let port = 1u8;
    let port_attr = ctx.query_port(port).ok()?;
    let pd = Rc::new(ctx.alloc_pd().ok()?);

    let send_callback = move |_cqe: Cqe, _entry: u64| {};
    let recv_callback = move |_cqe: Cqe, _entry: u64| {};

    let send_cq = Rc::new(ctx.create_mono_cq(RDMA_QUEUE_DEPTH as i32, send_callback, &CqConfig::default()).ok()?);
    let recv_cq = Rc::new(ctx.create_mono_cq(RDMA_QUEUE_DEPTH as i32, recv_callback, &CqConfig::default()).ok()?);

    let config = RcQpConfig {
        max_send_wr: RDMA_QUEUE_DEPTH as u32,
        max_recv_wr: RDMA_QUEUE_DEPTH as u32,
        max_send_sge: 1,
        max_recv_sge: 1,
        max_inline_data: 0,
        enable_scatter_to_cqe: false,
    };

    // Create two QPs and connect them in loopback
    let qp1 = ctx
        .rc_qp_builder::<u64, u64>(&pd, &config)
        .sq_mono_cq(&send_cq)
        .rq_mono_cq(&recv_cq)
        .build()
        .ok()?;

    let qp2 = ctx
        .rc_qp_builder::<u64, u64>(&pd, &config)
        .sq_mono_cq(&send_cq)
        .rq_mono_cq(&recv_cq)
        .build()
        .ok()?;

    send_cq.register(&qp1);
    recv_cq.register(&qp1);
    send_cq.register(&qp2);
    recv_cq.register(&qp2);

    let local_buf = AlignedBuffer::new(RDMA_BUFFER_SIZE);
    let remote_buf = AlignedBuffer::new(RDMA_BUFFER_SIZE);

    let access = if relaxed { full_access_relaxed() } else { full_access() };
    let local_mr = unsafe { pd.register(local_buf.as_ptr(), local_buf.size(), access) }.ok()?;
    let remote_mr = unsafe { pd.register(remote_buf.as_ptr(), remote_buf.size(), access) }.ok()?;

    // Connect QPs in loopback
    let remote1 = IbRemoteQpInfo {
        qp_number: qp1.borrow().qpn(),
        packet_sequence_number: 0,
        local_identifier: port_attr.lid,
    };
    let remote2 = IbRemoteQpInfo {
        qp_number: qp2.borrow().qpn(),
        packet_sequence_number: 0,
        local_identifier: port_attr.lid,
    };

    let access_bits = full_access().bits();
    qp1.borrow_mut()
        .connect(&remote2, port, 0, 4, 4, access_bits)
        .ok()?;
    qp2.borrow_mut()
        .connect(&remote1, port, 0, 4, 4, access_bits)
        .ok()?;

    let endpoint = RdmaLoopbackEndpoint {
        qp: qp1,
        send_cq,
        _recv_cq: recv_cq,
        local_mr,
        remote_mr,
        local_buf,
        remote_buf,
    };

    Some(RdmaLoopbackSetup {
        endpoint,
        _pd: pd,
        _ctx: ctx,
    })
}

// =============================================================================
// Server Thread
// =============================================================================

struct ServerSharedState {
    rx_count: Cell<usize>,
}

impl ServerSharedState {
    fn new() -> Self {
        Self {
            rx_count: Cell::new(0),
        }
    }

    fn reset(&self) {
        self.rx_count.set(0);
    }

    fn push(&self, _cqe: &Cqe) {
        self.rx_count.set(self.rx_count.get() + 1);
    }
}

fn server_thread_main(
    info_tx: Sender<ConnectionInfo>,
    info_rx: Receiver<ConnectionInfo>,
    ready_signal: Arc<AtomicU32>,
    stop_flag: Arc<AtomicBool>,
) {
    let ctx = match open_mlx5_device() {
        Some(c) => c,
        None => return,
    };

    let port = 1u8;
    let port_attr = match ctx.query_port(port) {
        Ok(attr) => attr,
        Err(_) => return,
    };

    let pd = Rc::new(match ctx.alloc_pd() {
        Ok(p) => p,
        Err(_) => return,
    });

    let shared_state = Rc::new(ServerSharedState::new());
    let shared_state_for_recv = shared_state.clone();

    let send_callback = move |_cqe: Cqe, _entry: u64| {};
    let recv_callback = move |cqe: Cqe, _entry: u64| {
        if cqe.opcode.is_responder() && cqe.syndrome == 0 {
            shared_state_for_recv.push(&cqe);
        }
    };

    let send_cq = match ctx.create_mono_cq::<RcQpForMonoCq<u64>, _>(SEND_RECV_QUEUE_DEPTH as i32, send_callback, &CqConfig::default()) {
        Ok(cq) => Rc::new(cq),
        Err(_) => return,
    };

    let recv_cq = match ctx.create_mono_cq::<RcQpForMonoCq<u64>, _>(SEND_RECV_QUEUE_DEPTH as i32, recv_callback, &CqConfig::default()) {
        Ok(cq) => Rc::new(cq),
        Err(_) => return,
    };

    let config = RcQpConfig {
        max_send_wr: SEND_RECV_QUEUE_DEPTH as u32,
        max_recv_wr: SEND_RECV_RQ_SIZE as u32,
        max_send_sge: 1,
        max_recv_sge: 1,
        max_inline_data: 256,
        enable_scatter_to_cqe: false,
    };

    let qp = match ctx
        .rc_qp_builder::<u64, u64>(&pd, &config)
        .sq_mono_cq(&send_cq)
        .rq_mono_cq(&recv_cq)
        .build()
    {
        Ok(q) => q,
        Err(_) => return,
    };

    send_cq.register(&qp);
    recv_cq.register(&qp);

    let send_buf = AlignedBuffer::new(SEND_RECV_BUFFER_SIZE);
    let recv_buf = AlignedBuffer::new(SEND_RECV_BUFFER_SIZE);

    let _send_mr = match unsafe { pd.register(send_buf.as_ptr(), send_buf.size(), full_access()) } {
        Ok(mr) => mr,
        Err(_) => return,
    };

    let recv_mr = match unsafe { pd.register(recv_buf.as_ptr(), recv_buf.size(), full_access()) } {
        Ok(mr) => mr,
        Err(_) => return,
    };

    let server_info = ConnectionInfo {
        qpn: qp.borrow().qpn(),
        lid: port_attr.lid,
    };
    if info_tx.send(server_info).is_err() {
        return;
    }

    let client_info = match info_rx.recv() {
        Ok(info) => info,
        Err(_) => return,
    };

    let client_remote = IbRemoteQpInfo {
        qp_number: client_info.qpn,
        packet_sequence_number: 0,
        local_identifier: client_info.lid,
    };

    let access = full_access().bits();
    if qp
        .borrow_mut()
        .connect(&client_remote, port, 0, 4, 4, access)
        .is_err()
    {
        return;
    }

    // Pre-post all receives
    for i in 0..SEND_RECV_RQ_SIZE {
        let offset = (i * 64) as u64;
        qp.borrow()
            .post_recv(i as u64, recv_buf.addr() + offset, 64, recv_mr.lkey())
            .unwrap();
    }
    qp.borrow().ring_rq_doorbell();

    ready_signal.store(1, Ordering::Release);

    // Echo server loop with 3:1 signaling ratio (like send_recv_small.rs)
    let echo_data = vec![0xBBu8; SMALL_MSG_SIZE];
    let mut recv_idx = 0usize;
    let mut send_count = 0usize;

    while !stop_flag.load(Ordering::Relaxed) {
        shared_state.reset();
        recv_cq.poll();
        recv_cq.flush();
        let count = shared_state.rx_count.get();

        if count == 0 {
            continue;
        }

        send_cq.poll();
        send_cq.flush();

        // Repost receives and send echoes
        {
            let qp_ref = qp.borrow();
            for _ in 0..count {
                let idx = recv_idx % SEND_RECV_QUEUE_DEPTH;
                let offset = (idx * 64) as u64;
                // Repost receive
                qp_ref
                    .post_recv(idx as u64, recv_buf.addr() + offset, 64, recv_mr.lkey())
                    .unwrap();
                recv_idx += 1;
            }
        }

        // Send echo responses
        {
            let qp_ref = qp.borrow();
            let ctx = qp_ref.emit_ctx().expect("emit_ctx failed");
            for _ in 0..count {
                // Echo back with inline data (3:1 signaling ratio)
                if send_count % SEND_RECV_SIGNAL_INTERVAL == SEND_RECV_SIGNAL_INTERVAL - 1 {
                    let _ = emit_wqe!(&ctx, send {
                        flags: WqeFlags::empty(),
                        inline: echo_data.as_slice(),
                        signaled: send_count as u64,
                    });
                } else {
                    let _ = emit_wqe!(&ctx, send {
                        flags: WqeFlags::empty(),
                        inline: echo_data.as_slice(),
                    });
                }
                send_count += 1;
            }
        }

        let qp_ref = qp.borrow();
        qp_ref.ring_rq_doorbell();
        qp_ref.ring_sq_doorbell();
    }
}

// =============================================================================
// Benchmark Functions
// =============================================================================

/// 32B inline send throughput benchmark (queue depth = 1024, 1/4 signaling ratio).
fn run_send_inline_throughput<SF, RF>(
    client: &mut SendRecvEndpoint<SF, RF>,
    iters: u64,
) -> Duration
where
    SF: Fn(Cqe, u64),
    RF: Fn(Cqe, u64),
{
    let send_data = vec![0xAAu8; SMALL_MSG_SIZE];
    // Use min of configured interval and iters to handle small iteration counts
    let signal_interval = SEND_RECV_SIGNAL_INTERVAL.min(iters as usize).max(1);

    // Initial fill with 3:1 signaling ratio (like send_recv_small.rs)
    let initial_fill = (iters as usize).min(SEND_RECV_QUEUE_DEPTH);
    {
        let qp = client.qp.borrow();
        let ctx = qp.emit_ctx().expect("emit_ctx failed");
        let full_batches = initial_fill / signal_interval;
        let remainder = initial_fill % signal_interval;

        for batch in 0..full_batches {
            let base = batch * signal_interval;
            // (signal_interval - 1) unsignaled
            for _ in 0..(signal_interval - 1) {
                let _ = emit_wqe!(&ctx, send {
                    flags: WqeFlags::empty(),
                    inline: send_data.as_slice(),
                });
            }
            // 1 signaled
            let idx = base + signal_interval - 1;
            let _ = emit_wqe!(&ctx, send {
                flags: WqeFlags::empty(),
                inline: send_data.as_slice(),
                signaled: idx as u64,
            });
        }

        // Remainder (all signaled for correctness)
        for j in 0..remainder {
            let idx = full_batches * signal_interval + j;
            let _ = emit_wqe!(&ctx, send {
                flags: WqeFlags::empty(),
                inline: send_data.as_slice(),
                signaled: idx as u64,
            });
        }
        qp.ring_sq_doorbell();
    }

    let start = std::time::Instant::now();
    let mut completed = 0u64;
    let mut inflight = initial_fill as u64;
    let mut sent = initial_fill as u64;
    let mut recv_idx = 0usize;

    while completed < iters {
        client.shared_state.reset();
        client.recv_cq.poll();
        client.recv_cq.flush();
        let rx_count = client.shared_state.rx_count.get();

        completed += rx_count as u64;
        inflight -= rx_count as u64;

        if rx_count == 0 {
            continue;
        }

        client.send_cq.poll();
        client.send_cq.flush();

        // Repost receives
        {
            let qp = client.qp.borrow();
            for _ in 0..rx_count {
                let idx = recv_idx % SEND_RECV_QUEUE_DEPTH;
                let offset = (idx * 64) as u64;
                qp.post_recv(idx as u64, client.recv_buf.addr() + offset, 64, client.recv_mr.lkey())
                    .unwrap();
                recv_idx += 1;
            }
        }

        // Send more if needed with 3:1 signaling ratio
        let remaining = iters.saturating_sub(sent);
        let can_send = (SEND_RECV_QUEUE_DEPTH as u64).saturating_sub(inflight).min(remaining) as usize;
        let to_send = can_send.min(rx_count);

        if to_send > 0 {
            let qp = client.qp.borrow();
            let ctx = qp.emit_ctx().expect("emit_ctx failed");
            let full_batches = to_send / signal_interval;
            let remainder = to_send % signal_interval;

            for batch in 0..full_batches {
                // (signal_interval - 1) unsignaled
                for _ in 0..(signal_interval - 1) {
                    let _ = emit_wqe!(&ctx, send {
                        flags: WqeFlags::empty(),
                        inline: send_data.as_slice(),
                    });
                }
                // 1 signaled
                let _ = emit_wqe!(&ctx, send {
                    flags: WqeFlags::empty(),
                    inline: send_data.as_slice(),
                    signaled: batch as u64,
                });
            }

            // Remainder (all signaled)
            for j in 0..remainder {
                let _ = emit_wqe!(&ctx, send {
                    flags: WqeFlags::empty(),
                    inline: send_data.as_slice(),
                    signaled: j as u64,
                });
            }

            inflight += to_send as u64;
            sent += to_send as u64;
        }

        // Ring doorbells
        {
            let qp = client.qp.borrow();
            qp.ring_rq_doorbell();
            if to_send > 0 {
                qp.ring_sq_doorbell();
            }
        }
    }

    // Drain remaining inflight
    while inflight > 0 {
        client.shared_state.reset();
        client.recv_cq.poll();
        client.recv_cq.flush();
        inflight -= client.shared_state.rx_count.get() as u64;

        client.send_cq.poll();
        client.send_cq.flush();
    }

    start.elapsed()
}

/// 32B inline send latency benchmark (ping-pong, queue depth = 1).
fn run_send_latency<SF, RF>(
    client: &mut SendRecvEndpoint<SF, RF>,
    iters: u64,
) -> Duration
where
    SF: Fn(Cqe, u64),
    RF: Fn(Cqe, u64),
{
    let send_data = vec![0xAAu8; SMALL_MSG_SIZE];

    let start = std::time::Instant::now();

    for i in 0..iters {
        let idx = (i as usize) % SEND_RECV_QUEUE_DEPTH;

        // Post single Send with doorbell
        {
            let qp = client.qp.borrow();
            let ctx = qp.emit_ctx().expect("emit_ctx failed");
            emit_wqe!(&ctx, send {
                flags: WqeFlags::empty(),
                inline: send_data.as_slice(),
                signaled: idx as u64,
            }).expect("emit_wqe failed");
            qp.ring_sq_doorbell();
        }

        // Wait for echo response
        loop {
            client.send_cq.poll();
            client.send_cq.flush();
            client.shared_state.reset();
            client.recv_cq.poll();
            client.recv_cq.flush();
            if client.shared_state.rx_count.get() > 0 {
                // Immediate replenish for latency benchmark
                let qp = client.qp.borrow();
                let replenish_idx = client.rq_next_idx % SEND_RECV_QUEUE_DEPTH;
                let replenish_offset = (replenish_idx * 64) as u64;
                qp.post_recv(replenish_idx as u64, client.recv_buf.addr() + replenish_offset, 64, client.recv_mr.lkey())
                    .unwrap();
                qp.ring_rq_doorbell();
                client.rq_next_idx += 1;
                break;
            }
        }
    }

    start.elapsed()
}

/// 4KiB WRITE benchmark.
fn run_write_1m<SF, RF>(
    endpoint: &mut RdmaLoopbackEndpoint<SF, RF>,
    iters: u64,
    relaxed: bool,
) -> Duration
where
    SF: Fn(Cqe, u64),
    RF: Fn(Cqe, u64),
{
    let remote_addr = endpoint.remote_buf.addr();
    let rkey = endpoint.remote_mr.rkey();
    let local_addr = endpoint.local_buf.addr();
    let lkey = endpoint.local_mr.lkey();
    let wqe_flags = if relaxed { WqeFlags::RELAXED_ORDERING } else { WqeFlags::empty() };
    // Use min of configured interval and iters to handle small iteration counts
    let signal_interval = RDMA_SIGNAL_INTERVAL.min(iters as usize).max(1);

    // Drain any pending completions first
    loop {
        let n = endpoint.send_cq.poll();
        endpoint.send_cq.flush();
        if n == 0 {
            break;
        }
    }

    let start = std::time::Instant::now();
    // Track signaled completions, not total operations
    let expected_signals = (iters as usize + signal_interval - 1) / signal_interval;
    let mut signals_received = 0usize;
    let mut posted = 0u64;
    let mut signals_posted = 0usize;
    let mut send_idx = 0usize;

    while signals_received < expected_signals {
        // Poll completions
        let n = endpoint.send_cq.poll();
        endpoint.send_cq.flush();
        signals_received += n;

        // Send more if we have capacity (based on signaled operations in flight)
        let signals_inflight = signals_posted - signals_received;
        let remaining = iters.saturating_sub(posted);
        // Limit inflight based on queue depth
        let max_signals_inflight = RDMA_QUEUE_DEPTH / signal_interval;
        let can_post_signals = max_signals_inflight.saturating_sub(signals_inflight);
        let can_send = (can_post_signals * signal_interval).min(remaining as usize);

        if can_send > 0 {
            let qp = endpoint.qp.borrow();
            let ctx = qp.emit_ctx().expect("emit_ctx failed");

            for _ in 0..can_send {
                // Signal at the end of each interval
                let is_interval_end = (send_idx + 1) % signal_interval == 0;
                let is_last = posted + 1 == iters;
                if is_interval_end || is_last {
                    emit_wqe!(&ctx, write {
                        flags: wqe_flags,
                        remote_addr: remote_addr,
                        rkey: rkey,
                        sge: { addr: local_addr, len: RDMA_MSG_SIZE as u32, lkey: lkey },
                        signaled: send_idx as u64,
                    }).expect("emit_wqe failed");
                    signals_posted += 1;
                } else {
                    emit_wqe!(&ctx, write {
                        flags: wqe_flags,
                        remote_addr: remote_addr,
                        rkey: rkey,
                        sge: { addr: local_addr, len: RDMA_MSG_SIZE as u32, lkey: lkey },
                    }).expect("emit_wqe failed");
                }
                send_idx += 1;
                posted += 1;
            }

            qp.ring_sq_doorbell();
        }
    }

    start.elapsed()
}

/// 4KiB READ benchmark.
fn run_read_1m<SF, RF>(
    endpoint: &mut RdmaLoopbackEndpoint<SF, RF>,
    iters: u64,
    relaxed: bool,
) -> Duration
where
    SF: Fn(Cqe, u64),
    RF: Fn(Cqe, u64),
{
    let remote_addr = endpoint.remote_buf.addr();
    let rkey = endpoint.remote_mr.rkey();
    let local_addr = endpoint.local_buf.addr();
    let lkey = endpoint.local_mr.lkey();
    let wqe_flags = if relaxed { WqeFlags::RELAXED_ORDERING } else { WqeFlags::empty() };
    // Use min of configured interval and iters to handle small iteration counts
    let signal_interval = RDMA_SIGNAL_INTERVAL.min(iters as usize).max(1);

    // Drain any pending completions first
    loop {
        let n = endpoint.send_cq.poll();
        endpoint.send_cq.flush();
        if n == 0 {
            break;
        }
    }

    let start = std::time::Instant::now();
    // Track signaled completions, not total operations
    let expected_signals = (iters as usize + signal_interval - 1) / signal_interval;
    let mut signals_received = 0usize;
    let mut posted = 0u64;
    let mut signals_posted = 0usize;
    let mut send_idx = 0usize;

    while signals_received < expected_signals {
        // Poll completions
        let n = endpoint.send_cq.poll();
        endpoint.send_cq.flush();
        signals_received += n;

        // Send more if we have capacity (based on signaled operations in flight)
        let signals_inflight = signals_posted - signals_received;
        let remaining = iters.saturating_sub(posted);
        // Limit inflight based on queue depth
        let max_signals_inflight = RDMA_QUEUE_DEPTH / signal_interval;
        let can_post_signals = max_signals_inflight.saturating_sub(signals_inflight);
        let can_send = (can_post_signals * signal_interval).min(remaining as usize);

        if can_send > 0 {
            let qp = endpoint.qp.borrow();
            let ctx = qp.emit_ctx().expect("emit_ctx failed");

            for _ in 0..can_send {
                // Signal at the end of each interval
                let is_interval_end = (send_idx + 1) % signal_interval == 0;
                let is_last = posted + 1 == iters;
                if is_interval_end || is_last {
                    emit_wqe!(&ctx, read {
                        flags: wqe_flags,
                        remote_addr: remote_addr,
                        rkey: rkey,
                        sge: { addr: local_addr, len: RDMA_MSG_SIZE as u32, lkey: lkey },
                        signaled: send_idx as u64,
                    }).expect("emit_wqe failed");
                    signals_posted += 1;
                } else {
                    emit_wqe!(&ctx, read {
                        flags: wqe_flags,
                        remote_addr: remote_addr,
                        rkey: rkey,
                        sge: { addr: local_addr, len: RDMA_MSG_SIZE as u32, lkey: lkey },
                    }).expect("emit_wqe failed");
                }
                send_idx += 1;
                posted += 1;
            }

            qp.ring_sq_doorbell();
        }
    }

    start.elapsed()
}

// =============================================================================
// Criterion Benchmarks
// =============================================================================

fn bench_send_inline_32b(c: &mut Criterion) {
    let setup = match setup_send_recv_benchmark() {
        Some(s) => s,
        None => {
            eprintln!("Skipping benchmark: no mlx5 device available");
            return;
        }
    };

    let _ctx = setup._ctx;
    let _pd = setup._pd;
    let client = RefCell::new(setup.client);
    let _server_handle = setup._server_handle;

    let mut group = c.benchmark_group("send_inline");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(3));
    group.throughput(Throughput::Elements(1));

    group.bench_function("32B", |b| {
        b.iter_custom(|iters| {
            run_send_inline_throughput(&mut client.borrow_mut(), iters)
        });
    });

    group.finish();
}

fn bench_send_latency_32b(c: &mut Criterion) {
    let setup = match setup_send_recv_benchmark() {
        Some(s) => s,
        None => {
            eprintln!("Skipping benchmark: no mlx5 device available");
            return;
        }
    };

    let _ctx = setup._ctx;
    let _pd = setup._pd;
    let client = RefCell::new(setup.client);
    let _server_handle = setup._server_handle;

    let mut group = c.benchmark_group("latency");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(1));
    group.throughput(Throughput::Elements(1));

    group.bench_function("32B", |b| {
        b.iter_custom(|iters| {
            run_send_latency(&mut client.borrow_mut(), iters)
        });
    });

    group.finish();
}

fn bench_write_1m(c: &mut Criterion) {
    let setup = match setup_rdma_loopback_benchmark(false) {
        Some(s) => s,
        None => {
            eprintln!("Skipping benchmark: no mlx5 device available");
            return;
        }
    };

    let _ctx = setup._ctx;
    let _pd = setup._pd;
    let endpoint = RefCell::new(setup.endpoint);

    let mut group = c.benchmark_group("write_1m");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(3));
    group.throughput(Throughput::Bytes(RDMA_MSG_SIZE as u64));

    group.bench_function("normal", |b| {
        b.iter_custom(|iters| {
            run_write_1m(&mut endpoint.borrow_mut(), iters, false)
        });
    });

    group.finish();
}

fn bench_write_1m_relaxed(c: &mut Criterion) {
    let setup = match setup_rdma_loopback_benchmark(true) {
        Some(s) => s,
        None => {
            eprintln!("Skipping benchmark: no mlx5 device available");
            return;
        }
    };

    let _ctx = setup._ctx;
    let _pd = setup._pd;
    let endpoint = RefCell::new(setup.endpoint);

    let mut group = c.benchmark_group("write_1m");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(3));
    group.throughput(Throughput::Bytes(RDMA_MSG_SIZE as u64));

    group.bench_function("relaxed", |b| {
        b.iter_custom(|iters| {
            run_write_1m(&mut endpoint.borrow_mut(), iters, true)
        });
    });

    group.finish();
}

fn bench_read_1m(c: &mut Criterion) {
    let setup = match setup_rdma_loopback_benchmark(false) {
        Some(s) => s,
        None => {
            eprintln!("Skipping benchmark: no mlx5 device available");
            return;
        }
    };

    let _ctx = setup._ctx;
    let _pd = setup._pd;
    let endpoint = RefCell::new(setup.endpoint);

    let mut group = c.benchmark_group("read_1m");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(3));
    group.throughput(Throughput::Bytes(RDMA_MSG_SIZE as u64));

    group.bench_function("normal", |b| {
        b.iter_custom(|iters| {
            run_read_1m(&mut endpoint.borrow_mut(), iters, false)
        });
    });

    group.finish();
}

fn bench_read_1m_relaxed(c: &mut Criterion) {
    let setup = match setup_rdma_loopback_benchmark(true) {
        Some(s) => s,
        None => {
            eprintln!("Skipping benchmark: no mlx5 device available");
            return;
        }
    };

    let _ctx = setup._ctx;
    let _pd = setup._pd;
    let endpoint = RefCell::new(setup.endpoint);

    let mut group = c.benchmark_group("read_1m");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(3));
    group.throughput(Throughput::Bytes(RDMA_MSG_SIZE as u64));

    group.bench_function("relaxed", |b| {
        b.iter_custom(|iters| {
            run_read_1m(&mut endpoint.borrow_mut(), iters, true)
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_send_inline_32b,
    bench_send_latency_32b,
    bench_write_1m,
    bench_write_1m_relaxed,
    bench_read_1m,
    bench_read_1m_relaxed,
);
criterion_main!(benches);
