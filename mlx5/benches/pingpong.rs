//! Ping-pong latency and throughput benchmarks for RDMA operations.
//!
//! Benchmarks:
//! - Throughput: WRITE with Immediate (queue depth = 64, batched doorbells)
//! - Low-latency: WRITE with Immediate (queue depth = 1, inline + blueflame)
//!
//! Run with:
//! ```bash
//! cargo bench --bench pingpong
//! ```

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use mlx5::cq::{CompletionQueue, Cqe, CqeOpcode};
use mlx5::device::{Context, DeviceList};
use mlx5::pd::{AccessFlags, MemoryRegion, Pd};
use mlx5::qp::{RcQp, RcQpConfig, RemoteQpInfo};
use mlx5::wqe::{WqeFlags, WqeOpcode};

// =============================================================================
// Constants
// =============================================================================

const QUEUE_DEPTH: usize = 64;
const PAGE_SIZE: usize = 4096;
const BUFFER_SIZE: usize = 64 * 256; // 64 entries * 256 bytes

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

// =============================================================================
// Connection Info for cross-thread setup
// =============================================================================

#[derive(Clone)]
struct ConnectionInfo {
    qpn: u32,
    lid: u16,
    buf_addr: u64,
    rkey: u32,
}

// =============================================================================
// Benchmark Context
// =============================================================================

/// Shared state for collecting CQEs from callback.
#[derive(Clone)]
struct SharedCqeState {
    rx_count: Rc<Cell<usize>>,
    rx_indices: Rc<RefCell<[usize; QUEUE_DEPTH]>>,
    rx_sizes: Rc<RefCell<[u32; QUEUE_DEPTH]>>,
    rx_is_write_imm: Rc<RefCell<[bool; QUEUE_DEPTH]>>,
}

impl SharedCqeState {
    fn new() -> Self {
        Self {
            rx_count: Rc::new(Cell::new(0)),
            rx_indices: Rc::new(RefCell::new([0; QUEUE_DEPTH])),
            rx_sizes: Rc::new(RefCell::new([0; QUEUE_DEPTH])),
            rx_is_write_imm: Rc::new(RefCell::new([false; QUEUE_DEPTH])),
        }
    }

    fn reset(&self) {
        self.rx_count.set(0);
    }

    fn push(&self, cqe: &Cqe) {
        let count = self.rx_count.get();
        if count < QUEUE_DEPTH {
            let idx = (cqe.imm as usize) % QUEUE_DEPTH;
            self.rx_indices.borrow_mut()[count] = idx;
            self.rx_sizes.borrow_mut()[count] = cqe.byte_cnt.min(256);
            self.rx_is_write_imm.borrow_mut()[count] = matches!(cqe.opcode, CqeOpcode::RespRdmaWriteImm);
            self.rx_count.set(count + 1);
        }
    }
}

/// Callback that collects responder CQEs into shared state.
fn make_callback(state: SharedCqeState) -> impl Fn(Cqe, u64) {
    move |cqe: Cqe, _entry: u64| {
        if cqe.opcode.is_responder() && cqe.syndrome == 0 {
            state.push(&cqe);
        }
    }
}

type BenchCallback = Box<dyn Fn(Cqe, u64)>;
type BenchQp = RcQp<u64, BenchCallback>;

struct EndpointState {
    qp: Rc<RefCell<BenchQp>>,
    send_cq: Rc<CompletionQueue>,
    recv_cq: Rc<CompletionQueue>,
    shared_state: SharedCqeState,
    // MRs must be dropped before buffers (drop order is declaration order)
    send_mr: MemoryRegion,
    recv_mr: MemoryRegion,
    send_buf: AlignedBuffer,
    recv_buf: AlignedBuffer,
    remote_addr: u64,
    remote_rkey: u32,
}

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

struct BenchmarkSetup {
    client: EndpointState,
    _server_handle: ServerHandle,
    // Field order matters for drop order: PD must be dropped before Context
    _pd: Rc<Pd>,
    // Context must be dropped LAST - all resources depend on it
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

fn create_endpoint(
    ctx: &Context,
    pd: &Rc<Pd>,
) -> Option<(
    Rc<RefCell<BenchQp>>,
    Rc<CompletionQueue>,
    Rc<CompletionQueue>,
    SharedCqeState,
    AlignedBuffer,
    AlignedBuffer,
    MemoryRegion,
    MemoryRegion,
)> {
    let mut send_cq = ctx.create_cq(256).ok()?;
    send_cq.init_direct_access().ok()?;
    let send_cq = Rc::new(send_cq);
    let mut recv_cq = ctx.create_cq(256).ok()?;
    recv_cq.init_direct_access().ok()?;
    let recv_cq = Rc::new(recv_cq);

    let config = RcQpConfig {
        max_send_wr: 256,
        max_recv_wr: 256,
        max_send_sge: 1,
        max_recv_sge: 1,
        max_inline_data: 256,
    };

    let shared_state = SharedCqeState::new();
    let callback: BenchCallback = Box::new(make_callback(shared_state.clone()));

    let qp = ctx
        .create_rc_qp(pd, &send_cq, &recv_cq, &config, callback)
        .ok()?;

    let send_buf = AlignedBuffer::new(BUFFER_SIZE);
    let recv_buf = AlignedBuffer::new(BUFFER_SIZE);

    let send_mr =
        unsafe { pd.register(send_buf.as_ptr(), send_buf.size(), full_access()) }.ok()?;
    let recv_mr =
        unsafe { pd.register(recv_buf.as_ptr(), recv_buf.size(), full_access()) }.ok()?;

    Some((qp, send_cq, recv_cq, shared_state, send_buf, recv_buf, send_mr, recv_mr))
}

fn setup_benchmark() -> Option<BenchmarkSetup> {
    let ctx = open_mlx5_device()?;
    let port = 1u8;
    let port_attr = ctx.query_port(port).ok()?;
    let pd = Rc::new(ctx.alloc_pd().ok()?);

    // Create client endpoint
    let (client_qp, client_send_cq, client_recv_cq, client_shared_state, client_send_buf, client_recv_buf, client_send_mr, client_recv_mr) =
        create_endpoint(&ctx, &pd)?;

    // Client connection info
    let client_info = ConnectionInfo {
        qpn: client_qp.borrow().qpn(),
        lid: port_attr.lid,
        buf_addr: client_recv_buf.addr(),
        rkey: client_recv_mr.rkey(),
    };

    // Channel for server to send its connection info
    let (server_info_tx, server_info_rx): (Sender<ConnectionInfo>, Receiver<ConnectionInfo>) =
        mpsc::channel();
    // Channel to send client info to server
    let (client_info_tx, client_info_rx): (Sender<ConnectionInfo>, Receiver<ConnectionInfo>) =
        mpsc::channel();
    // Signal for server ready
    let server_ready = Arc::new(AtomicU32::new(0));
    let server_ready_clone = server_ready.clone();

    let stop_flag = Arc::new(AtomicBool::new(false));
    let server_stop = stop_flag.clone();

    // Start server thread (creates its own context)
    let handle = thread::spawn(move || {
        server_thread_main(
            server_info_tx,
            client_info_rx,
            server_ready_clone,
            server_stop,
        );
    });

    // Send client info to server
    client_info_tx.send(client_info.clone()).ok()?;

    // Wait for server info
    let server_info = server_info_rx.recv().ok()?;

    // Wait for server to be ready
    while server_ready.load(Ordering::Acquire) == 0 {
        std::hint::spin_loop();
    }

    // Connect client QP to server
    let server_remote = RemoteQpInfo {
        qpn: server_info.qpn,
        psn: 0,
        lid: server_info.lid,
    };

    let access = full_access().bits();
    client_qp
        .borrow_mut()
        .connect(&server_remote, port, 0, 4, 4, access)
        .ok()?;

    // Pre-post receives on client
    for i in 0..QUEUE_DEPTH {
        let offset = (i * 256) as u64;
        let _ = client_qp
            .borrow()
            .recv_builder(i as u64)
            .map(|b| b.sge(client_recv_buf.addr() + offset, 256, client_recv_mr.lkey()).finish());
    }
    client_qp.borrow().ring_rq_doorbell();

    let client = EndpointState {
        qp: client_qp,
        send_cq: client_send_cq,
        recv_cq: client_recv_cq,
        shared_state: client_shared_state,
        send_mr: client_send_mr,
        recv_mr: client_recv_mr,
        send_buf: client_send_buf,
        recv_buf: client_recv_buf,
        remote_addr: server_info.buf_addr,
        remote_rkey: server_info.rkey,
    };

    let server_handle = ServerHandle {
        stop_flag,
        handle: Some(handle),
    };

    Some(BenchmarkSetup {
        client,
        _server_handle: server_handle,
        _pd: pd,
        _ctx: ctx,
    })
}

// =============================================================================
// Server Thread
// =============================================================================

fn server_thread_main(
    info_tx: Sender<ConnectionInfo>,
    info_rx: Receiver<ConnectionInfo>,
    ready_signal: Arc<AtomicU32>,
    stop_flag: Arc<AtomicBool>,
) {
    // Create separate device context for server
    let ctx = match open_mlx5_device() {
        Some(c) => c,
        None => {
            eprintln!("Server: Failed to open device");
            return;
        }
    };

    let port = 1u8;
    let port_attr = match ctx.query_port(port) {
        Ok(attr) => attr,
        Err(_) => {
            eprintln!("Server: Failed to query port");
            return;
        }
    };

    let pd = Rc::new(match ctx.alloc_pd() {
        Ok(p) => p,
        Err(_) => {
            eprintln!("Server: Failed to alloc PD");
            return;
        }
    });

    // Create server endpoint
    let (qp, send_cq, recv_cq, shared_state, send_buf, recv_buf, send_mr, recv_mr) =
        match create_endpoint(&ctx, &pd) {
            Some(e) => e,
            None => {
                eprintln!("Server: Failed to create endpoint");
                return;
            }
        };

    // Send server info to client
    let server_info = ConnectionInfo {
        qpn: qp.borrow().qpn(),
        lid: port_attr.lid,
        buf_addr: recv_buf.addr(),
        rkey: recv_mr.rkey(),
    };
    if info_tx.send(server_info).is_err() {
        eprintln!("Server: Failed to send connection info");
        return;
    }

    // Receive client info
    let client_info = match info_rx.recv() {
        Ok(info) => info,
        Err(_) => {
            eprintln!("Server: Failed to receive client info");
            return;
        }
    };

    // Connect server QP to client
    let client_remote = RemoteQpInfo {
        qpn: client_info.qpn,
        psn: 0,
        lid: client_info.lid,
    };

    let access = full_access().bits();
    if qp
        .borrow_mut()
        .connect(&client_remote, port, 0, 4, 4, access)
        .is_err()
    {
        eprintln!("Server: Failed to connect QP");
        return;
    }

    // Pre-post receives
    for i in 0..QUEUE_DEPTH {
        let offset = (i * 256) as u64;
        let _ = qp
            .borrow()
            .recv_builder(i as u64)
            .map(|b| b.sge(recv_buf.addr() + offset, 256, recv_mr.lkey()).finish());
    }
    qp.borrow().ring_rq_doorbell();

    // Signal ready
    ready_signal.store(1, Ordering::Release);

    // Server loop - optimized processing order:
    // 1. RX CQE処理 (collect indices and sizes via callback)
    // 2. TX CQE処理 (free WQE slots)
    // 3. RX WQE再補充
    // 4. TX リクエスト再補充 (echo responses)
    // 5. Ring doorbells
    let remote_addr = client_info.buf_addr;
    let remote_rkey = client_info.rkey;

    while !stop_flag.load(Ordering::Relaxed) {
        // 1. RX CQE処理: poll recv CQ (callback collects completions)
        shared_state.reset();
        recv_cq.poll();
        recv_cq.flush();
        let rx_count = shared_state.rx_count.get();

        if rx_count == 0 {
            continue;
        }

        // 2. TX CQE処理: drain send CQ to free WQE slots
        send_cq.poll();
        send_cq.flush();

        // Copy shared state data for use below
        let rx_indices = *shared_state.rx_indices.borrow();
        let rx_sizes = *shared_state.rx_sizes.borrow();
        let rx_is_write_imm = *shared_state.rx_is_write_imm.borrow();

        // 3. RX WQE再補充
        {
            let qp_ref = qp.borrow();
            for i in 0..rx_count {
                let idx = rx_indices[i];
                let offset = (idx * 256) as u64;
                let _ = qp_ref
                    .recv_builder(idx as u64)
                    .map(|b| b.sge(recv_buf.addr() + offset, 256, recv_mr.lkey()).finish());
            }
        }

        // 4. TX リクエスト再補充: queue echo responses
        {
            let qp_ref = qp.borrow();
            for i in 0..rx_count {
                let idx = rx_indices[i];
                let offset = (idx * 256) as u64;
                let size = rx_sizes[i];

                if rx_is_write_imm[i] {
                    // Echo with WRITE+IMM
                    let _ = qp_ref.wqe_builder(idx as u64).map(|b| {
                        b.ctrl(WqeOpcode::RdmaWriteImm, WqeFlags::empty(), idx as u32)
                            .rdma(remote_addr + offset, remote_rkey)
                            .sge(send_buf.addr() + offset, size, send_mr.lkey())
                            .finish()
                    });
                } else {
                    // Echo with SEND
                    let _ = qp_ref.wqe_builder(idx as u64).map(|b| {
                        b.ctrl(WqeOpcode::Send, WqeFlags::empty(), 0)
                            .sge(send_buf.addr() + offset, size, send_mr.lkey())
                            .finish()
                    });
                }
            }
        }

        // 5. Ring doorbells once each
        {
            let qp_ref = qp.borrow();
            qp_ref.ring_rq_doorbell();
            qp_ref.ring_sq_doorbell();
        }
    }
}

// =============================================================================
// Benchmark Functions
// =============================================================================

/// Throughput benchmark: WRITE+IMM with queue depth = 64, batched doorbells.
///
/// Optimized processing order for maximum queue efficiency:
/// 1. RX CQE処理 (decrement inflight, collect indices via callback)
/// 2. TX CQE処理 (free WQE slots via dispatch)
/// 3. RX WQE再補充 (batch all recv reposts)
/// 4. TX リクエスト再補充 (batch all send posts within inflight/slot limits)
/// 5. Ring TX/RQ doorbells once each
fn run_throughput_bench(client: &mut EndpointState, iters: u64, size: usize) -> Duration {
    let size = size.min(256) as u32;

    // Initial fill - post QUEUE_DEPTH WQEs, then ring doorbell once
    {
        let qp = client.qp.borrow();
        for i in 0..QUEUE_DEPTH {
            let offset = (i * 256) as u64;
            let _ = qp.wqe_builder(i as u64).map(|b| {
                b.ctrl(WqeOpcode::RdmaWriteImm, WqeFlags::empty(), i as u32)
                    .rdma(client.remote_addr + offset, client.remote_rkey)
                    .sge(client.send_buf.addr() + offset, size, client.send_mr.lkey())
                    .finish()
            });
        }
        qp.ring_sq_doorbell();
    }

    let start = std::time::Instant::now();
    let mut completed = 0u64;
    let mut inflight = QUEUE_DEPTH as u64;

    while completed < iters {
        // 1. RX CQE処理: poll recv CQ (callback collects completions)
        client.shared_state.reset();
        client.recv_cq.poll();
        client.recv_cq.flush();
        let rx_count = client.shared_state.rx_count.get();

        // Update counters
        completed += rx_count as u64;
        inflight -= rx_count as u64;

        // Skip rest if no completions (busy wait)
        if rx_count == 0 {
            continue;
        }

        // 2. TX CQE処理: drain send CQ to free WQE slots
        client.send_cq.poll();
        client.send_cq.flush();

        // Copy shared state data for use below
        let rx_indices = *client.shared_state.rx_indices.borrow();

        // 3. RX WQE再補充: batch all recv reposts
        {
            let qp = client.qp.borrow();
            for i in 0..rx_count {
                let idx = rx_indices[i];
                let offset = (idx * 256) as u64;
                let _ = qp
                    .recv_builder(idx as u64)
                    .map(|b| b.sge(client.recv_buf.addr() + offset, 256, client.recv_mr.lkey()).finish());
            }
        }

        // 4. TX リクエスト再補充: post sends within inflight limit and available slots
        let remaining = iters.saturating_sub(completed);
        let can_send = (QUEUE_DEPTH as u64).saturating_sub(inflight).min(remaining) as usize;
        let to_send = can_send.min(rx_count);

        if to_send > 0 {
            let qp = client.qp.borrow();
            for i in 0..to_send {
                let idx = rx_indices[i];
                let offset = (idx * 256) as u64;
                let _ = qp.wqe_builder(idx as u64).map(|b| {
                    b.ctrl(WqeOpcode::RdmaWriteImm, WqeFlags::empty(), idx as u32)
                        .rdma(client.remote_addr + offset, client.remote_rkey)
                        .sge(client.send_buf.addr() + offset, size, client.send_mr.lkey())
                        .finish()
                });
            }
            inflight += to_send as u64;
        }

        // 5. Ring doorbells once each
        {
            let qp = client.qp.borrow();
            qp.ring_rq_doorbell();
            if to_send > 0 {
                qp.ring_sq_doorbell();
            }
        }
    }

    // Drain remaining inflight messages for clean state
    while inflight > 0 {
        client.shared_state.reset();
        client.recv_cq.poll();
        client.recv_cq.flush();
        let rx_count = client.shared_state.rx_count.get();
        inflight -= rx_count as u64;

        let rx_indices = *client.shared_state.rx_indices.borrow();
        let qp = client.qp.borrow();
        for i in 0..rx_count {
            let idx = rx_indices[i];
            let offset = (idx * 256) as u64;
            let _ = qp
                .recv_builder(idx as u64)
                .map(|b| b.sge(client.recv_buf.addr() + offset, 256, client.recv_mr.lkey()).finish());
        }
        client.send_cq.poll();
        client.send_cq.flush();
        std::hint::spin_loop();
    }
    client.qp.borrow().ring_rq_doorbell();

    start.elapsed()
}

/// Low-latency benchmark: WRITE+IMM with queue depth = 1, inline + blueflame.
///
/// Sends one message at a time with inline data and blueflame for minimum latency.
/// Measures round-trip time per operation.
fn run_lowlatency_bench(client: &mut EndpointState, iters: u64, size: usize) -> Duration {
    let size = size.min(64); // Inline data limit
    let data = vec![0xABu8; size];

    let start = std::time::Instant::now();

    for i in 0..iters {
        let idx = (i as usize) % QUEUE_DEPTH;

        // Post single WRITE+IMM with inline data + blueflame
        let _ = client.qp.borrow().wqe_builder(idx as u64).map(|b| {
            let offset = (idx * 256) as u64;
            b.ctrl(WqeOpcode::RdmaWriteImm, WqeFlags::empty(), idx as u32)
                .rdma(client.remote_addr + offset, client.remote_rkey)
                .inline_data(&data)
                .finish_with_blueflame()
        });

        // Wait for completion (recv CQ)
        loop {
            // Poll send CQ to keep it from filling up
            client.send_cq.poll();
            client.send_cq.flush();

            // Check for recv completion via callback
            client.shared_state.reset();
            client.recv_cq.poll();
            client.recv_cq.flush();

            if client.shared_state.rx_count.get() > 0 {
                // Repost recv immediately
                let recv_idx = client.shared_state.rx_indices.borrow()[0];
                let offset = (recv_idx * 256) as u64;
                let qp = client.qp.borrow();
                let _ = qp
                    .recv_builder(recv_idx as u64)
                    .map(|b| b.sge(client.recv_buf.addr() + offset, 256, client.recv_mr.lkey()).finish());
                qp.ring_rq_doorbell();
                break;
            }
            std::hint::spin_loop();
        }
    }

    start.elapsed()
}

// =============================================================================
// Criterion Benchmarks
// =============================================================================

fn pingpong_benchmarks(c: &mut Criterion) {
    let setup = match setup_benchmark() {
        Some(s) => s,
        None => {
            eprintln!("Skipping benchmarks: no mlx5 device available");
            return;
        }
    };

    // Drop order is REVERSE of declaration order in Rust.
    // Context must be dropped LAST, so it must be declared FIRST.
    let _ctx = setup._ctx;
    let _pd = setup._pd;
    let client = RefCell::new(setup.client);
    let mut server_handle = setup._server_handle;

    let size = 32usize;

    // Throughput benchmark: queue depth = 64, batched doorbells
    {
        let mut group = c.benchmark_group("throughput");
        group.sample_size(10);
        group.measurement_time(Duration::from_secs(3));
        group.throughput(Throughput::Elements(1)); // Per operation throughput

        group.bench_with_input(
            BenchmarkId::new("write_imm_qd64", size),
            &size,
            |b, &size| {
                b.iter_custom(|iters| run_throughput_bench(&mut client.borrow_mut(), iters, size));
            },
        );

        group.finish();
    }

    // Low-latency benchmark: queue depth = 1, inline + blueflame
    {
        let mut group = c.benchmark_group("latency");
        group.sample_size(10);
        group.measurement_time(Duration::from_secs(1));

        group.bench_with_input(
            BenchmarkId::new("write_imm_inline_bf", size),
            &size,
            |b, &size| {
                b.iter_custom(|iters| run_lowlatency_bench(&mut client.borrow_mut(), iters, size));
            },
        );

        group.finish();
    }

    // Cleanup: stop server first, then drain CQs
    server_handle.stop();

    // Drain any remaining completions
    let client = client.into_inner();
    loop {
        let mut drained = false;
        client.shared_state.reset();
        client.recv_cq.poll();
        if client.shared_state.rx_count.get() > 0 {
            drained = true;
        }
        client.recv_cq.flush();
        if client.send_cq.poll() > 0 {
            drained = true;
        }
        client.send_cq.flush();
        if !drained {
            break;
        }
    }
}

criterion_group!(benches, pingpong_benchmarks);
criterion_main!(benches);
