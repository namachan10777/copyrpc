//! Send/Recv benchmark for small messages (≤32 bytes).
//!
//! Compares performance with and without scatter-to-CQE:
//! - scatter-to-CQE enabled: Small data is inlined directly in CQE
//! - scatter-to-CQE disabled: Data goes to receive buffer
//!
//! Run with:
//! ```bash
//! taskset -c 0,2,4,6,8 cargo bench --bench send_recv_small
//! ```

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use mlx5::cq::{CqConfig, Cqe};
use mlx5::device::{Context, DeviceList};
use mlx5::mono_cq::MonoCqRc;
use mlx5::pd::{AccessFlags, MemoryRegion, Pd};
use mlx5::qp::{RcQpConfig, RcQpForMonoCq};
use mlx5::transport::IbRemoteQpInfo;
use mlx5::wqe::WqeFlags;

// =============================================================================
// Constants
// =============================================================================

const QUEUE_DEPTH: usize = 1024;
const PAGE_SIZE: usize = 4096;
const BUFFER_SIZE: usize = QUEUE_DEPTH * 64; // entries * 64 bytes for small messages

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
}

// =============================================================================
// Shared CQE State
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

    fn push(&self, cqe: &Cqe) {
        if cqe.opcode.is_responder() && cqe.syndrome == 0 {
            self.rx_count.set(self.rx_count.get() + 1);
        }
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
// Endpoint State
// =============================================================================

struct EndpointState<SF, RF>
where
    SF: Fn(Cqe, u64),
    RF: Fn(Cqe, u64),
{
    qp: Rc<RefCell<RcQpForMonoCq<u64>>>,
    send_cq: Rc<MonoCqRc<u64, SF>>,
    recv_cq: Rc<MonoCqRc<u64, RF>>,
    shared_state: SharedCqeState,
    send_mr: MemoryRegion,
    recv_mr: MemoryRegion,
    send_buf: AlignedBuffer,
    recv_buf: AlignedBuffer,
}

struct BenchmarkSetup<SF, RF>
where
    SF: Fn(Cqe, u64),
    RF: Fn(Cqe, u64),
{
    client: EndpointState<SF, RF>,
    _server_handle: ServerHandle,
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

fn setup_benchmark(
    enable_scatter_to_cqe: bool,
) -> Option<BenchmarkSetup<impl Fn(Cqe, u64), impl Fn(Cqe, u64)>> {
    let ctx = open_mlx5_device()?;
    let port = 1u8;
    let port_attr = ctx.query_port(port).ok()?;
    let pd = Rc::new(ctx.alloc_pd().ok()?);

    let shared_state = SharedCqeState::new();
    let shared_state_for_recv = shared_state.clone();

    let send_callback = move |_cqe: Cqe, _entry: u64| {};
    let recv_callback = move |cqe: Cqe, _entry: u64| {
        shared_state_for_recv.push(&cqe);
    };

    let send_cq = Rc::new(ctx.create_mono_cq(QUEUE_DEPTH as i32, send_callback, &CqConfig::default()).ok()?);
    let recv_cq = Rc::new(ctx.create_mono_cq(QUEUE_DEPTH as i32, recv_callback, &CqConfig::default()).ok()?);

    let config = RcQpConfig {
        max_send_wr: QUEUE_DEPTH as u32,
        max_recv_wr: QUEUE_DEPTH as u32,
        max_send_sge: 1,
        max_recv_sge: 1,
        max_inline_data: 64,
        enable_scatter_to_cqe,
    };

    let qp = ctx
        .rc_qp_builder::<u64, u64>(&pd, &config)
        .sq_mono_cq(&send_cq)
        .rq_mono_cq(&recv_cq)
        .build()
        .ok()?;

    send_cq.register(&qp);
    recv_cq.register(&qp);

    let send_buf = AlignedBuffer::new(BUFFER_SIZE);
    let recv_buf = AlignedBuffer::new(BUFFER_SIZE);

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
            enable_scatter_to_cqe,
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

    // Pre-post receives
    for i in 0..QUEUE_DEPTH {
        qp.borrow()
            .post_recv(i as u64, recv_buf.addr() + (i * 64) as u64, 64, recv_mr.lkey())
            .unwrap();
    }
    qp.borrow().ring_rq_doorbell();

    let client = EndpointState {
        qp,
        send_cq,
        recv_cq,
        shared_state,
        send_mr,
        recv_mr,
        send_buf,
        recv_buf,
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
    enable_scatter_to_cqe: bool,
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

    let rx_count = Rc::new(Cell::new(0usize));
    let rx_count_for_recv = rx_count.clone();

    let send_callback = move |_cqe: Cqe, _entry: u64| {};
    let recv_callback = move |cqe: Cqe, _entry: u64| {
        if cqe.opcode.is_responder() && cqe.syndrome == 0 {
            rx_count_for_recv.set(rx_count_for_recv.get() + 1);
        }
    };

    let send_cq = match ctx.create_mono_cq(QUEUE_DEPTH as i32, send_callback, &CqConfig::default()) {
        Ok(cq) => Rc::new(cq),
        Err(_) => return,
    };

    let recv_cq = match ctx.create_mono_cq(QUEUE_DEPTH as i32, recv_callback, &CqConfig::default()) {
        Ok(cq) => Rc::new(cq),
        Err(_) => return,
    };

    let config = RcQpConfig {
        max_send_wr: QUEUE_DEPTH as u32,
        max_recv_wr: QUEUE_DEPTH as u32,
        max_send_sge: 1,
        max_recv_sge: 1,
        max_inline_data: 64,
        enable_scatter_to_cqe,
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

    let send_buf = AlignedBuffer::new(BUFFER_SIZE);
    let recv_buf = AlignedBuffer::new(BUFFER_SIZE);

    let send_mr = match unsafe { pd.register(send_buf.as_ptr(), send_buf.size(), full_access()) } {
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

    // Pre-post receives
    for i in 0..QUEUE_DEPTH {
        qp.borrow()
            .post_recv(i as u64, recv_buf.addr() + (i * 64) as u64, 64, recv_mr.lkey())
            .unwrap();
    }
    qp.borrow().ring_rq_doorbell();

    ready_signal.store(1, Ordering::Release);

    // Echo server loop with 3:1 signaling ratio
    let echo_data = vec![0xBBu8; 32];
    let mut recv_idx = 0usize;
    let mut send_count = 0usize;
    while !stop_flag.load(Ordering::Relaxed) {
        rx_count.set(0);
        recv_cq.poll();
        recv_cq.flush();
        let count = rx_count.get();

        if count == 0 {
            continue;
        }

        send_cq.poll();
        send_cq.flush();

        // Repost receives and send echoes
        {
            let mut qp_ref = qp.borrow_mut();
            for _ in 0..count {
                let idx = recv_idx % QUEUE_DEPTH;

                // Repost receive
                qp_ref
                    .post_recv(idx as u64, recv_buf.addr() + (idx * 64) as u64, 64, recv_mr.lkey())
                    .unwrap();

                // Echo back with inline data (3:1 signaling ratio)
                if send_count % 4 == 3 {
                    let _ = qp_ref
                        .sq_wqe()
                        .unwrap()
                        .send(WqeFlags::COMPLETION)
                        .unwrap()
                        .inline(&echo_data)
                        .finish_signaled(idx as u64);
                } else {
                    let _ = qp_ref
                        .sq_wqe()
                        .unwrap()
                        .send(WqeFlags::empty())
                        .unwrap()
                        .inline(&echo_data)
                        .finish_unsignaled();
                }

                recv_idx += 1;
                send_count += 1;
            }
            qp_ref.ring_rq_doorbell();
            qp_ref.ring_sq_doorbell();
        }
    }

    // Keep MRs alive until server exits
    drop(send_mr);
    drop(recv_mr);
}

// =============================================================================
// Benchmark Functions
// =============================================================================

/// Low-latency Send/Recv benchmark (queue depth = 1, inline send).
fn run_latency_bench<SF, RF>(
    client: &mut EndpointState<SF, RF>,
    iters: u64,
    size: usize,
) -> Duration
where
    SF: Fn(Cqe, u64),
    RF: Fn(Cqe, u64),
{
    let size = size.min(32);
    let send_data = vec![0xAAu8; size];

    let start = std::time::Instant::now();

    for i in 0..iters {
        let idx = (i as usize) % QUEUE_DEPTH;

        // Post Send with inline data
        {
            let mut qp = client.qp.borrow_mut();
            let _ = qp
                .sq_wqe()
                .unwrap()
                .send(WqeFlags::COMPLETION)
                .unwrap()
                .inline(&send_data)
                .finish_signaled(idx as u64);
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
                // Repost receive
                let qp = client.qp.borrow();
                qp.post_recv(
                    idx as u64,
                    client.recv_buf.addr() + (idx * 64) as u64,
                    64,
                    client.recv_mr.lkey(),
                )
                .unwrap();
                qp.ring_rq_doorbell();
                break;
            }
        }
    }

    start.elapsed()
}

/// Throughput Send/Recv benchmark (batched doorbells, inline send, 3:1 unsignaled ratio).
fn run_throughput_bench<SF, RF>(
    client: &mut EndpointState<SF, RF>,
    iters: u64,
    size: usize,
) -> Duration
where
    SF: Fn(Cqe, u64),
    RF: Fn(Cqe, u64),
{
    let size = size.min(32);
    let send_data = vec![0xAAu8; size];

    let initial_batch = (iters as usize).min(QUEUE_DEPTH);
    let mut recv_idx = 0usize;

    // Initial fill with 3:1 signaling ratio
    {
        let mut qp = client.qp.borrow_mut();
        let full_batches = initial_batch / 4;
        let remainder = initial_batch % 4;

        for batch in 0..full_batches {
            let base = batch * 4;
            // 3 unsignaled
            for _j in 0..3 {
                let _ = qp
                    .sq_wqe()
                    .unwrap()
                    .send(WqeFlags::empty())
                    .unwrap()
                    .inline(&send_data)
                    .finish_unsignaled();
            }
            // 1 signaled
            let idx = base + 3;
            let _ = qp
                .sq_wqe()
                .unwrap()
                .send(WqeFlags::COMPLETION)
                .unwrap()
                .inline(&send_data)
                .finish_signaled(idx as u64);
        }

        // Remainder (all signaled)
        for j in 0..remainder {
            let idx = full_batches * 4 + j;
            let _ = qp
                .sq_wqe()
                .unwrap()
                .send(WqeFlags::COMPLETION)
                .unwrap()
                .inline(&send_data)
                .finish_signaled(idx as u64);
        }
        qp.ring_sq_doorbell();
    }

    let start = std::time::Instant::now();
    let mut completed = 0u64;
    let mut inflight = initial_batch as u64;
    let mut sent = initial_batch as u64;

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
                let idx = recv_idx % QUEUE_DEPTH;
                qp.post_recv(
                    idx as u64,
                    client.recv_buf.addr() + (idx * 64) as u64,
                    64,
                    client.recv_mr.lkey(),
                )
                .unwrap();
                recv_idx += 1;
            }
        }

        // Send more if needed with 3:1 signaling ratio
        let remaining = iters.saturating_sub(sent);
        let can_send = (QUEUE_DEPTH as u64).saturating_sub(inflight).min(remaining) as usize;
        let to_send = can_send.min(rx_count);

        if to_send > 0 {
            let mut qp = client.qp.borrow_mut();
            let full_batches = to_send / 4;
            let remainder = to_send % 4;

            for batch in 0..full_batches {
                // 3 unsignaled
                for _ in 0..3 {
                    let _ = qp
                        .sq_wqe()
                        .unwrap()
                        .send(WqeFlags::empty())
                        .unwrap()
                        .inline(&send_data)
                        .finish_unsignaled();
                }
                // 1 signaled
                let _ = qp
                    .sq_wqe()
                    .unwrap()
                    .send(WqeFlags::COMPLETION)
                    .unwrap()
                    .inline(&send_data)
                    .finish_signaled(batch as u64);
            }

            // Remainder (all signaled)
            for j in 0..remainder {
                let _ = qp
                    .sq_wqe()
                    .unwrap()
                    .send(WqeFlags::COMPLETION)
                    .unwrap()
                    .inline(&send_data)
                    .finish_signaled(j as u64);
            }

            inflight += to_send as u64;
            sent += to_send as u64;
        }

        {
            let qp = client.qp.borrow();
            qp.ring_rq_doorbell();
            if to_send > 0 {
                qp.ring_sq_doorbell();
            }
        }
    }

    // Drain remaining
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

// =============================================================================
// Criterion Benchmarks
// =============================================================================

fn benchmarks(c: &mut Criterion) {
    let sizes = [8usize, 16, 32];

    // Benchmark with scatter-to-CQE enabled
    {
        let setup = match setup_benchmark(true) {
            Some(s) => s,
            None => {
                eprintln!("Skipping benchmarks: no mlx5 device available");
                return;
            }
        };

        let _ctx = setup._ctx;
        let _pd = setup._pd;
        let client = RefCell::new(setup.client);
        let mut server_handle = setup._server_handle;

        // Latency benchmark
        {
            let mut group = c.benchmark_group("send_recv_latency_scatter_cqe");
            group.sample_size(10);
            group.measurement_time(Duration::from_secs(1));

            for &size in &sizes {
                group.bench_with_input(
                    BenchmarkId::new("enabled", size),
                    &size,
                    |b, &size| {
                        b.iter_custom(|iters| {
                            run_latency_bench(&mut client.borrow_mut(), iters, size)
                        });
                    },
                );
            }

            group.finish();
        }

        // Throughput benchmark
        {
            let mut group = c.benchmark_group("send_recv_throughput_scatter_cqe");
            group.sample_size(10);
            group.measurement_time(Duration::from_secs(3));
            group.throughput(Throughput::Elements(1));

            for &size in &sizes {
                group.bench_with_input(
                    BenchmarkId::new("enabled", size),
                    &size,
                    |b, &size| {
                        b.iter_custom(|iters| {
                            run_throughput_bench(&mut client.borrow_mut(), iters, size)
                        });
                    },
                );
            }

            group.finish();
        }

        server_handle.stop();
    }

    // Benchmark with scatter-to-CQE disabled
    {
        let setup = match setup_benchmark(false) {
            Some(s) => s,
            None => {
                eprintln!("Skipping benchmarks: no mlx5 device available");
                return;
            }
        };

        let _ctx = setup._ctx;
        let _pd = setup._pd;
        let client = RefCell::new(setup.client);
        let mut server_handle = setup._server_handle;

        // Latency benchmark
        {
            let mut group = c.benchmark_group("send_recv_latency_no_scatter");
            group.sample_size(10);
            group.measurement_time(Duration::from_secs(1));

            for &size in &sizes {
                group.bench_with_input(
                    BenchmarkId::new("disabled", size),
                    &size,
                    |b, &size| {
                        b.iter_custom(|iters| {
                            run_latency_bench(&mut client.borrow_mut(), iters, size)
                        });
                    },
                );
            }

            group.finish();
        }

        // Throughput benchmark
        {
            let mut group = c.benchmark_group("send_recv_throughput_no_scatter");
            group.sample_size(10);
            group.measurement_time(Duration::from_secs(3));
            group.throughput(Throughput::Elements(1));

            for &size in &sizes {
                group.bench_with_input(
                    BenchmarkId::new("disabled", size),
                    &size,
                    |b, &size| {
                        b.iter_custom(|iters| {
                            run_throughput_bench(&mut client.borrow_mut(), iters, size)
                        });
                    },
                );
            }

            group.finish();
        }

        server_handle.stop();
    }
}

criterion_group!(benches, benchmarks);
criterion_main!(benches);
