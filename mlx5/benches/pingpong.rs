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
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use mlx5::cq::{CqConfig, Cqe, CqeOpcode};
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

const QUEUE_DEPTH: usize = 1024;
const PAGE_SIZE: usize = 4096;
const BUFFER_SIZE: usize = QUEUE_DEPTH * 256; // entries * 256 bytes

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
            self.rx_is_write_imm.borrow_mut()[count] =
                matches!(cqe.opcode, CqeOpcode::RespRdmaWriteImm);
            self.rx_count.set(count + 1);
        }
    }
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

// =============================================================================
// MonoCq Endpoint State (for inlined callback benchmark)
// =============================================================================

/// Endpoint state using MonoCq for inlined callback dispatch.
/// Generic over callback types to enable true inlining (no Box<dyn Fn>).
struct MonoCqEndpointState<SF, RF>
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
    remote_addr: u64,
    remote_rkey: u32,
}

struct MonoCqBenchmarkSetup<SF, RF>
where
    SF: Fn(Cqe, u64),
    RF: Fn(Cqe, u64),
{
    client: MonoCqEndpointState<SF, RF>,
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

fn setup_mono_cq_benchmark() -> Option<MonoCqBenchmarkSetup<impl Fn(Cqe, u64), impl Fn(Cqe, u64)>> {
    let ctx = open_mlx5_device()?;
    let port = 1u8;
    let port_attr = ctx.query_port(port).ok()?;
    let pd = Rc::new(ctx.alloc_pd().ok()?);

    // Create shared state for callback
    let shared_state = SharedCqeState::new();
    let shared_state_for_recv = shared_state.clone();

    // Create MonoCq callbacks directly (no Box<dyn Fn> - enables inlining)
    let send_callback = move |_cqe: Cqe, _entry: u64| {
        // SQ completions - no-op for this benchmark
    };

    let recv_callback = move |cqe: Cqe, _entry: u64| {
        if cqe.opcode.is_responder() && cqe.syndrome == 0 {
            shared_state_for_recv.push(&cqe);
        }
    };

    // Create MonoCqs with concrete closure types (wrapped in Rc for builder API)
    let send_cq = Rc::new(ctx.create_mono_cq(QUEUE_DEPTH as i32, send_callback, &CqConfig::default()).ok()?);
    let recv_cq = Rc::new(ctx.create_mono_cq(QUEUE_DEPTH as i32, recv_callback, &CqConfig::default()).ok()?);

    let config = RcQpConfig {
        max_send_wr: QUEUE_DEPTH as u32,
        max_recv_wr: QUEUE_DEPTH as u32,
        max_send_sge: 1,
        max_recv_sge: 1,
        max_inline_data: 256,
        enable_scatter_to_cqe: false,
    };

    // Create QP for MonoCq using builder pattern
    let qp = ctx
        .rc_qp_builder::<u64, u64>(&pd, &config)
        .sq_mono_cq(&send_cq)
        .rq_mono_cq(&recv_cq)
        .build()
        .ok()?;

    // Register QP with MonoCqs for completion dispatch
    send_cq.register(&qp);
    recv_cq.register(&qp);

    let send_buf = AlignedBuffer::new(BUFFER_SIZE);
    let recv_buf = AlignedBuffer::new(BUFFER_SIZE);

    let send_mr = unsafe { pd.register(send_buf.as_ptr(), send_buf.size(), full_access()) }.ok()?;
    let recv_mr = unsafe { pd.register(recv_buf.as_ptr(), recv_buf.size(), full_access()) }.ok()?;

    // Connection info
    let client_info = ConnectionInfo {
        qpn: qp.borrow().qpn(),
        lid: port_attr.lid,
        buf_addr: recv_buf.addr(),
        rkey: recv_mr.rkey(),
    };

    // Channels for server communication
    let (server_info_tx, server_info_rx): (Sender<ConnectionInfo>, Receiver<ConnectionInfo>) =
        mpsc::channel();
    let (client_info_tx, client_info_rx): (Sender<ConnectionInfo>, Receiver<ConnectionInfo>) =
        mpsc::channel();
    let server_ready = Arc::new(AtomicU32::new(0));
    let server_ready_clone = server_ready.clone();

    let stop_flag = Arc::new(AtomicBool::new(false));
    let server_stop = stop_flag.clone();

    // Start server thread (uses existing dynamic dispatch CQ)
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

    // Connect client QP
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
        let offset = (i * 256) as u64;
        qp.borrow()
            .post_recv(i as u64, recv_buf.addr() + offset, 256, recv_mr.lkey())
            .unwrap();
    }
    qp.borrow().ring_rq_doorbell();

    let client = MonoCqEndpointState {
        qp,
        send_cq,
        recv_cq,
        shared_state,
        send_mr,
        recv_mr,
        send_buf,
        recv_buf,
        remote_addr: server_info.buf_addr,
        remote_rkey: server_info.rkey,
    };

    let server_handle = ServerHandle {
        stop_flag,
        handle: Some(handle),
    };

    Some(MonoCqBenchmarkSetup {
        client,
        _server_handle: server_handle,
        _pd: pd,
        _ctx: ctx,
    })
}

// =============================================================================
// Server Thread (using MonoCq for inlined callback dispatch)
// =============================================================================

/// Server-side shared state (non-Rc version for thread safety).
struct ServerSharedState {
    rx_count: Cell<usize>,
    rx_indices: RefCell<[usize; QUEUE_DEPTH]>,
    rx_sizes: RefCell<[u32; QUEUE_DEPTH]>,
    rx_is_write_imm: RefCell<[bool; QUEUE_DEPTH]>,
}

impl ServerSharedState {
    fn new() -> Self {
        Self {
            rx_count: Cell::new(0),
            rx_indices: RefCell::new([0; QUEUE_DEPTH]),
            rx_sizes: RefCell::new([0; QUEUE_DEPTH]),
            rx_is_write_imm: RefCell::new([false; QUEUE_DEPTH]),
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
            self.rx_is_write_imm.borrow_mut()[count] =
                matches!(cqe.opcode, CqeOpcode::RespRdmaWriteImm);
            self.rx_count.set(count + 1);
        }
    }
}

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

    // Create server endpoint using MonoCq (inlined callback dispatch)
    let shared_state = Rc::new(ServerSharedState::new());
    let shared_state_for_recv = shared_state.clone();

    // Create MonoCq callbacks (no Box<dyn Fn> - enables inlining)
    let send_callback = move |_cqe: Cqe, _entry: u64| {
        // SQ completions - no-op for server
    };

    let recv_callback = move |cqe: Cqe, _entry: u64| {
        if cqe.opcode.is_responder() && cqe.syndrome == 0 {
            shared_state_for_recv.push(&cqe);
        }
    };

    // Create MonoCqs (wrapped in Rc for builder API)
    let send_cq = match ctx.create_mono_cq::<RcQpForMonoCq<u64>, _>(QUEUE_DEPTH as i32, send_callback, &CqConfig::default()) {
        Ok(cq) => Rc::new(cq),
        Err(_) => {
            eprintln!("Server: Failed to create send CQ");
            return;
        }
    };

    let recv_cq = match ctx.create_mono_cq::<RcQpForMonoCq<u64>, _>(QUEUE_DEPTH as i32, recv_callback, &CqConfig::default()) {
        Ok(cq) => Rc::new(cq),
        Err(_) => {
            eprintln!("Server: Failed to create recv CQ");
            return;
        }
    };

    let config = RcQpConfig {
        max_send_wr: QUEUE_DEPTH as u32,
        max_recv_wr: QUEUE_DEPTH as u32,
        max_send_sge: 1,
        max_recv_sge: 1,
        max_inline_data: 256,
        enable_scatter_to_cqe: false,
    };

    // Create QP for MonoCq using builder pattern
    let qp = match ctx
        .rc_qp_builder::<u64, u64>(&pd, &config)
        .sq_mono_cq(&send_cq)
        .rq_mono_cq(&recv_cq)
        .build()
    {
        Ok(q) => q,
        Err(_) => {
            eprintln!("Server: Failed to create QP");
            return;
        }
    };

    // Register QP with MonoCqs for completion dispatch
    send_cq.register(&qp);
    recv_cq.register(&qp);

    let send_buf = AlignedBuffer::new(BUFFER_SIZE);
    let recv_buf = AlignedBuffer::new(BUFFER_SIZE);

    let send_mr = match unsafe { pd.register(send_buf.as_ptr(), send_buf.size(), full_access()) } {
        Ok(mr) => mr,
        Err(_) => {
            eprintln!("Server: Failed to register send MR");
            return;
        }
    };

    let recv_mr = match unsafe { pd.register(recv_buf.as_ptr(), recv_buf.size(), full_access()) } {
        Ok(mr) => mr,
        Err(_) => {
            eprintln!("Server: Failed to register recv MR");
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
        eprintln!("Server: Failed to connect QP");
        return;
    }

    // Pre-post receives
    for i in 0..QUEUE_DEPTH {
        let offset = (i * 256) as u64;
        qp.borrow()
            .post_recv(i as u64, recv_buf.addr() + offset, 256, recv_mr.lkey())
            .unwrap();
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

    // Pre-allocate echo data for inline sending
    let echo_data = vec![0xBBu8; 32];

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
                qp_ref
                    .post_recv(idx as u64, recv_buf.addr() + offset, 256, recv_mr.lkey())
                    .unwrap();
            }
        }

        // 4. TX リクエスト再補充: queue echo responses (using inline data)
        {
            let qp_ref = qp.borrow();
            let ctx = qp_ref.emit_ctx().expect("emit_ctx failed");
            for i in 0..rx_count {
                let idx = rx_indices[i];
                let offset = (idx * 256) as u64;

                if rx_is_write_imm[i] {
                    // Echo with WRITE+IMM using inline data
                    let _ = emit_wqe!(&ctx, write_imm {
                        flags: WqeFlags::empty(),
                        remote_addr: remote_addr + offset,
                        rkey: remote_rkey,
                        imm: idx as u32,
                        inline: echo_data.as_slice(),
                        signaled: idx as u64,
                    });
                } else {
                    // Echo with SEND using inline data
                    let _ = emit_wqe!(&ctx, send {
                        flags: WqeFlags::empty(),
                        inline: echo_data.as_slice(),
                        signaled: idx as u64,
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

/// Throughput benchmark using MonoCq with builder API for inlined callback dispatch.
fn run_throughput_bench_mono_cq<SF, RF>(
    client: &mut MonoCqEndpointState<SF, RF>,
    iters: u64,
    size: usize,
) -> Duration
where
    SF: Fn(Cqe, u64),
    RF: Fn(Cqe, u64),
{
    let size = size.min(256);
    let rkey = client.remote_rkey;
    let remote_addr = client.remote_addr;

    // Pre-allocate send data for inline
    let send_data = vec![0xAAu8; size];

    // Initial fill using inline data
    {
        let qp = client.qp.borrow();
        let ctx = qp.emit_ctx().expect("emit_ctx failed");
        for batch in 0..(QUEUE_DEPTH / 4) {
            let base = batch * 4;
            for j in 0..3 {
                let i = base + j;
                let offset = (i * 256) as u64;
                emit_wqe!(&ctx, write_imm {
                    flags: WqeFlags::empty(),
                    remote_addr: remote_addr + offset,
                    rkey: rkey,
                    imm: i as u32,
                    inline: send_data.as_slice(),
                }).expect("emit_wqe failed");
            }
            let i = base + 3;
            let offset = (i * 256) as u64;
            emit_wqe!(&ctx, write_imm {
                flags: WqeFlags::empty(),
                remote_addr: remote_addr + offset,
                rkey: rkey,
                imm: i as u32,
                inline: send_data.as_slice(),
                signaled: i as u64,
            }).expect("emit_wqe failed");
        }
        qp.ring_sq_doorbell();
    }

    let start = std::time::Instant::now();
    let mut completed = 0u64;
    let mut inflight = QUEUE_DEPTH as u64;

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

        let rx_indices = *client.shared_state.rx_indices.borrow();

        {
            let qp = client.qp.borrow();
            for i in 0..rx_count {
                let idx = rx_indices[i];
                let offset = (idx * 256) as u64;
                qp.post_recv(idx as u64, client.recv_buf.addr() + offset, 256, client.recv_mr.lkey())
                    .unwrap();
            }
        }

        let remaining = iters.saturating_sub(completed);
        let can_send = (QUEUE_DEPTH as u64).saturating_sub(inflight).min(remaining) as usize;
        let to_send = can_send.min(rx_count);

        if to_send > 0 {
            let qp = client.qp.borrow();
            let ctx = qp.emit_ctx().expect("emit_ctx failed");
            let full_batches = to_send / 4;
            let remainder = to_send % 4;

            for batch in 0..full_batches {
                let base = batch * 4;
                for j in 0..3 {
                    let idx = rx_indices[base + j];
                    let offset = (idx * 256) as u64;
                    emit_wqe!(&ctx, write_imm {
                        flags: WqeFlags::empty(),
                        remote_addr: remote_addr + offset,
                        rkey: rkey,
                        imm: idx as u32,
                        inline: send_data.as_slice(),
                    }).expect("emit_wqe failed");
                }
                let idx = rx_indices[base + 3];
                let offset = (idx * 256) as u64;
                emit_wqe!(&ctx, write_imm {
                    flags: WqeFlags::empty(),
                    remote_addr: remote_addr + offset,
                    rkey: rkey,
                    imm: idx as u32,
                    inline: send_data.as_slice(),
                    signaled: idx as u64,
                }).expect("emit_wqe failed");
            }

            let base = full_batches * 4;
            for j in 0..remainder {
                let idx = rx_indices[base + j];
                let offset = (idx * 256) as u64;
                emit_wqe!(&ctx, write_imm {
                    flags: WqeFlags::empty(),
                    remote_addr: remote_addr + offset,
                    rkey: rkey,
                    imm: idx as u32,
                    inline: send_data.as_slice(),
                    signaled: idx as u64,
                }).expect("emit_wqe failed");
            }

            inflight += to_send as u64;
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
        let rx_count = client.shared_state.rx_count.get();
        inflight -= rx_count as u64;

        let rx_indices = *client.shared_state.rx_indices.borrow();
        let qp = client.qp.borrow();
        for i in 0..rx_count {
            let idx = rx_indices[i];
            let offset = (idx * 256) as u64;
            qp.post_recv(idx as u64, client.recv_buf.addr() + offset, 256, client.recv_mr.lkey())
                .unwrap();
        }
        client.send_cq.poll();
        client.send_cq.flush();
    }
    client.qp.borrow().ring_rq_doorbell();

    start.elapsed()
}

// =============================================================================
// Criterion Benchmarks
// =============================================================================

/// Low-latency benchmark using MonoCq with builder + blueflame + inline data.
fn run_lowlatency_bench_mono_cq<SF, RF>(
    client: &mut MonoCqEndpointState<SF, RF>,
    iters: u64,
    size: usize,
) -> Duration
where
    SF: Fn(Cqe, u64),
    RF: Fn(Cqe, u64),
{
    let size = size.min(256);
    let rkey = client.remote_rkey;
    let remote_addr = client.remote_addr;

    // Pre-allocate send data for inline
    let send_data = vec![0xAAu8; size];

    let start = std::time::Instant::now();

    for i in 0..iters {
        let idx = (i as usize) % QUEUE_DEPTH;
        let offset = (idx * 256) as u64;
        let imm = idx as u32;

        // Post single WRITE+IMM using inline data
        {
            let qp = client.qp.borrow();
            let ctx = qp.emit_ctx().expect("emit_ctx failed");
            emit_wqe!(&ctx, write_imm {
                flags: WqeFlags::empty(),
                remote_addr: remote_addr + offset,
                rkey: rkey,
                imm: imm,
                inline: send_data.as_slice(),
                signaled: idx as u64,
            }).expect("emit_wqe failed");
            qp.ring_sq_doorbell();
        }

        // Wait for completion
        loop {
            client.send_cq.poll();
            client.send_cq.flush();

            client.shared_state.reset();
            client.recv_cq.poll();
            client.recv_cq.flush();

            if client.shared_state.rx_count.get() > 0 {
                let recv_idx = client.shared_state.rx_indices.borrow()[0];
                let offset = (recv_idx * 256) as u64;
                let qp = client.qp.borrow();
                qp.post_recv(recv_idx as u64, client.recv_buf.addr() + offset, 256, client.recv_mr.lkey())
                    .unwrap();
                qp.ring_rq_doorbell();
                break;
            }
        }
    }

    start.elapsed()
}

/// Benchmarks - measures throughput and latency with MonoCq inlined callback dispatch.
fn benchmarks(c: &mut Criterion) {
    let setup = match setup_mono_cq_benchmark() {
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

    let size = 32usize;

    // Throughput benchmark using builder API
    {
        let mut group = c.benchmark_group("throughput");
        group.sample_size(10);
        group.measurement_time(Duration::from_secs(3));
        group.throughput(Throughput::Elements(1));

        group.bench_with_input(BenchmarkId::new("builder", size), &size, |b, &size| {
            b.iter_custom(|iters| {
                run_throughput_bench_mono_cq(&mut client.borrow_mut(), iters, size)
            });
        });

        group.finish();
    }

    // Low-latency benchmark: queue depth = 1, blueflame
    {
        let mut group = c.benchmark_group("latency");
        group.sample_size(10);
        group.measurement_time(Duration::from_secs(1));

        group.bench_with_input(BenchmarkId::new("blueflame", size), &size, |b, &size| {
            b.iter_custom(|iters| {
                run_lowlatency_bench_mono_cq(&mut client.borrow_mut(), iters, size)
            });
        });

        group.finish();
    }

    // Cleanup
    server_handle.stop();

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

criterion_group!(benches, benchmarks);
criterion_main!(benches);
