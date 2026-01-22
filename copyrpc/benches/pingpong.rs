//! 8-Endpoint 128-concurrent ping-pong benchmark comparing WRITE+IMM vs SEND-RECV.
//!
//! This benchmark compares two RDMA communication patterns:
//! - RDMA WRITE+IMM: One-sided write with immediate data notification
//! - SEND-RECV: Two-sided messaging
//!
//! Run with:
//! ```bash
//! cargo bench --package copyrpc --bench pingpong
//! ```

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

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

const NUM_ENDPOINTS: usize = 8;
const REQUESTS_PER_EP: usize = 128;
const TOTAL_QUEUE_DEPTH: usize = NUM_ENDPOINTS * REQUESTS_PER_EP;
const PAGE_SIZE: usize = 4096;
const MESSAGE_SIZE: usize = 32;
const RING_SIZE: usize = REQUESTS_PER_EP * 256;

// =============================================================================
// Benchmark Mode
// =============================================================================

#[derive(Clone, Copy, PartialEq)]
enum BenchMode {
    WriteImm,
    SendRecv,
}

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
// Access Flags
// =============================================================================

fn full_access() -> AccessFlags {
    AccessFlags::LOCAL_WRITE
        | AccessFlags::REMOTE_WRITE
        | AccessFlags::REMOTE_READ
        | AccessFlags::REMOTE_ATOMIC
}

// =============================================================================
// Connection Info
// =============================================================================

#[derive(Clone)]
struct EndpointConnectionInfo {
    qpn: u32,
    lid: u16,
    buf_addr: u64,
    rkey: u32,
}

#[derive(Clone)]
struct MultiEndpointConnectionInfo {
    endpoints: Vec<EndpointConnectionInfo>,
}

// =============================================================================
// Shared CQE State
// =============================================================================

struct SharedCqeState {
    rx_count: Cell<usize>,
    rx_completions: RefCell<[(u8, u8); TOTAL_QUEUE_DEPTH]>,
    mode: BenchMode,
}

impl SharedCqeState {
    fn new(mode: BenchMode) -> Self {
        Self {
            rx_count: Cell::new(0),
            rx_completions: RefCell::new([(0, 0); TOTAL_QUEUE_DEPTH]),
            mode,
        }
    }

    fn reset(&self) {
        self.rx_count.set(0);
    }

    fn push(&self, cqe: &Cqe, entry: u64) {
        let count = self.rx_count.get();
        if count < TOTAL_QUEUE_DEPTH {
            let (ep_id, slot_id) = match self.mode {
                BenchMode::WriteImm => {
                    // IMM encodes: (endpoint_id << 8) | slot_id
                    let imm = cqe.imm;
                    (((imm >> 8) & 0xFF) as u8, (imm & 0xFF) as u8)
                }
                BenchMode::SendRecv => {
                    // For SEND-RECV, use the entry (wr_id) which contains ep_id << 8 | slot_id
                    let slot_id = (entry & 0xFF) as u8;
                    let ep_id = ((entry >> 8) & 0xFF) as u8;
                    (ep_id, slot_id)
                }
            };
            self.rx_completions.borrow_mut()[count] = (ep_id, slot_id);
            self.rx_count.set(count + 1);
        }
    }
}

// =============================================================================
// Endpoint State
// =============================================================================

struct EndpointState {
    qp: Rc<RefCell<RcQpForMonoCq<u64>>>,
    send_buf: AlignedBuffer,
    recv_buf: AlignedBuffer,
    send_mr: MemoryRegion,
    recv_mr: MemoryRegion,
    remote_addr: u64,
    remote_rkey: u32,
    endpoint_id: u8,
}

// =============================================================================
// Multi-Endpoint Client
// =============================================================================

struct MultiEndpointClient<SF, RF>
where
    SF: Fn(Cqe, u64),
    RF: Fn(Cqe, u64),
{
    endpoints: Vec<EndpointState>,
    send_cq: Rc<MonoCqRc<u64, SF>>,
    recv_cq: Rc<MonoCqRc<u64, RF>>,
    shared_state: Rc<SharedCqeState>,
    mode: BenchMode,
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
// Benchmark Setup
// =============================================================================

struct BenchmarkSetup<SF, RF>
where
    SF: Fn(Cqe, u64),
    RF: Fn(Cqe, u64),
{
    client: MultiEndpointClient<SF, RF>,
    _server_handle: ServerHandle,
    _pd: Rc<Pd>,
    _ctx: Context,
}

fn open_mlx5_device() -> Option<Context> {
    let device_list = DeviceList::list().ok()?;
    for device in device_list.iter() {
        if let Ok(ctx) = device.open() {
            return Some(ctx);
        }
    }
    None
}

fn setup_benchmark(mode: BenchMode) -> Option<BenchmarkSetup<impl Fn(Cqe, u64), impl Fn(Cqe, u64)>> {
    let ctx = open_mlx5_device()?;
    let port = 1u8;
    let port_attr = ctx.query_port(port).ok()?;
    let pd = Rc::new(ctx.alloc_pd().ok()?);

    let shared_state = Rc::new(SharedCqeState::new(mode));
    let shared_state_for_recv = shared_state.clone();

    let send_callback = move |_cqe: Cqe, _entry: u64| {};

    let recv_callback = move |cqe: Cqe, entry: u64| {
        if cqe.opcode.is_responder() && cqe.syndrome == 0 {
            shared_state_for_recv.push(&cqe, entry);
        }
    };

    let send_cq = Rc::new(
        ctx.create_mono_cq(TOTAL_QUEUE_DEPTH as i32, send_callback, &CqConfig::default())
            .ok()?,
    );
    let recv_cq = Rc::new(
        ctx.create_mono_cq(TOTAL_QUEUE_DEPTH as i32, recv_callback, &CqConfig::default())
            .ok()?,
    );

    let config = RcQpConfig {
        max_send_wr: REQUESTS_PER_EP as u32,
        max_recv_wr: REQUESTS_PER_EP as u32,
        max_send_sge: 1,
        max_recv_sge: 1,
        max_inline_data: MESSAGE_SIZE as u32,
        enable_scatter_to_cqe: false,
    };

    let mut endpoints = Vec::with_capacity(NUM_ENDPOINTS);
    let mut client_infos = Vec::with_capacity(NUM_ENDPOINTS);

    for ep_id in 0..NUM_ENDPOINTS {
        let qp = ctx
            .rc_qp_builder::<u64, u64>(&pd, &config)
            .sq_mono_cq(&send_cq)
            .rq_mono_cq(&recv_cq)
            .build()
            .ok()?;

        send_cq.register(&qp);
        recv_cq.register(&qp);

        let send_buf = AlignedBuffer::new(RING_SIZE);
        let recv_buf = AlignedBuffer::new(RING_SIZE);

        let send_mr =
            unsafe { pd.register(send_buf.as_ptr(), send_buf.size(), full_access()) }.ok()?;
        let recv_mr =
            unsafe { pd.register(recv_buf.as_ptr(), recv_buf.size(), full_access()) }.ok()?;

        client_infos.push(EndpointConnectionInfo {
            qpn: qp.borrow().qpn(),
            lid: port_attr.lid,
            buf_addr: recv_buf.addr(),
            rkey: recv_mr.rkey(),
        });

        endpoints.push(EndpointState {
            qp,
            send_buf,
            recv_buf,
            send_mr,
            recv_mr,
            remote_addr: 0,
            remote_rkey: 0,
            endpoint_id: ep_id as u8,
        });
    }

    let (server_info_tx, server_info_rx): (
        Sender<MultiEndpointConnectionInfo>,
        Receiver<MultiEndpointConnectionInfo>,
    ) = mpsc::channel();
    let (client_info_tx, client_info_rx): (
        Sender<MultiEndpointConnectionInfo>,
        Receiver<MultiEndpointConnectionInfo>,
    ) = mpsc::channel();
    let server_ready = Arc::new(AtomicU32::new(0));
    let server_ready_clone = server_ready.clone();

    let stop_flag = Arc::new(AtomicBool::new(false));
    let server_stop = stop_flag.clone();

    let handle = thread::spawn(move || {
        server_thread_main(server_info_tx, client_info_rx, server_ready_clone, server_stop, mode);
    });

    client_info_tx
        .send(MultiEndpointConnectionInfo {
            endpoints: client_infos,
        })
        .ok()?;
    let server_info = server_info_rx.recv().ok()?;

    while server_ready.load(Ordering::Acquire) == 0 {
        std::hint::spin_loop();
    }

    let access = full_access().bits();
    for (i, ep) in endpoints.iter_mut().enumerate() {
        let server_ep = &server_info.endpoints[i];

        let remote = IbRemoteQpInfo {
            qp_number: server_ep.qpn,
            packet_sequence_number: 0,
            local_identifier: server_ep.lid,
        };

        ep.qp.borrow_mut().connect(&remote, port, 0, 4, 4, access).ok()?;

        ep.remote_addr = server_ep.buf_addr;
        ep.remote_rkey = server_ep.rkey;

        // Pre-post receives with wr_id = (ep_id << 8) | slot_id
        for slot in 0..REQUESTS_PER_EP {
            let offset = (slot * 256) as u64;
            let wr_id = ((ep.endpoint_id as u64) << 8) | (slot as u64);
            ep.qp
                .borrow()
                .post_recv(wr_id, ep.recv_buf.addr() + offset, 256, ep.recv_mr.lkey())
                .unwrap();
        }
        ep.qp.borrow().ring_rq_doorbell();
    }

    let server_handle = ServerHandle {
        stop_flag,
        handle: Some(handle),
    };

    Some(BenchmarkSetup {
        client: MultiEndpointClient {
            endpoints,
            send_cq,
            recv_cq,
            shared_state,
            mode,
        },
        _server_handle: server_handle,
        _pd: pd,
        _ctx: ctx,
    })
}

// =============================================================================
// Server Thread
// =============================================================================

fn server_thread_main(
    info_tx: Sender<MultiEndpointConnectionInfo>,
    info_rx: Receiver<MultiEndpointConnectionInfo>,
    ready_signal: Arc<AtomicU32>,
    stop_flag: Arc<AtomicBool>,
    mode: BenchMode,
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

    let shared_state = Rc::new(SharedCqeState::new(mode));
    let shared_state_for_recv = shared_state.clone();

    let send_callback = move |_cqe: Cqe, _entry: u64| {};
    let recv_callback = move |cqe: Cqe, entry: u64| {
        if cqe.opcode.is_responder() && cqe.syndrome == 0 {
            shared_state_for_recv.push(&cqe, entry);
        }
    };

    let send_cq = match ctx
        .create_mono_cq::<RcQpForMonoCq<u64>, _>(TOTAL_QUEUE_DEPTH as i32, send_callback, &CqConfig::default())
    {
        Ok(cq) => Rc::new(cq),
        Err(_) => return,
    };

    let recv_cq = match ctx
        .create_mono_cq::<RcQpForMonoCq<u64>, _>(TOTAL_QUEUE_DEPTH as i32, recv_callback, &CqConfig::default())
    {
        Ok(cq) => Rc::new(cq),
        Err(_) => return,
    };

    let config = RcQpConfig {
        max_send_wr: REQUESTS_PER_EP as u32,
        max_recv_wr: REQUESTS_PER_EP as u32,
        max_send_sge: 1,
        max_recv_sge: 1,
        max_inline_data: MESSAGE_SIZE as u32,
        enable_scatter_to_cqe: false,
    };

    let mut endpoints: Vec<EndpointState> = Vec::with_capacity(NUM_ENDPOINTS);
    let mut server_infos = Vec::with_capacity(NUM_ENDPOINTS);

    for ep_id in 0..NUM_ENDPOINTS {
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

        let send_buf = AlignedBuffer::new(RING_SIZE);
        let recv_buf = AlignedBuffer::new(RING_SIZE);

        let send_mr = match unsafe { pd.register(send_buf.as_ptr(), send_buf.size(), full_access()) }
        {
            Ok(mr) => mr,
            Err(_) => return,
        };

        let recv_mr = match unsafe { pd.register(recv_buf.as_ptr(), recv_buf.size(), full_access()) }
        {
            Ok(mr) => mr,
            Err(_) => return,
        };

        server_infos.push(EndpointConnectionInfo {
            qpn: qp.borrow().qpn(),
            lid: port_attr.lid,
            buf_addr: recv_buf.addr(),
            rkey: recv_mr.rkey(),
        });

        endpoints.push(EndpointState {
            qp,
            send_buf,
            recv_buf,
            send_mr,
            recv_mr,
            remote_addr: 0,
            remote_rkey: 0,
            endpoint_id: ep_id as u8,
        });
    }

    if info_tx
        .send(MultiEndpointConnectionInfo {
            endpoints: server_infos,
        })
        .is_err()
    {
        return;
    }

    let client_info = match info_rx.recv() {
        Ok(info) => info,
        Err(_) => return,
    };

    let access = full_access().bits();
    for (i, ep) in endpoints.iter_mut().enumerate() {
        let client_ep = &client_info.endpoints[i];

        let remote = IbRemoteQpInfo {
            qp_number: client_ep.qpn,
            packet_sequence_number: 0,
            local_identifier: client_ep.lid,
        };

        if ep.qp.borrow_mut().connect(&remote, port, 0, 4, 4, access).is_err() {
            return;
        }

        ep.remote_addr = client_ep.buf_addr;
        ep.remote_rkey = client_ep.rkey;

        // Pre-post receives with wr_id = (ep_id << 8) | slot_id
        for slot in 0..REQUESTS_PER_EP {
            let offset = (slot * 256) as u64;
            let wr_id = ((ep.endpoint_id as u64) << 8) | (slot as u64);
            ep.qp
                .borrow()
                .post_recv(wr_id, ep.recv_buf.addr() + offset, 256, ep.recv_mr.lkey())
                .unwrap();
        }
        ep.qp.borrow().ring_rq_doorbell();
    }

    ready_signal.store(1, Ordering::Release);

    // Server loop
    while !stop_flag.load(Ordering::Relaxed) {
        shared_state.reset();
        recv_cq.poll();
        recv_cq.flush();

        let rx_count = shared_state.rx_count.get();
        if rx_count == 0 {
            continue;
        }

        send_cq.poll();
        send_cq.flush();

        let completions = *shared_state.rx_completions.borrow();

        for i in 0..rx_count {
            let (ep_id, slot_id) = completions[i];
            let ep = &endpoints[ep_id as usize];
            let offset = (slot_id as usize * 256) as u64;

            // Repost receive
            let wr_id = ((ep_id as u64) << 8) | (slot_id as u64);
            ep.qp
                .borrow()
                .post_recv(wr_id, ep.recv_buf.addr() + offset, 256, ep.recv_mr.lkey())
                .unwrap();

            // Send response
            match mode {
                BenchMode::WriteImm => {
                    let imm = ((ep_id as u32) << 8) | (slot_id as u32);
                    ep.qp
                        .borrow_mut()
                        .sq_wqe()
                        .unwrap()
                        .write_imm(
                            WqeFlags::empty(),
                            ep.remote_addr + offset,
                            ep.remote_rkey,
                            imm,
                        )
                        .unwrap()
                        .sge(ep.send_buf.addr() + offset, MESSAGE_SIZE as u32, ep.send_mr.lkey())
                        .finish_signaled(slot_id as u64)
                        .unwrap();
                }
                BenchMode::SendRecv => {
                    ep.qp
                        .borrow_mut()
                        .sq_wqe()
                        .unwrap()
                        .send(WqeFlags::empty())
                        .unwrap()
                        .sge(ep.send_buf.addr() + offset, MESSAGE_SIZE as u32, ep.send_mr.lkey())
                        .finish_signaled(slot_id as u64)
                        .unwrap();
                }
            }
        }

        // Ring doorbells
        for i in 0..rx_count {
            let (ep_id, _) = completions[i];
            let ep = &endpoints[ep_id as usize];
            ep.qp.borrow().ring_rq_doorbell();
            ep.qp.borrow().ring_sq_doorbell();
        }
    }
}

// =============================================================================
// Benchmark Functions
// =============================================================================

fn run_throughput_bench<SF, RF>(
    client: &mut MultiEndpointClient<SF, RF>,
    iters: u64,
) -> Duration
where
    SF: Fn(Cqe, u64),
    RF: Fn(Cqe, u64),
{
    let mode = client.mode;

    // Initial fill
    for ep in &client.endpoints {
        let mut qp = ep.qp.borrow_mut();
        for slot in 0..REQUESTS_PER_EP {
            let offset = (slot * 256) as u64;
            let signaled = (slot % 4) == 3;

            match mode {
                BenchMode::WriteImm => {
                    let imm = ((ep.endpoint_id as u32) << 8) | (slot as u32);
                    if signaled {
                        qp.sq_wqe()
                            .unwrap()
                            .write_imm(WqeFlags::empty(), ep.remote_addr + offset, ep.remote_rkey, imm)
                            .unwrap()
                            .sge(ep.send_buf.addr() + offset, MESSAGE_SIZE as u32, ep.send_mr.lkey())
                            .finish_signaled(slot as u64)
                            .unwrap();
                    } else {
                        qp.sq_wqe()
                            .unwrap()
                            .write_imm(WqeFlags::empty(), ep.remote_addr + offset, ep.remote_rkey, imm)
                            .unwrap()
                            .sge(ep.send_buf.addr() + offset, MESSAGE_SIZE as u32, ep.send_mr.lkey())
                            .finish_unsignaled()
                            .unwrap();
                    }
                }
                BenchMode::SendRecv => {
                    if signaled {
                        qp.sq_wqe()
                            .unwrap()
                            .send(WqeFlags::empty())
                            .unwrap()
                            .sge(ep.send_buf.addr() + offset, MESSAGE_SIZE as u32, ep.send_mr.lkey())
                            .finish_signaled(slot as u64)
                            .unwrap();
                    } else {
                        qp.sq_wqe()
                            .unwrap()
                            .send(WqeFlags::empty())
                            .unwrap()
                            .sge(ep.send_buf.addr() + offset, MESSAGE_SIZE as u32, ep.send_mr.lkey())
                            .finish_unsignaled()
                            .unwrap();
                    }
                }
            }
        }
        qp.ring_sq_doorbell();
    }

    let start = std::time::Instant::now();
    let mut completed = 0u64;
    let mut inflight = TOTAL_QUEUE_DEPTH as u64;

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

        let completions = *client.shared_state.rx_completions.borrow();

        // Repost receives
        for i in 0..rx_count {
            let (ep_id, slot_id) = completions[i];
            let ep = &client.endpoints[ep_id as usize];
            let offset = (slot_id as usize * 256) as u64;
            let wr_id = ((ep_id as u64) << 8) | (slot_id as u64);
            ep.qp
                .borrow()
                .post_recv(wr_id, ep.recv_buf.addr() + offset, 256, ep.recv_mr.lkey())
                .unwrap();
        }

        // Send new requests
        let remaining = iters.saturating_sub(completed);
        let can_send = (TOTAL_QUEUE_DEPTH as u64).saturating_sub(inflight).min(remaining) as usize;
        let to_send = can_send.min(rx_count);

        for i in 0..to_send {
            let (ep_id, slot_id) = completions[i];
            let ep = &client.endpoints[ep_id as usize];
            let offset = (slot_id as usize * 256) as u64;
            let signaled = (i % 4) == 3;

            let mut qp = ep.qp.borrow_mut();
            match mode {
                BenchMode::WriteImm => {
                    let imm = ((ep_id as u32) << 8) | (slot_id as u32);
                    if signaled {
                        qp.sq_wqe()
                            .unwrap()
                            .write_imm(WqeFlags::empty(), ep.remote_addr + offset, ep.remote_rkey, imm)
                            .unwrap()
                            .sge(ep.send_buf.addr() + offset, MESSAGE_SIZE as u32, ep.send_mr.lkey())
                            .finish_signaled(slot_id as u64)
                            .unwrap();
                    } else {
                        qp.sq_wqe()
                            .unwrap()
                            .write_imm(WqeFlags::empty(), ep.remote_addr + offset, ep.remote_rkey, imm)
                            .unwrap()
                            .sge(ep.send_buf.addr() + offset, MESSAGE_SIZE as u32, ep.send_mr.lkey())
                            .finish_unsignaled()
                            .unwrap();
                    }
                }
                BenchMode::SendRecv => {
                    if signaled {
                        qp.sq_wqe()
                            .unwrap()
                            .send(WqeFlags::empty())
                            .unwrap()
                            .sge(ep.send_buf.addr() + offset, MESSAGE_SIZE as u32, ep.send_mr.lkey())
                            .finish_signaled(slot_id as u64)
                            .unwrap();
                    } else {
                        qp.sq_wqe()
                            .unwrap()
                            .send(WqeFlags::empty())
                            .unwrap()
                            .sge(ep.send_buf.addr() + offset, MESSAGE_SIZE as u32, ep.send_mr.lkey())
                            .finish_unsignaled()
                            .unwrap();
                    }
                }
            }
        }

        inflight += to_send as u64;

        // Ring doorbells
        for i in 0..rx_count {
            let (ep_id, _) = completions[i];
            let ep = &client.endpoints[ep_id as usize];
            ep.qp.borrow().ring_rq_doorbell();
        }
        for i in 0..to_send {
            let (ep_id, _) = completions[i];
            let ep = &client.endpoints[ep_id as usize];
            ep.qp.borrow().ring_sq_doorbell();
        }
    }

    // Drain remaining
    while inflight > 0 {
        client.shared_state.reset();
        client.recv_cq.poll();
        client.recv_cq.flush();

        let rx_count = client.shared_state.rx_count.get();
        inflight -= rx_count as u64;

        let completions = *client.shared_state.rx_completions.borrow();
        for i in 0..rx_count {
            let (ep_id, slot_id) = completions[i];
            let ep = &client.endpoints[ep_id as usize];
            let offset = (slot_id as usize * 256) as u64;
            let wr_id = ((ep_id as u64) << 8) | (slot_id as u64);
            ep.qp
                .borrow()
                .post_recv(wr_id, ep.recv_buf.addr() + offset, 256, ep.recv_mr.lkey())
                .unwrap();
        }

        client.send_cq.poll();
        client.send_cq.flush();
    }

    for ep in &client.endpoints {
        ep.qp.borrow().ring_rq_doorbell();
    }

    start.elapsed()
}

fn cleanup_client<SF, RF>(client: MultiEndpointClient<SF, RF>)
where
    SF: Fn(Cqe, u64),
    RF: Fn(Cqe, u64),
{
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

// =============================================================================
// Criterion Benchmarks
// =============================================================================

fn benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("copyrpc_pingpong");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(5));
    group.throughput(Throughput::Elements(1));

    // WRITE+IMM benchmark
    if let Some(setup) = setup_benchmark(BenchMode::WriteImm) {
        let _ctx = setup._ctx;
        let _pd = setup._pd;
        let client = RefCell::new(setup.client);
        let mut server_handle = setup._server_handle;

        group.bench_function(
            BenchmarkId::new("write_imm", format!("8ep_128c_{}B", MESSAGE_SIZE)),
            |b| {
                b.iter_custom(|iters| run_throughput_bench(&mut client.borrow_mut(), iters));
            },
        );

        server_handle.stop();
        cleanup_client(client.into_inner());
    } else {
        eprintln!("Skipping WRITE+IMM benchmark: no mlx5 device available");
    }

    // SEND-RECV benchmark
    if let Some(setup) = setup_benchmark(BenchMode::SendRecv) {
        let _ctx = setup._ctx;
        let _pd = setup._pd;
        let client = RefCell::new(setup.client);
        let mut server_handle = setup._server_handle;

        group.bench_function(
            BenchmarkId::new("send_recv", format!("8ep_128c_{}B", MESSAGE_SIZE)),
            |b| {
                b.iter_custom(|iters| run_throughput_bench(&mut client.borrow_mut(), iters));
            },
        );

        server_handle.stop();
        cleanup_client(client.into_inner());
    } else {
        eprintln!("Skipping SEND-RECV benchmark: no mlx5 device available");
    }

    group.finish();
}

criterion_group!(benches, benchmarks);
criterion_main!(benches);
