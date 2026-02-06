//! Multi-QP pingpong throughput benchmark.
//!
//! Compares RC (512 QPs), UD (1 QP), and DC (1 DCI) for 512 concurrent
//! connections with queue depth 1 each. Server: 16 threads, Client: 1 thread.
//!
//! Optimizations applied:
//! - 1/8 signaling ratio (reduce CQ pressure)
//! - Sliding window pipeline (overlap NIC and CPU)
//! - MonoCq for UD/DC (inlined callbacks)
//! - Scatter-to-CQE for RC (inline recv data in CQE)
//! - Doorbell batching for RC server
//!
//! Run with:
//! ```bash
//! cargo bench --bench multi_qp
//! ```

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};

use mlx5::cq::{Cq, CqConfig, Cqe};
use mlx5::dc::{DciConfig, DciForMonoCq, DctConfig};
use mlx5::device::Context;
use mlx5::emit_dci_wqe;
use mlx5::emit_ud_wqe;
use mlx5::emit_wqe;
use mlx5::mono_cq::MonoCqRc;
use mlx5::pd::{AccessFlags, MemoryRegion, Pd};
use mlx5::qp::{RcQpConfig, RcQpForMonoCq};
use mlx5::srq::SrqConfig;
use mlx5::test_utils::{self, AlignedBuffer};
use mlx5::transport::IbRemoteQpInfo;
use mlx5::ud::{UdQpConfig, UdQpForMonoCq};
use mlx5::wqe::WqeFlags;
use mlx5::wqe::emit::{DcAvIb, UdAvIb};

// =============================================================================
// Constants
// =============================================================================

const NUM_CONNECTIONS: usize = 512;
const NUM_SERVER_THREADS: usize = 16;
const QPS_PER_THREAD: usize = NUM_CONNECTIONS / NUM_SERVER_THREADS; // 32
const SMALL_MSG_SIZE: usize = 32;
const RECV_BUF_ENTRY_SIZE: usize = 64;
const UD_RECV_ENTRY_SIZE: usize = 128;
const DC_KEY: u64 = 0x1234_5678_ABCD_EF00;
const SIGNAL_INTERVAL: usize = 8;

// RC QP depth: needs headroom for signaling interval
const RC_MAX_SEND_WR: u32 = 16;
const RC_MAX_RECV_WR: u32 = 16;

// =============================================================================
// Helpers
// =============================================================================

fn full_access() -> AccessFlags {
    test_utils::full_access()
}

fn open_mlx5_device() -> Option<Context> {
    test_utils::open_mlx5_device()
}

// =============================================================================
// Connection Info
// =============================================================================

#[derive(Clone)]
struct ConnectionInfo {
    qpn: u32,
    lid: u16,
}

#[derive(Clone)]
struct UdConnectionInfo {
    qpn: u32,
    lid: u16,
    qkey: u32,
}

#[derive(Clone)]
struct DcConnectionInfo {
    dctn: u32,
    dc_key: u64,
    lid: u16,
}

// =============================================================================
// Server Handle
// =============================================================================

struct MultiServerHandle {
    stop_flag: Arc<AtomicBool>,
    handles: Vec<JoinHandle<()>>,
}

impl MultiServerHandle {
    fn stop(&mut self) {
        self.stop_flag.store(true, Ordering::SeqCst);
        for handle in self.handles.drain(..) {
            let _ = handle.join();
        }
    }
}

impl Drop for MultiServerHandle {
    fn drop(&mut self) {
        self.stop();
    }
}

// =============================================================================
// Shared State (client: count only)
// =============================================================================

#[derive(Clone)]
struct ClientRecvState {
    rx_count: Rc<Cell<usize>>,
}

impl ClientRecvState {
    fn new() -> Self {
        Self {
            rx_count: Rc::new(Cell::new(0)),
        }
    }

    fn reset(&self) {
        self.rx_count.set(0);
    }
}

// =============================================================================
// Server Recv State (tracks QP indices)
// =============================================================================

struct ServerRecvState {
    received: RefCell<Vec<u64>>,
}

impl ServerRecvState {
    fn new() -> Self {
        Self {
            received: RefCell::new(Vec::with_capacity(QPS_PER_THREAD)),
        }
    }

    fn push(&self, entry: u64) {
        self.received.borrow_mut().push(entry);
    }

    fn drain(&self) -> Vec<u64> {
        std::mem::take(&mut *self.received.borrow_mut())
    }
}

// =============================================================================
// RC Multi-QP Benchmark
// =============================================================================

struct RcMultiQpSetup<SF, RF>
where
    SF: Fn(Cqe, u64),
    RF: Fn(Cqe, u64),
{
    qps: Vec<Rc<RefCell<RcQpForMonoCq<u64>>>>,
    send_cq: Rc<MonoCqRc<u64, SF>>,
    recv_cq: Rc<MonoCqRc<u64, RF>>,
    client_state: ClientRecvState,
    _send_mr: MemoryRegion,
    recv_mr: MemoryRegion,
    _send_buf: AlignedBuffer,
    recv_buf: AlignedBuffer,
    _server_handle: MultiServerHandle,
    _pd: Rc<Pd>,
    _ctx: Context,
}

fn rc_server_thread_main(
    server_info_tx: Sender<Vec<ConnectionInfo>>,
    client_info_rx: Receiver<Vec<ConnectionInfo>>,
    ready_counter: Arc<AtomicU32>,
    stop_flag: Arc<AtomicBool>,
) {
    let ctx = match open_mlx5_device() {
        Some(c) => c,
        None => return,
    };
    let port = 1u8;
    let port_attr = match ctx.query_port(port) {
        Ok(a) => a,
        Err(_) => return,
    };
    let pd = Rc::new(match ctx.alloc_pd() {
        Ok(p) => p,
        Err(_) => return,
    });

    let recv_state = Rc::new(ServerRecvState::new());
    let recv_state_cb = recv_state.clone();

    let send_callback = move |_cqe: Cqe, _entry: u64| {};
    let recv_callback = move |cqe: Cqe, entry: u64| {
        if cqe.opcode.is_responder() && cqe.syndrome == 0 {
            recv_state_cb.push(entry);
        }
    };

    let send_cq = match ctx.create_mono_cq::<RcQpForMonoCq<u64>, _>(
        QPS_PER_THREAD as i32, send_callback, &CqConfig::default(),
    ) {
        Ok(cq) => Rc::new(cq),
        Err(_) => return,
    };
    let recv_cq = match ctx.create_mono_cq::<RcQpForMonoCq<u64>, _>(
        QPS_PER_THREAD as i32, recv_callback, &CqConfig::default(),
    ) {
        Ok(cq) => Rc::new(cq),
        Err(_) => return,
    };

    let config = RcQpConfig {
        max_send_wr: RC_MAX_SEND_WR,
        max_recv_wr: RC_MAX_RECV_WR,
        max_send_sge: 1,
        max_recv_sge: 1,
        max_inline_data: 256,
        enable_scatter_to_cqe: true,
    };

    let recv_buf = AlignedBuffer::new(QPS_PER_THREAD * RECV_BUF_ENTRY_SIZE);
    let recv_mr = match unsafe { pd.register(recv_buf.as_ptr(), recv_buf.size(), full_access()) } {
        Ok(mr) => mr,
        Err(_) => return,
    };

    let mut qps = Vec::with_capacity(QPS_PER_THREAD);
    let mut server_infos = Vec::with_capacity(QPS_PER_THREAD);

    for _ in 0..QPS_PER_THREAD {
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
        server_infos.push(ConnectionInfo {
            qpn: qp.borrow().qpn(),
            lid: port_attr.lid,
        });
        qps.push(qp);
    }

    if server_info_tx.send(server_infos).is_err() {
        return;
    }

    let client_infos = match client_info_rx.recv() {
        Ok(v) => v,
        Err(_) => return,
    };

    let access = full_access().bits();
    for (i, info) in client_infos.iter().enumerate() {
        let remote = IbRemoteQpInfo {
            qp_number: info.qpn,
            packet_sequence_number: 0,
            local_identifier: info.lid,
        };
        if qps[i].borrow_mut().connect(&remote, port, 0, 4, 4, access).is_err() {
            return;
        }
    }

    // Pre-post recv WQEs
    for i in 0..QPS_PER_THREAD {
        let offset = (i * RECV_BUF_ENTRY_SIZE) as u64;
        qps[i]
            .borrow()
            .post_recv(i as u64, recv_buf.addr() + offset, RECV_BUF_ENTRY_SIZE as u32, recv_mr.lkey())
            .unwrap();
        qps[i].borrow().ring_rq_doorbell();
    }

    ready_counter.fetch_add(1, Ordering::Release);

    // Echo loop with doorbell batching + 1/8 signaling
    let echo_data = vec![0xBBu8; SMALL_MSG_SIZE];
    let mut send_count = 0usize;

    while !stop_flag.load(Ordering::Relaxed) {
        recv_cq.poll();
        recv_cq.flush();
        let received = recv_state.drain();
        if received.is_empty() {
            continue;
        }

        send_cq.poll();
        send_cq.flush();

        // Phase 1: Repost all recvs
        for &qp_idx in &received {
            let idx = qp_idx as usize;
            let offset = (idx * RECV_BUF_ENTRY_SIZE) as u64;
            qps[idx]
                .borrow()
                .post_recv(idx as u64, recv_buf.addr() + offset, RECV_BUF_ENTRY_SIZE as u32, recv_mr.lkey())
                .unwrap();
        }

        // Phase 2: Ring all recv doorbells
        for &qp_idx in &received {
            qps[qp_idx as usize].borrow().ring_rq_doorbell();
        }

        // Phase 3: Emit all sends with 1/8 signaling
        for &qp_idx in &received {
            let idx = qp_idx as usize;
            let qp = qps[idx].borrow();
            let ctx = qp.emit_ctx().expect("emit_ctx failed");
            if send_count % SIGNAL_INTERVAL == SIGNAL_INTERVAL - 1 {
                let _ = emit_wqe!(&ctx, send {
                    flags: WqeFlags::empty(),
                    inline: echo_data.as_slice(),
                    signaled: idx as u64,
                });
            } else {
                let _ = emit_wqe!(&ctx, send {
                    flags: WqeFlags::empty(),
                    inline: echo_data.as_slice(),
                });
            }
            send_count += 1;
        }

        // Phase 4: Ring all send doorbells
        for &qp_idx in &received {
            qps[qp_idx as usize].borrow().ring_sq_doorbell();
        }
    }
}

fn setup_rc_multi_qp_benchmark() -> Option<RcMultiQpSetup<impl Fn(Cqe, u64), impl Fn(Cqe, u64)>> {
    let ctx = open_mlx5_device()?;
    let port = 1u8;
    let port_attr = ctx.query_port(port).ok()?;
    let pd = Rc::new(ctx.alloc_pd().ok()?);

    let client_state = ClientRecvState::new();
    let client_state_cb = client_state.clone();

    let send_callback = move |_cqe: Cqe, _entry: u64| {};
    let recv_callback = move |cqe: Cqe, _entry: u64| {
        if cqe.opcode.is_responder() && cqe.syndrome == 0 {
            client_state_cb.rx_count.set(client_state_cb.rx_count.get() + 1);
        }
    };

    let send_cq = Rc::new(ctx.create_mono_cq(
        NUM_CONNECTIONS as i32, send_callback, &CqConfig::default(),
    ).ok()?);
    let recv_cq = Rc::new(ctx.create_mono_cq(
        NUM_CONNECTIONS as i32, recv_callback, &CqConfig::default(),
    ).ok()?);

    let config = RcQpConfig {
        max_send_wr: RC_MAX_SEND_WR,
        max_recv_wr: RC_MAX_RECV_WR,
        max_send_sge: 1,
        max_recv_sge: 1,
        max_inline_data: 256,
        enable_scatter_to_cqe: true,
    };

    let send_buf = AlignedBuffer::new(NUM_CONNECTIONS * RECV_BUF_ENTRY_SIZE);
    let recv_buf = AlignedBuffer::new(NUM_CONNECTIONS * RECV_BUF_ENTRY_SIZE);
    let send_mr = unsafe { pd.register(send_buf.as_ptr(), send_buf.size(), full_access()) }.ok()?;
    let recv_mr = unsafe { pd.register(recv_buf.as_ptr(), recv_buf.size(), full_access()) }.ok()?;

    // Create 512 client QPs
    let mut qps = Vec::with_capacity(NUM_CONNECTIONS);
    for _ in 0..NUM_CONNECTIONS {
        let qp = ctx
            .rc_qp_builder::<u64, u64>(&pd, &config)
            .sq_mono_cq(&send_cq)
            .rq_mono_cq(&recv_cq)
            .build()
            .ok()?;
        send_cq.register(&qp);
        recv_cq.register(&qp);
        qps.push(qp);
    }

    // Spawn server threads and exchange connection info
    let stop_flag = Arc::new(AtomicBool::new(false));
    let ready_counter = Arc::new(AtomicU32::new(0));
    let mut handles = Vec::with_capacity(NUM_SERVER_THREADS);

    let mut all_server_infos: Vec<Option<Vec<ConnectionInfo>>> = (0..NUM_SERVER_THREADS).map(|_| None).collect();

    let mut server_info_rxs = Vec::new();
    let mut client_info_txs = Vec::new();

    for _ in 0..NUM_SERVER_THREADS {
        let (si_tx, si_rx) = mpsc::channel::<Vec<ConnectionInfo>>();
        let (ci_tx, ci_rx) = mpsc::channel::<Vec<ConnectionInfo>>();
        let ready = ready_counter.clone();
        let stop = stop_flag.clone();

        handles.push(thread::spawn(move || {
            rc_server_thread_main(si_tx, ci_rx, ready, stop);
        }));

        server_info_rxs.push(si_rx);
        client_info_txs.push(ci_tx);
    }

    // Receive server infos
    for (t, rx) in server_info_rxs.iter().enumerate() {
        all_server_infos[t] = Some(rx.recv().ok()?);
    }

    // Send client infos to each server thread
    for t in 0..NUM_SERVER_THREADS {
        let base = t * QPS_PER_THREAD;
        let client_infos: Vec<ConnectionInfo> = (0..QPS_PER_THREAD)
            .map(|i| ConnectionInfo {
                qpn: qps[base + i].borrow().qpn(),
                lid: port_attr.lid,
            })
            .collect();
        client_info_txs[t].send(client_infos).ok()?;
    }

    // Connect client QPs
    let access = full_access().bits();
    for t in 0..NUM_SERVER_THREADS {
        let server_infos = all_server_infos[t].as_ref().unwrap();
        let base = t * QPS_PER_THREAD;
        for i in 0..QPS_PER_THREAD {
            let remote = IbRemoteQpInfo {
                qp_number: server_infos[i].qpn,
                packet_sequence_number: 0,
                local_identifier: server_infos[i].lid,
            };
            qps[base + i]
                .borrow_mut()
                .connect(&remote, port, 0, 4, 4, access)
                .ok()?;
        }
    }

    // Pre-post recv WQEs
    for i in 0..NUM_CONNECTIONS {
        let offset = (i * RECV_BUF_ENTRY_SIZE) as u64;
        qps[i]
            .borrow()
            .post_recv(i as u64, recv_buf.addr() + offset, RECV_BUF_ENTRY_SIZE as u32, recv_mr.lkey())
            .unwrap();
        qps[i].borrow().ring_rq_doorbell();
    }

    // Wait for all server threads ready
    while ready_counter.load(Ordering::Acquire) < NUM_SERVER_THREADS as u32 {
        std::hint::spin_loop();
    }

    Some(RcMultiQpSetup {
        qps,
        send_cq,
        recv_cq,
        client_state,
        _send_mr: send_mr,
        recv_mr,
        _send_buf: send_buf,
        recv_buf,
        _server_handle: MultiServerHandle { stop_flag, handles },
        _pd: pd,
        _ctx: ctx,
    })
}

fn run_rc_multi_qp_throughput<SF, RF>(
    setup: &RcMultiQpSetup<SF, RF>,
    iters: u64,
) -> Duration
where
    SF: Fn(Cqe, u64),
    RF: Fn(Cqe, u64),
{
    let send_data = vec![0xAAu8; SMALL_MSG_SIZE];
    let total = iters as usize * NUM_CONNECTIONS;
    let signal_interval = SIGNAL_INTERVAL.min(total).max(1);

    // Initial fill: post 1 send per QP with 1/8 signaling
    let mut send_count = 0usize;
    for i in 0..NUM_CONNECTIONS {
        let qp = setup.qps[i].borrow();
        let ctx = qp.emit_ctx().expect("emit_ctx failed");
        if send_count % signal_interval == signal_interval - 1 {
            let _ = emit_wqe!(&ctx, send {
                flags: WqeFlags::empty(),
                inline: send_data.as_slice(),
                signaled: i as u64,
            });
        } else {
            let _ = emit_wqe!(&ctx, send {
                flags: WqeFlags::empty(),
                inline: send_data.as_slice(),
            });
        }
        qp.ring_sq_doorbell();
        send_count += 1;
    }

    let start = std::time::Instant::now();
    let mut completed = 0usize;
    let mut sent = NUM_CONNECTIONS; // already sent initial fill
    let mut recv_idx = 0usize;

    while completed < total {
        setup.client_state.reset();
        setup.recv_cq.poll();
        setup.recv_cq.flush();
        let rx_count = setup.client_state.rx_count.get();

        completed += rx_count;

        if rx_count == 0 {
            continue;
        }

        setup.send_cq.poll();
        setup.send_cq.flush();

        // Repost recv WQEs (round-robin across QPs)
        for _ in 0..rx_count {
            let idx = recv_idx % NUM_CONNECTIONS;
            let offset = (idx * RECV_BUF_ENTRY_SIZE) as u64;
            let qp = setup.qps[idx].borrow();
            qp.post_recv(
                idx as u64,
                setup.recv_buf.addr() + offset,
                RECV_BUF_ENTRY_SIZE as u32,
                setup.recv_mr.lkey(),
            )
            .unwrap();
            qp.ring_rq_doorbell();
            recv_idx += 1;
        }

        // Post new sends if needed (1 per completed, round-robin)
        let to_send = rx_count.min(total - sent);
        for _ in 0..to_send {
            let qp_idx = sent % NUM_CONNECTIONS;
            let qp = setup.qps[qp_idx].borrow();
            let ctx = qp.emit_ctx().expect("emit_ctx failed");
            if send_count % signal_interval == signal_interval - 1 {
                let _ = emit_wqe!(&ctx, send {
                    flags: WqeFlags::empty(),
                    inline: send_data.as_slice(),
                    signaled: qp_idx as u64,
                });
            } else {
                let _ = emit_wqe!(&ctx, send {
                    flags: WqeFlags::empty(),
                    inline: send_data.as_slice(),
                });
            }
            qp.ring_sq_doorbell();
            send_count += 1;
            sent += 1;
        }
    }

    start.elapsed()
}

// =============================================================================
// UD Multi-QP Benchmark (MonoCq)
// =============================================================================

type UdMonoCqSendCb = Box<dyn Fn(Cqe, u64)>;
type UdMonoCqRecvCb = Box<dyn Fn(Cqe, u64)>;

struct UdMultiQpSetup {
    qp: Rc<RefCell<UdQpForMonoCq<u64>>>,
    send_cq: Rc<mlx5::mono_cq::MonoCq<UdQpForMonoCq<u64>, UdMonoCqSendCb>>,
    recv_cq: Rc<mlx5::mono_cq::MonoCq<UdQpForMonoCq<u64>, UdMonoCqRecvCb>>,
    client_state: ClientRecvState,
    _send_mr: MemoryRegion,
    recv_mr: MemoryRegion,
    _send_buf: AlignedBuffer,
    recv_buf: AlignedBuffer,
    server_avs: Vec<UdAvIb>,
    _server_handle: MultiServerHandle,
    _pd: Rc<Pd>,
    _ctx: Context,
}

fn ud_server_thread_main(
    server_info_tx: Sender<UdConnectionInfo>,
    client_info_rx: Receiver<UdConnectionInfo>,
    ready_counter: Arc<AtomicU32>,
    stop_flag: Arc<AtomicBool>,
) {
    let ctx = match open_mlx5_device() {
        Some(c) => c,
        None => return,
    };
    let port = 1u8;
    let port_attr = match ctx.query_port(port) {
        Ok(a) => a,
        Err(_) => return,
    };
    let pd = Rc::new(match ctx.alloc_pd() {
        Ok(p) => p,
        Err(_) => return,
    });

    let recv_count = Rc::new(Cell::new(0usize));
    let recv_count_cb = recv_count.clone();

    let send_callback: Box<dyn Fn(Cqe, u64)> = Box::new(move |_cqe: Cqe, _entry: u64| {});
    let recv_callback: Box<dyn Fn(Cqe, u64)> = Box::new(move |cqe: Cqe, _entry: u64| {
        if cqe.opcode.is_responder() && cqe.syndrome == 0 {
            recv_count_cb.set(recv_count_cb.get() + 1);
        }
    });

    let send_cq = match ctx.create_mono_cq::<UdQpForMonoCq<u64>, _>(
        QPS_PER_THREAD as i32, send_callback, &CqConfig::default(),
    ) {
        Ok(cq) => Rc::new(cq),
        Err(_) => return,
    };
    let recv_cq = match ctx.create_mono_cq::<UdQpForMonoCq<u64>, _>(
        QPS_PER_THREAD as i32, recv_callback, &CqConfig::default(),
    ) {
        Ok(cq) => Rc::new(cq),
        Err(_) => return,
    };

    let qkey = 0x11111111u32;
    let ud_config = UdQpConfig {
        max_send_wr: QPS_PER_THREAD as u32,
        max_recv_wr: QPS_PER_THREAD as u32,
        max_send_sge: 1,
        max_recv_sge: 1,
        max_inline_data: 256,
        qkey,
    };

    let qp = match ctx
        .ud_qp_builder::<u64, u64>(&pd, &ud_config)
        .sq_mono_cq(&send_cq)
        .rq_mono_cq(&recv_cq)
        .build()
    {
        Ok(q) => q,
        Err(_) => return,
    };

    send_cq.register(&qp);
    recv_cq.register(&qp);

    if qp.borrow_mut().activate(port, 0).is_err() {
        return;
    }

    let recv_buf = AlignedBuffer::new(QPS_PER_THREAD * UD_RECV_ENTRY_SIZE);
    let recv_mr = match unsafe { pd.register(recv_buf.as_ptr(), recv_buf.size(), full_access()) } {
        Ok(mr) => mr,
        Err(_) => return,
    };

    let server_info = UdConnectionInfo {
        qpn: qp.borrow().qpn(),
        lid: port_attr.lid,
        qkey,
    };
    if server_info_tx.send(server_info).is_err() {
        return;
    }

    let client_info = match client_info_rx.recv() {
        Ok(info) => info,
        Err(_) => return,
    };

    // Pre-post recv WQEs
    for i in 0..QPS_PER_THREAD {
        let offset = (i * UD_RECV_ENTRY_SIZE) as u64;
        qp.borrow()
            .post_recv(i as u64, recv_buf.addr() + offset, UD_RECV_ENTRY_SIZE as u32, recv_mr.lkey())
            .unwrap();
    }
    qp.borrow().ring_rq_doorbell();

    ready_counter.fetch_add(1, Ordering::Release);

    let echo_data = vec![0xBBu8; SMALL_MSG_SIZE];
    let client_av = UdAvIb::new(client_info.qpn, client_info.qkey, client_info.lid);
    let mut recv_idx = 0usize;
    let mut send_count = 0usize;

    while !stop_flag.load(Ordering::Relaxed) {
        recv_count.set(0);
        recv_cq.poll();
        recv_cq.flush();
        let count = recv_count.get();
        if count == 0 {
            continue;
        }

        send_cq.poll();
        send_cq.flush();

        // Repost recv
        {
            let qp_ref = qp.borrow();
            for _ in 0..count {
                let idx = recv_idx % QPS_PER_THREAD;
                let offset = (idx * UD_RECV_ENTRY_SIZE) as u64;
                qp_ref
                    .post_recv(idx as u64, recv_buf.addr() + offset, UD_RECV_ENTRY_SIZE as u32, recv_mr.lkey())
                    .unwrap();
                recv_idx += 1;
            }
        }

        // Echo with 1/8 signaling
        {
            let qp_ref = qp.borrow();
            let ctx = qp_ref.emit_ctx().expect("emit_ctx failed");
            for _ in 0..count {
                if send_count % SIGNAL_INTERVAL == SIGNAL_INTERVAL - 1 {
                    let _ = emit_ud_wqe!(&ctx, send {
                        av: client_av,
                        flags: WqeFlags::empty(),
                        inline: echo_data.as_slice(),
                        signaled: send_count as u64,
                    });
                } else {
                    let _ = emit_ud_wqe!(&ctx, send {
                        av: client_av,
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

fn setup_ud_multi_qp_benchmark() -> Option<UdMultiQpSetup> {
    let ctx = open_mlx5_device()?;
    let port = 1u8;
    let port_attr = ctx.query_port(port).ok()?;
    let pd = Rc::new(ctx.alloc_pd().ok()?);

    let client_state = ClientRecvState::new();
    let client_state_cb = client_state.clone();

    let send_callback: UdMonoCqSendCb = Box::new(move |_cqe: Cqe, _entry: u64| {});
    let recv_callback: UdMonoCqRecvCb = Box::new(move |cqe: Cqe, _entry: u64| {
        if cqe.opcode.is_responder() && cqe.syndrome == 0 {
            client_state_cb.rx_count.set(client_state_cb.rx_count.get() + 1);
        }
    });

    let send_cq = Rc::new(ctx.create_mono_cq::<UdQpForMonoCq<u64>, _>(
        NUM_CONNECTIONS as i32, send_callback, &CqConfig::default(),
    ).ok()?);
    let recv_cq = Rc::new(ctx.create_mono_cq::<UdQpForMonoCq<u64>, _>(
        NUM_CONNECTIONS as i32, recv_callback, &CqConfig::default(),
    ).ok()?);

    let qkey = 0x11111111u32;
    let ud_config = UdQpConfig {
        max_send_wr: NUM_CONNECTIONS as u32,
        max_recv_wr: NUM_CONNECTIONS as u32,
        max_send_sge: 1,
        max_recv_sge: 1,
        max_inline_data: 256,
        qkey,
    };

    let qp = ctx
        .ud_qp_builder::<u64, u64>(&pd, &ud_config)
        .sq_mono_cq(&send_cq)
        .rq_mono_cq(&recv_cq)
        .build()
        .ok()?;

    send_cq.register(&qp);
    recv_cq.register(&qp);

    qp.borrow_mut().activate(port, 0).ok()?;

    let send_buf = AlignedBuffer::new(NUM_CONNECTIONS * UD_RECV_ENTRY_SIZE);
    let recv_buf = AlignedBuffer::new(NUM_CONNECTIONS * UD_RECV_ENTRY_SIZE);
    let send_mr = unsafe { pd.register(send_buf.as_ptr(), send_buf.size(), full_access()) }.ok()?;
    let recv_mr = unsafe { pd.register(recv_buf.as_ptr(), recv_buf.size(), full_access()) }.ok()?;

    let client_info = UdConnectionInfo {
        qpn: qp.borrow().qpn(),
        lid: port_attr.lid,
        qkey,
    };

    // Spawn server threads
    let stop_flag = Arc::new(AtomicBool::new(false));
    let ready_counter = Arc::new(AtomicU32::new(0));
    let mut handles = Vec::with_capacity(NUM_SERVER_THREADS);
    let mut server_info_rxs = Vec::new();
    let mut client_info_txs = Vec::new();

    for _ in 0..NUM_SERVER_THREADS {
        let (si_tx, si_rx) = mpsc::channel::<UdConnectionInfo>();
        let (ci_tx, ci_rx) = mpsc::channel::<UdConnectionInfo>();
        let ready = ready_counter.clone();
        let stop = stop_flag.clone();

        handles.push(thread::spawn(move || {
            ud_server_thread_main(si_tx, ci_rx, ready, stop);
        }));

        server_info_rxs.push(si_rx);
        client_info_txs.push(ci_tx);
    }

    // Exchange connection info
    let mut server_avs = Vec::with_capacity(NUM_CONNECTIONS);
    for t in 0..NUM_SERVER_THREADS {
        let info = server_info_rxs[t].recv().ok()?;
        for _ in 0..QPS_PER_THREAD {
            server_avs.push(UdAvIb::new(info.qpn, info.qkey, info.lid));
        }
        client_info_txs[t].send(client_info.clone()).ok()?;
    }

    // Pre-post recv WQEs
    for i in 0..NUM_CONNECTIONS {
        let offset = (i * UD_RECV_ENTRY_SIZE) as u64;
        qp.borrow()
            .post_recv(i as u64, recv_buf.addr() + offset, UD_RECV_ENTRY_SIZE as u32, recv_mr.lkey())
            .unwrap();
    }
    qp.borrow().ring_rq_doorbell();

    // Wait for all server threads ready
    while ready_counter.load(Ordering::Acquire) < NUM_SERVER_THREADS as u32 {
        std::hint::spin_loop();
    }

    Some(UdMultiQpSetup {
        qp,
        send_cq,
        recv_cq,
        client_state,
        _send_mr: send_mr,
        recv_mr,
        _send_buf: send_buf,
        recv_buf,
        server_avs,
        _server_handle: MultiServerHandle { stop_flag, handles },
        _pd: pd,
        _ctx: ctx,
    })
}

fn run_ud_multi_qp_throughput(setup: &UdMultiQpSetup, iters: u64) -> Duration {
    let send_data = vec![0xAAu8; SMALL_MSG_SIZE];
    let total = iters as usize * NUM_CONNECTIONS;
    let signal_interval = SIGNAL_INTERVAL.min(total).max(1);

    // Initial fill with 1/8 signaling
    let initial_fill = total.min(NUM_CONNECTIONS);
    let mut send_count = 0usize;
    {
        let qp = setup.qp.borrow();
        let ctx = qp.emit_ctx().expect("emit_ctx failed");
        for i in 0..initial_fill {
            if send_count % signal_interval == signal_interval - 1 {
                let _ = emit_ud_wqe!(&ctx, send {
                    av: setup.server_avs[i % NUM_CONNECTIONS],
                    flags: WqeFlags::empty(),
                    inline: send_data.as_slice(),
                    signaled: send_count as u64,
                });
            } else {
                let _ = emit_ud_wqe!(&ctx, send {
                    av: setup.server_avs[i % NUM_CONNECTIONS],
                    flags: WqeFlags::empty(),
                    inline: send_data.as_slice(),
                });
            }
            send_count += 1;
        }
        qp.ring_sq_doorbell();
    }

    let start = std::time::Instant::now();
    let mut completed = 0usize;
    let mut inflight = initial_fill;
    let mut sent = initial_fill;
    let mut recv_idx = 0usize;

    while completed < total {
        setup.client_state.reset();
        setup.recv_cq.poll();
        setup.recv_cq.flush();
        let rx_count = setup.client_state.rx_count.get();

        completed += rx_count;
        inflight -= rx_count;

        if rx_count == 0 {
            continue;
        }

        setup.send_cq.poll();
        setup.send_cq.flush();

        // Repost recv WQEs
        {
            let qp = setup.qp.borrow();
            for _ in 0..rx_count {
                let idx = recv_idx % NUM_CONNECTIONS;
                let offset = (idx * UD_RECV_ENTRY_SIZE) as u64;
                qp.post_recv(
                    idx as u64,
                    setup.recv_buf.addr() + offset,
                    UD_RECV_ENTRY_SIZE as u32,
                    setup.recv_mr.lkey(),
                )
                .unwrap();
                recv_idx += 1;
            }
        }

        // Send more if needed with 1/8 signaling
        let remaining = total - sent;
        let can_send = (NUM_CONNECTIONS - inflight).min(remaining).min(rx_count);

        if can_send > 0 {
            let qp = setup.qp.borrow();
            let ctx = qp.emit_ctx().expect("emit_ctx failed");
            for _ in 0..can_send {
                let av_idx = sent % NUM_CONNECTIONS;
                if send_count % signal_interval == signal_interval - 1 {
                    let _ = emit_ud_wqe!(&ctx, send {
                        av: setup.server_avs[av_idx],
                        flags: WqeFlags::empty(),
                        inline: send_data.as_slice(),
                        signaled: send_count as u64,
                    });
                } else {
                    let _ = emit_ud_wqe!(&ctx, send {
                        av: setup.server_avs[av_idx],
                        flags: WqeFlags::empty(),
                        inline: send_data.as_slice(),
                    });
                }
                send_count += 1;
                sent += 1;
            }
            inflight += can_send;
        }

        // Ring doorbells
        {
            let qp = setup.qp.borrow();
            qp.ring_rq_doorbell();
            if can_send > 0 {
                qp.ring_sq_doorbell();
            }
        }
    }

    // Drain remaining inflight
    while inflight > 0 {
        setup.client_state.reset();
        setup.recv_cq.poll();
        setup.recv_cq.flush();
        inflight -= setup.client_state.rx_count.get();

        setup.send_cq.poll();
        setup.send_cq.flush();
    }

    start.elapsed()
}

// =============================================================================
// DC Multi-QP Benchmark (MonoCq for DCI send, regular Cq for DCT recv)
// =============================================================================

type DcMonoCqSendCb = Box<dyn Fn(Cqe, u64)>;

struct DcMultiQpSetup {
    dci: Rc<RefCell<DciForMonoCq<u64>>>,
    send_cq: Rc<mlx5::mono_cq::MonoCq<DciForMonoCq<u64>, DcMonoCqSendCb>>,
    recv_cq: Rc<Cq>,
    _dct: mlx5::dc::Dct<u64>,
    srq: mlx5::srq::Srq<u64>,
    recv_mr: MemoryRegion,
    recv_buf: AlignedBuffer,
    server_avs: Vec<DcAvIb>,
    _server_handle: MultiServerHandle,
    _pd: Rc<Pd>,
    _ctx: Context,
}

fn dc_server_thread_main(
    server_info_tx: Sender<DcConnectionInfo>,
    client_info_rx: Receiver<DcConnectionInfo>,
    ready_counter: Arc<AtomicU32>,
    stop_flag: Arc<AtomicBool>,
) {
    let ctx = match open_mlx5_device() {
        Some(c) => c,
        None => return,
    };
    let port = 1u8;
    let port_attr = match ctx.query_port(port) {
        Ok(a) => a,
        Err(_) => return,
    };
    let pd = Rc::new(match ctx.alloc_pd() {
        Ok(p) => p,
        Err(_) => return,
    });

    let send_callback: DcMonoCqSendCb = Box::new(move |_cqe: Cqe, _entry: u64| {});

    let send_cq = match ctx.create_mono_cq::<DciForMonoCq<u64>, _>(
        QPS_PER_THREAD as i32, send_callback, &CqConfig::default(),
    ) {
        Ok(cq) => Rc::new(cq),
        Err(_) => return,
    };
    let recv_cq = match ctx.create_cq(QPS_PER_THREAD as i32, &CqConfig::default()) {
        Ok(cq) => Rc::new(cq),
        Err(_) => return,
    };

    // Create SRQ for DCT
    let srq_config = SrqConfig {
        max_wr: QPS_PER_THREAD as u32,
        max_sge: 1,
    };
    let srq = match pd.create_srq::<u64>(&srq_config) {
        Ok(s) => s,
        Err(_) => return,
    };

    // Create DCT
    let dct_config = DctConfig { dc_key: DC_KEY };
    let mut dct = match ctx
        .dct_builder::<u64>(&pd, &srq, &dct_config)
        .recv_cq(&recv_cq)
        .build()
    {
        Ok(d) => d,
        Err(_) => return,
    };

    let access = full_access().bits();
    if dct.activate(port, access, 12).is_err() {
        return;
    }

    // Create DCI for echo (MonoCq)
    let dci_config = DciConfig {
        max_send_wr: QPS_PER_THREAD as u32,
        max_send_sge: 1,
        max_inline_data: 256,
    };
    let dci = match ctx
        .dci_builder::<u64>(&pd, &dci_config)
        .sq_mono_cq(&send_cq)
        .build()
    {
        Ok(d) => d,
        Err(_) => return,
    };

    send_cq.register(&dci);

    if dci.borrow_mut().activate(port, 0, 4).is_err() {
        return;
    }

    let recv_buf = AlignedBuffer::new(QPS_PER_THREAD * RECV_BUF_ENTRY_SIZE);
    let recv_mr = match unsafe { pd.register(recv_buf.as_ptr(), recv_buf.size(), full_access()) } {
        Ok(mr) => mr,
        Err(_) => return,
    };

    let server_info = DcConnectionInfo {
        dctn: dct.dctn(),
        dc_key: DC_KEY,
        lid: port_attr.lid,
    };
    if server_info_tx.send(server_info).is_err() {
        return;
    }

    let client_info = match client_info_rx.recv() {
        Ok(info) => info,
        Err(_) => return,
    };

    // Pre-post recv WQEs on SRQ
    for i in 0..QPS_PER_THREAD {
        let offset = (i * RECV_BUF_ENTRY_SIZE) as u64;
        srq.post_recv(i as u64, recv_buf.addr() + offset, RECV_BUF_ENTRY_SIZE as u32, recv_mr.lkey())
            .unwrap();
    }
    srq.ring_doorbell();

    ready_counter.fetch_add(1, Ordering::Release);

    let echo_data = vec![0xBBu8; SMALL_MSG_SIZE];
    let client_av = DcAvIb {
        dc_key: client_info.dc_key,
        dctn: client_info.dctn,
        dlid: client_info.lid,
    };
    let mut recv_idx = 0usize;
    let mut send_count = 0usize;

    while !stop_flag.load(Ordering::Relaxed) {
        let count = recv_cq.poll();
        recv_cq.flush();
        if count == 0 {
            continue;
        }

        send_cq.poll();
        send_cq.flush();

        // Process SRQ completions
        for _ in 0..count {
            srq.process_recv_completion(0);
        }

        // Repost recv on SRQ
        for _ in 0..count {
            let idx = recv_idx % QPS_PER_THREAD;
            let offset = (idx * RECV_BUF_ENTRY_SIZE) as u64;
            srq.post_recv(idx as u64, recv_buf.addr() + offset, RECV_BUF_ENTRY_SIZE as u32, recv_mr.lkey())
                .unwrap();
            recv_idx += 1;
        }
        srq.ring_doorbell();

        // Echo via DCI with 1/8 signaling
        {
            let dci_ref = dci.borrow();
            let ctx = dci_ref.emit_ctx().expect("emit_ctx failed");
            for _ in 0..count {
                if send_count % SIGNAL_INTERVAL == SIGNAL_INTERVAL - 1 {
                    let _ = emit_dci_wqe!(&ctx, send {
                        av: client_av,
                        flags: WqeFlags::empty(),
                        inline: echo_data.as_slice(),
                        signaled: send_count as u64,
                    });
                } else {
                    let _ = emit_dci_wqe!(&ctx, send {
                        av: client_av,
                        flags: WqeFlags::empty(),
                        inline: echo_data.as_slice(),
                    });
                }
                send_count += 1;
            }
            dci_ref.ring_sq_doorbell();
        }
    }
}

fn setup_dc_multi_qp_benchmark() -> Option<DcMultiQpSetup> {
    let ctx = open_mlx5_device()?;
    let port = 1u8;
    let port_attr = ctx.query_port(port).ok()?;
    let pd = Rc::new(ctx.alloc_pd().ok()?);

    let send_callback: DcMonoCqSendCb = Box::new(move |_cqe: Cqe, _entry: u64| {});

    let send_cq = Rc::new(ctx.create_mono_cq::<DciForMonoCq<u64>, _>(
        NUM_CONNECTIONS as i32, send_callback, &CqConfig::default(),
    ).ok()?);
    let recv_cq = Rc::new(ctx.create_cq(NUM_CONNECTIONS as i32, &CqConfig::default()).ok()?);

    // Client SRQ + DCT (for receiving echo)
    let srq_config = SrqConfig {
        max_wr: NUM_CONNECTIONS as u32,
        max_sge: 1,
    };
    let srq = pd.create_srq::<u64>(&srq_config).ok()?;

    let dct_config = DctConfig { dc_key: DC_KEY };
    let mut dct = ctx
        .dct_builder::<u64>(&pd, &srq, &dct_config)
        .recv_cq(&recv_cq)
        .build()
        .ok()?;

    let access = full_access().bits();
    dct.activate(port, access, 12).ok()?;

    // Client DCI (for sending, MonoCq)
    let dci_config = DciConfig {
        max_send_wr: NUM_CONNECTIONS as u32,
        max_send_sge: 1,
        max_inline_data: 256,
    };
    let dci = ctx
        .dci_builder::<u64>(&pd, &dci_config)
        .sq_mono_cq(&send_cq)
        .build()
        .ok()?;

    send_cq.register(&dci);

    dci.borrow_mut().activate(port, 0, 4).ok()?;

    let recv_buf = AlignedBuffer::new(NUM_CONNECTIONS * RECV_BUF_ENTRY_SIZE);
    let recv_mr = unsafe { pd.register(recv_buf.as_ptr(), recv_buf.size(), full_access()) }.ok()?;

    let client_info = DcConnectionInfo {
        dctn: dct.dctn(),
        dc_key: DC_KEY,
        lid: port_attr.lid,
    };

    // Spawn server threads
    let stop_flag = Arc::new(AtomicBool::new(false));
    let ready_counter = Arc::new(AtomicU32::new(0));
    let mut handles = Vec::with_capacity(NUM_SERVER_THREADS);
    let mut server_info_rxs = Vec::new();
    let mut client_info_txs = Vec::new();

    for _ in 0..NUM_SERVER_THREADS {
        let (si_tx, si_rx) = mpsc::channel::<DcConnectionInfo>();
        let (ci_tx, ci_rx) = mpsc::channel::<DcConnectionInfo>();
        let ready = ready_counter.clone();
        let stop = stop_flag.clone();

        handles.push(thread::spawn(move || {
            dc_server_thread_main(si_tx, ci_rx, ready, stop);
        }));

        server_info_rxs.push(si_rx);
        client_info_txs.push(ci_tx);
    }

    // Exchange connection info
    let mut server_avs = Vec::with_capacity(NUM_CONNECTIONS);
    for t in 0..NUM_SERVER_THREADS {
        let info = server_info_rxs[t].recv().ok()?;
        for _ in 0..QPS_PER_THREAD {
            server_avs.push(DcAvIb {
                dc_key: info.dc_key,
                dctn: info.dctn,
                dlid: info.lid,
            });
        }
        client_info_txs[t].send(client_info.clone()).ok()?;
    }

    // Pre-post recv on client SRQ
    for i in 0..NUM_CONNECTIONS {
        let offset = (i * RECV_BUF_ENTRY_SIZE) as u64;
        srq.post_recv(i as u64, recv_buf.addr() + offset, RECV_BUF_ENTRY_SIZE as u32, recv_mr.lkey())
            .unwrap();
    }
    srq.ring_doorbell();

    // Wait for all server threads ready
    while ready_counter.load(Ordering::Acquire) < NUM_SERVER_THREADS as u32 {
        std::hint::spin_loop();
    }

    Some(DcMultiQpSetup {
        dci,
        send_cq,
        recv_cq,
        _dct: dct,
        srq,
        recv_mr,
        recv_buf,
        server_avs,
        _server_handle: MultiServerHandle { stop_flag, handles },
        _pd: pd,
        _ctx: ctx,
    })
}

fn run_dc_multi_qp_throughput(setup: &DcMultiQpSetup, iters: u64) -> Duration {
    let send_data = vec![0xAAu8; SMALL_MSG_SIZE];
    let total = iters as usize * NUM_CONNECTIONS;
    let signal_interval = SIGNAL_INTERVAL.min(total).max(1);

    // Initial fill with 1/8 signaling
    let initial_fill = total.min(NUM_CONNECTIONS);
    let mut send_count = 0usize;
    {
        let dci = setup.dci.borrow();
        let ctx = dci.emit_ctx().expect("emit_ctx failed");
        for i in 0..initial_fill {
            if send_count % signal_interval == signal_interval - 1 {
                let _ = emit_dci_wqe!(&ctx, send {
                    av: setup.server_avs[i],
                    flags: WqeFlags::empty(),
                    inline: send_data.as_slice(),
                    signaled: send_count as u64,
                });
            } else {
                let _ = emit_dci_wqe!(&ctx, send {
                    av: setup.server_avs[i],
                    flags: WqeFlags::empty(),
                    inline: send_data.as_slice(),
                });
            }
            send_count += 1;
        }
        dci.ring_sq_doorbell();
    }

    let start = std::time::Instant::now();
    let mut completed = 0usize;
    let mut inflight = initial_fill;
    let mut sent = initial_fill;

    while completed < total {
        let rx_count = setup.recv_cq.poll();
        setup.recv_cq.flush();

        completed += rx_count;
        inflight -= rx_count;

        if rx_count == 0 {
            continue;
        }

        setup.send_cq.poll();
        setup.send_cq.flush();

        // Process SRQ completions
        for _ in 0..rx_count {
            setup.srq.process_recv_completion(0);
        }

        // Repost recv on SRQ
        for i in 0..rx_count {
            let idx = (completed - rx_count + i) % NUM_CONNECTIONS;
            let offset = (idx * RECV_BUF_ENTRY_SIZE) as u64;
            setup.srq.post_recv(
                idx as u64,
                setup.recv_buf.addr() + offset,
                RECV_BUF_ENTRY_SIZE as u32,
                setup.recv_mr.lkey(),
            )
            .unwrap();
        }
        setup.srq.ring_doorbell();

        // Send more if needed with 1/8 signaling
        let remaining = total - sent;
        let can_send = (NUM_CONNECTIONS - inflight).min(remaining).min(rx_count);

        if can_send > 0 {
            let dci = setup.dci.borrow();
            let ctx = dci.emit_ctx().expect("emit_ctx failed");
            for _ in 0..can_send {
                let av_idx = sent % NUM_CONNECTIONS;
                if send_count % signal_interval == signal_interval - 1 {
                    let _ = emit_dci_wqe!(&ctx, send {
                        av: setup.server_avs[av_idx],
                        flags: WqeFlags::empty(),
                        inline: send_data.as_slice(),
                        signaled: send_count as u64,
                    });
                } else {
                    let _ = emit_dci_wqe!(&ctx, send {
                        av: setup.server_avs[av_idx],
                        flags: WqeFlags::empty(),
                        inline: send_data.as_slice(),
                    });
                }
                send_count += 1;
                sent += 1;
            }
            inflight += can_send;
            dci.ring_sq_doorbell();
        }
    }

    // Drain remaining inflight
    while inflight > 0 {
        let rx_count = setup.recv_cq.poll();
        setup.recv_cq.flush();
        inflight -= rx_count;

        setup.send_cq.poll();
        setup.send_cq.flush();

        for _ in 0..rx_count {
            setup.srq.process_recv_completion(0);
        }
    }

    start.elapsed()
}

// =============================================================================
// Criterion Benchmarks
// =============================================================================

fn bench_rc_512qp(c: &mut Criterion) {
    let setup = match setup_rc_multi_qp_benchmark() {
        Some(s) => s,
        None => {
            eprintln!("Skipping RC benchmark: no mlx5 device available");
            return;
        }
    };

    let mut group = c.benchmark_group("multi_qp_512");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(5));
    group.throughput(Throughput::Elements(NUM_CONNECTIONS as u64));

    group.bench_function("rc_send_recv", |b| {
        b.iter_custom(|iters| run_rc_multi_qp_throughput(&setup, iters));
    });

    group.finish();
}

fn bench_ud_512qp(c: &mut Criterion) {
    let setup = match setup_ud_multi_qp_benchmark() {
        Some(s) => s,
        None => {
            eprintln!("Skipping UD benchmark: no mlx5 device available");
            return;
        }
    };

    let mut group = c.benchmark_group("multi_qp_512");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(5));
    group.throughput(Throughput::Elements(NUM_CONNECTIONS as u64));

    group.bench_function("ud_send_recv", |b| {
        b.iter_custom(|iters| run_ud_multi_qp_throughput(&setup, iters));
    });

    group.finish();
}

fn bench_dc_512qp(c: &mut Criterion) {
    let setup = match setup_dc_multi_qp_benchmark() {
        Some(s) => s,
        None => {
            eprintln!("Skipping DC benchmark: no mlx5 device available");
            return;
        }
    };

    let mut group = c.benchmark_group("multi_qp_512");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(5));
    group.throughput(Throughput::Elements(NUM_CONNECTIONS as u64));

    group.bench_function("dc_send_recv", |b| {
        b.iter_custom(|iters| run_dc_multi_qp_throughput(&setup, iters));
    });

    group.finish();
}

criterion_group!(benches, bench_rc_512qp, bench_ud_512qp, bench_dc_512qp);
criterion_main!(benches);
