use std::collections::VecDeque;
use std::sync::atomic::AtomicBool;

use fastmap::FastMap;

use crate::message::*;
use crate::shm::{ShmServer, ShmSlot};
use crate::slot;
use crate::storage::ShardedStore;

// === Type aliases ===
pub type DaemonFlux = inproc::Flux<DelegatePayload, usize>;

type CopyrpcCtx = copyrpc::dc::Context<SlotPtrs>;

/// Pointers passed as copyrpc user_data. Avoids slab indirection.
#[derive(Clone, Copy)]
pub struct SlotPtrs {
    pub req: *mut u8,
    pub resp: *mut u8,
    pub slot_idx: u32,
}

// === Connection info for MPI exchange ===

#[derive(Clone, Copy, Debug, Default)]
#[repr(C)]
pub struct DcEndpointConnectionInfo {
    pub dct_number: u32,
    pub local_identifier: u16,
    _padding: u16,
    pub dc_key: u64,
    pub recv_ring_addr: u64,
    pub recv_ring_rkey: u32,
    pub endpoint_id: u32,
    pub recv_ring_size: u64,
    pub initial_credit: u64,
}

pub const DC_CONNECTION_INFO_SIZE: usize = std::mem::size_of::<DcEndpointConnectionInfo>();

impl DcEndpointConnectionInfo {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        dct_number: u32,
        local_identifier: u16,
        dc_key: u64,
        recv_ring_addr: u64,
        recv_ring_rkey: u32,
        endpoint_id: u32,
        recv_ring_size: u64,
        initial_credit: u64,
    ) -> Self {
        Self {
            dct_number,
            local_identifier,
            _padding: 0,
            dc_key,
            recv_ring_addr,
            recv_ring_rkey,
            endpoint_id,
            recv_ring_size,
            initial_credit,
        }
    }

    pub fn to_bytes(self) -> Vec<u8> {
        unsafe {
            std::slice::from_raw_parts(&self as *const Self as *const u8, DC_CONNECTION_INFO_SIZE)
                .to_vec()
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        assert!(bytes.len() >= DC_CONNECTION_INFO_SIZE);
        unsafe {
            let mut info = Self::default();
            std::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                &mut info as *mut Self as *mut u8,
                DC_CONNECTION_INFO_SIZE,
            );
            info
        }
    }
}

// === Metrics constants ===
const RTT_HIST_BUCKETS: usize = 300; // 0-30μs in 100ns buckets
const RTT_HIST_BUCKET_NS: u64 = 100;

// === Metrics types ===

struct PhaseSample {
    cq_poll_ns: u64,
    incoming_ns: u64,
    gq_scan_ns: u64,
    flush_ns: u64,
}

#[allow(dead_code)]
struct EventSample {
    shm_req: u64,
    shm_local: u64,
    shm_remote: u64,
    cqe_resp: u64,
    incoming_req: u64,
    flush: u64,
    loops: u64,
}

// === Helpers ===

fn handle_local(store: &mut ShardedStore, req: &Request) -> Response {
    match *req {
        Request::AggPut { key, value, .. } => {
            store.put(key, value);
            Response::AggPutOk
        }
        Request::AggGet { key, .. } => match store.get(key) {
            Some(v) => Response::AggGetOk { value: v },
            None => Response::AggGetNotFound,
        },
    }
}

// === RC QP connection info (used by copyrpc_direct_backend) ===

#[derive(Clone, Copy, Debug, Default)]
#[repr(C)]
pub struct EndpointConnectionInfo {
    pub qp_number: u32,
    pub packet_sequence_number: u32,
    pub local_identifier: u16,
    _padding: u16,
    pub recv_ring_addr: u64,
    pub recv_ring_rkey: u32,
    _padding2: u32,
    pub recv_ring_size: u64,
    pub initial_credit: u64,
}

pub const CONNECTION_INFO_SIZE: usize = std::mem::size_of::<EndpointConnectionInfo>();

impl EndpointConnectionInfo {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        qp_number: u32,
        packet_sequence_number: u32,
        local_identifier: u16,
        recv_ring_addr: u64,
        recv_ring_rkey: u32,
        recv_ring_size: u64,
        initial_credit: u64,
    ) -> Self {
        Self {
            qp_number,
            packet_sequence_number,
            local_identifier,
            _padding: 0,
            recv_ring_addr,
            recv_ring_rkey,
            _padding2: 0,
            recv_ring_size,
            initial_credit,
        }
    }

    pub fn to_bytes(self) -> Vec<u8> {
        unsafe {
            std::slice::from_raw_parts(&self as *const Self as *const u8, CONNECTION_INFO_SIZE)
                .to_vec()
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        assert!(bytes.len() >= CONNECTION_INFO_SIZE);
        unsafe {
            let mut info = Self::default();
            std::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                &mut info as *mut Self as *mut u8,
                CONNECTION_INFO_SIZE,
            );
            info
        }
    }
}

pub fn auto_adjust_ring_size(
    cli_ring_size: usize,
    inflight_per_endpoint: usize,
    rank: u32,
) -> usize {
    const CREDIT_PER_CALL: usize = 64;
    let min_ring = (4 * inflight_per_endpoint * CREDIT_PER_CALL).next_power_of_two();
    let ring_size = cli_ring_size.max(min_ring);
    if ring_size != cli_ring_size {
        eprintln!(
            "[rank {rank}] copyrpc ring_size auto-adjusted: {cli_ring_size} -> {ring_size} \
             (inflight/ep={inflight_per_endpoint})"
        );
    }
    ring_size
}

pub struct CopyrpcDcSetup {
    pub local_info_tx: std::sync::mpsc::Sender<Vec<DcEndpointConnectionInfo>>,
    pub remote_info_rx: std::sync::mpsc::Receiver<Vec<DcEndpointConnectionInfo>>,
    pub device_index: usize,
    pub port: u8,
    pub ring_size: usize,
    pub my_remote_ranks: Vec<u32>,
}

// === Main daemon entry point ===

#[allow(clippy::too_many_arguments)]
pub fn run_daemon(
    daemon_id: usize,
    my_rank: u32,
    num_daemons: usize,
    key_range: u64,
    shm_server: ShmServer,
    mut flux: DaemonFlux,
    copyrpc_setup: Option<CopyrpcDcSetup>,
    stop_flag: &AtomicBool,
    ready_barrier: &std::sync::Barrier,
) {
    let mut store = ShardedStore::new(key_range, num_daemons as u64, daemon_id as u64);

    // Setup copyrpc (DC transport)
    let copyrpc_ctx: Option<Box<CopyrpcCtx>>;
    let mut copyrpc_endpoints: Vec<copyrpc::dc::Endpoint<SlotPtrs>> = Vec::new();
    let mut my_remote_ranks: Vec<u32> = Vec::new();

    if let Some(setup) = copyrpc_setup {
        let ctx = Box::new(
            copyrpc::dc::ContextBuilder::new()
                .device_index(setup.device_index)
                .port(setup.port)
                .dci_config(mlx5::dc::DciConfig {
                    max_send_wr: 256,
                    max_send_sge: 1,
                    max_inline_data: 256,
                })
                .srq_config(mlx5::srq::SrqConfig {
                    max_wr: 16384,
                    max_sge: 1,
                })
                .cq_size(4096)
                .build()
                .expect("Failed to create copyrpc dc context"),
        );

        let ep_config = copyrpc::dc::EndpointConfig {
            send_ring_size: setup.ring_size,
            recv_ring_size: setup.ring_size,
        };

        my_remote_ranks = setup.my_remote_ranks;
        let num_endpoints = my_remote_ranks.len();

        let mut local_infos = Vec::with_capacity(num_endpoints);
        for _ in 0..num_endpoints {
            let ep = ctx
                .create_endpoint(&ep_config)
                .expect("Failed to create copyrpc dc endpoint");
            let (info, lid, _) = ep.local_info(ctx.lid(), ctx.port());
            local_infos.push(DcEndpointConnectionInfo::new(
                info.dct_number,
                lid,
                info.dc_key,
                info.recv_ring_addr,
                info.recv_ring_rkey,
                info.endpoint_id,
                info.recv_ring_size,
                info.initial_credit,
            ));
            copyrpc_endpoints.push(ep);
        }

        setup
            .local_info_tx
            .send(local_infos)
            .expect("Failed to send copyrpc local info");

        let remote_infos = setup
            .remote_info_rx
            .recv()
            .expect("Failed to receive copyrpc remote info");

        for (i, ep) in copyrpc_endpoints.iter_mut().enumerate() {
            let r = &remote_infos[i];
            ep.connect(
                &copyrpc::dc::RemoteEndpointInfo {
                    dct_number: r.dct_number,
                    dc_key: r.dc_key,
                    local_identifier: r.local_identifier,
                    recv_ring_addr: r.recv_ring_addr,
                    recv_ring_rkey: r.recv_ring_rkey,
                    recv_ring_size: r.recv_ring_size,
                    initial_credit: r.initial_credit,
                    endpoint_id: r.endpoint_id,
                },
                0,
                ctx.port(),
            )
            .expect("Failed to connect copyrpc endpoint");
        }

        copyrpc_ctx = Some(ctx);
    } else {
        copyrpc_ctx = None;
    }

    ready_barrier.wait();

    let ctx_ref = copyrpc_ctx.as_deref();

    let mut rank_to_ep_index: FastMap<usize> = FastMap::new();
    for (i, &rank) in my_remote_ranks.iter().enumerate() {
        rank_to_ep_index.insert(rank, i);
    }

    let num_daemons_u64 = num_daemons as u64;

    // Pending incoming copyrpc requests waiting for Flux response
    let mut pending_copyrpc_handles: Vec<Option<copyrpc::dc::RecvHandle<'_, SlotPtrs>>> =
        Vec::new();
    let mut free_slots: Vec<usize> = Vec::new();

    // Ground queue: indices of client slots NOT currently inflight over copyrpc
    let max_clients = shm_server.max_clients() as usize;
    let slots: Vec<ShmSlot> = (0..max_clients)
        .map(|cid| shm_server.slot(cid as u32))
        .collect();
    let mut last_req_seq: Vec<u64> = vec![0; max_clients];
    let mut ground_queue: VecDeque<usize> = VecDeque::with_capacity(max_clients);
    let mut connected_count = 0usize;

    // Raw pointers for callback access without borrow conflicts.
    let ground_queue_ptr = std::ptr::addr_of_mut!(ground_queue);

    // === Metrics instrumentation ===
    const SAMPLE_MASK: u64 = 4095; // sample every 4096 iterations
    let mut loop_counter: u64 = 0;
    let mut loop_time_samples: Vec<u64> = Vec::with_capacity(16384);
    let mut loop_start = fastant::Instant::now();

    // Event counters (cumulative, recorded every SAMPLE_MASK iterations)
    let mut total_shm_req: u64 = 0;
    let mut total_shm_local: u64 = 0;
    let mut total_shm_remote: u64 = 0;
    let mut total_cqe_resp: u64 = 0;
    let mut total_incoming_req: u64 = 0;
    let mut total_flush: u64 = 0;
    let mut total_ringfull_retry: u64 = 0;

    // Per-sample event snapshots (for computing events/loop)
    let mut prev_shm_req: u64 = 0;
    let mut prev_shm_local: u64 = 0;
    let mut prev_shm_remote: u64 = 0;
    let mut prev_cqe_resp: u64 = 0;
    let mut prev_incoming_req: u64 = 0;
    let mut prev_flush: u64 = 0;

    // Phase timing samples (ns per sampled iteration)
    let mut phase_samples: Vec<PhaseSample> = Vec::with_capacity(16384);

    // Per-sample event rate snapshots
    let mut event_samples: Vec<EventSample> = Vec::with_capacity(16384);

    // Copyrpc round-trip time measurement
    let mut send_timestamps: Vec<fastant::Instant> = vec![fastant::Instant::now(); max_clients];
    let send_timestamps_ptr = send_timestamps.as_mut_ptr();
    let mut flush_timestamps: Vec<fastant::Instant> = vec![fastant::Instant::now(); max_clients];
    let flush_timestamps_ptr = flush_timestamps.as_mut_ptr();
    let mut rtt_hist = [0u64; RTT_HIST_BUCKETS];
    let mut rtt_sum: u64 = 0;
    let mut rtt_count: u64 = 0;
    let mut rtt_max: u64 = 0;
    let rtt_hist_ptr = rtt_hist.as_mut_ptr();
    let rtt_sum_ptr = std::ptr::addr_of_mut!(rtt_sum);
    let rtt_count_ptr = std::ptr::addr_of_mut!(rtt_count);
    let rtt_max_ptr = std::ptr::addr_of_mut!(rtt_max);

    // RTT sub-phase: call→flush and flush→callback histograms
    let mut call_to_flush_hist = [0u64; RTT_HIST_BUCKETS];
    let call_to_flush_hist_ptr = call_to_flush_hist.as_mut_ptr();
    let mut flush_to_cb_hist = [0u64; RTT_HIST_BUCKETS];
    let flush_to_cb_hist_ptr = flush_to_cb_hist.as_mut_ptr();

    // Track which slots need flush timestamp update
    let mut pending_flush_slots: Vec<usize> = Vec::with_capacity(32);

    // Incoming request turnaround: CQ detect → reply flush
    let mut incoming_turnaround_hist = [0u64; RTT_HIST_BUCKETS];

    // Ground queue size samples
    let mut gq_size_samples: Vec<u64> = Vec::with_capacity(16384);

    while !stop_flag.load(std::sync::atomic::Ordering::Relaxed) {
        // Discover newly connected clients
        if connected_count < max_clients {
            let new_count = shm_server.connected_count() as usize;
            while connected_count < new_count && connected_count < max_clients {
                ground_queue.push_back(connected_count);
                connected_count += 1;
            }
        }

        let is_sample = (loop_counter & SAMPLE_MASK) == 0 && loop_counter > 0;

        // --- Prefetch: Phase 2 用の SHM slot を先行ロード ---
        #[cfg(target_arch = "x86_64")]
        {
            let prefetch_count = ground_queue.len().min(4);
            for i in 0..prefetch_count {
                let slot_idx = ground_queue[i];
                unsafe {
                    std::arch::x86_64::_mm_prefetch(
                        slots[slot_idx].req as *const i8,
                        std::arch::x86_64::_MM_HINT_T0,
                    );
                }
            }
        }

        // === Phase 1: Poll recv CQ (no flush — separated to Phase 3) ===
        let incoming_before = total_incoming_req;
        let iter_start = fastant::Instant::now();
        let phase1_start = if is_sample { Some(iter_start) } else { None };
        if let Some(ctx) = ctx_ref {
            let cqe_count = ctx.poll_recv_counted(|ptrs: SlotPtrs, data: &[u8]| unsafe {
                // Copyrpc RTT measurement
                let now = fastant::Instant::now();
                let sidx = ptrs.slot_idx as usize;
                let ts = &*send_timestamps_ptr.add(sidx);
                let rtt_ns = now.duration_since(*ts).as_nanos() as u64;
                if rtt_ns < 1_000_000_000 {
                    *rtt_sum_ptr += rtt_ns;
                    *rtt_count_ptr += 1;
                    if rtt_ns > *rtt_max_ptr {
                        *rtt_max_ptr = rtt_ns;
                    }
                    let bucket =
                        (rtt_ns / RTT_HIST_BUCKET_NS).min(RTT_HIST_BUCKETS as u64 - 1) as usize;
                    *rtt_hist_ptr.add(bucket) += 1;

                    // Sub-phase: call→flush and flush→callback
                    let flush_ts = &*flush_timestamps_ptr.add(sidx);
                    let c2f_ns = flush_ts.duration_since(*ts).as_nanos() as u64;
                    let f2cb_ns = now.duration_since(*flush_ts).as_nanos() as u64;
                    let c2f_bucket =
                        (c2f_ns / RTT_HIST_BUCKET_NS).min(RTT_HIST_BUCKETS as u64 - 1) as usize;
                    *call_to_flush_hist_ptr.add(c2f_bucket) += 1;
                    let f2cb_bucket =
                        (f2cb_ns / RTT_HIST_BUCKET_NS).min(RTT_HIST_BUCKETS as u64 - 1) as usize;
                    *flush_to_cb_hist_ptr.add(f2cb_bucket) += 1;
                }

                let req_seq = slot::slot_read_seq(ptrs.req);
                let resp = RemoteResponse::from_bytes(data).response;
                slot::slot_write(ptrs.resp, req_seq, resp.as_bytes());
                (*ground_queue_ptr).push_back(sidx);
            });
            total_cqe_resp += cqe_count as u64;
        }

        // === Phase 1b: Handle incoming remote requests ===
        let phase1b_start = if is_sample { Some(fastant::Instant::now()) } else { None };
        if let Some(ctx) = ctx_ref {
            while let Some(recv_handle) = ctx.recv() {
                total_incoming_req += 1;

                let req = RemoteRequest::from_bytes(recv_handle.data()).request;
                let target_daemon = ShardedStore::owner_of(req.key(), num_daemons_u64) as usize;

                if target_daemon == daemon_id {
                    let resp = handle_local(&mut store, &req);
                    let remote_resp = RemoteResponse { response: resp };
                    loop {
                        match recv_handle.reply(remote_resp.as_bytes()) {
                            Ok(()) => break,
                            Err(copyrpc::error::Error::RingFull) => {
                                if stop_flag.load(std::sync::atomic::Ordering::Relaxed) {
                                    break;
                                }
                                ctx.poll(|ptrs: SlotPtrs, data: &[u8]| unsafe {
                                    let req_seq = slot::slot_read_seq(ptrs.req);
                                    let resp2 = RemoteResponse::from_bytes(data).response;
                                    slot::slot_write(ptrs.resp, req_seq, resp2.as_bytes());
                                    (*ground_queue_ptr).push_back(ptrs.slot_idx as usize);
                                });
                            }
                            Err(_) => break,
                        }
                    }
                } else {
                    // Forward to owning daemon via Flux
                    let idx = if let Some(i) = free_slots.pop() {
                        pending_copyrpc_handles[i] = Some(recv_handle);
                        i
                    } else {
                        let i = pending_copyrpc_handles.len();
                        pending_copyrpc_handles.push(Some(recv_handle));
                        i
                    };
                    let _ = flux.call(target_daemon, DelegatePayload::Req(req), idx);
                }
            }
        }

        // --- Prefetch: 次の Phase 1 用の CQ entry を先行ロード ---
        if let Some(ctx) = ctx_ref {
            ctx.prefetch_recv_cqe();
        }

        // === Phase 2: Ground Queue Scan ===
        let phase2_start = if is_sample { Some(fastant::Instant::now()) } else { None };
        let scan_len = ground_queue.len();
        for _ in 0..scan_len {
            let slot_idx = ground_queue.pop_front().unwrap();
            let s = &slots[slot_idx];

            let req_seq = unsafe { slot::slot_read_seq(s.req) };
            if req_seq <= last_req_seq[slot_idx] {
                ground_queue.push_back(slot_idx);
                continue;
            }
            last_req_seq[slot_idx] = req_seq;
            total_shm_req += 1;

            let payload = unsafe { slot::slot_read_payload(s.req) };
            let req = Request::from_bytes(payload);

            let target_rank = req.rank();
            if target_rank == my_rank {
                total_shm_local += 1;
                let resp = handle_local(&mut store, &req);
                unsafe { slot::slot_write(s.resp, req_seq, resp.as_bytes()) };
                ground_queue.push_back(slot_idx);
                continue;
            }

            total_shm_remote += 1;

            let Some(ctx) = ctx_ref else {
                // No copyrpc: respond with not-found
                let resp = Response::AggGetNotFound;
                unsafe { slot::slot_write(s.resp, req_seq, resp.as_bytes()) };
                ground_queue.push_back(slot_idx);
                continue;
            };

            let ep_idx = *rank_to_ep_index
                .get(target_rank)
                .expect("target_rank not in my_remote_ranks");
            let ptrs = SlotPtrs {
                req: s.req,
                resp: s.resp,
                slot_idx: slot_idx as u32,
            };
            let remote_req = RemoteRequest { request: req };
            match copyrpc_endpoints[ep_idx].call(remote_req.as_bytes(), ptrs, 0) {
                Ok(_) => {
                    unsafe {
                        *send_timestamps_ptr.add(slot_idx) = fastant::Instant::now();
                    }
                    pending_flush_slots.push(slot_idx);
                }
                Err(copyrpc::error::CallError::RingFull(_))
                | Err(copyrpc::error::CallError::InsufficientCredit(_)) => {
                    total_ringfull_retry += 1;
                    // Rollback: allow re-read next iteration
                    last_req_seq[slot_idx] = req_seq - 1;
                    ground_queue.push_back(slot_idx);
                    ctx.flush_endpoints();
                }
                Err(copyrpc::error::CallError::Other(_)) => {
                    let resp = Response::AggGetNotFound;
                    unsafe { slot::slot_write(s.resp, req_seq, resp.as_bytes()) };
                    ground_queue.push_back(slot_idx);
                }
            }
        }

        // Flux processing (only needed with multiple daemons per node)
        if num_daemons > 1 {
            let mut ready_flux_responses: Vec<(usize, Response)> = Vec::new();
            flux.poll(|pending_idx: usize, data: DelegatePayload| {
                if let DelegatePayload::Resp(resp) = data {
                    ready_flux_responses.push((pending_idx, resp));
                }
            });

            for (pending_idx, resp) in ready_flux_responses.drain(..) {
                let Some(recv_handle) = pending_copyrpc_handles[pending_idx].as_ref() else {
                    continue;
                };
                let remote_resp = RemoteResponse { response: resp };
                let mut done = false;
                loop {
                    match recv_handle.reply(remote_resp.as_bytes()) {
                        Ok(()) => {
                            done = true;
                            break;
                        }
                        Err(copyrpc::error::Error::RingFull) => {
                            if stop_flag.load(std::sync::atomic::Ordering::Relaxed) {
                                break;
                            }
                            if let Some(ctx) = ctx_ref {
                                ctx.poll(|ptrs: SlotPtrs, data: &[u8]| unsafe {
                                    let req_seq = slot::slot_read_seq(ptrs.req);
                                    let resp2 = RemoteResponse::from_bytes(data).response;
                                    slot::slot_write(ptrs.resp, req_seq, resp2.as_bytes());
                                    (*ground_queue_ptr).push_back(ptrs.slot_idx as usize);
                                });
                            } else {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
                if done {
                    pending_copyrpc_handles[pending_idx] = None;
                    free_slots.push(pending_idx);
                }
            }

            while let Some((_from, flux_token, payload)) = flux.try_recv_raw() {
                if let DelegatePayload::Req(req) = payload {
                    let resp = handle_local(&mut store, &req);
                    flux.reply(flux_token, DelegatePayload::Resp(resp));
                }
            }
        }

        // === Phase 3: Flush dirty endpoints ===
        let phase3_start = if is_sample { Some(fastant::Instant::now()) } else { None };
        if let Some(ctx) = ctx_ref {
            if ctx.has_dirty_endpoints() {
                total_flush += 1;
                ctx.flush_endpoints();
                // Record flush timestamp for all slots pending since last flush
                let flush_now = fastant::Instant::now();
                for &sidx in &pending_flush_slots {
                    unsafe {
                        *flush_timestamps_ptr.add(sidx) = flush_now;
                    }
                }
                pending_flush_slots.clear();

                // Record incoming turnaround for iterations that processed incoming requests
                if total_incoming_req > incoming_before {
                    let turnaround_ns = flush_now.duration_since(iter_start).as_nanos() as u64;
                    if turnaround_ns < 1_000_000_000 {
                        let bucket = (turnaround_ns / RTT_HIST_BUCKET_NS)
                            .min(RTT_HIST_BUCKETS as u64 - 1) as usize;
                        incoming_turnaround_hist[bucket] += 1;
                    }
                }
            }
        }

        // === Sampling: record metrics every SAMPLE_MASK iterations ===
        if is_sample {
            let now = fastant::Instant::now();
            let elapsed_ns = now.duration_since(loop_start).as_nanos() as u64;
            let per_iter_ns = elapsed_ns / (SAMPLE_MASK + 1);
            loop_time_samples.push(per_iter_ns);
            loop_start = now;

            // Phase timing for this sampled iteration
            if let (Some(p1), Some(p1b), Some(p2), Some(p3)) =
                (phase1_start, phase1b_start, phase2_start, phase3_start)
            {
                phase_samples.push(PhaseSample {
                    cq_poll_ns: p1b.duration_since(p1).as_nanos() as u64,
                    incoming_ns: p2.duration_since(p1b).as_nanos() as u64,
                    gq_scan_ns: p3.duration_since(p2).as_nanos() as u64,
                    flush_ns: now.duration_since(p3).as_nanos() as u64,
                });
            }

            // Event rate snapshot
            let interval = SAMPLE_MASK + 1;
            event_samples.push(EventSample {
                shm_req: total_shm_req - prev_shm_req,
                shm_local: total_shm_local - prev_shm_local,
                shm_remote: total_shm_remote - prev_shm_remote,
                cqe_resp: total_cqe_resp - prev_cqe_resp,
                incoming_req: total_incoming_req - prev_incoming_req,
                flush: total_flush - prev_flush,
                loops: interval,
            });
            prev_shm_req = total_shm_req;
            prev_shm_local = total_shm_local;
            prev_shm_remote = total_shm_remote;
            prev_cqe_resp = total_cqe_resp;
            prev_incoming_req = total_incoming_req;
            prev_flush = total_flush;

            // Ground queue size
            gq_size_samples.push(ground_queue.len() as u64);
        }
        loop_counter += 1;
    }

    // === Print comprehensive metrics at shutdown ===
    print_daemon_metrics(
        daemon_id,
        loop_counter,
        total_shm_req,
        total_shm_local,
        total_shm_remote,
        total_cqe_resp,
        total_incoming_req,
        total_flush,
        total_ringfull_retry,
        &loop_time_samples,
        &phase_samples,
        &event_samples,
        &rtt_hist,
        rtt_sum,
        rtt_count,
        rtt_max,
        &gq_size_samples,
        &call_to_flush_hist,
        &flush_to_cb_hist,
        &incoming_turnaround_hist,
    );
}

fn percentile(sorted: &[u64], pct: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = (sorted.len() * pct / 100).min(sorted.len() - 1);
    sorted[idx]
}

fn hist_percentile(hist: &[u64], total: u64, pct: u64, bucket_size_ns: u64) -> u64 {
    let target = total * pct / 100;
    let mut cumulative = 0u64;
    for (i, &count) in hist.iter().enumerate() {
        cumulative += count;
        if cumulative >= target {
            return i as u64 * bucket_size_ns;
        }
    }
    (hist.len() as u64 - 1) * bucket_size_ns
}

#[allow(clippy::too_many_arguments)]
fn print_daemon_metrics(
    daemon_id: usize,
    loop_counter: u64,
    total_shm_req: u64,
    total_shm_local: u64,
    total_shm_remote: u64,
    total_cqe_resp: u64,
    total_incoming_req: u64,
    total_flush: u64,
    total_ringfull_retry: u64,
    loop_time_samples: &[u64],
    phase_samples: &[PhaseSample],
    event_samples: &[EventSample],
    rtt_hist: &[u64; RTT_HIST_BUCKETS],
    rtt_sum: u64,
    rtt_count: u64,
    rtt_max: u64,
    gq_size_samples: &[u64],
    call_to_flush_hist: &[u64; RTT_HIST_BUCKETS],
    flush_to_cb_hist: &[u64; RTT_HIST_BUCKETS],
    incoming_turnaround_hist: &[u64; RTT_HIST_BUCKETS],
) {
    let n_samples = loop_time_samples.len();
    eprintln!(
        "[daemon-{daemon_id}] metrics ({loop_counter} loops, {n_samples} samples):"
    );

    // Cumulative event totals
    let events_per_loop = |total: u64| -> f64 {
        if loop_counter > 0 {
            total as f64 / loop_counter as f64
        } else {
            0.0
        }
    };
    eprintln!(
        "  events/loop: shm_req={:.4} local={:.4} remote={:.4} cqe_resp={:.4} \
         incoming={:.4} flush={:.4} ringfull={:.6}",
        events_per_loop(total_shm_req),
        events_per_loop(total_shm_local),
        events_per_loop(total_shm_remote),
        events_per_loop(total_cqe_resp),
        events_per_loop(total_incoming_req),
        events_per_loop(total_flush),
        events_per_loop(total_ringfull_retry),
    );
    eprintln!(
        "  totals: shm_req={total_shm_req} local={total_shm_local} remote={total_shm_remote} \
         cqe_resp={total_cqe_resp} incoming={total_incoming_req} flush={total_flush} \
         ringfull={total_ringfull_retry}",
    );

    // Loop time distribution
    if !loop_time_samples.is_empty() {
        let mut sorted = loop_time_samples.to_vec();
        sorted.sort_unstable();
        let sum: u64 = sorted.iter().sum();
        let mean = sum / sorted.len() as u64;
        eprintln!(
            "  loop_ns (p50/p90/p99/max): {}/{}/{}/{} mean={}",
            percentile(&sorted, 50),
            percentile(&sorted, 90),
            percentile(&sorted, 99),
            sorted.last().copied().unwrap_or(0),
            mean,
        );

        // Histogram with logarithmic buckets
        let buckets = [50, 100, 200, 500, 1000, 2000, 5000, 10000, 50000, 100000, u64::MAX];
        let labels = [
            "<=50ns", "<=100ns", "<=200ns", "<=500ns", "<=1us", "<=2us",
            "<=5us", "<=10us", "<=50us", "<=100us", ">100us",
        ];
        let mut counts = vec![0usize; buckets.len()];
        for &v in &sorted {
            for (i, &b) in buckets.iter().enumerate() {
                if v <= b {
                    counts[i] += 1;
                    break;
                }
            }
        }
        eprint!("  loop_histogram:");
        for (i, &c) in counts.iter().enumerate() {
            if c > 0 {
                eprint!(" {}={}", labels[i], c);
            }
        }
        eprintln!();
    }

    // Phase timing distribution
    if !phase_samples.is_empty() {
        let mut cq: Vec<u64> = phase_samples.iter().map(|s| s.cq_poll_ns).collect();
        let mut inc: Vec<u64> = phase_samples.iter().map(|s| s.incoming_ns).collect();
        let mut gq: Vec<u64> = phase_samples.iter().map(|s| s.gq_scan_ns).collect();
        let mut fl: Vec<u64> = phase_samples.iter().map(|s| s.flush_ns).collect();
        cq.sort_unstable();
        inc.sort_unstable();
        gq.sort_unstable();
        fl.sort_unstable();
        eprintln!(
            "  phase_ns cq_poll  (p50/p90/p99): {}/{}/{}",
            percentile(&cq, 50), percentile(&cq, 90), percentile(&cq, 99),
        );
        eprintln!(
            "  phase_ns incoming (p50/p90/p99): {}/{}/{}",
            percentile(&inc, 50), percentile(&inc, 90), percentile(&inc, 99),
        );
        eprintln!(
            "  phase_ns gq_scan  (p50/p90/p99): {}/{}/{}",
            percentile(&gq, 50), percentile(&gq, 90), percentile(&gq, 99),
        );
        eprintln!(
            "  phase_ns flush    (p50/p90/p99): {}/{}/{}",
            percentile(&fl, 50), percentile(&fl, 90), percentile(&fl, 99),
        );
    }

    // Event rate per sample window
    if !event_samples.is_empty() {
        let mut req_rates: Vec<f64> = event_samples
            .iter()
            .map(|s| s.shm_req as f64 / s.loops as f64)
            .collect();
        let mut resp_rates: Vec<f64> = event_samples
            .iter()
            .map(|s| s.cqe_resp as f64 / s.loops as f64)
            .collect();
        req_rates.sort_by(|a, b| a.partial_cmp(b).unwrap());
        resp_rates.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let req_p50 = req_rates[req_rates.len() / 2];
        let resp_p50 = resp_rates[resp_rates.len() / 2];
        eprintln!(
            "  event_rate/loop (p50): shm_req={:.4} cqe_resp={:.4}",
            req_p50, resp_p50,
        );
    }

    // Copyrpc RTT statistics
    if rtt_count > 0 {
        let rtt_mean = rtt_sum / rtt_count;
        let p50 = hist_percentile(rtt_hist, rtt_count, 50, RTT_HIST_BUCKET_NS);
        let p90 = hist_percentile(rtt_hist, rtt_count, 90, RTT_HIST_BUCKET_NS);
        let p99 = hist_percentile(rtt_hist, rtt_count, 99, RTT_HIST_BUCKET_NS);
        eprintln!(
            "  copyrpc_rtt_ns (count={rtt_count}): mean={rtt_mean} p50={p50} p90={p90} p99={p99} max={rtt_max}",
        );
        eprintln!(
            "  copyrpc_rtt_us: mean={:.2} p50={:.2} p90={:.2} p99={:.2} max={:.2}",
            rtt_mean as f64 / 1000.0,
            p50 as f64 / 1000.0,
            p90 as f64 / 1000.0,
            p99 as f64 / 1000.0,
            rtt_max as f64 / 1000.0,
        );

        // RTT histogram: aggregate into 1μs buckets for readable output
        eprint!("  rtt_hist_us:");
        let us_buckets = RTT_HIST_BUCKETS * RTT_HIST_BUCKET_NS as usize / 1000;
        for us in 0..us_buckets {
            let lo_bucket = us * 1000 / RTT_HIST_BUCKET_NS as usize;
            let hi_bucket = ((us + 1) * 1000 / RTT_HIST_BUCKET_NS as usize).min(RTT_HIST_BUCKETS);
            let count: u64 = rtt_hist[lo_bucket..hi_bucket].iter().sum();
            if count > 0 {
                eprint!(" {}us={}", us, count);
            }
        }
        eprintln!();
    }

    // RTT sub-phase: call→flush histogram
    {
        let c2f_total: u64 = call_to_flush_hist.iter().sum();
        if c2f_total > 0 {
            let c2f_p50 = hist_percentile(call_to_flush_hist, c2f_total, 50, RTT_HIST_BUCKET_NS);
            let c2f_p90 = hist_percentile(call_to_flush_hist, c2f_total, 90, RTT_HIST_BUCKET_NS);
            let c2f_p99 = hist_percentile(call_to_flush_hist, c2f_total, 99, RTT_HIST_BUCKET_NS);
            eprintln!(
                "  call_to_flush_ns (count={c2f_total}): p50={c2f_p50} p90={c2f_p90} p99={c2f_p99}",
            );
            eprintln!(
                "  call_to_flush_us: p50={:.2} p90={:.2} p99={:.2}",
                c2f_p50 as f64 / 1000.0,
                c2f_p90 as f64 / 1000.0,
                c2f_p99 as f64 / 1000.0,
            );
            eprint!("  call_to_flush_hist_us:");
            let us_buckets = RTT_HIST_BUCKETS * RTT_HIST_BUCKET_NS as usize / 1000;
            for us in 0..us_buckets {
                let lo = us * 1000 / RTT_HIST_BUCKET_NS as usize;
                let hi = ((us + 1) * 1000 / RTT_HIST_BUCKET_NS as usize).min(RTT_HIST_BUCKETS);
                let count: u64 = call_to_flush_hist[lo..hi].iter().sum();
                if count > 0 {
                    eprint!(" {}us={}", us, count);
                }
            }
            eprintln!();
        }
    }

    // RTT sub-phase: flush→callback histogram
    {
        let f2cb_total: u64 = flush_to_cb_hist.iter().sum();
        if f2cb_total > 0 {
            let f2cb_p50 = hist_percentile(flush_to_cb_hist, f2cb_total, 50, RTT_HIST_BUCKET_NS);
            let f2cb_p90 = hist_percentile(flush_to_cb_hist, f2cb_total, 90, RTT_HIST_BUCKET_NS);
            let f2cb_p99 = hist_percentile(flush_to_cb_hist, f2cb_total, 99, RTT_HIST_BUCKET_NS);
            eprintln!(
                "  flush_to_cb_ns (count={f2cb_total}): p50={f2cb_p50} p90={f2cb_p90} p99={f2cb_p99}",
            );
            eprintln!(
                "  flush_to_cb_us: p50={:.2} p90={:.2} p99={:.2}",
                f2cb_p50 as f64 / 1000.0,
                f2cb_p90 as f64 / 1000.0,
                f2cb_p99 as f64 / 1000.0,
            );
            eprint!("  flush_to_cb_hist_us:");
            let us_buckets = RTT_HIST_BUCKETS * RTT_HIST_BUCKET_NS as usize / 1000;
            for us in 0..us_buckets {
                let lo = us * 1000 / RTT_HIST_BUCKET_NS as usize;
                let hi = ((us + 1) * 1000 / RTT_HIST_BUCKET_NS as usize).min(RTT_HIST_BUCKETS);
                let count: u64 = flush_to_cb_hist[lo..hi].iter().sum();
                if count > 0 {
                    eprint!(" {}us={}", us, count);
                }
            }
            eprintln!();
        }
    }

    // Incoming turnaround: CQ detect → reply flush
    {
        let inc_total: u64 = incoming_turnaround_hist.iter().sum();
        if inc_total > 0 {
            let inc_p50 =
                hist_percentile(incoming_turnaround_hist, inc_total, 50, RTT_HIST_BUCKET_NS);
            let inc_p90 =
                hist_percentile(incoming_turnaround_hist, inc_total, 90, RTT_HIST_BUCKET_NS);
            let inc_p99 =
                hist_percentile(incoming_turnaround_hist, inc_total, 99, RTT_HIST_BUCKET_NS);
            eprintln!(
                "  incoming_turnaround_ns (count={inc_total}): p50={inc_p50} p90={inc_p90} p99={inc_p99}",
            );
            eprintln!(
                "  incoming_turnaround_us: p50={:.2} p90={:.2} p99={:.2}",
                inc_p50 as f64 / 1000.0,
                inc_p90 as f64 / 1000.0,
                inc_p99 as f64 / 1000.0,
            );
        }
    }

    // Ground queue size statistics
    if !gq_size_samples.is_empty() {
        let mut sorted_gq = gq_size_samples.to_vec();
        sorted_gq.sort_unstable();
        let gq_sum: u64 = sorted_gq.iter().sum();
        let gq_mean = gq_sum as f64 / sorted_gq.len() as f64;
        eprintln!(
            "  gq_size: mean={:.1} p50={} p90={} p99={} max={}",
            gq_mean,
            percentile(&sorted_gq, 50),
            percentile(&sorted_gq, 90),
            percentile(&sorted_gq, 99),
            sorted_gq.last().copied().unwrap_or(0),
        );
    }
}
