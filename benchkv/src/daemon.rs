use std::sync::atomic::{AtomicBool, Ordering};

use crate::message::*;
use crate::qd_sample::{QdCollector, QdSample};
use crate::storage::ShardedStore;

// === Type aliases ===

/// Flux token is now a `usize` index into pending copyrpc recv handle buffer.
pub type DaemonFlux = inproc::Flux<DelegatePayload, usize>;

type CopyrpcCtx = copyrpc::Context<ipc::RequestToken>;

// === Connection info for MPI exchange ===

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

// === Helpers ===

fn handle_local(store: &mut ShardedStore, req: &Request) -> Response {
    match *req {
        Request::MetaPut { key, value, .. } => {
            store.put(key, value);
            Response::MetaPutOk
        }
        Request::MetaGet { key, .. } => match store.get(key) {
            Some(v) => Response::MetaGetOk { value: v },
            None => Response::MetaGetNotFound,
        },
    }
}

// === copyrpc ring size auto-adjustment ===

/// Compute minimum copyrpc ring_size to avoid credit starvation.
///
/// Each inflight copyrpc call costs 64 bytes of credit
/// (padded_message_size(0) + FLOW_METADATA_SIZE = 32 + 32).
/// Initial credit = ring_size / 4, so:
///   min_ring_size = next_power_of_two(4 * inflight_per_endpoint * 64)
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

// === copyrpc setup ===

pub struct CopyrpcSetup {
    pub local_info_tx: std::sync::mpsc::Sender<Vec<EndpointConnectionInfo>>,
    pub remote_info_rx: std::sync::mpsc::Receiver<Vec<EndpointConnectionInfo>>,
    pub device_index: usize,
    pub port: u8,
    pub ring_size: usize,
    /// Remote ranks this daemon is responsible for (where rank % num_daemons == daemon_id).
    pub my_remote_ranks: Vec<u32>,
}

// === Main daemon entry point ===

#[allow(clippy::too_many_arguments)]
pub fn run_daemon(
    daemon_id: usize,
    my_rank: u32,
    num_daemons: usize,
    key_range: u64,
    queue_depth: u32,
    mut server: ipc::Server<Request, Response>,
    mut flux: DaemonFlux,
    copyrpc_setup: Option<CopyrpcSetup>,
    stop_flag: &AtomicBool,
    ready_barrier: &std::sync::Barrier,
    qd_sample_interval: Option<u32>,
) -> Vec<QdSample> {
    let mut store = ShardedStore::new(key_range, num_daemons as u64, daemon_id as u64);

    // Setup copyrpc
    let copyrpc_ctx: Option<CopyrpcCtx>;
    let mut copyrpc_endpoints: Vec<copyrpc::Endpoint<ipc::RequestToken>> = Vec::new();
    let mut my_remote_ranks: Vec<u32> = Vec::new();

    if let Some(setup) = copyrpc_setup {
        let ctx: CopyrpcCtx = copyrpc::ContextBuilder::new()
            .device_index(setup.device_index)
            .port(setup.port)
            .srq_config(mlx5::srq::SrqConfig {
                max_wr: 16384,
                max_sge: 1,
            })
            .cq_size(4096)
            .build()
            .expect("Failed to create copyrpc context");

        let ep_config = copyrpc::EndpointConfig {
            send_ring_size: setup.ring_size,
            recv_ring_size: setup.ring_size,
            ..Default::default()
        };

        my_remote_ranks = setup.my_remote_ranks;
        let num_endpoints = my_remote_ranks.len();
        let mut local_infos = Vec::with_capacity(num_endpoints);
        for _ in 0..num_endpoints {
            let ep = ctx
                .create_endpoint(&ep_config)
                .expect("Failed to create copyrpc endpoint");
            let (info, lid, _) = ep.local_info(ctx.lid(), ctx.port());
            local_infos.push(EndpointConnectionInfo::new(
                info.qp_number,
                0,
                lid,
                info.recv_ring_addr,
                info.recv_ring_rkey,
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
                &copyrpc::RemoteEndpointInfo {
                    qp_number: r.qp_number,
                    packet_sequence_number: r.packet_sequence_number,
                    local_identifier: r.local_identifier,
                    recv_ring_addr: r.recv_ring_addr,
                    recv_ring_rkey: r.recv_ring_rkey,
                    recv_ring_size: r.recv_ring_size,
                    initial_credit: r.initial_credit,
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

    // Borrow copyrpc_ctx for the entire event loop scope
    let ctx_ref = copyrpc_ctx.as_ref();

    // Map target_rank to endpoint index using my_remote_ranks ordering
    let rank_to_ep_index = |target_rank: u32| -> usize {
        my_remote_ranks
            .iter()
            .position(|&r| r == target_rank)
            .expect("target_rank not in my_remote_ranks")
    };

    let num_daemons_u64 = num_daemons as u64;

    // Pending copyrpc recv handles waiting for Flux response
    let mut pending_copyrpc_handles: Vec<Option<copyrpc::RecvHandle<'_, ipc::RequestToken>>> =
        Vec::new();
    let mut free_slots: Vec<usize> = Vec::new();

    // IPC poll control: always poll to avoid latency spikes at high NP.
    // Previous adaptive skip (IPC_POLL_INTERVAL=4) caused bistable throughput:
    // low copyrpc_resp → skip IPC → lower throughput feedback loop.

    // Per-client inflight tracking: skip polling clients at max QD
    let max_clients = server.max_clients() as usize;
    let mut inflight = vec![0u32; max_clients];
    let mut skip = vec![false; max_clients];

    // QD sampling
    let mut qd_collector = qd_sample_interval.map(|iv| QdCollector::new(iv, 500_000));
    let mut copyrpc_inflight_count: u32 = 0;

    // Buffer for nested ctx.poll() responses inside flux.poll() closure
    let mut nested_resp_buf: Vec<(ipc::RequestToken, Response)> = Vec::new();

    // Performance counters
    let perf_start = std::time::Instant::now();
    let mut perf_loop_count: u64 = 0;
    let mut perf_copyrpc_resp: u64 = 0;
    let mut perf_incoming_req: u64 = 0;
    let mut perf_ipc_req: u64 = 0;
    let mut perf_local_req: u64 = 0;
    let mut perf_ipc_poll_count: u64 = 0;

    // Latency sampling: copyrpc round-trip per request
    let mut lat_call_ts: Vec<std::time::Instant> = vec![perf_start; max_clients];
    let mut lat_call_ep: Vec<u16> = vec![0; max_clients];
    let mut lat_call_valid: Vec<bool> = vec![false; max_clients];
    let mut lat_flush_ts: Vec<std::time::Instant> = vec![perf_start; max_clients];
    let mut lat_flushed: Vec<bool> = vec![false; max_clients];
    let mut lat_samples_ns: Vec<u64> = Vec::with_capacity(1_000_000);
    let mut lat_samples_ep: Vec<u16> = Vec::with_capacity(1_000_000);
    let mut lat_local_ns: Vec<u64> = Vec::with_capacity(1_000_000);
    let mut lat_remote_ns: Vec<u64> = Vec::with_capacity(1_000_000);
    // Loop period distribution
    let mut loop_period_ns: Vec<u32> = Vec::with_capacity(1_000_000);
    let mut loop_prev_ts = perf_start;
    // ctx.poll() duration + incoming server latency
    let mut poll_duration_ns: Vec<u32> = Vec::with_capacity(1_000_000);
    let mut incoming_reply_pending = false;
    let mut incoming_reply_ts = perf_start;
    let mut incoming_server_lat_ns: Vec<u32> = Vec::with_capacity(100_000);
    // IPC turn-around: time from copyrpc response (server.reply) to next request pickup
    let mut ipc_reply_at: Vec<std::time::Instant> = vec![perf_start; max_clients];
    let mut ipc_reply_pending: Vec<bool> = vec![false; max_clients];
    let mut ipc_turnaround_ns: Vec<u32> = Vec::with_capacity(1_000_000);
    // Phase 2 timing
    let mut phase2_duration_ns: Vec<u32> = Vec::with_capacity(1_000_000);

    while !stop_flag.load(Ordering::Relaxed) {
        perf_loop_count += 1;
        // Loop period measurement
        {
            let loop_now = std::time::Instant::now();
            if loop_period_ns.len() < 1_000_000 {
                loop_period_ns.push((loop_now - loop_prev_ts).as_nanos() as u32);
            }
            loop_prev_ts = loop_now;
        }
        // === Phase 1: copyrpc poll + recv ===
        // Process RDMA completions first so ipc replies go out before ipc poll,
        // giving clients the best chance to submit new requests.
        let mut copyrpc_resp_count = 0u32;
        if let Some(ctx) = ctx_ref {
            let poll_start = std::time::Instant::now();
            ctx.poll(|token: ipc::RequestToken, data: &[u8]| {
                let resp = RemoteResponse::from_bytes(data).response;
                let cid = token.client_id().0 as usize;
                server.reply(token, resp);
                inflight[cid] -= 1;
                copyrpc_inflight_count -= 1;
                copyrpc_resp_count += 1;
                // IPC turn-around tracking
                ipc_reply_at[cid] = std::time::Instant::now();
                ipc_reply_pending[cid] = true;
                // Latency sampling (local/remote split)
                if lat_call_valid[cid] {
                    lat_call_valid[cid] = false;
                    if lat_samples_ns.len() < 1_000_000 {
                        let total_ns = lat_call_ts[cid].elapsed().as_nanos() as u64;
                        lat_samples_ns.push(total_ns);
                        lat_samples_ep.push(lat_call_ep[cid]);
                        if lat_flushed[cid] {
                            let local_ns = (lat_flush_ts[cid] - lat_call_ts[cid]).as_nanos() as u64;
                            lat_local_ns.push(local_ns);
                            lat_remote_ns.push(total_ns.saturating_sub(local_ns));
                        } else {
                            lat_local_ns.push(0);
                            lat_remote_ns.push(total_ns);
                        }
                    }
                    lat_flushed[cid] = false;
                }
            });

            // ctx.poll() duration + incoming server latency
            {
                let post_poll_ts = std::time::Instant::now();
                if poll_duration_ns.len() < 1_000_000 {
                    poll_duration_ns.push((post_poll_ts - poll_start).as_nanos() as u32);
                }
                if incoming_reply_pending {
                    incoming_reply_pending = false;
                    if incoming_server_lat_ns.len() < 100_000 {
                        incoming_server_lat_ns
                            .push((post_poll_ts - incoming_reply_ts).as_nanos() as u32);
                    }
                }
            }

            perf_copyrpc_resp += copyrpc_resp_count as u64;

            // Mark pending calls as flushed (ctx.poll step 4 did the flush)
            {
                let post_flush_ts = std::time::Instant::now();
                for cid in 0..max_clients {
                    if lat_call_valid[cid] && !lat_flushed[cid] {
                        lat_flush_ts[cid] = post_flush_ts;
                        lat_flushed[cid] = true;
                    }
                }
            }

            // copyrpc recv (incoming requests from remote nodes)
            while let Some(recv_handle) = ctx.recv() {
                let req = RemoteRequest::from_bytes(recv_handle.data()).request;
                let target_daemon = ShardedStore::owner_of(req.key(), num_daemons_u64) as usize;

                perf_incoming_req += 1;
                if target_daemon == daemon_id {
                    // This daemon owns the key → handle locally
                    let resp = handle_local(&mut store, &req);
                    let remote_resp = RemoteResponse { response: resp };
                    loop {
                        match recv_handle.reply(remote_resp.as_bytes()) {
                            Ok(()) => {
                                incoming_reply_pending = true;
                                incoming_reply_ts = std::time::Instant::now();
                                break;
                            }
                            Err(copyrpc::error::Error::RingFull) => {
                                ctx.poll(|token: ipc::RequestToken, data: &[u8]| {
                                    let resp = RemoteResponse::from_bytes(data).response;
                                    let cid = token.client_id().0 as usize;
                                    server.reply(token, resp);
                                    inflight[cid] -= 1;
                                    copyrpc_inflight_count -= 1;
                                    copyrpc_resp_count += 1;
                                });
                                continue;
                            }
                            Err(_) => break,
                        }
                    }
                } else {
                    // Different daemon owns the key → Flux forward
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

        // === Phase 2: ipc poll + recv + inline copyrpc send ===
        // Always poll IPC to minimize client turn-around latency.
        let phase2_start = std::time::Instant::now();
        {
            for i in 0..max_clients {
                skip[i] = inflight[i] >= queue_depth;
            }
            server.poll_with_skip(&skip);
            perf_ipc_poll_count += 1;
        }

        while let Some(handle) = server.recv() {
            perf_ipc_req += 1;
            let cid = handle.client_id().0 as usize;
            // IPC turn-around measurement
            if ipc_reply_pending[cid] {
                ipc_reply_pending[cid] = false;
                if ipc_turnaround_ns.len() < 1_000_000 {
                    ipc_turnaround_ns
                        .push((std::time::Instant::now() - ipc_reply_at[cid]).as_nanos() as u32);
                }
            }
            inflight[cid] += 1;
            let req = *handle.data();
            let target_rank = req.rank();

            if target_rank == my_rank {
                perf_local_req += 1;
                debug_assert_eq!(
                    ShardedStore::owner_of(req.key(), num_daemons_u64) as usize,
                    daemon_id
                );
                let resp = handle_local(&mut store, &req);
                handle.reply(resp);
                inflight[cid] -= 1;
            } else if let Some(ctx) = ctx_ref {
                let token = handle.into_token();
                let remote_req = RemoteRequest { request: req };
                let ep_idx = rank_to_ep_index(target_rank);
                let mut pending = token;
                loop {
                    match copyrpc_endpoints[ep_idx].call(remote_req.as_bytes(), pending, 0u64) {
                        Ok(_) => {
                            copyrpc_inflight_count += 1;
                            lat_call_ts[cid] = std::time::Instant::now();
                            lat_call_ep[cid] = ep_idx as u16;
                            lat_call_valid[cid] = true;
                            lat_flushed[cid] = false;
                            break;
                        }
                        Err(copyrpc::error::CallError::RingFull(returned))
                        | Err(copyrpc::error::CallError::InsufficientCredit(returned)) => {
                            pending = returned;
                            ctx.poll(|token: ipc::RequestToken, data: &[u8]| {
                                let resp = RemoteResponse::from_bytes(data).response;
                                let cid = token.client_id().0 as usize;
                                server.reply(token, resp);
                                inflight[cid] -= 1;
                                copyrpc_inflight_count -= 1;
                                if lat_call_valid[cid] {
                                    lat_call_valid[cid] = false;
                                    if lat_samples_ns.len() < 1_000_000 {
                                        let total_ns = lat_call_ts[cid].elapsed().as_nanos() as u64;
                                        lat_samples_ns.push(total_ns);
                                        lat_samples_ep.push(lat_call_ep[cid]);
                                        if lat_flushed[cid] {
                                            let local_ns = (lat_flush_ts[cid] - lat_call_ts[cid])
                                                .as_nanos()
                                                as u64;
                                            lat_local_ns.push(local_ns);
                                            lat_remote_ns.push(total_ns.saturating_sub(local_ns));
                                        } else {
                                            lat_local_ns.push(0);
                                            lat_remote_ns.push(total_ns);
                                        }
                                    }
                                    lat_flushed[cid] = false;
                                }
                            });
                            continue;
                        }
                        Err(copyrpc::error::CallError::Other(_)) => break,
                    }
                }
            }
        }

        // Phase 2 duration
        if phase2_duration_ns.len() < 1_000_000 {
            phase2_duration_ns.push((std::time::Instant::now() - phase2_start).as_nanos() as u32);
        }

        // === QD sample point ===
        if let Some(ref mut c) = qd_collector {
            c.tick(
                copyrpc_inflight_count,
                flux.pending_count() as u32,
                inflight.iter().sum(),
                0,
            );
        }

        // === Phase 3: Flux poll + recv ===
        flux.poll(|pending_idx: usize, data: DelegatePayload| {
            if let DelegatePayload::Resp(resp) = data {
                if let Some(recv_handle) = pending_copyrpc_handles[pending_idx].take() {
                    free_slots.push(pending_idx);
                    let remote_resp = RemoteResponse { response: resp };
                    if let Some(ctx) = ctx_ref {
                        loop {
                            match recv_handle.reply(remote_resp.as_bytes()) {
                                Ok(()) => break,
                                Err(copyrpc::error::Error::RingFull) => {
                                    ctx.poll(|token: ipc::RequestToken, data: &[u8]| {
                                        let r = RemoteResponse::from_bytes(data).response;
                                        nested_resp_buf.push((token, r));
                                    });
                                    continue;
                                }
                                Err(_) => break,
                            }
                        }
                    }
                }
            }
        });

        // Process nested responses from flux.poll()
        for (token, resp) in nested_resp_buf.drain(..) {
            let cid = token.client_id().0 as usize;
            server.reply(token, resp);
            inflight[cid] -= 1;
            copyrpc_inflight_count -= 1;
        }

        while let Some((_from, flux_token, payload)) = flux.try_recv_raw() {
            if let DelegatePayload::Req(req) = payload {
                debug_assert_eq!(
                    ShardedStore::owner_of(req.key(), num_daemons_u64) as usize,
                    daemon_id
                );
                let resp = handle_local(&mut store, &req);
                flux.reply(flux_token, DelegatePayload::Resp(resp));
            }
        }
    }

    // Print performance counters
    let perf_elapsed = perf_start.elapsed().as_secs_f64();
    if perf_elapsed > 0.1 {
        eprintln!(
            "  daemon {} perf: loop={:.2}MHz copyrpc_resp={:.2}M/s incoming={:.2}M/s ipc={:.2}M/s local={:.2}M/s ipc_poll={:.2}MHz loop_total={}",
            daemon_id,
            perf_loop_count as f64 / perf_elapsed / 1e6,
            perf_copyrpc_resp as f64 / perf_elapsed / 1e6,
            perf_incoming_req as f64 / perf_elapsed / 1e6,
            perf_ipc_req as f64 / perf_elapsed / 1e6,
            perf_local_req as f64 / perf_elapsed / 1e6,
            perf_ipc_poll_count as f64 / perf_elapsed / 1e6,
            perf_loop_count,
        );
    }

    // Print copyrpc RTT latency distribution
    if !lat_samples_ns.is_empty() {
        lat_samples_ns.sort_unstable();
        let n = lat_samples_ns.len();
        let avg = lat_samples_ns.iter().sum::<u64>() / n as u64;
        let p50 = lat_samples_ns[n / 2];
        let p99 = lat_samples_ns[n * 99 / 100];
        let p999 = lat_samples_ns[n.saturating_sub(1).min(n * 999 / 1000)];
        let min = lat_samples_ns[0];
        let max = lat_samples_ns[n - 1];
        eprintln!(
            "  daemon {} copyrpc_rtt: n={} avg={:.1}us p50={:.1}us p99={:.1}us p999={:.1}us min={:.1}us max={:.1}us",
            daemon_id,
            n,
            avg as f64 / 1000.0,
            p50 as f64 / 1000.0,
            p99 as f64 / 1000.0,
            p999 as f64 / 1000.0,
            min as f64 / 1000.0,
            max as f64 / 1000.0,
        );

        // Per-endpoint breakdown
        let num_eps = my_remote_ranks.len();
        if num_eps > 1 {
            for ep in 0..num_eps {
                let mut ep_lats: Vec<u64> = lat_samples_ns
                    .iter()
                    .zip(lat_samples_ep.iter())
                    .filter(|&(_, &e)| e as usize == ep)
                    .map(|(&ns, _)| ns)
                    .collect();
                if ep_lats.is_empty() {
                    continue;
                }
                ep_lats.sort_unstable();
                let en = ep_lats.len();
                let eavg = ep_lats.iter().sum::<u64>() / en as u64;
                let ep50 = ep_lats[en / 2];
                let ep99 = ep_lats[en * 99 / 100];
                eprintln!(
                    "    ep[{}]→rank{}: n={} avg={:.1}us p50={:.1}us p99={:.1}us",
                    ep,
                    my_remote_ranks[ep],
                    en,
                    eavg as f64 / 1000.0,
                    ep50 as f64 / 1000.0,
                    ep99 as f64 / 1000.0,
                );
            }
        }

        // Local/Remote RTT split
        if !lat_local_ns.is_empty() {
            lat_local_ns.sort_unstable();
            lat_remote_ns.sort_unstable();
            let ln = lat_local_ns.len();
            let rn = lat_remote_ns.len();
            eprintln!(
                "  daemon {} local_delay: p50={:.1}us p99={:.1}us max={:.1}us | remote_rtt: p50={:.1}us p99={:.1}us max={:.1}us",
                daemon_id,
                lat_local_ns[ln / 2] as f64 / 1000.0,
                lat_local_ns[ln * 99 / 100] as f64 / 1000.0,
                lat_local_ns[ln - 1] as f64 / 1000.0,
                lat_remote_ns[rn / 2] as f64 / 1000.0,
                lat_remote_ns[rn * 99 / 100] as f64 / 1000.0,
                lat_remote_ns[rn - 1] as f64 / 1000.0,
            );
        }
    }

    // Print loop period distribution
    if !loop_period_ns.is_empty() {
        loop_period_ns.sort_unstable();
        let n = loop_period_ns.len();
        let avg = loop_period_ns.iter().map(|&x| x as u64).sum::<u64>() / n as u64;
        eprintln!(
            "  daemon {} loop_period: n={} avg={:.0}ns p50={:.0}ns p99={:.0}ns p999={:.0}ns max={:.0}ns",
            daemon_id,
            n,
            avg,
            loop_period_ns[n / 2],
            loop_period_ns[n * 99 / 100],
            loop_period_ns[n.saturating_sub(1).min(n * 999 / 1000)],
            loop_period_ns[n - 1],
        );
    }

    // Print ctx.poll() duration distribution
    if !poll_duration_ns.is_empty() {
        poll_duration_ns.sort_unstable();
        let n = poll_duration_ns.len();
        let avg = poll_duration_ns.iter().map(|&x| x as u64).sum::<u64>() / n as u64;
        eprintln!(
            "  daemon {} poll_dur: n={} avg={:.0}ns p50={:.0}ns p99={:.0}ns p999={:.0}ns max={:.0}ns",
            daemon_id,
            n,
            avg,
            poll_duration_ns[n / 2],
            poll_duration_ns[n * 99 / 100],
            poll_duration_ns[n.saturating_sub(1).min(n * 999 / 1000)],
            poll_duration_ns[n - 1],
        );
    }

    // Print incoming server latency (reply → next flush)
    if !incoming_server_lat_ns.is_empty() {
        incoming_server_lat_ns.sort_unstable();
        let n = incoming_server_lat_ns.len();
        let avg = incoming_server_lat_ns
            .iter()
            .map(|&x| x as u64)
            .sum::<u64>()
            / n as u64;
        eprintln!(
            "  daemon {} incoming_srv_lat: n={} avg={:.1}us p50={:.1}us p99={:.1}us max={:.1}us",
            daemon_id,
            n,
            avg as f64 / 1000.0,
            incoming_server_lat_ns[n / 2] as f64 / 1000.0,
            incoming_server_lat_ns[n * 99 / 100] as f64 / 1000.0,
            incoming_server_lat_ns[n - 1] as f64 / 1000.0,
        );
    }

    // Print IPC turn-around latency
    if !ipc_turnaround_ns.is_empty() {
        ipc_turnaround_ns.sort_unstable();
        let n = ipc_turnaround_ns.len();
        let avg = ipc_turnaround_ns.iter().map(|&x| x as u64).sum::<u64>() / n as u64;
        eprintln!(
            "  daemon {} ipc_turnaround: n={} avg={:.1}us p50={:.1}us p99={:.1}us max={:.1}us",
            daemon_id,
            n,
            avg as f64 / 1000.0,
            ipc_turnaround_ns[n / 2] as f64 / 1000.0,
            ipc_turnaround_ns[n * 99 / 100] as f64 / 1000.0,
            ipc_turnaround_ns[n - 1] as f64 / 1000.0,
        );
    }

    // Print Phase 2 duration
    if !phase2_duration_ns.is_empty() {
        phase2_duration_ns.sort_unstable();
        let n = phase2_duration_ns.len();
        let avg = phase2_duration_ns.iter().map(|&x| x as u64).sum::<u64>() / n as u64;
        eprintln!(
            "  daemon {} phase2_dur: n={} avg={:.0}ns p50={:.0}ns p99={:.0}ns p999={:.0}ns max={:.0}ns",
            daemon_id,
            n,
            avg,
            phase2_duration_ns[n / 2],
            phase2_duration_ns[n * 99 / 100],
            phase2_duration_ns[n.saturating_sub(1).min(n * 999 / 1000)],
            phase2_duration_ns[n - 1],
        );
    }

    qd_collector.map_or_else(Vec::new, |c| c.into_samples())
}
