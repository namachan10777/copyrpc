use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use fastmap::FastMap;
use mpi::collective::CommunicatorCollectives;

use crate::Cli;
use crate::affinity;
use crate::daemon::{CopyrpcSetup, DaemonFlux, EndpointConnectionInfo, auto_adjust_ring_size};
use crate::message::*;
use crate::mpi_util;
use crate::parquet_out;
use crate::storage::ShardedStore;
use crate::workload;

/// copyrpc user_data for delegation: identifies the delegationrpc response slot.
#[derive(Clone, Copy)]
#[repr(C)]
struct DelegToken {
    client_id: u32,
    resp_slot_idx: u32,
}

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

/// Daemon #0: reads from delegationrpc shared ring + copyrpc RDMA + Flux + ipc.
#[allow(clippy::too_many_arguments)]
fn run_daemon_0(
    my_rank: u32,
    num_daemons: usize,
    key_range: u64,
    queue_depth: u32,
    poll_budget: u32,
    mut ipc_server: ipc::Server<Request, Response>,
    mut deleg_server: delegationrpc::Server<Request, Response>,
    mut flux: DaemonFlux,
    copyrpc_setup: Option<CopyrpcSetup>,
    stop_flag: &AtomicBool,
    ready_barrier: &Barrier,
) {
    let mut store = ShardedStore::new(key_range, num_daemons as u64, 0);

    // Setup copyrpc — Daemon#0 gets ALL remote rank endpoints
    let copyrpc_ctx: Option<copyrpc::Context<DelegToken>>;
    let mut copyrpc_endpoints: Vec<copyrpc::Endpoint<DelegToken>> = Vec::new();
    let mut my_remote_ranks: Vec<u32> = Vec::new();

    if let Some(setup) = copyrpc_setup {
        let ctx: copyrpc::Context<DelegToken> = copyrpc::ContextBuilder::new()
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

    let ctx_ref = copyrpc_ctx.as_ref();
    let mut rank_to_ep_index = FastMap::new();
    for (ep_idx, &remote_rank) in my_remote_ranks.iter().enumerate() {
        rank_to_ep_index.insert(remote_rank, ep_idx);
    }

    let num_daemons_u64 = num_daemons as u64;

    // Pending copyrpc recv handles waiting for Flux response
    let mut pending_copyrpc_handles: Vec<Option<copyrpc::RecvHandle<'_, DelegToken>>> = Vec::new();
    let mut free_slots: Vec<usize> = Vec::new();

    // Per-client ipc inflight tracking
    let max_ipc_clients = ipc_server.max_clients() as usize;
    let mut ipc_inflight = vec![0u32; max_ipc_clients];
    let mut ipc_skip = vec![false; max_ipc_clients];

    // Backlog for copyrpc recv_handle.reply() when send ring is full
    let mut reply_backlog: Vec<(copyrpc::RecvHandle<'_, DelegToken>, Vec<u8>)> = Vec::new();
    // Backlog for copyrpc endpoint.call() when ring full / insufficient credit
    let mut call_backlog: Vec<(usize, RemoteRequest, DelegToken)> = Vec::new();

    // Performance instrumentation
    let perf_start = std::time::Instant::now();
    let mut perf_loop_count: u64 = 0;
    let mut perf_copyrpc_resp: u64 = 0;
    let mut perf_deleg_req: u64 = 0;
    let mut perf_incoming_req: u64 = 0;
    let mut perf_ipc_req: u64 = 0;

    let cap = 1_000_000usize;
    let mut loop_period_ns: Vec<u32> = Vec::with_capacity(cap);
    let mut phase1_ns: Vec<u32> = Vec::with_capacity(cap);
    let mut phase1_poll_ns: Vec<u32> = Vec::with_capacity(cap);
    let mut phase1_recv_ns: Vec<u32> = Vec::with_capacity(cap);
    let mut poll_s1_ns: Vec<u32> = Vec::with_capacity(cap);
    let mut poll_s2_ns: Vec<u32> = Vec::with_capacity(cap);
    let mut poll_s3_ns: Vec<u32> = Vec::with_capacity(cap);
    let mut poll_s4_ns: Vec<u32> = Vec::with_capacity(cap);
    let mut phase2_ns: Vec<u32> = Vec::with_capacity(cap);
    let mut phase3_ns: Vec<u32> = Vec::with_capacity(cap);
    let mut phase4_ns: Vec<u32> = Vec::with_capacity(cap);
    let mut perf_copyrpc_incoming: u64 = 0;
    let mut loop_prev_ts = perf_start;

    while !stop_flag.load(Ordering::Relaxed) {
        perf_loop_count += 1;
        let loop_now = std::time::Instant::now();
        if loop_period_ns.len() < cap {
            loop_period_ns.push((loop_now - loop_prev_ts).as_nanos() as u32);
        }
        loop_prev_ts = loop_now;

        // === Phase 1: copyrpc poll (RDMA completions → delegationrpc response slots) ===
        let p1_start = std::time::Instant::now();
        let mut copyrpc_resp_count = 0u32;
        let mut copyrpc_incoming_count = 0u32;
        let mut p1_recv_start = p1_start;
        if let Some(ctx) = ctx_ref {
            let (mut last_s1, mut last_s2, mut last_s3, mut last_s4) = (0u32, 0u32, 0u32, 0u32);
            for _spin in 0..poll_budget {
                let (s1, s2, s3, s4, _rc) = ctx.poll_timed(|token: DelegToken, data: &[u8]| {
                    let resp = RemoteResponse::from_bytes(data).response;
                    deleg_server.reply(token.client_id, token.resp_slot_idx, resp);
                    copyrpc_resp_count += 1;
                });
                last_s1 = s1;
                last_s2 = s2;
                last_s3 = s3;
                last_s4 = s4;
                if copyrpc_resp_count >= poll_budget {
                    break;
                }
            }

            if poll_s1_ns.len() < cap {
                poll_s1_ns.push(last_s1);
                poll_s2_ns.push(last_s2);
                poll_s3_ns.push(last_s3);
                poll_s4_ns.push(last_s4);
            }
            if phase1_poll_ns.len() < cap {
                phase1_poll_ns.push((std::time::Instant::now() - p1_start).as_nanos() as u32);
            }

            // Drain reply backlog (recv_handle.reply retries from previous loop)
            reply_backlog.retain_mut(|(handle, data)| match handle.reply(data) {
                Ok(()) => false,
                Err(copyrpc::error::Error::RingFull) => true,
                Err(e) => {
                    eprintln!("[daemon0] reply backlog error: {e}");
                    false
                }
            });

            // copyrpc recv (incoming requests from remote nodes)
            p1_recv_start = std::time::Instant::now();
            while let Some(recv_handle) = ctx.recv() {
                copyrpc_incoming_count += 1;
                let req = RemoteRequest::from_bytes(recv_handle.data()).request;
                let target_daemon = ShardedStore::owner_of(req.key(), num_daemons_u64) as usize;

                if target_daemon == 0 {
                    let resp = handle_local(&mut store, &req);
                    let remote_resp = RemoteResponse { response: resp };
                    match recv_handle.reply(remote_resp.as_bytes()) {
                        Ok(()) => {}
                        Err(copyrpc::error::Error::RingFull) => {
                            reply_backlog.push((recv_handle, remote_resp.as_bytes().to_vec()));
                            continue; // recv_handle moved; skip to next ctx.recv()
                        }
                        Err(e) => {
                            eprintln!("[daemon0] Phase1 reply error: {e}");
                        }
                    }
                } else {
                    // Forward to correct daemon via Flux
                    let idx = if let Some(i) = free_slots.pop() {
                        pending_copyrpc_handles[i] = Some(recv_handle);
                        i
                    } else {
                        let i = pending_copyrpc_handles.len();
                        pending_copyrpc_handles.push(Some(recv_handle));
                        i
                    };
                    if let Err(e) = flux.call(target_daemon, DelegatePayload::Req(req), idx) {
                        eprintln!("[daemon0] Flux call error: {e:?}");
                        // Reclaim slot; recv_handle is dropped (SRQ buffer freed)
                        pending_copyrpc_handles[idx].take();
                        free_slots.push(idx);
                    }
                }
            }
        }

        perf_copyrpc_resp += copyrpc_resp_count as u64;
        perf_copyrpc_incoming += copyrpc_incoming_count as u64;
        if phase1_recv_ns.len() < cap {
            phase1_recv_ns.push((std::time::Instant::now() - p1_recv_start).as_nanos() as u32);
        }
        if phase1_ns.len() < cap {
            phase1_ns.push((std::time::Instant::now() - p1_start).as_nanos() as u32);
        }

        // === Phase 2: delegationrpc poll (client remote submissions → copyrpc send) ===
        let p2_start = std::time::Instant::now();
        let mut deleg_req_count = 0u32;

        // Drain call backlog (endpoint.call retries from previous loop)
        call_backlog.retain_mut(|(ep_idx, req, token)| {
            match copyrpc_endpoints[*ep_idx].call(req.as_bytes(), *token, 0u64) {
                Ok(_) => false,
                Err(copyrpc::error::CallError::RingFull(t))
                | Err(copyrpc::error::CallError::InsufficientCredit(t)) => {
                    *token = t;
                    true
                }
                Err(copyrpc::error::CallError::Other(e)) => {
                    eprintln!("[daemon0] call backlog fatal: {e}");
                    false
                }
            }
        });

        deleg_server.poll();
        while let Some(entry) = deleg_server.recv() {
            let target_rank = entry.payload.rank();

            deleg_req_count += 1;
            if target_rank == my_rank {
                let resp = handle_local(&mut store, &entry.payload);
                deleg_server.reply(entry.client_id, entry.resp_slot_idx, resp);
            } else if let Some(_ctx) = ctx_ref {
                let token = DelegToken {
                    client_id: entry.client_id,
                    resp_slot_idx: entry.resp_slot_idx,
                };
                let remote_req = RemoteRequest {
                    request: entry.payload,
                };
                let ep_idx = *rank_to_ep_index
                    .get(target_rank)
                    .expect("target_rank not in my_remote_ranks");
                match copyrpc_endpoints[ep_idx].call(remote_req.as_bytes(), token, 0u64) {
                    Ok(_) => {}
                    Err(copyrpc::error::CallError::RingFull(returned))
                    | Err(copyrpc::error::CallError::InsufficientCredit(returned)) => {
                        call_backlog.push((ep_idx, remote_req, returned));
                    }
                    Err(copyrpc::error::CallError::Other(e)) => {
                        eprintln!("[daemon0] Phase2 call fatal: {e}");
                        // token consumed by Other — use backup to respond to client
                        deleg_server.reply(
                            entry.client_id,
                            entry.resp_slot_idx,
                            Response::MetaGetNotFound,
                        );
                    }
                }
            }
        }
        deleg_server.advance_tail();
        perf_deleg_req += deleg_req_count as u64;
        if phase2_ns.len() < cap {
            phase2_ns.push((std::time::Instant::now() - p2_start).as_nanos() as u32);
        }

        // === Phase 3: ipc poll (local requests for daemon#0's shard) ===
        let p3_start = std::time::Instant::now();
        for i in 0..max_ipc_clients {
            ipc_skip[i] = ipc_inflight[i] >= queue_depth;
        }
        ipc_server.poll_with_skip(&ipc_skip);

        let mut ipc_req_count = 0u32;
        while let Some(handle) = ipc_server.recv() {
            let cid = handle.client_id().0 as usize;
            ipc_inflight[cid] += 1;
            let req = *handle.data();
            debug_assert_eq!(req.rank(), my_rank);
            debug_assert_eq!(
                ShardedStore::owner_of(req.key(), num_daemons_u64) as usize,
                0
            );
            let resp = handle_local(&mut store, &req);
            handle.reply(resp);
            ipc_inflight[cid] -= 1;
            ipc_req_count += 1;
        }
        perf_ipc_req += ipc_req_count as u64;
        if phase3_ns.len() < cap {
            phase3_ns.push((std::time::Instant::now() - p3_start).as_nanos() as u32);
        }

        // === Phase 4: Flux poll + recv ===
        let p4_start = std::time::Instant::now();
        flux.poll(|pending_idx: usize, data: DelegatePayload| {
            if let DelegatePayload::Resp(resp) = data
                && let Some(recv_handle) = pending_copyrpc_handles[pending_idx].take()
            {
                free_slots.push(pending_idx);
                let remote_resp = RemoteResponse { response: resp };
                if ctx_ref.is_some() {
                    match recv_handle.reply(remote_resp.as_bytes()) {
                        Ok(()) => {}
                        Err(copyrpc::error::Error::RingFull) => {
                            reply_backlog.push((recv_handle, remote_resp.as_bytes().to_vec()));
                        }
                        Err(e) => {
                            eprintln!("[daemon0] Phase4 reply error: {e}");
                        }
                    }
                }
            }
        });

        let mut incoming_req_count = 0u32;
        while let Some((_from, flux_token, payload)) = flux.try_recv_raw() {
            if let DelegatePayload::Req(req) = payload {
                debug_assert_eq!(
                    ShardedStore::owner_of(req.key(), num_daemons_u64) as usize,
                    0
                );
                let resp = handle_local(&mut store, &req);
                flux.reply(flux_token, DelegatePayload::Resp(resp));
                incoming_req_count += 1;
            }
        }
        perf_incoming_req += incoming_req_count as u64;
        if phase4_ns.len() < cap {
            phase4_ns.push((std::time::Instant::now() - p4_start).as_nanos() as u32);
        }
    }

    // === Performance output ===
    let elapsed = perf_start.elapsed().as_secs_f64();
    let n = loop_period_ns.len();
    if n > 0 {
        let median = |v: &mut Vec<u32>| -> u32 {
            v.sort_unstable();
            v[v.len() / 2]
        };
        let avg = |v: &[u32]| -> f64 { v.iter().map(|&x| x as f64).sum::<f64>() / v.len() as f64 };

        let mut lp = loop_period_ns;
        let mut p1 = phase1_ns;
        let mut p1p = phase1_poll_ns;
        let mut p1r = phase1_recv_ns;
        let mut p2 = phase2_ns;
        let mut p3 = phase3_ns;
        let mut p4 = phase4_ns;

        eprintln!(
            "[deleg-daemon0] rank={} loops={} elapsed={:.2}s",
            my_rank, perf_loop_count, elapsed
        );
        eprintln!(
            "  loop_period: median={}ns avg={:.0}ns freq={:.3}MHz",
            median(&mut lp),
            avg(&lp),
            perf_loop_count as f64 / elapsed / 1e6
        );
        eprintln!(
            "  phase1(total):     median={}ns avg={:.0}ns",
            median(&mut p1),
            avg(&p1)
        );
        eprintln!(
            "  phase1(ctx.poll):  median={}ns avg={:.0}ns",
            median(&mut p1p),
            avg(&p1p)
        );
        let mut s1 = poll_s1_ns;
        let mut s2 = poll_s2_ns;
        let mut s3 = poll_s3_ns;
        let mut s4 = poll_s4_ns;
        eprintln!(
            "    poll.recv_cq:  median={}ns avg={:.0}ns",
            median(&mut s1),
            avg(&s1)
        );
        eprintln!(
            "    poll.srq:      median={}ns avg={:.0}ns",
            median(&mut s2),
            avg(&s2)
        );
        eprintln!(
            "    poll.send_cq:  median={}ns avg={:.0}ns",
            median(&mut s3),
            avg(&s3)
        );
        eprintln!(
            "    poll.ep_flush: median={}ns avg={:.0}ns",
            median(&mut s4),
            avg(&s4)
        );
        eprintln!(
            "  phase1(ctx.recv):  median={}ns avg={:.0}ns",
            median(&mut p1r),
            avg(&p1r)
        );
        eprintln!(
            "  phase2(deleg):     median={}ns avg={:.0}ns",
            median(&mut p2),
            avg(&p2)
        );
        eprintln!(
            "  phase3(ipc):       median={}ns avg={:.0}ns",
            median(&mut p3),
            avg(&p3)
        );
        eprintln!(
            "  phase4(flux):      median={}ns avg={:.0}ns",
            median(&mut p4),
            avg(&p4)
        );
        eprintln!(
            "  copyrpc_resp={} copyrpc_incoming={} deleg_req={} ipc_req={} flux_incoming={}",
            perf_copyrpc_resp,
            perf_copyrpc_incoming,
            perf_deleg_req,
            perf_ipc_req,
            perf_incoming_req
        );
        eprintln!(
            "  per_loop: resp={:.2} incoming={:.2} deleg={:.2} ipc={:.2}",
            perf_copyrpc_resp as f64 / perf_loop_count as f64,
            perf_copyrpc_incoming as f64 / perf_loop_count as f64,
            perf_deleg_req as f64 / perf_loop_count as f64,
            perf_ipc_req as f64 / perf_loop_count as f64
        );
    }
}

/// Daemon #1..K: ipc (local requests) + Flux (forwarded from Daemon#0).
#[allow(clippy::too_many_arguments)]
fn run_daemon_worker(
    daemon_id: usize,
    my_rank: u32,
    num_daemons: usize,
    key_range: u64,
    queue_depth: u32,
    mut ipc_server: ipc::Server<Request, Response>,
    mut flux: DaemonFlux,
    stop_flag: &AtomicBool,
    ready_barrier: &Barrier,
) {
    let mut store = ShardedStore::new(key_range, num_daemons as u64, daemon_id as u64);
    let num_daemons_u64 = num_daemons as u64;
    let max_ipc_clients = ipc_server.max_clients() as usize;
    let mut ipc_inflight = vec![0u32; max_ipc_clients];
    let mut ipc_skip = vec![false; max_ipc_clients];

    ready_barrier.wait();

    while !stop_flag.load(Ordering::Relaxed) {
        // ipc: local requests for this shard
        for i in 0..max_ipc_clients {
            ipc_skip[i] = ipc_inflight[i] >= queue_depth;
        }
        ipc_server.poll_with_skip(&ipc_skip);

        while let Some(handle) = ipc_server.recv() {
            let cid = handle.client_id().0 as usize;
            ipc_inflight[cid] += 1;
            let req = *handle.data();
            debug_assert_eq!(req.rank(), my_rank);
            debug_assert_eq!(
                ShardedStore::owner_of(req.key(), num_daemons_u64) as usize,
                daemon_id
            );
            let resp = handle_local(&mut store, &req);
            handle.reply(resp);
            ipc_inflight[cid] -= 1;
        }

        // Flux: delegated requests from Daemon#0
        flux.poll(|_, _| {});

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
}

/// Client for delegation backend: ipc (local) + delegationrpc (remote).
///
/// Each client independently manages its own walltime:
/// 1. Warmup: measure throughput with small batch
/// 2. Calculate adaptive check interval (~500ms worth of completions)
/// 3. Main loop: check walltime every interval or every 1s (fallback)
#[allow(clippy::too_many_arguments)]
fn run_delegation_client(
    ipc_paths: &[String],
    deleg_path: &str,
    num_daemons: usize,
    my_rank: u32,
    pattern: &[crate::workload::AccessEntry],
    queue_depth: u32,
    duration: Duration,
    batch_size: u32,
    bench_start: Instant,
) -> Vec<parquet_out::BatchRecord> {
    let mut ipc_clients: Vec<ipc::Client<Request, Response, (), _>> = ipc_paths
        .iter()
        .map(|path| {
            unsafe { ipc::Client::<Request, Response, (), _>::connect(path, |(), _resp| {}) }
                .expect("ipc client connect failed")
        })
        .collect();

    let mut deleg_client =
        unsafe { delegationrpc::Client::<Request, Response>::connect(deleg_path) }
            .expect("delegationrpc client connect failed");

    let mut pattern_idx = 0usize;

    fn make_request(entry: &crate::workload::AccessEntry) -> Request {
        if entry.is_read {
            Request::MetaGet {
                rank: entry.rank,
                key: entry.key,
            }
        } else {
            Request::MetaPut {
                rank: entry.rank,
                key: entry.key,
                value: entry.key,
            }
        }
    }

    // Helper: poll all transports and return total completions
    let poll_all = |ipc_clients: &mut [ipc::Client<Request, Response, (), _>],
                    deleg_client: &mut delegationrpc::Client<Request, Response>|
     -> Result<u32, ()> {
        let mut total_n = 0u32;
        for c in ipc_clients.iter_mut() {
            match c.poll() {
                Ok(n) => total_n += n,
                Err(_) => return Err(()),
            }
        }
        total_n += deleg_client.poll(|_, _| {});
        Ok(total_n)
    };

    // Helper: refill one request
    let refill_one = |ipc_clients: &mut [ipc::Client<Request, Response, (), _>],
                      deleg_client: &mut delegationrpc::Client<Request, Response>,
                      pattern: &[crate::workload::AccessEntry],
                      pattern_idx: &mut usize,
                      my_rank: u32,
                      num_daemons: usize|
     -> bool {
        let entry = &pattern[*pattern_idx % pattern.len()];
        *pattern_idx += 1;
        if entry.rank == my_rank {
            let daemon = (entry.key % num_daemons as u64) as usize;
            ipc_clients[daemon].call(make_request(entry), ()).is_ok()
        } else {
            deleg_client.call(make_request(entry)).is_ok()
        }
    };

    // Initial fill
    for _ in 0..queue_depth {
        if !refill_one(
            &mut ipc_clients,
            &mut deleg_client,
            pattern,
            &mut pattern_idx,
            my_rank,
            num_daemons,
        ) {
            return Vec::new();
        }
    }

    // ---- Warmup: measure throughput ----
    let warmup_target = 1000u32;
    let warmup_start = Instant::now();
    let mut warmup_completed = 0u32;
    while warmup_completed < warmup_target {
        let total_n = match poll_all(&mut ipc_clients, &mut deleg_client) {
            Ok(n) => n,
            Err(()) => return Vec::new(),
        };
        for _ in 0..total_n {
            if !refill_one(
                &mut ipc_clients,
                &mut deleg_client,
                pattern,
                &mut pattern_idx,
                my_rank,
                num_daemons,
            ) {
                return Vec::new();
            }
        }
        warmup_completed += total_n;
    }
    let warmup_dur = warmup_start.elapsed().as_secs_f64();
    let throughput = warmup_completed as f64 / warmup_dur;
    let adaptive_check_interval = (throughput * 0.5).max(100.0) as u32;

    // ---- Main loop: walltime-based termination ----
    let deadline = bench_start + duration;
    let mut records = Vec::new();
    let mut completed_in_batch = 0u32;
    let mut completed_since_check = 0u32;
    let mut last_time_check = Instant::now();

    loop {
        let total_n = match poll_all(&mut ipc_clients, &mut deleg_client) {
            Ok(n) => n,
            Err(()) => return records,
        };

        // Refill
        for _ in 0..total_n {
            if !refill_one(
                &mut ipc_clients,
                &mut deleg_client,
                pattern,
                &mut pattern_idx,
                my_rank,
                num_daemons,
            ) {
                return records;
            }
        }

        // Record batches
        completed_in_batch += total_n;
        while completed_in_batch >= batch_size {
            completed_in_batch -= batch_size;
            records.push(parquet_out::BatchRecord {
                elapsed_ns: bench_start.elapsed().as_nanos() as u64,
                batch_size,
            });
        }

        // Walltime check: every adaptive_check_interval completions OR every 1s
        completed_since_check += total_n;
        if completed_since_check >= adaptive_check_interval
            || last_time_check.elapsed() >= Duration::from_secs(1)
        {
            completed_since_check = 0;
            last_time_check = Instant::now();
            if Instant::now() >= deadline {
                break;
            }
        }

        if total_n == 0 {
            std::hint::spin_loop();
        }
    }
    records
}

// ============================================================
// Orchestrator
// ============================================================

#[allow(clippy::too_many_arguments)]
pub fn run_delegation(
    cli: &Cli,
    world: &mpi::topology::SimpleCommunicator,
    rank: u32,
    size: u32,
    num_daemons: usize,
    num_clients: usize,
) -> Vec<parquet_out::BenchRow> {
    let queue_depth = cli.queue_depth;

    // CPU affinity
    let available_cores = affinity::get_available_cores(cli.device_index);
    let (ranks_on_node, rank_on_node) = mpi_util::node_local_rank(world);
    let (daemon_cores, client_cores) = affinity::assign_cores(
        &available_cores,
        num_daemons,
        num_clients,
        ranks_on_node,
        rank_on_node,
    );

    // Generate access patterns
    let pattern_len = 10000;
    let patterns: Vec<Vec<workload::AccessEntry>> = (0..num_clients)
        .map(|c| {
            workload::generate_pattern(
                size,
                cli.key_range,
                cli.read_ratio,
                cli.distribution,
                pattern_len,
                rank as u64 * 1000 + c as u64,
            )
        })
        .collect();

    // Create ipc servers (one per daemon, for local requests)
    let ipc_paths: Vec<String> = (0..num_daemons)
        .map(|d| format!("/benchkv_deleg_ipc_{}_{}", rank, d))
        .collect();

    let max_clients_per_daemon = num_clients as u32;
    let mut ipc_servers: Vec<Option<ipc::Server<Request, Response>>> = ipc_paths
        .iter()
        .map(|path| {
            Some(
                unsafe {
                    ipc::Server::<Request, Response>::create(
                        path,
                        max_clients_per_daemon,
                        queue_depth,
                        0,
                    )
                }
                .expect("Failed to create ipc server"),
            )
        })
        .collect();

    // Create delegationrpc server (one, for Daemon#0, remote requests)
    let deleg_path = format!("/benchkv_deleg_ring_{}", rank);
    let deleg_ring_depth = (num_clients as u32 * queue_depth)
        .max(1024)
        .next_power_of_two();
    let deleg_resp_depth = queue_depth.next_power_of_two(); // per-client response slots
    let mut deleg_server: Option<delegationrpc::Server<Request, Response>> = Some(
        unsafe {
            delegationrpc::Server::<Request, Response>::create(
                &deleg_path,
                num_clients as u32,
                deleg_ring_depth,
                deleg_resp_depth,
            )
        }
        .expect("Failed to create delegationrpc server"),
    );

    // Create Flux network
    let flux_capacity = 1024;
    let flux_inflight_max = 256;
    let mut flux_nodes: Vec<Option<DaemonFlux>> = inproc::create_flux::<DelegatePayload, usize>(
        num_daemons.max(1),
        flux_capacity,
        flux_inflight_max,
    )
    .into_iter()
    .map(Some)
    .collect();

    // Shared state
    let stop_flag = Arc::new(AtomicBool::new(false));
    let ready_barrier = Arc::new(Barrier::new(num_daemons + 1));

    // copyrpc setup — Daemon#0 gets ALL remote rank endpoints
    let copyrpc_local_rx;
    let copyrpc_remote_tx;
    let mut copyrpc_setup_for_d0: Option<CopyrpcSetup> = None;

    if size > 1 {
        let all_remote_ranks: Vec<u32> = (0..size).filter(|&r| r != rank).collect();
        let (local_tx, local_rx) = std::sync::mpsc::channel();
        let (remote_tx, remote_rx) = std::sync::mpsc::channel();
        copyrpc_local_rx = Some(local_rx);
        copyrpc_remote_tx = Some(remote_tx);

        // Auto-adjust ring_size: each remote endpoint needs enough credit for
        // ceil(num_clients * queue_depth / size) inflight calls
        let inflight_per_ep =
            (num_clients * queue_depth as usize + size as usize - 1) / size as usize;
        let ring_size = auto_adjust_ring_size(cli.ring_size, inflight_per_ep, rank);

        copyrpc_setup_for_d0 = Some(CopyrpcSetup {
            local_info_tx: local_tx,
            remote_info_rx: remote_rx,
            device_index: cli.device_index,
            port: cli.port,
            ring_size,
            my_remote_ranks: all_remote_ranks,
        });
    } else {
        copyrpc_local_rx = None;
        copyrpc_remote_tx = None;
    }

    // Spawn daemon threads
    let mut daemon_handles = Vec::with_capacity(num_daemons);

    let poll_budget = cli.poll_budget;
    for d in 0..num_daemons {
        let ipc_srv = ipc_servers[d].take().unwrap();
        let flux = flux_nodes[d].take().unwrap();
        let stop = stop_flag.clone();
        let barrier = ready_barrier.clone();
        let key_range = cli.key_range;
        let core = daemon_cores.get(d).copied();

        if d == 0 {
            // Daemon#0: delegationrpc + copyrpc + Flux + ipc
            let deleg_srv = deleg_server.take().unwrap();
            let setup = copyrpc_setup_for_d0.take();

            daemon_handles.push(std::thread::spawn(move || {
                if let Some(core_id) = core {
                    affinity::pin_thread(core_id, "daemon-0");
                }
                run_daemon_0(
                    rank,
                    num_daemons,
                    key_range,
                    queue_depth,
                    poll_budget,
                    ipc_srv,
                    deleg_srv,
                    flux,
                    setup,
                    &stop,
                    &barrier,
                );
            }));
        } else {
            // Daemon#1..K: ipc + Flux only
            daemon_handles.push(std::thread::spawn(move || {
                if let Some(core_id) = core {
                    affinity::pin_thread(core_id, &format!("daemon-{}", d));
                }
                run_daemon_worker(
                    d,
                    rank,
                    num_daemons,
                    key_range,
                    queue_depth,
                    ipc_srv,
                    flux,
                    &stop,
                    &barrier,
                );
            }));
        }
    }

    // Main thread: MPI exchange for copyrpc (only Daemon#0, all remote ranks)
    if size > 1 {
        let local_rx = copyrpc_local_rx.unwrap();
        let remote_tx = copyrpc_remote_tx.unwrap();

        // Daemon#0 sends all endpoint infos ordered by remote ranks (0..size, excluding self)
        let local_infos: Vec<EndpointConnectionInfo> = local_rx
            .recv()
            .expect("Failed to receive daemon#0 local info");

        let all_remote_ranks: Vec<u32> = (0..size).filter(|&r| r != rank).collect();

        let mut remote_infos = Vec::with_capacity(all_remote_ranks.len());
        for (i, &peer_rank) in all_remote_ranks.iter().enumerate() {
            let send_bytes = local_infos[i].to_bytes();
            let recv_bytes =
                mpi_util::exchange_bytes(world, rank as i32, peer_rank as i32, &send_bytes);
            remote_infos.push(EndpointConnectionInfo::from_bytes(&recv_bytes));
        }

        remote_tx
            .send(remote_infos)
            .expect("Failed to send remote info to daemon#0");
    }

    // Wait for all daemons to be ready
    ready_barrier.wait();

    std::thread::sleep(Duration::from_millis(10));
    world.barrier();

    // Spawn client threads (walltime-based: each client self-terminates after duration)
    let bench_start = Instant::now();
    let total_duration = Duration::from_secs(cli.duration * cli.runs as u64);
    let mut client_handles = Vec::with_capacity(num_clients);
    for c in 0..num_clients {
        let ipc_ps = ipc_paths.clone();
        let deleg_p = deleg_path.clone();
        let pattern = patterns[c].clone();
        let core = client_cores.get(c).copied();
        let batch_size = cli.batch_size;
        let bs = bench_start;
        let nd = num_daemons;
        let dur = total_duration;

        client_handles.push(std::thread::spawn(move || {
            if let Some(core_id) = core {
                affinity::pin_thread(core_id, &format!("client-{}", c));
            }
            run_delegation_client(
                &ipc_ps,
                &deleg_p,
                nd,
                rank,
                &pattern,
                queue_depth,
                dur,
                batch_size,
                bs,
            )
        }));
    }

    // Wait for all clients to self-terminate (no MPI barrier during measurement)
    let client_batches: Vec<Vec<parquet_out::BatchRecord>> = client_handles
        .into_iter()
        .map(|h| h.join().unwrap())
        .collect();

    // Stop daemons (clients already stopped)
    stop_flag.store(true, Ordering::Release);

    for h in daemon_handles {
        h.join().expect("Daemon thread panicked");
    }

    // Run boundaries: derive from known duration (no MPI barrier needed)
    let run_duration_ns = cli.duration * 1_000_000_000;
    let mut run_boundaries = Vec::new();
    for run in 0..cli.runs {
        let run_start_ns = run as u64 * run_duration_ns;
        let run_end_ns = (run as u64 + 1) * run_duration_ns;
        run_boundaries.push((run, run_start_ns, run_end_ns));
    }

    // Log RPS per run
    for &(run, start_ns, end_ns) in &run_boundaries {
        let rps = parquet_out::compute_run_rps(&client_batches, start_ns, end_ns);
        eprintln!("  rank {} run {}: {:.0} RPS", rank, run + 1, rps);
        let mut total_rps = 0.0f64;
        world.all_reduce_into(
            &rps,
            &mut total_rps,
            mpi::collective::SystemOperation::sum(),
        );
        if rank == 0 {
            eprintln!("  total run {}: {:.0} RPS", run + 1, total_rps);
        }
    }

    parquet_out::rows_from_batches(
        "delegation",
        rank,
        &client_batches,
        &run_boundaries,
        num_daemons as u32,
        num_clients as u32,
        queue_depth,
        cli.key_range,
    )
}
