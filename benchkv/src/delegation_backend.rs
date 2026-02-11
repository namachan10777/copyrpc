use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use mpi::collective::CommunicatorCollectives;

use crate::affinity;
use crate::daemon::{CopyrpcSetup, DaemonFlux, EndpointConnectionInfo};
use crate::message::*;
use crate::mpi_util;
use crate::parquet_out;
use crate::storage::ShardedStore;
use crate::workload;
use crate::Cli;

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
    let rank_to_ep_index = |target_rank: u32| -> usize {
        my_remote_ranks
            .iter()
            .position(|&r| r == target_rank)
            .expect("target_rank not in my_remote_ranks")
    };

    let num_daemons_u64 = num_daemons as u64;

    // Pending copyrpc recv handles waiting for Flux response
    let mut pending_copyrpc_handles: Vec<Option<copyrpc::RecvHandle<'_, DelegToken>>> = Vec::new();
    let mut free_slots: Vec<usize> = Vec::new();

    // Per-client ipc inflight tracking
    let max_ipc_clients = ipc_server.max_clients() as usize;
    let mut ipc_inflight = vec![0u32; max_ipc_clients];
    let mut ipc_skip = vec![false; max_ipc_clients];

    // Buffer for nested ctx.poll() responses inside flux.poll() closure
    let mut nested_resp_buf: Vec<(DelegToken, Response)> = Vec::new();

    while !stop_flag.load(Ordering::Relaxed) {
        // === Phase 1: copyrpc poll (RDMA completions → delegationrpc response slots) ===
        if let Some(ctx) = ctx_ref {
            ctx.poll(|token: DelegToken, data: &[u8]| {
                let resp = RemoteResponse::from_bytes(data).response;
                deleg_server.reply(token.client_id, token.resp_slot_idx, resp);
            });

            // copyrpc recv (incoming requests from remote nodes)
            while let Some(recv_handle) = ctx.recv() {
                let req = RemoteRequest::from_bytes(recv_handle.data()).request;
                let target_daemon = ShardedStore::owner_of(req.key(), num_daemons_u64) as usize;

                if target_daemon == 0 {
                    let resp = handle_local(&mut store, &req);
                    let remote_resp = RemoteResponse { response: resp };
                    loop {
                        match recv_handle.reply(remote_resp.as_bytes()) {
                            Ok(()) => break,
                            Err(copyrpc::error::Error::RingFull) => {
                                ctx.poll(|token: DelegToken, data: &[u8]| {
                                    let resp = RemoteResponse::from_bytes(data).response;
                                    deleg_server.reply(
                                        token.client_id,
                                        token.resp_slot_idx,
                                        resp,
                                    );
                                });
                                continue;
                            }
                            Err(_) => break,
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
                    let _ = flux.call(target_daemon, DelegatePayload::Req(req), idx);
                }
            }
        }

        // === Phase 2: delegationrpc poll (client remote submissions → copyrpc send) ===
        deleg_server.poll();
        while let Some(entry) = deleg_server.recv() {
            let target_rank = entry.payload.rank();

            if target_rank == my_rank {
                // Shouldn't normally happen, but handle gracefully
                let target_daemon = ShardedStore::owner_of(entry.payload.key(), num_daemons_u64) as usize;
                if target_daemon == 0 {
                    let resp = handle_local(&mut store, &entry.payload);
                    deleg_server.reply(entry.client_id, entry.resp_slot_idx, resp);
                } else {
                    // Can't easily forward via Flux here since we don't have copyrpc recv handles.
                    // Just handle locally (wrong shard but functionally ok for benchmark).
                    let resp = handle_local(&mut store, &entry.payload);
                    deleg_server.reply(entry.client_id, entry.resp_slot_idx, resp);
                }
            } else if let Some(ctx) = ctx_ref {
                let token = DelegToken {
                    client_id: entry.client_id,
                    resp_slot_idx: entry.resp_slot_idx,
                };
                let remote_req = RemoteRequest {
                    request: entry.payload,
                };
                let ep_idx = rank_to_ep_index(target_rank);
                let mut pending_token = token;
                loop {
                    match copyrpc_endpoints[ep_idx].call(
                        remote_req.as_bytes(),
                        pending_token,
                        0u64,
                    ) {
                        Ok(_) => break,
                        Err(copyrpc::error::CallError::RingFull(returned)) => {
                            pending_token = returned;
                            ctx.poll(|t: DelegToken, data: &[u8]| {
                                let resp = RemoteResponse::from_bytes(data).response;
                                deleg_server.reply(t.client_id, t.resp_slot_idx, resp);
                            });
                            continue;
                        }
                        Err(_) => break,
                    }
                }
            }
        }
        deleg_server.advance_tail();

        // === Phase 3: ipc poll (local requests for daemon#0's shard) ===
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
                0
            );
            let resp = handle_local(&mut store, &req);
            handle.reply(resp);
            ipc_inflight[cid] -= 1;
        }

        // === Phase 4: Flux poll + recv ===
        flux.poll(|pending_idx: usize, data: DelegatePayload| {
            if let DelegatePayload::Resp(resp) = data
                && let Some(recv_handle) = pending_copyrpc_handles[pending_idx].take()
            {
                free_slots.push(pending_idx);
                let remote_resp = RemoteResponse { response: resp };
                if let Some(ctx) = ctx_ref {
                    loop {
                        match recv_handle.reply(remote_resp.as_bytes()) {
                            Ok(()) => break,
                            Err(copyrpc::error::Error::RingFull) => {
                                ctx.poll(|token: DelegToken, data: &[u8]| {
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
        });

        for (token, resp) in nested_resp_buf.drain(..) {
            deleg_server.reply(token.client_id, token.resp_slot_idx, resp);
        }

        while let Some((_from, flux_token, payload)) = flux.try_recv_raw() {
            if let DelegatePayload::Req(req) = payload {
                debug_assert_eq!(
                    ShardedStore::owner_of(req.key(), num_daemons_u64) as usize,
                    0
                );
                let resp = handle_local(&mut store, &req);
                flux.reply(flux_token, DelegatePayload::Resp(resp));
            }
        }
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
#[allow(clippy::too_many_arguments)]
fn run_delegation_client(
    ipc_paths: &[String],
    deleg_path: &str,
    num_daemons: usize,
    my_rank: u32,
    pattern: &[crate::workload::AccessEntry],
    queue_depth: u32,
    stop_flag: &AtomicBool,
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

    let pattern_len = pattern.len();
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

    // Initial fill
    for _ in 0..queue_depth {
        let entry = &pattern[pattern_idx % pattern_len];
        pattern_idx += 1;
        if entry.rank == my_rank {
            let daemon = (entry.key % num_daemons as u64) as usize;
            if ipc_clients[daemon].call(make_request(entry), ()).is_err() {
                return Vec::new();
            }
        } else if deleg_client.call(make_request(entry)).is_err() {
            return Vec::new();
        }
    }

    let mut records = Vec::new();
    let mut completed_in_batch = 0u32;

    while !stop_flag.load(Ordering::Relaxed) {
        let mut total_n = 0u32;

        // Poll ipc clients
        for c in &mut ipc_clients {
            match c.poll() {
                Ok(n) => total_n += n,
                Err(_) => return records,
            }
        }

        // Poll delegationrpc client
        total_n += deleg_client.poll(|_, _| {});

        // Refill
        for _ in 0..total_n {
            let entry = &pattern[pattern_idx % pattern_len];
            pattern_idx += 1;
            if entry.rank == my_rank {
                let daemon = (entry.key % num_daemons as u64) as usize;
                if ipc_clients[daemon].call(make_request(entry), ()).is_err() {
                    return records;
                }
            } else if deleg_client.call(make_request(entry)).is_err() {
                return records;
            }
        }

        completed_in_batch += total_n;
        while completed_in_batch >= batch_size {
            completed_in_batch -= batch_size;
            records.push(parquet_out::BatchRecord {
                elapsed_ns: bench_start.elapsed().as_nanos() as u64,
                batch_size,
            });
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
    let deleg_ring_depth = 1024u32.next_power_of_two(); // shared ring capacity
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
        copyrpc_setup_for_d0 = Some(CopyrpcSetup {
            local_info_tx: local_tx,
            remote_info_rx: remote_rx,
            device_index: cli.device_index,
            port: cli.port,
            ring_size: cli.ring_size,
            my_remote_ranks: all_remote_ranks,
        });
    } else {
        copyrpc_local_rx = None;
        copyrpc_remote_tx = None;
    }

    // Spawn daemon threads
    let mut daemon_handles = Vec::with_capacity(num_daemons);

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
        let local_infos: Vec<EndpointConnectionInfo> =
            local_rx.recv().expect("Failed to receive daemon#0 local info");

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

    // Spawn client threads
    let bench_start = Instant::now();
    let mut client_handles = Vec::with_capacity(num_clients);
    for c in 0..num_clients {
        let ipc_ps = ipc_paths.clone();
        let deleg_p = deleg_path.clone();
        let pattern = patterns[c].clone();
        let stop = stop_flag.clone();
        let core = client_cores.get(c).copied();
        let batch_size = cli.batch_size;
        let bs = bench_start;
        let nd = num_daemons;

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
                &stop,
                batch_size,
                bs,
            )
        }));
    }

    // Benchmark: run boundary loop
    let duration = Duration::from_secs(cli.duration);
    let mut run_boundaries = Vec::new();

    for run in 0..cli.runs {
        world.barrier();
        let run_start_ns = bench_start.elapsed().as_nanos() as u64;
        std::thread::sleep(duration);
        let run_end_ns = bench_start.elapsed().as_nanos() as u64;
        run_boundaries.push((run, run_start_ns, run_end_ns));
    }

    // Stop
    stop_flag.store(true, Ordering::Release);

    for h in daemon_handles {
        h.join().expect("Daemon thread panicked");
    }

    let client_batches: Vec<Vec<parquet_out::BatchRecord>> = client_handles
        .into_iter()
        .map(|h| h.join().unwrap())
        .collect();

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
