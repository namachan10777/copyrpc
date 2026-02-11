//! CopyrpcDirect backend for benchkv.
//!
//! Architecture: Client → copyrpc endpoint → Daemon (direct, no ipc, no Flux)
//! Each client thread owns its own copyrpc::Context with endpoints to all daemons (all ranks).
//! Each daemon thread owns its own copyrpc::Context with endpoints to all clients (all ranks).
//! Self-rank connections are included (RDMA loopback), matching UCX AM topology.

use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use mpi::collective::CommunicatorCollectives;

use crate::Cli;
use crate::affinity;
use crate::daemon::EndpointConnectionInfo;
use crate::message::{RemoteRequest, RemoteResponse, Request, Response};
use crate::mpi_util;
use crate::parquet_out;
use crate::parquet_out::BatchRecord;
use crate::storage::ShardedStore;
use crate::workload::AccessEntry;

// === Thread-local response counter for client poll callback ===

thread_local! {
    static DIRECT_COMPLETED: RefCell<u64> = const { RefCell::new(0) };
}

// === Info exchange messages ===

/// Per-thread info bundle sent from worker threads to main for MPI exchange.
struct ThreadLocalInfos {
    /// Thread index (0..num_daemons for daemons, num_daemons..total for clients)
    thread_idx: usize,
    /// Endpoint connection infos, ordered by the endpoint ordering convention.
    /// Daemon d: S*C infos ordered [(rank0,c0), (rank0,c1), ..., (rankS-1,cC-1)]
    /// Client c: S*D infos ordered [(rank0,d0), (rank0,d1), ..., (rankS-1,dD-1)]
    infos: Vec<EndpointConnectionInfo>,
}

// === Main orchestrator ===

#[allow(clippy::too_many_arguments)]
pub fn run_copyrpc_direct(
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
    let patterns: Vec<Vec<AccessEntry>> = (0..num_clients)
        .map(|c| {
            crate::workload::generate_pattern(
                size,
                cli.key_range,
                cli.read_ratio,
                cli.distribution,
                pattern_len,
                rank as u64 * 1000 + c as u64,
            )
        })
        .collect();

    // Channels: threads send their local endpoint infos to main
    let (info_tx, info_rx) = std::sync::mpsc::channel::<ThreadLocalInfos>();

    // Channels: main sends remote infos back to each thread
    let total_threads = num_daemons + num_clients;
    let (peer_txs, peer_rxs): (Vec<_>, Vec<_>) = (0..total_threads)
        .map(|_| std::sync::mpsc::channel::<Vec<EndpointConnectionInfo>>())
        .unzip();

    // Shared state
    let stop_flag = Arc::new(AtomicBool::new(false));

    let ready_barrier = Arc::new(Barrier::new(total_threads + 1));

    let key_range = cli.key_range;
    let device_index = cli.device_index;
    let port = cli.port;
    let ring_size = cli.ring_size;

    let bench_start = Instant::now();
    let mut daemon_handles = Vec::with_capacity(num_daemons);
    let mut peer_rxs_iter: Vec<_> = peer_rxs.into_iter().collect();

    let num_ranks = size as usize;

    // Spawn daemon threads
    for d in 0..num_daemons {
        let info_tx = info_tx.clone();
        let peer_rx = peer_rxs_iter.remove(0);
        let stop = stop_flag.clone();
        let barrier = ready_barrier.clone();
        let core = daemon_cores.get(d).copied();

        daemon_handles.push(std::thread::spawn(move || {
            if let Some(core_id) = core {
                affinity::pin_thread(core_id, &format!("direct-daemon-{}", d));
            }

            // Create copyrpc context
            let ctx: copyrpc::Context<()> = copyrpc::ContextBuilder::new()
                .device_index(device_index)
                .port(port)
                .srq_config(mlx5::srq::SrqConfig {
                    max_wr: 16384,
                    max_sge: 1,
                })
                .cq_size(4096)
                .build()
                .expect("Failed to create copyrpc context for daemon");

            let ep_config = copyrpc::EndpointConfig {
                send_ring_size: ring_size,
                recv_ring_size: ring_size,
                ..Default::default()
            };

            // Create S*C endpoints: [(rank0,c0), (rank0,c1), ..., (rankS-1, cC-1)]
            let num_eps = num_ranks * num_clients;
            let mut endpoints = Vec::with_capacity(num_eps);
            let mut local_infos = Vec::with_capacity(num_eps);

            for _ in 0..num_eps {
                let ep = ctx
                    .create_endpoint(&ep_config)
                    .expect("Failed to create daemon endpoint");
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
                endpoints.push(ep);
            }

            // Send local infos to main
            info_tx
                .send(ThreadLocalInfos {
                    thread_idx: d,
                    infos: local_infos,
                })
                .unwrap();

            // Receive remote infos from main and connect
            let remote_infos = peer_rx.recv().expect("Failed to receive remote infos");
            assert_eq!(remote_infos.len(), num_eps);
            for (i, ep) in endpoints.iter_mut().enumerate() {
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
                .expect("Failed to connect daemon endpoint");
            }

            barrier.wait();

            // Daemon event loop
            let mut store = ShardedStore::new(key_range, num_daemons as u64, d as u64);
            run_direct_daemon(&ctx, &endpoints, &mut store, d, num_daemons, &stop);
        }));
    }

    // Spawn client threads
    let mut client_handles: Vec<std::thread::JoinHandle<Vec<BatchRecord>>> =
        Vec::with_capacity(num_clients);
    for c in 0..num_clients {
        let info_tx = info_tx.clone();
        let peer_rx = peer_rxs_iter.remove(0);
        let pattern = patterns[c].clone();
        let stop = stop_flag.clone();
        let barrier = ready_barrier.clone();
        let batch_size = cli.batch_size;
        let bs = bench_start;
        let core = client_cores.get(c).copied();
        let my_rank = rank;

        client_handles.push(std::thread::spawn(move || {
            if let Some(core_id) = core {
                affinity::pin_thread(core_id, &format!("direct-client-{}", c));
            }

            // Create copyrpc context
            let ctx: copyrpc::Context<()> = copyrpc::ContextBuilder::new()
                .device_index(device_index)
                .port(port)
                .srq_config(mlx5::srq::SrqConfig {
                    max_wr: 16384,
                    max_sge: 1,
                })
                .cq_size(4096)
                .build()
                .expect("Failed to create copyrpc context for client");

            let ep_config = copyrpc::EndpointConfig {
                send_ring_size: ring_size,
                recv_ring_size: ring_size,
                ..Default::default()
            };

            // Create S*D endpoints: [(rank0,d0), (rank0,d1), ..., (rankS-1, dD-1)]
            let num_eps = num_ranks * num_daemons;
            let mut endpoints = Vec::with_capacity(num_eps);
            let mut local_infos = Vec::with_capacity(num_eps);

            for _ in 0..num_eps {
                let ep = ctx
                    .create_endpoint(&ep_config)
                    .expect("Failed to create client endpoint");
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
                endpoints.push(ep);
            }

            // Send local infos to main
            info_tx
                .send(ThreadLocalInfos {
                    thread_idx: num_daemons + c,
                    infos: local_infos,
                })
                .unwrap();

            // Receive remote infos from main and connect
            let remote_infos = peer_rx.recv().expect("Failed to receive remote infos");
            assert_eq!(remote_infos.len(), num_eps);
            for (i, ep) in endpoints.iter_mut().enumerate() {
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
                .expect("Failed to connect client endpoint");
            }

            barrier.wait();

            // Client event loop
            run_direct_client_loop(
                &ctx,
                &endpoints,
                &pattern,
                queue_depth,
                num_daemons,
                my_rank,
                &stop,
                batch_size,
                bs,
            )
        }));
    }
    drop(info_tx);

    // Main thread: collect local infos from all threads
    let mut local_infos_by_thread: Vec<Option<Vec<EndpointConnectionInfo>>> =
        (0..total_threads).map(|_| None).collect();
    for _ in 0..total_threads {
        let tli = info_rx.recv().expect("Failed to receive info from thread");
        local_infos_by_thread[tli.thread_idx] = Some(tli.infos);
    }
    let local_infos_by_thread: Vec<Vec<EndpointConnectionInfo>> = local_infos_by_thread
        .into_iter()
        .map(|o| o.unwrap())
        .collect();

    // MPI exchange and distribute peer infos
    //
    // Send buffer for peer_rank P:
    //   [d0's C infos for P][d1's C infos for P]...[c0's D infos for P][c1's D infos for P]...
    //   Size: (D*C + C*D) * sizeof(EndpointConnectionInfo)
    //
    // Recv buffer from P:
    //   [P_d0's C infos for my rank][P_d1's C infos for my rank]...[P_c0's D infos for my rank][P_c1's D infos for my rank]...
    //
    // Pairing:
    //   My daemon d, ep for (P, client c) ← recv_buf[D*C + c*D + d]  (P's client c → my daemon d)
    //   My client c, ep for (P, daemon d) ← recv_buf[d*C + c]        (P's daemon d → my client c)

    let info_size = crate::daemon::CONNECTION_INFO_SIZE;
    let infos_per_exchange = num_daemons * num_clients + num_clients * num_daemons;

    // Build per-peer remote infos
    // For my_rank: cross-reference local infos directly
    // For other ranks: MPI exchange
    let mut remote_infos_for_thread: Vec<Vec<EndpointConnectionInfo>> =
        (0..total_threads).map(|_| Vec::new()).collect();

    // Pre-allocate remote infos: daemon d has S*C entries, client c has S*D entries
    for slot in remote_infos_for_thread.iter_mut().take(num_daemons) {
        *slot = vec![EndpointConnectionInfo::default(); num_ranks * num_clients];
    }
    for c in 0..num_clients {
        remote_infos_for_thread[num_daemons + c] =
            vec![EndpointConnectionInfo::default(); num_ranks * num_daemons];
    }

    for peer_rank in 0..size {
        let p = peer_rank as usize;

        if peer_rank == rank {
            // Self-rank: cross-reference local infos
            // My daemon d, ep for (my_rank, client c) at index [rank * C + c]
            //   ← my client c's info for (my_rank, daemon d) at index [rank * D + d]
            // My client c, ep for (my_rank, daemon d) at index [rank * D + d]
            //   ← my daemon d's info for (my_rank, client c) at index [rank * C + c]
            for d in 0..num_daemons {
                for c in 0..num_clients {
                    let daemon_ep_idx = rank as usize * num_clients + c;
                    let client_ep_idx = rank as usize * num_daemons + d;
                    // daemon d's ep for (my_rank, c) ← client c's local info for (my_rank, d)
                    remote_infos_for_thread[d][daemon_ep_idx] =
                        local_infos_by_thread[num_daemons + c][client_ep_idx];
                    // client c's ep for (my_rank, d) ← daemon d's local info for (my_rank, c)
                    remote_infos_for_thread[num_daemons + c][client_ep_idx] =
                        local_infos_by_thread[d][daemon_ep_idx];
                }
            }
        } else {
            // Build send buffer for this peer
            let mut send_buf = vec![0u8; infos_per_exchange * info_size];
            let mut offset = 0;

            // First: all daemon infos for peer P
            // daemon d's endpoints for P are at index [P * C + c] for c in 0..C
            for daemon_infos in local_infos_by_thread.iter().take(num_daemons) {
                for c in 0..num_clients {
                    let ep_idx = p * num_clients + c;
                    let bytes = daemon_infos[ep_idx].to_bytes();
                    send_buf[offset..offset + info_size].copy_from_slice(&bytes);
                    offset += info_size;
                }
            }

            // Then: all client infos for peer P
            // client c's endpoints for P are at index [P * D + d] for d in 0..D
            for c in 0..num_clients {
                for d in 0..num_daemons {
                    let ep_idx = p * num_daemons + d;
                    let bytes = local_infos_by_thread[num_daemons + c][ep_idx].to_bytes();
                    send_buf[offset..offset + info_size].copy_from_slice(&bytes);
                    offset += info_size;
                }
            }

            // Exchange with peer
            let recv_buf =
                mpi_util::exchange_bytes(world, rank as i32, peer_rank as i32, &send_buf);

            // Parse recv buffer:
            // [P_d0→my_c0..cC-1][P_d1→my_c0..cC-1]...[P_c0→my_d0..dD-1][P_c1→my_d0..dD-1]...
            //
            // My daemon d, ep for (P, client c) ← recv_buf index: D*C + c*D + d
            // My client c, ep for (P, daemon d) ← recv_buf index: d*C + c
            for (d, daemon_remote) in remote_infos_for_thread
                .iter_mut()
                .enumerate()
                .take(num_daemons)
            {
                for c in 0..num_clients {
                    let recv_idx = num_daemons * num_clients + c * num_daemons + d;
                    let buf_offset = recv_idx * info_size;
                    let info = EndpointConnectionInfo::from_bytes(
                        &recv_buf[buf_offset..buf_offset + info_size],
                    );
                    let daemon_ep_idx = p * num_clients + c;
                    daemon_remote[daemon_ep_idx] = info;
                }
            }

            for c in 0..num_clients {
                for d in 0..num_daemons {
                    let recv_idx = d * num_clients + c;
                    let buf_offset = recv_idx * info_size;
                    let info = EndpointConnectionInfo::from_bytes(
                        &recv_buf[buf_offset..buf_offset + info_size],
                    );
                    let client_ep_idx = p * num_daemons + d;
                    remote_infos_for_thread[num_daemons + c][client_ep_idx] = info;
                }
            }
        }
    }

    // Send remote infos to each thread
    for (t, tx) in peer_txs.iter().enumerate() {
        tx.send(std::mem::take(&mut remote_infos_for_thread[t]))
            .unwrap();
    }

    // Wait for connections
    std::thread::sleep(Duration::from_millis(100));
    world.barrier();
    ready_barrier.wait();

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
        h.join().expect("Thread panicked");
    }

    let client_batches: Vec<Vec<BatchRecord>> = client_handles
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
        "copyrpc_direct",
        rank,
        &client_batches,
        &run_boundaries,
        num_daemons as u32,
        num_clients as u32,
        queue_depth,
        cli.key_range,
    )
}

// === Daemon event loop ===

fn run_direct_daemon(
    ctx: &copyrpc::Context<()>,
    _endpoints: &[copyrpc::Endpoint<()>],
    store: &mut ShardedStore,
    daemon_id: usize,
    num_daemons: usize,
    stop: &AtomicBool,
) {
    let num_daemons_u64 = num_daemons as u64;

    while !stop.load(Ordering::Relaxed) {
        ctx.poll(|(), _data: &[u8]| {
            // Daemon does not issue calls, so this should never be invoked.
        });

        while let Some(handle) = ctx.recv() {
            let req = RemoteRequest::from_bytes(handle.data()).request;

            debug_assert_eq!(
                ShardedStore::owner_of(req.key(), num_daemons_u64) as usize,
                daemon_id
            );

            let resp = handle_local(store, &req);
            let remote_resp = RemoteResponse { response: resp };

            loop {
                match handle.reply(remote_resp.as_bytes()) {
                    Ok(()) => break,
                    Err(copyrpc::error::Error::RingFull) => {
                        ctx.poll(|(), _| {});
                        continue;
                    }
                    Err(_) => break,
                }
            }
        }
    }
}

// === Client event loop ===

#[allow(clippy::too_many_arguments)]
fn run_direct_client_loop(
    ctx: &copyrpc::Context<()>,
    endpoints: &[copyrpc::Endpoint<()>],
    pattern: &[AccessEntry],
    queue_depth: u32,
    num_daemons: usize,
    _my_rank: u32,
    stop_flag: &AtomicBool,
    batch_size: u32,
    bench_start: Instant,
) -> Vec<BatchRecord> {
    let pattern_len = pattern.len();
    let mut pattern_idx = 0usize;

    DIRECT_COMPLETED.with(|c| *c.borrow_mut() = 0);
    let mut sent = 0u64;
    let mut completed_base = 0u64;
    let mut records = Vec::new();
    let mut completed_in_batch = 0u32;

    // Initial fill
    for _ in 0..queue_depth {
        let entry = &pattern[pattern_idx % pattern_len];
        pattern_idx += 1;

        let ep_idx = daemon_endpoint_index(entry, num_daemons);
        let req = make_request(entry);
        let remote_req = RemoteRequest { request: req };

        match endpoints[ep_idx].call(remote_req.as_bytes(), (), 0u64) {
            Ok(_) => {
                sent += 1;
            }
            Err(
                copyrpc::error::CallError::RingFull(_)
                | copyrpc::error::CallError::InsufficientCredit(_),
            ) => {
                ctx.poll(|(), _| {
                    DIRECT_COMPLETED.with(|c| *c.borrow_mut() += 1);
                });
                // Retry once
                match endpoints[ep_idx].call(remote_req.as_bytes(), (), 0u64) {
                    Ok(_) => {
                        sent += 1;
                    }
                    Err(_) => break,
                }
            }
            Err(_) => break,
        }
    }

    while !stop_flag.load(Ordering::Relaxed) {
        ctx.poll(|(), _| {
            DIRECT_COMPLETED.with(|c| *c.borrow_mut() += 1);
        });

        let total_completed = DIRECT_COMPLETED.with(|c| *c.borrow());
        let new = total_completed - completed_base;
        if new > 0 {
            completed_base = total_completed;
            completed_in_batch += new as u32;
            while completed_in_batch >= batch_size {
                completed_in_batch -= batch_size;
                records.push(BatchRecord {
                    elapsed_ns: bench_start.elapsed().as_nanos() as u64,
                    batch_size,
                });
            }
        }

        // Refill pipeline
        while sent - total_completed < queue_depth as u64 {
            let entry = &pattern[pattern_idx % pattern_len];
            pattern_idx += 1;

            let ep_idx = daemon_endpoint_index(entry, num_daemons);
            let req = make_request(entry);
            let remote_req = RemoteRequest { request: req };

            match endpoints[ep_idx].call(remote_req.as_bytes(), (), 0u64) {
                Ok(_) => {
                    sent += 1;
                }
                Err(
                    copyrpc::error::CallError::RingFull(_)
                    | copyrpc::error::CallError::InsufficientCredit(_),
                ) => {
                    break;
                }
                Err(_) => break,
            }
        }
    }
    records
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

fn make_request(entry: &AccessEntry) -> Request {
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

/// Map access entry to daemon endpoint index.
/// endpoints ordered: [rank0_daemon0, rank0_daemon1, ..., rank1_daemon0, ...]
#[inline]
fn daemon_endpoint_index(entry: &AccessEntry, num_daemons: usize) -> usize {
    let target_daemon = (entry.key % num_daemons as u64) as usize;
    entry.rank as usize * num_daemons + target_daemon
}
