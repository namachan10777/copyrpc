//! eRPC backend for benchkv.
//!
//! Architecture: Client → eRPC → Daemon (direct, no shm_spsc, no Flux)
//! Each client thread has sessions to all daemons (local + remote).
//! Each daemon thread accepts sessions from all clients (local + remote).

use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use mpi::collective::CommunicatorCollectives;
use erpc::{RemoteInfo, Rpc, RpcConfig};
use mlx5::device::DeviceList;

use crate::affinity;
use crate::epoch::EpochCollector;
use crate::message::{RemoteRequest, RemoteResponse, Request, Response};
use crate::mpi_util;
use crate::parquet_out;
use crate::storage::ShardedStore;
use crate::workload::AccessEntry;
use crate::Cli;

// === Connection info ===

#[derive(Clone, Copy, Debug, Default)]
#[repr(C)]
struct ErpcConnectionInfo {
    qpn: u32,
    qkey: u32,
    lid: u16,
    _pad: u16,
}

const INFO_SIZE: usize = std::mem::size_of::<ErpcConnectionInfo>();

impl ErpcConnectionInfo {
    fn to_bytes(self) -> Vec<u8> {
        unsafe { std::slice::from_raw_parts(&self as *const Self as *const u8, INFO_SIZE).to_vec() }
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        assert!(bytes.len() >= INFO_SIZE);
        unsafe {
            let mut info = Self::default();
            std::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                &mut info as *mut Self as *mut u8,
                INFO_SIZE,
            );
            info
        }
    }
}

// === Thread-local completion counter ===

thread_local! {
    static COMPLETED: RefCell<u64> = const { RefCell::new(0) };
}

// === Main orchestrator ===

#[allow(clippy::too_many_arguments)]
pub fn run_erpc(
    cli: &Cli,
    world: &mpi::topology::SimpleCommunicator,
    rank: u32,
    size: u32,
    num_daemons: usize,
    num_clients: usize,
    session_credits: usize,
    req_window: usize,
) -> Vec<parquet_out::BenchRow> {
    let queue_depth = cli.queue_depth;

    // CPU affinity
    let available_cores = affinity::get_available_cores(cli.device_index);
    let (daemon_cores, client_cores) =
        affinity::assign_cores(&available_cores, num_daemons, num_clients, rank, size);

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

    // Channels for info exchange: each thread sends its info to main
    let (info_tx, info_rx) =
        std::sync::mpsc::channel::<(usize, ErpcConnectionInfo)>();

    // Channels for main to send peer infos to each thread
    let total_threads = num_daemons + num_clients;
    let (peer_txs, peer_rxs): (Vec<_>, Vec<_>) = (0..total_threads)
        .map(|_| std::sync::mpsc::channel::<Vec<(usize, ErpcConnectionInfo)>>())
        .unzip();

    // Shared state
    let stop_flag = Arc::new(AtomicBool::new(false));
    let completions: Vec<Arc<AtomicU64>> = (0..num_clients)
        .map(|_| Arc::new(AtomicU64::new(0)))
        .collect();

    // Barrier: all threads + main
    let ready_barrier = Arc::new(Barrier::new(total_threads + 1));

    let device_index = cli.device_index;
    let port = cli.port;
    let key_range = cli.key_range;

    let mut handles = Vec::with_capacity(total_threads);
    let mut peer_rxs_iter: Vec<_> = peer_rxs.into_iter().collect();

    // Spawn daemon threads (indices 0..num_daemons)
    for d in 0..num_daemons {
        let info_tx = info_tx.clone();
        let peer_rx = peer_rxs_iter.remove(0);
        let stop = stop_flag.clone();
        let barrier = ready_barrier.clone();
        let core = daemon_cores.get(d).copied();

        handles.push(std::thread::spawn(move || {
            if let Some(core_id) = core {
                affinity::pin_thread(core_id, &format!("erpc-daemon-{}", d));
            }

            let device_list = DeviceList::list().expect("Failed to list devices");
            let device = device_list.get(device_index).expect("Device not found");
            let ctx = device.open().expect("Failed to open device");

            let config = RpcConfig::default()
                .with_req_window(req_window)
                .with_session_credits(session_credits)
                .with_num_recv_buffers(req_window * 4)
                .with_max_send_wr((req_window * 4) as u32)
                .with_max_recv_wr((req_window * 4) as u32);

            let rpc: Rpc<()> = Rpc::new(&ctx, port, config, |_, _| {})
                .expect("Failed to create daemon Rpc");

            let local = rpc.local_info();
            info_tx
                .send((
                    d,
                    ErpcConnectionInfo {
                        qpn: local.qpn,
                        qkey: local.qkey,
                        lid: local.lid,
                        _pad: 0,
                    },
                ))
                .unwrap();

            // Receive peer infos from main thread
            let peer_infos = peer_rx.recv().unwrap();

            // Create sessions with all clients
            for (_peer_idx, remote_info) in &peer_infos {
                let _session = rpc
                    .create_session(&RemoteInfo {
                        qpn: remote_info.qpn,
                        qkey: remote_info.qkey,
                        lid: remote_info.lid,
                    })
                    .expect("Failed to create daemon session");
            }

            // Handshake polling
            let expected_sessions = peer_infos.len();
            let handshake_start = Instant::now();
            while handshake_start.elapsed() < Duration::from_secs(5) {
                rpc.run_event_loop_once();
                if rpc.active_sessions() >= expected_sessions {
                    break;
                }
                std::hint::spin_loop();
            }

            barrier.wait();

            // Daemon event loop
            let mut store = ShardedStore::new(key_range, num_daemons as u64, d as u64);

            while !stop.load(Ordering::Relaxed) {
                rpc.run_event_loop_once();
                for _ in 0..16 {
                    if let Some(req) = rpc.recv() {
                        let data = req.data(&rpc);
                        let remote_req = RemoteRequest::from_bytes(data);
                        let resp = handle_local(&mut store, &remote_req.request);
                        let remote_resp = RemoteResponse { response: resp };
                        let _ = rpc.reply(&req, remote_resp.as_bytes());
                    } else {
                        break;
                    }
                }
            }

            // Drain
            let drain_deadline = Instant::now() + Duration::from_secs(2);
            while Instant::now() < drain_deadline {
                rpc.run_event_loop_once();
                let mut got_any = false;
                for _ in 0..16 {
                    if let Some(req) = rpc.recv() {
                        got_any = true;
                        let data = req.data(&rpc);
                        let remote_req = RemoteRequest::from_bytes(data);
                        let resp = handle_local(&mut store, &remote_req.request);
                        let remote_resp = RemoteResponse { response: resp };
                        let _ = rpc.reply(&req, remote_resp.as_bytes());
                    } else {
                        break;
                    }
                }
                if !got_any {
                    break;
                }
            }
        }));
    }

    // Spawn client threads (indices num_daemons..total_threads)
    for c in 0..num_clients {
        let info_tx = info_tx.clone();
        let peer_rx = peer_rxs_iter.remove(0);
        let pattern = patterns[c].clone();
        let stop = stop_flag.clone();
        let barrier = ready_barrier.clone();
        let comp = completions[c].clone();
        let core = client_cores.get(c).copied();

        handles.push(std::thread::spawn(move || {
            if let Some(core_id) = core {
                affinity::pin_thread(core_id, &format!("erpc-client-{}", c));
            }

            let device_list = DeviceList::list().expect("Failed to list devices");
            let device = device_list.get(device_index).expect("Device not found");
            let ctx = device.open().expect("Failed to open device");

            let config = RpcConfig::default()
                .with_req_window(req_window)
                .with_session_credits(session_credits)
                .with_num_recv_buffers(req_window * 4)
                .with_max_send_wr((req_window * 4) as u32)
                .with_max_recv_wr((req_window * 4) as u32);

            let rpc: Rpc<()> = Rpc::new(&ctx, port, config, |_, _| {
                COMPLETED.with(|c| *c.borrow_mut() += 1);
            })
            .expect("Failed to create client Rpc");

            let local = rpc.local_info();
            info_tx
                .send((
                    num_daemons + c,
                    ErpcConnectionInfo {
                        qpn: local.qpn,
                        qkey: local.qkey,
                        lid: local.lid,
                        _pad: 0,
                    },
                ))
                .unwrap();

            // Receive peer infos from main thread (all daemons from all ranks)
            let peer_infos = peer_rx.recv().unwrap();

            // Create sessions: peer_infos are ordered as
            // [(rank0_daemon0, info), (rank0_daemon1, info), ..., (rank1_daemon0, info), ...]
            let mut sessions = Vec::with_capacity(peer_infos.len());
            for (_peer_idx, remote_info) in &peer_infos {
                let session = rpc
                    .create_session(&RemoteInfo {
                        qpn: remote_info.qpn,
                        qkey: remote_info.qkey,
                        lid: remote_info.lid,
                    })
                    .expect("Failed to create client session");
                sessions.push(session);
            }

            // Handshake polling
            let handshake_start = Instant::now();
            let expected = sessions.len();
            while handshake_start.elapsed() < Duration::from_secs(5) {
                rpc.run_event_loop_once();
                let connected = sessions
                    .iter()
                    .filter(|s| rpc.is_session_connected(**s))
                    .count();
                if connected >= expected {
                    break;
                }
                std::hint::spin_loop();
            }

            barrier.wait();

            // Client event loop
            run_erpc_client_loop(
                &rpc,
                &sessions,
                &pattern,
                queue_depth,
                num_daemons,
                size as usize,
                &stop,
                &comp,
            );
        }));
    }
    drop(info_tx);

    // Main thread: collect local infos
    let mut local_infos: Vec<Option<ErpcConnectionInfo>> =
        (0..total_threads).map(|_| None).collect();
    for _ in 0..total_threads {
        let (idx, info) = info_rx.recv().expect("Failed to receive info from thread");
        local_infos[idx] = Some(info);
    }
    let local_infos: Vec<ErpcConnectionInfo> =
        local_infos.into_iter().map(|o| o.unwrap()).collect();

    // MPI exchange with all remote ranks
    let local_bytes: Vec<u8> = local_infos.iter().flat_map(|i| i.to_bytes()).collect();

    // Collect all remote rank infos: remote_all_infos[peer_rank] = Vec<ErpcConnectionInfo>
    let mut all_rank_infos: Vec<Vec<ErpcConnectionInfo>> = Vec::with_capacity(size as usize);
    for r in 0..size {
        if r == rank {
            all_rank_infos.push(local_infos.clone());
        } else {
            let remote_bytes = mpi_util::exchange_bytes(world, rank as i32, r as i32, &local_bytes);
            let mut infos = Vec::with_capacity(total_threads);
            for t in 0..total_threads {
                let offset = t * INFO_SIZE;
                infos.push(ErpcConnectionInfo::from_bytes(
                    &remote_bytes[offset..offset + INFO_SIZE],
                ));
            }
            all_rank_infos.push(infos);
        }
    }

    // Distribute peer infos to each thread
    // Daemon d needs: all client infos from all ranks
    for peer_tx in peer_txs.iter().take(num_daemons) {
        let mut peer_infos = Vec::new();
        for (r, rank_infos) in all_rank_infos.iter().enumerate() {
            for c in 0..num_clients {
                let global_idx = r * total_threads + num_daemons + c;
                peer_infos.push((global_idx, rank_infos[num_daemons + c]));
            }
        }
        peer_tx.send(peer_infos).unwrap();
    }

    // Client c needs: all daemon infos from all ranks
    for c in 0..num_clients {
        let mut peer_infos = Vec::new();
        for (r, rank_infos) in all_rank_infos.iter().enumerate() {
            for (d, info) in rank_infos.iter().enumerate().take(num_daemons) {
                let global_idx = r * total_threads + d;
                peer_infos.push((global_idx, *info));
            }
        }
        peer_txs[num_daemons + c].send(peer_infos).unwrap();
    }

    // Wait for all threads to be ready
    std::thread::sleep(Duration::from_millis(100));
    world.barrier();
    ready_barrier.wait();

    // Epoch sampling loop
    let mut all_rows = Vec::new();
    let duration = Duration::from_secs(cli.duration);
    let interval = Duration::from_millis(cli.interval_ms);

    for run in 0..cli.runs {
        for c in &completions {
            c.store(0, Ordering::Release);
        }

        let mut collector = EpochCollector::new(interval);
        let start = Instant::now();

        while start.elapsed() < duration {
            std::thread::sleep(Duration::from_millis(1));
            let mut total_delta = 0u64;
            for c in &completions {
                total_delta += c.swap(0, Ordering::Relaxed);
            }
            if total_delta > 0 {
                collector.record(total_delta);
            }
        }

        collector.finish();
        let steady = collector.steady_state(cli.trim);
        let rows = parquet_out::rows_from_epochs(
            "erpc",
            steady,
            rank,
            num_daemons as u32,
            num_clients as u32,
            queue_depth,
            cli.key_range,
            run,
        );

        if !steady.is_empty() {
            let avg_rps: f64 = rows.iter().map(|r| r.rps).sum::<f64>() / rows.len() as f64;
            eprintln!(
                "  rank {} run {}: avg {:.0} RPS ({} steady epochs)",
                rank,
                run + 1,
                avg_rps,
                steady.len()
            );
        }

        all_rows.extend(rows);
    }

    // Stop
    stop_flag.store(true, Ordering::Release);

    for h in handles {
        h.join().expect("Thread panicked");
    }

    all_rows
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

/// Client event loop: pipeline requests to all daemons via eRPC sessions.
/// sessions are ordered: [rank0_daemon0, rank0_daemon1, ..., rank1_daemon0, ...]
#[allow(clippy::too_many_arguments)]
fn run_erpc_client_loop(
    rpc: &Rpc<()>,
    sessions: &[erpc::SessionHandle],
    pattern: &[AccessEntry],
    queue_depth: u32,
    num_daemons: usize,
    num_ranks: usize,
    stop_flag: &AtomicBool,
    completions: &AtomicU64,
) {
    let pattern_len = pattern.len();
    let mut pattern_idx = 0usize;

    COMPLETED.with(|c| *c.borrow_mut() = 0);
    let mut sent = 0u64;
    let mut completed_base = 0u64;

    // Initial fill
    for _ in 0..queue_depth {
        let entry = &pattern[pattern_idx % pattern_len];
        pattern_idx += 1;

        let target_session_idx = session_index(entry, num_daemons, num_ranks);
        let req = make_request(entry);
        let remote_req = RemoteRequest { request: req };

        if rpc
            .try_call(sessions[target_session_idx], 1, remote_req.as_bytes(), ())
            .is_ok()
        {
            sent += 1;
        } else {
            break;
        }
    }

    while !stop_flag.load(Ordering::Relaxed) {
        rpc.run_event_loop_once();

        let total_completed = COMPLETED.with(|c| *c.borrow());
        let new = total_completed - completed_base;
        if new > 0 {
            completed_base = total_completed;
            completions.fetch_add(new, Ordering::Relaxed);
        }

        // Refill pipeline
        while sent - total_completed < queue_depth as u64 {
            let entry = &pattern[pattern_idx % pattern_len];
            pattern_idx += 1;

            let target_session_idx = session_index(entry, num_daemons, num_ranks);
            let req = make_request(entry);
            let remote_req = RemoteRequest { request: req };

            if rpc
                .try_call(sessions[target_session_idx], 1, remote_req.as_bytes(), ())
                .is_ok()
            {
                sent += 1;
            } else {
                break;
            }
        }
    }

    // Drain
    let drain_deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < drain_deadline {
        rpc.run_event_loop_once();
        let total_completed = COMPLETED.with(|c| *c.borrow());
        if total_completed >= sent {
            break;
        }
    }
}

/// Map access entry to session index.
/// Sessions ordered: [rank0_daemon0, rank0_daemon1, ..., rank1_daemon0, ...]
#[inline]
fn session_index(entry: &AccessEntry, num_daemons: usize, _num_ranks: usize) -> usize {
    let target_daemon = (entry.key % num_daemons as u64) as usize;
    entry.rank as usize * num_daemons + target_daemon
}
