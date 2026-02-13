//! UCX Active Message backend for benchkv.
//!
//! Architecture: Client → UCX AM → Daemon (direct, no ipc, no Flux)
//! Each client thread has endpoints to all daemons (local + remote).
//! Each daemon thread has endpoints to all clients (for replies).
//!
//! AM protocol:
//! - AM ID 0 (REQUEST): header = UcxNetworkRequest, no body
//! - AM ID 1 (RESPONSE): header = RemoteResponse bytes, no body

use std::cell::RefCell;
use std::ffi::c_void;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use mpi::collective::CommunicatorCollectives;

use crate::Cli;
use crate::affinity;
use crate::message::{RemoteResponse, Request, Response};
use crate::mpi_util;
use crate::parquet_out;
use crate::parquet_out::BatchRecord;
use crate::storage::ShardedStore;
use crate::ucx_am::{UcpContext, UcpEndpoint, UcpWorker};
use crate::workload::AccessEntry;

// === Connection info (fixed-size for MPI exchange) ===

const MAX_UCX_ADDR_SIZE: usize = 1024;

#[repr(C)]
#[derive(Clone)]
struct UcxConnectionInfo {
    addr_len: u32,
    _pad: u32,
    addr: [u8; MAX_UCX_ADDR_SIZE],
}

const UCX_INFO_SIZE: usize = std::mem::size_of::<UcxConnectionInfo>();

impl Default for UcxConnectionInfo {
    fn default() -> Self {
        Self {
            addr_len: 0,
            _pad: 0,
            addr: [0u8; MAX_UCX_ADDR_SIZE],
        }
    }
}

impl UcxConnectionInfo {
    fn from_address(addr: &[u8]) -> Self {
        assert!(addr.len() <= MAX_UCX_ADDR_SIZE);
        let mut info = Self {
            addr_len: addr.len() as u32,
            ..Self::default()
        };
        info.addr[..addr.len()].copy_from_slice(addr);
        info
    }

    fn to_bytes(&self) -> Vec<u8> {
        unsafe {
            std::slice::from_raw_parts(self as *const Self as *const u8, UCX_INFO_SIZE).to_vec()
        }
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        assert!(bytes.len() >= UCX_INFO_SIZE);
        unsafe {
            let mut info = Self::default();
            std::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                &mut info as *mut Self as *mut u8,
                UCX_INFO_SIZE,
            );
            info
        }
    }

    fn address(&self) -> &[u8] {
        &self.addr[..self.addr_len as usize]
    }
}

// === Network message types ===

/// Request sent over UCX AM (header of AM ID 0).
/// Includes sender_id so daemon knows which endpoint to reply through.
#[derive(Clone, Copy)]
#[repr(C)]
struct UcxNetworkRequest {
    sender_id: u32,
    _pad: u32,
    request: Request,
}

impl UcxNetworkRequest {
    fn as_bytes(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                self as *const Self as *const u8,
                std::mem::size_of::<Self>(),
            )
        }
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        assert!(bytes.len() >= std::mem::size_of::<Self>());
        unsafe { std::ptr::read_unaligned(bytes.as_ptr() as *const Self) }
    }
}

// === Thread-local queues ===

thread_local! {
    /// Daemon: received requests from AM callback
    static UCX_RECV_QUEUE: RefCell<Vec<(u32, Request)>> = const { RefCell::new(Vec::new()) };
    /// Client: completed responses counter
    static UCX_COMPLETED: RefCell<u64> = const { RefCell::new(0) };
}

// === AM callbacks ===

/// Daemon AM callback for AM ID 0 (REQUEST).
/// Parses UcxNetworkRequest from header, pushes to thread-local queue.
unsafe extern "C" fn daemon_am_recv_cb(
    _arg: *mut c_void,
    header: *const c_void,
    header_length: usize,
    _data: *mut c_void,
    _length: usize,
    _param: *const ucx_sys::ucp_am_recv_param_t,
) -> ucx_sys::ucs_status_t {
    let header_bytes = unsafe { std::slice::from_raw_parts(header as *const u8, header_length) };
    let ucx_req = UcxNetworkRequest::from_bytes(header_bytes);
    UCX_RECV_QUEUE.with(|q| {
        q.borrow_mut().push((ucx_req.sender_id, ucx_req.request));
    });
    ucx_sys::ucs_status_t_UCS_OK as ucx_sys::ucs_status_t
}

/// Client AM callback for AM ID 1 (RESPONSE).
/// Increments thread-local completed counter.
unsafe extern "C" fn client_am_recv_cb(
    _arg: *mut c_void,
    _header: *const c_void,
    _header_length: usize,
    _data: *mut c_void,
    _length: usize,
    _param: *const ucx_sys::ucp_am_recv_param_t,
) -> ucx_sys::ucs_status_t {
    UCX_COMPLETED.with(|c| *c.borrow_mut() += 1);
    ucx_sys::ucs_status_t_UCS_OK as ucx_sys::ucs_status_t
}

// === Main orchestrator ===

#[allow(clippy::too_many_arguments)]
pub fn run_ucx_am(
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
    let (ranks_on_node, rank_on_node) = crate::mpi_util::node_local_rank(world);
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

    // Channels: threads send their connection info to main
    let (info_tx, info_rx) = std::sync::mpsc::channel::<(usize, UcxConnectionInfo)>();

    // Channels: main sends peer infos to each thread
    let total_threads = num_daemons + num_clients;
    let (peer_txs, peer_rxs): (Vec<_>, Vec<_>) = (0..total_threads)
        .map(|_| std::sync::mpsc::channel::<Vec<UcxConnectionInfo>>())
        .unzip();

    // Shared state
    let stop_flag = Arc::new(AtomicBool::new(false));

    let ready_barrier = Arc::new(Barrier::new(total_threads + 1));

    let key_range = cli.key_range;

    let bench_start = Instant::now();
    let mut daemon_handles = Vec::with_capacity(num_daemons);
    let mut peer_rxs_iter: Vec<_> = peer_rxs.into_iter().collect();

    // Spawn daemon threads
    for d in 0..num_daemons {
        let info_tx = info_tx.clone();
        let peer_rx = peer_rxs_iter.remove(0);
        let stop = stop_flag.clone();
        let barrier = ready_barrier.clone();
        let core = daemon_cores.get(d).copied();

        daemon_handles.push(std::thread::spawn(move || {
            if let Some(core_id) = core {
                affinity::pin_thread(core_id, &format!("ucx-daemon-{}", d));
            }

            let ctx = UcpContext::new().expect("Failed to create UCP context");
            let worker = UcpWorker::new(&ctx).expect("Failed to create UCP worker");
            let addr = worker.address().expect("Failed to get worker address");
            let conn_info = UcxConnectionInfo::from_address(&addr);

            info_tx.send((d, conn_info)).unwrap();

            // Register AM handler for requests (AM ID 0)
            worker
                .set_am_handler(0, Some(daemon_am_recv_cb), std::ptr::null_mut())
                .expect("Failed to set AM handler");

            // Receive peer infos: all client connection infos from all ranks
            let client_infos = peer_rx.recv().unwrap();

            // Create endpoints to all clients (for sending replies)
            let mut client_endpoints = Vec::with_capacity(client_infos.len());
            for info in &client_infos {
                let ep = UcpEndpoint::new(&worker, info.address())
                    .expect("Failed to create UCX endpoint to client");
                client_endpoints.push(ep);
            }

            // Progress to establish connections
            for _ in 0..1000 {
                worker.progress();
            }

            barrier.wait();

            // Daemon event loop
            let mut store = ShardedStore::new(key_range, num_daemons as u64, d as u64);

            while !stop.load(Ordering::Relaxed) {
                worker.progress();

                // Drain received requests
                let requests: Vec<(u32, Request)> =
                    UCX_RECV_QUEUE.with(|q| std::mem::take(&mut *q.borrow_mut()));

                for (sender_id, req) in requests {
                    let resp = handle_local(&mut store, &req);
                    let remote_resp = RemoteResponse { response: resp };

                    // Reply via endpoint to the sender
                    if let Some(ep) = client_endpoints.get(sender_id as usize) {
                        let _ = ep.am_send_nbx(1, remote_resp.as_bytes(), &[]);
                    }
                    // Flush reply and receive new requests to reduce remote latency
                    worker.progress();
                }
            }
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
        let core = client_cores.get(c).copied();
        let my_rank = rank;
        let batch_size = cli.batch_size;
        let bs = bench_start;

        client_handles.push(std::thread::spawn(move || {
            if let Some(core_id) = core {
                affinity::pin_thread(core_id, &format!("ucx-client-{}", c));
            }

            let ctx = UcpContext::new().expect("Failed to create UCP context");
            let worker = UcpWorker::new(&ctx).expect("Failed to create UCP worker");
            let addr = worker.address().expect("Failed to get worker address");
            let conn_info = UcxConnectionInfo::from_address(&addr);

            info_tx.send((num_daemons + c, conn_info)).unwrap();

            // Register AM handler for responses (AM ID 1)
            worker
                .set_am_handler(1, Some(client_am_recv_cb), std::ptr::null_mut())
                .expect("Failed to set AM handler");

            // Receive peer infos: all daemon connection infos from all ranks
            let daemon_infos = peer_rx.recv().unwrap();

            // Create endpoints to all daemons
            let mut daemon_endpoints = Vec::with_capacity(daemon_infos.len());
            for info in &daemon_infos {
                let ep = UcpEndpoint::new(&worker, info.address())
                    .expect("Failed to create UCX endpoint to daemon");
                daemon_endpoints.push(ep);
            }

            // Progress to establish connections
            for _ in 0..1000 {
                worker.progress();
            }

            barrier.wait();

            // Client event loop
            run_ucx_client_loop(
                &worker,
                &daemon_endpoints,
                &pattern,
                queue_depth,
                num_daemons,
                size as usize,
                my_rank,
                c as u32,
                num_clients as u32,
                &stop,
                batch_size,
                bs,
            )
        }));
    }
    drop(info_tx);

    // Main thread: collect local infos
    let mut local_infos: Vec<Option<UcxConnectionInfo>> =
        (0..total_threads).map(|_| None).collect();
    for _ in 0..total_threads {
        let (idx, info) = info_rx.recv().expect("Failed to receive info from thread");
        local_infos[idx] = Some(info);
    }
    let local_infos: Vec<UcxConnectionInfo> = local_infos.into_iter().map(|o| o.unwrap()).collect();

    // MPI exchange
    let local_bytes: Vec<u8> = local_infos.iter().flat_map(|i| i.to_bytes()).collect();

    let mut all_rank_infos: Vec<Vec<UcxConnectionInfo>> = Vec::with_capacity(size as usize);
    for r in 0..size {
        if r == rank {
            all_rank_infos.push(local_infos.clone());
        } else {
            let remote_bytes = mpi_util::exchange_bytes(world, rank as i32, r as i32, &local_bytes);
            let mut infos = Vec::with_capacity(total_threads);
            for t in 0..total_threads {
                let offset = t * UCX_INFO_SIZE;
                infos.push(UcxConnectionInfo::from_bytes(
                    &remote_bytes[offset..offset + UCX_INFO_SIZE],
                ));
            }
            all_rank_infos.push(infos);
        }
    }

    // Distribute peer infos to each thread
    // Daemon d needs: all client infos from all ranks
    // Ordered as: [rank0_client0, rank0_client1, ..., rank1_client0, ...]
    for peer_tx in peer_txs.iter().take(num_daemons) {
        let mut peer_infos = Vec::new();
        for rank_infos in &all_rank_infos {
            for client_info in rank_infos.iter().skip(num_daemons).take(num_clients) {
                peer_infos.push(client_info.clone());
            }
        }
        peer_tx.send(peer_infos).unwrap();
    }

    // Client c needs: all daemon infos from all ranks
    // Ordered as: [rank0_daemon0, rank0_daemon1, ..., rank1_daemon0, ...]
    for c in 0..num_clients {
        let mut peer_infos = Vec::new();
        for rank_infos in &all_rank_infos {
            for daemon_info in rank_infos.iter().take(num_daemons) {
                peer_infos.push(daemon_info.clone());
            }
        }
        peer_txs[num_daemons + c].send(peer_infos).unwrap();
    }

    // Wait for threads
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

    let peak_process_rss_kb = crate::memstat::report_peak_process_memory(world, rank, "ucx_am");

    parquet_out::rows_from_batches(
        "ucx_am",
        rank,
        &client_batches,
        &run_boundaries,
        num_daemons as u32,
        num_clients as u32,
        queue_depth,
        cli.key_range,
        peak_process_rss_kb,
    )
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

/// Client event loop: pipeline requests to all daemons via UCX AM.
/// daemon_endpoints ordered: [rank0_daemon0, rank0_daemon1, ..., rank1_daemon0, ...]
#[allow(clippy::too_many_arguments)]
fn run_ucx_client_loop(
    worker: &UcpWorker,
    daemon_endpoints: &[UcpEndpoint],
    pattern: &[AccessEntry],
    queue_depth: u32,
    num_daemons: usize,
    _num_ranks: usize,
    my_rank: u32,
    my_client_id: u32,
    num_clients: u32,
    stop_flag: &AtomicBool,
    batch_size: u32,
    bench_start: Instant,
) -> Vec<BatchRecord> {
    let pattern_len = pattern.len();
    let mut pattern_idx = 0usize;

    // sender_id encodes: rank * num_clients + client_id
    let sender_id = my_rank * num_clients + my_client_id;

    UCX_COMPLETED.with(|c| *c.borrow_mut() = 0);
    let mut sent = 0u64;
    let mut completed_base = 0u64;
    let mut records = Vec::new();
    let mut completed_in_batch = 0u32;

    // Initial fill
    for _ in 0..queue_depth {
        let entry = &pattern[pattern_idx % pattern_len];
        pattern_idx += 1;

        let target_ep_idx = daemon_endpoint_index(entry, num_daemons);
        let req = make_request(entry);
        let ucx_req = UcxNetworkRequest {
            sender_id,
            _pad: 0,
            request: req,
        };

        if daemon_endpoints[target_ep_idx]
            .am_send_nbx(0, ucx_req.as_bytes(), &[])
            .is_ok()
        {
            sent += 1;
        } else {
            // If send fails (non-inline), progress and retry once
            worker.progress();
            if daemon_endpoints[target_ep_idx]
                .am_send_nbx(0, ucx_req.as_bytes(), &[])
                .is_ok()
            {
                sent += 1;
            } else {
                break;
            }
        }
    }

    while !stop_flag.load(Ordering::Relaxed) {
        worker.progress();

        let total_completed = UCX_COMPLETED.with(|c| *c.borrow());
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

            let target_ep_idx = daemon_endpoint_index(entry, num_daemons);
            let req = make_request(entry);
            let ucx_req = UcxNetworkRequest {
                sender_id,
                _pad: 0,
                request: req,
            };

            if daemon_endpoints[target_ep_idx]
                .am_send_nbx(0, ucx_req.as_bytes(), &[])
                .is_ok()
            {
                sent += 1;
            } else {
                break;
            }
        }
    }

    records
}

/// Map access entry to daemon endpoint index.
/// daemon_endpoints ordered: [rank0_daemon0, rank0_daemon1, ..., rank1_daemon0, ...]
#[inline]
fn daemon_endpoint_index(entry: &AccessEntry, num_daemons: usize) -> usize {
    let target_daemon = (entry.key % num_daemons as u64) as usize;
    entry.rank as usize * num_daemons + target_daemon
}
