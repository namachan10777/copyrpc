use std::ptr;
use std::sync::atomic::AtomicBool;

use crate::message::*;
use crate::qd_sample::{QdCollector, QdSample};
use crate::storage::ShardedStore;

// === Type aliases ===

pub type DaemonFlux = inproc::Flux<DelegatePayload, usize>;

type CopyrpcCtx = copyrpc::dc::Context<usize>;

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

fn fallback_response(req: &Request) -> Response {
    match req {
        Request::MetaPut { .. } => Response::MetaPutOk,
        Request::MetaGet { .. } => Response::MetaGetNotFound,
    }
}

// === copyrpc ring size auto-adjustment ===

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

pub struct CopyrpcDcSetup {
    pub local_info_tx: std::sync::mpsc::Sender<Vec<DcEndpointConnectionInfo>>,
    pub remote_info_rx: std::sync::mpsc::Receiver<Vec<DcEndpointConnectionInfo>>,
    pub device_index: usize,
    pub port: u8,
    pub ring_size: usize,
    pub my_remote_ranks: Vec<u32>,
}

pub struct CopyrpcSetup {
    pub local_info_tx: std::sync::mpsc::Sender<Vec<EndpointConnectionInfo>>,
    pub remote_info_rx: std::sync::mpsc::Receiver<Vec<EndpointConnectionInfo>>,
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
    _queue_depth: u32,
    server: ipc::Server<Request, Response>,
    mut flux: DaemonFlux,
    copyrpc_setup: Option<CopyrpcDcSetup>,
    stop_flag: &AtomicBool,
    ready_barrier: &std::sync::Barrier,
    qd_sample_interval: Option<u32>,
) -> Vec<QdSample> {
    let mut store = ShardedStore::new(key_range, num_daemons as u64, daemon_id as u64);
    let server = server;

    // Setup copyrpc (DC transport)
    let copyrpc_ctx: Option<Box<CopyrpcCtx>>;
    let mut copyrpc_endpoints: Vec<copyrpc::dc::Endpoint<usize>> = Vec::new();
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

    let rank_to_ep_index = |target_rank: u32| -> usize {
        my_remote_ranks
            .iter()
            .position(|&r| r == target_rank)
            .expect("target_rank not in my_remote_ranks")
    };

    let num_daemons_u64 = num_daemons as u64;

    // Pending incoming copyrpc requests waiting for Flux response
    let mut pending_copyrpc_handles: Vec<Option<copyrpc::dc::RecvHandle<'_, usize>>> = Vec::new();
    let mut free_slots: Vec<usize> = Vec::new();

    // Fixed client slot tracking
    let max_clients = server.max_clients() as usize;
    let mut slot_ptrs: Vec<*mut ClientSlot> = vec![ptr::null_mut(); max_clients];
    let mut connected = vec![false; max_clients];
    let mut in_preparing = vec![false; max_clients];
    let mut inflight = vec![false; max_clients];
    let mut preparing_stack: Vec<usize> = Vec::with_capacity(max_clients);
    let mut inflights_stack: Vec<usize> = Vec::with_capacity(max_clients);

    let mut copyrpc_inflight_count: u32 = 0;

    // QD sampling
    let mut qd_collector = qd_sample_interval.map(|iv| QdCollector::new(iv, 500_000));

    // Adaptive batching knobs
    const POLL_BUDGET: usize = 16;
    const MIN_BATCH_REQS: usize = 8;
    const MAX_BATCH_WAIT_NS: u64 = 3_000;

    while !stop_flag.load(std::sync::atomic::Ordering::Relaxed) {
        // Discover newly connected clients and add their fixed slots to preparing.
        for cid in 0..max_clients {
            if connected[cid] {
                continue;
            }
            let client_id = ipc::ClientId(cid as u32);
            if !server.is_client_connected(client_id) {
                continue;
            }
            if let Some(ptr) = server.client_extra_buffer(client_id) {
                slot_ptrs[cid] = unsafe { slot_from_extra(ptr) };
                connected[cid] = true;
                unsafe { slot_init(slot_ptrs[cid]) };
                if !inflight[cid] && !in_preparing[cid] {
                    in_preparing[cid] = true;
                    preparing_stack.push(cid);
                }
            }
        }

        // Phase 1: process copyrpc completions and incoming remote requests.
        if let Some(ctx) = ctx_ref {
            ctx.poll(|slot_idx: usize, data: &[u8]| {
                if slot_idx >= max_clients || !connected[slot_idx] {
                    return;
                }
                let resp = RemoteResponse::from_bytes(data).response;
                unsafe { slot_complete(slot_ptrs[slot_idx], resp) };

                if inflight[slot_idx] {
                    inflight[slot_idx] = false;
                    if let Some(pos) = inflights_stack.iter().position(|&x| x == slot_idx) {
                        inflights_stack.swap_remove(pos);
                    }
                }
                if !in_preparing[slot_idx] {
                    in_preparing[slot_idx] = true;
                    preparing_stack.push(slot_idx);
                }
                copyrpc_inflight_count = copyrpc_inflight_count.saturating_sub(1);
            });

            while let Some(recv_handle) = ctx.recv() {
                let req = RemoteRequest::from_bytes(recv_handle.data()).request;
                let target_daemon = ShardedStore::owner_of(req.key(), num_daemons_u64) as usize;

                if target_daemon == daemon_id {
                    let resp = handle_local(&mut store, &req);
                    let remote_resp = RemoteResponse { response: resp };
                    loop {
                        match recv_handle.reply(remote_resp.as_bytes()) {
                            Ok(()) => break,
                            Err(copyrpc::error::Error::RingFull) => {
                                ctx.flush_endpoints();
                            }
                            Err(_) => break,
                        }
                    }
                } else {
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

        // Phase 2: poll preparing slots and enqueue remote requests in batches.
        let mut scans = 0usize;
        let mut remote_enqueued = 0usize;
        let start = std::time::Instant::now();

        while scans < POLL_BUDGET
            || (remote_enqueued > 0
                && remote_enqueued < MIN_BATCH_REQS
                && start.elapsed().as_nanos() < MAX_BATCH_WAIT_NS as u128)
        {
            let Some(slot_idx) = preparing_stack.pop() else {
                break;
            };
            in_preparing[slot_idx] = false;
            scans += 1;

            if !connected[slot_idx] {
                continue;
            }

            let slot_ptr = slot_ptrs[slot_idx];
            let Some(req) = (unsafe { slot_try_take_ready(slot_ptr) }) else {
                if !inflight[slot_idx] && !in_preparing[slot_idx] {
                    in_preparing[slot_idx] = true;
                    preparing_stack.push(slot_idx);
                }
                continue;
            };

            let target_rank = req.rank();
            if target_rank == my_rank {
                debug_assert_eq!(
                    ShardedStore::owner_of(req.key(), num_daemons_u64) as usize,
                    daemon_id
                );
                let resp = handle_local(&mut store, &req);
                unsafe { slot_complete(slot_ptr, resp) };

                if !in_preparing[slot_idx] {
                    in_preparing[slot_idx] = true;
                    preparing_stack.push(slot_idx);
                }
                continue;
            }

            let Some(ctx) = ctx_ref else {
                let resp = fallback_response(&req);
                unsafe { slot_complete(slot_ptr, resp) };
                if !in_preparing[slot_idx] {
                    in_preparing[slot_idx] = true;
                    preparing_stack.push(slot_idx);
                }
                continue;
            };

            let remote_req = RemoteRequest { request: req };
            let ep_idx = rank_to_ep_index(target_rank);
            match copyrpc_endpoints[ep_idx].call(remote_req.as_bytes(), slot_idx, 0) {
                Ok(_) => {
                    inflight[slot_idx] = true;
                    inflights_stack.push(slot_idx);
                    copyrpc_inflight_count += 1;
                    remote_enqueued += 1;
                }
                Err(copyrpc::error::CallError::RingFull(_))
                | Err(copyrpc::error::CallError::InsufficientCredit(_)) => {
                    unsafe { slot_restore_ready(slot_ptr) };
                    if !in_preparing[slot_idx] {
                        in_preparing[slot_idx] = true;
                        preparing_stack.push(slot_idx);
                    }
                    ctx.flush_endpoints();
                    break;
                }
                Err(copyrpc::error::CallError::Other(_)) => {
                    let resp = fallback_response(&req);
                    unsafe { slot_complete(slot_ptr, resp) };
                    if !in_preparing[slot_idx] {
                        in_preparing[slot_idx] = true;
                        preparing_stack.push(slot_idx);
                    }
                }
            }
        }

        if remote_enqueued > 0 {
            if let Some(ctx) = ctx_ref {
                ctx.flush_endpoints();
            }
        }

        // Phase 3: Flux processing
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
                        if let Some(ctx) = ctx_ref {
                            ctx.flush_endpoints();
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
                debug_assert_eq!(
                    ShardedStore::owner_of(req.key(), num_daemons_u64) as usize,
                    daemon_id
                );
                let resp = handle_local(&mut store, &req);
                flux.reply(flux_token, DelegatePayload::Resp(resp));
            }
        }

        if let Some(ref mut c) = qd_collector {
            c.tick(
                copyrpc_inflight_count,
                flux.pending_count() as u32,
                inflights_stack.len() as u32,
                preparing_stack.len() as u32,
            );
        }

        if remote_enqueued == 0 && scans == 0 {
            std::hint::spin_loop();
        }
    }

    qd_collector.map_or_else(Vec::new, |c| c.into_samples())
}
