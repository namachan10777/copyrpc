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

    while !stop_flag.load(std::sync::atomic::Ordering::Relaxed) {
        // Discover newly connected clients
        if connected_count < max_clients {
            let new_count = shm_server.connected_count() as usize;
            while connected_count < new_count && connected_count < max_clients {
                ground_queue.push_back(connected_count);
                connected_count += 1;
            }
        }

        // === Phase 1: Poll recv CQ + flush dirty endpoints (like ctx.poll()) ===
        if let Some(ctx) = ctx_ref {
            ctx.poll(|ptrs: SlotPtrs, data: &[u8]| unsafe {

                let req_seq = slot::slot_read_seq(ptrs.req);
                let resp = RemoteResponse::from_bytes(data).response;
                slot::slot_write(ptrs.resp, req_seq, resp.as_bytes());
                (*ground_queue_ptr).push_back(ptrs.slot_idx as usize);
            });
        }

        // 1b. Handle incoming remote requests
        if let Some(ctx) = ctx_ref {
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

        // === Phase 2: Ground Queue Scan ===
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

            let payload = unsafe { slot::slot_read_payload(s.req) };
            let req = Request::from_bytes(payload);

            let target_rank = req.rank();
            if target_rank == my_rank {
                let resp = handle_local(&mut store, &req);
                unsafe { slot::slot_write(s.resp, req_seq, resp.as_bytes()) };
                ground_queue.push_back(slot_idx);
                continue;
            }

            let Some(ctx) = ctx_ref else {
                // No copyrpc: respond with not-found
                let resp = Response::MetaGetNotFound;
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
                Ok(_) => {}
                Err(copyrpc::error::CallError::RingFull(_))
                | Err(copyrpc::error::CallError::InsufficientCredit(_)) => {
                    // Rollback: allow re-read next iteration
                    last_req_seq[slot_idx] = req_seq - 1;
                    ground_queue.push_back(slot_idx);
                    ctx.flush_endpoints();
                }
                Err(copyrpc::error::CallError::Other(_)) => {
                    let resp = Response::MetaGetNotFound;
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
        if let Some(ctx) = ctx_ref {
            if ctx.has_dirty_endpoints() {
                ctx.flush_endpoints();
            }
        }
    }
}
