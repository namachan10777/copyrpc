use std::sync::atomic::{AtomicBool, Ordering};

use shm_spsc::ClientId;

use crate::message::*;
use crate::storage::ShardedStore;

// === Type aliases ===

pub type DelegateOnResponse = fn(&mut DelegateUserData, DelegatePayload);
pub type DaemonFlux = thread_channel::Flux<DelegatePayload, DelegateUserData, DelegateOnResponse>;

type CopyrpcOnResponse = fn(CopyrpcOrigin, &[u8]);
type CopyrpcCtx = copyrpc::Context<CopyrpcOrigin, CopyrpcOnResponse>;

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
    pub consumer_addr: u64,
    pub consumer_rkey: u32,
    _padding3: u32,
}

pub const CONNECTION_INFO_SIZE: usize = std::mem::size_of::<EndpointConnectionInfo>();

impl EndpointConnectionInfo {
    pub fn to_bytes(self) -> Vec<u8> {
        unsafe {
            std::slice::from_raw_parts(
                &self as *const Self as *const u8,
                CONNECTION_INFO_SIZE,
            )
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

// === Callbacks ===

pub fn on_delegate_response(user_data: &mut DelegateUserData, data: DelegatePayload) {
    if let DelegatePayload::Resp(resp) = data {
        match user_data {
            DelegateUserData::ClientRequest {
                client_id,
                slot_index,
            } => {
                FLUX_RESPONSES.with(|r| {
                    r.borrow_mut().push(FluxResponseEntry {
                        client_id: *client_id,
                        slot_index: *slot_index,
                        response: resp,
                    });
                });
            }
            DelegateUserData::CopyrpcRecv { pending_id } => {
                COPYRPC_RECV_FLUX_RESPONSES.with(|r| {
                    r.borrow_mut().push(CopyrpcRecvFluxResponse {
                        pending_id: *pending_id,
                        response: resp,
                    });
                });
            }
        }
    }
}

fn on_copyrpc_response(origin: CopyrpcOrigin, data: &[u8]) {
    let remote_resp = RemoteResponse::from_bytes(data);
    COPYRPC_RESPONSES.with(|r| {
        r.borrow_mut().push(CopyrpcResponseEntry {
            origin,
            response: remote_resp.response,
        });
    });
}

// === Helpers ===

fn drain_flux_responses() -> Vec<FluxResponseEntry> {
    FLUX_RESPONSES.with(|r| std::mem::take(&mut *r.borrow_mut()))
}

fn drain_copyrpc_responses() -> Vec<CopyrpcResponseEntry> {
    COPYRPC_RESPONSES.with(|r| std::mem::take(&mut *r.borrow_mut()))
}

fn drain_copyrpc_recv_flux_responses() -> Vec<CopyrpcRecvFluxResponse> {
    COPYRPC_RECV_FLUX_RESPONSES.with(|r| std::mem::take(&mut *r.borrow_mut()))
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

fn reply_flux(flux: &mut DaemonFlux, to: usize, token: u64, payload: DelegatePayload) {
    let mut p = payload;
    loop {
        match flux.reply(to, token, p) {
            Ok(()) => break,
            Err(thread_channel::SendError::Full(v)) => {
                p = v;
                flux.poll();
            }
            Err(_) => break,
        }
    }
}

struct PendingRequest {
    client_id: u32,
    slot_index: u32,
    request: Request,
}

// === copyrpc setup ===

pub struct CopyrpcSetup {
    pub local_info_tx: std::sync::mpsc::Sender<Vec<EndpointConnectionInfo>>,
    pub remote_info_rx: std::sync::mpsc::Receiver<Vec<EndpointConnectionInfo>>,
    pub device_index: usize,
    pub port: u8,
    pub ring_size: usize,
    pub num_remote_ranks: usize,
}

// === Main daemon entry point ===

#[allow(clippy::too_many_arguments)]
pub fn run_daemon(
    daemon_id: usize,
    my_rank: u32,
    num_daemons: usize,
    key_range: u64,
    mut server: shm_spsc::Server<Request, Response>,
    mut flux: DaemonFlux,
    copyrpc_setup: Option<CopyrpcSetup>,
    stop_flag: &AtomicBool,
    ready_barrier: &std::sync::Barrier,
) {
    let mut store = ShardedStore::new(key_range, num_daemons as u64, daemon_id as u64);

    // Setup copyrpc if this is Daemon #0 on multi-node
    let copyrpc_ctx: Option<CopyrpcCtx>;
    let mut copyrpc_endpoints: Vec<copyrpc::Endpoint<CopyrpcOrigin>> = Vec::new();

    if let Some(setup) = copyrpc_setup {
        let ctx: CopyrpcCtx = copyrpc::ContextBuilder::new()
            .device_index(setup.device_index)
            .port(setup.port)
            .srq_config(mlx5::srq::SrqConfig {
                max_wr: 16384,
                max_sge: 1,
            })
            .cq_size(4096)
            .on_response(on_copyrpc_response as CopyrpcOnResponse)
            .build()
            .expect("Failed to create copyrpc context");

        let ep_config = copyrpc::EndpointConfig {
            send_ring_size: setup.ring_size,
            recv_ring_size: setup.ring_size,
            ..Default::default()
        };

        let mut local_infos = Vec::with_capacity(setup.num_remote_ranks);
        for _ in 0..setup.num_remote_ranks {
            let ep = ctx
                .create_endpoint(&ep_config)
                .expect("Failed to create copyrpc endpoint");
            let (info, lid, _) = ep.local_info(ctx.lid(), ctx.port());
            local_infos.push(EndpointConnectionInfo {
                qp_number: info.qp_number,
                packet_sequence_number: 0,
                local_identifier: lid,
                _padding: 0,
                recv_ring_addr: info.recv_ring_addr,
                recv_ring_rkey: info.recv_ring_rkey,
                _padding2: 0,
                recv_ring_size: info.recv_ring_size,
                consumer_addr: info.consumer_addr,
                consumer_rkey: info.consumer_rkey,
                _padding3: 0,
            });
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
                    consumer_addr: r.consumer_addr,
                    consumer_rkey: r.consumer_rkey,
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

    let rank_to_ep = |target_rank: u32| -> usize {
        if target_rank < my_rank {
            target_rank as usize
        } else {
            (target_rank - 1) as usize
        }
    };

    let mut pending_shm: Vec<PendingRequest> = Vec::new();
    let mut remote_queue: Vec<(CopyrpcOrigin, Request, u32)> = Vec::new();
    // Pending copyrpc recv handles awaiting Flux round-trip
    let mut pending_copyrpc_handles: Vec<(
        u32,
        copyrpc::RecvHandle<'_, CopyrpcOrigin, CopyrpcOnResponse>,
    )> = Vec::new();
    let mut next_copyrpc_pending_id: u32 = 0;

    while !stop_flag.load(Ordering::Relaxed) {
        // === Phase 1: Process shm_spsc client requests ===
        pending_shm.clear();
        server.process_batch_deferred(|client_id, slot_idx, req| {
            let target_rank = req.rank();
            let target_daemon = ShardedStore::owner_of(req.key(), num_daemons as u64) as usize;

            if target_rank == my_rank && target_daemon == daemon_id {
                Some(handle_local(&mut store, &req))
            } else {
                pending_shm.push(PendingRequest {
                    client_id: client_id.0,
                    slot_index: slot_idx,
                    request: req,
                });
                None
            }
        });

        // Route non-local requests
        for p in pending_shm.drain(..) {
            let target_rank = p.request.rank();
            let target_daemon =
                ShardedStore::owner_of(p.request.key(), num_daemons as u64) as usize;

            if target_rank == my_rank {
                // Same rank, different daemon shard
                let _ = flux.call(
                    target_daemon,
                    DelegatePayload::Req(p.request),
                    DelegateUserData::ClientRequest {
                        client_id: p.client_id,
                        slot_index: p.slot_index,
                    },
                );
            } else if daemon_id == 0 {
                // Daemon #0: queue for copyrpc
                remote_queue.push((
                    CopyrpcOrigin::LocalClient {
                        client_id: p.client_id,
                        slot_index: p.slot_index,
                    },
                    p.request,
                    target_rank,
                ));
            } else {
                // Delegate to Daemon #0 via Flux (it will copyrpc forward)
                let _ = flux.call(
                    0,
                    DelegatePayload::Req(p.request),
                    DelegateUserData::ClientRequest {
                        client_id: p.client_id,
                        slot_index: p.slot_index,
                    },
                );
            }
        }

        // === Phase 2: Flux poll ===
        flux.poll();

        // === Phase 3: Process Flux received requests ===
        while let Some((from, token, payload)) = flux.try_recv_raw() {
            match payload {
                DelegatePayload::Req(req) => {
                    let target_rank = req.rank();
                    if target_rank == my_rank {
                        // Key is in our shard — handle locally and reply
                        let resp = handle_local(&mut store, &req);
                        reply_flux(&mut flux, from, token, DelegatePayload::Resp(resp));
                    } else if daemon_id == 0 {
                        // Daemon #0: received delegation for remote rank, queue for copyrpc
                        remote_queue.push((
                            CopyrpcOrigin::FluxDelegate {
                                flux_from: from as u32,
                                flux_token: token,
                            },
                            req,
                            target_rank,
                        ));
                    }
                }
                DelegatePayload::Resp(_) => {} // shouldn't happen via try_recv_raw
            }
        }

        // === Phase 4: [Daemon #0] copyrpc ===
        if let Some(ctx) = ctx_ref {
            // 4a. Send queued remote requests via copyrpc
            for (origin, req, target_rank) in remote_queue.drain(..) {
                let remote_req = RemoteRequest { request: req };
                let ep_idx = rank_to_ep(target_rank);
                loop {
                    match copyrpc_endpoints[ep_idx].call(remote_req.as_bytes(), origin.clone()) {
                        Ok(_) => break,
                        Err(copyrpc::error::Error::RingFull) => {
                            ctx.poll();
                            // Drain copyrpc responses to make progress
                            for entry in drain_copyrpc_responses() {
                                complete_copyrpc_response(
                                    entry,
                                    &mut server,
                                    &mut flux,
                                    &mut pending_copyrpc_handles,
                                    ctx,
                                );
                            }
                            continue;
                        }
                        Err(_) => break,
                    }
                }
            }

            // 4b. copyrpc poll
            ctx.poll();

            // 4c. copyrpc recv (incoming requests from remote nodes)
            while let Some(recv_handle) = ctx.recv() {
                let data = recv_handle.data();
                let remote_req = RemoteRequest::from_bytes(&data);
                let req = remote_req.request;
                let target_daemon =
                    ShardedStore::owner_of(req.key(), num_daemons as u64) as usize;

                if target_daemon == daemon_id {
                    // Handle locally
                    let resp = handle_local(&mut store, &req);
                    let remote_resp = RemoteResponse { response: resp };
                    loop {
                        match recv_handle.reply(remote_resp.as_bytes()) {
                            Ok(()) => break,
                            Err(copyrpc::error::Error::RingFull) => {
                                ctx.poll();
                                continue;
                            }
                            Err(_) => break,
                        }
                    }
                } else {
                    // Forward to target daemon via Flux
                    let pending_id = next_copyrpc_pending_id;
                    next_copyrpc_pending_id = next_copyrpc_pending_id.wrapping_add(1);
                    let _ = flux.call(
                        target_daemon,
                        DelegatePayload::Req(req),
                        DelegateUserData::CopyrpcRecv { pending_id },
                    );
                    pending_copyrpc_handles.push((pending_id, recv_handle));
                }
            }

            // 4d. Drain copyrpc on_response results
            for entry in drain_copyrpc_responses() {
                complete_copyrpc_response(
                    entry,
                    &mut server,
                    &mut flux,
                    &mut pending_copyrpc_handles,
                    ctx,
                );
            }

            // 4e. Drain copyrpc recv Flux forwarding responses
            for resp in drain_copyrpc_recv_flux_responses() {
                if let Some(pos) = pending_copyrpc_handles
                    .iter()
                    .position(|(id, _)| *id == resp.pending_id)
                {
                    let (_, handle) = pending_copyrpc_handles.swap_remove(pos);
                    let remote_resp = RemoteResponse {
                        response: resp.response,
                    };
                    loop {
                        match handle.reply(remote_resp.as_bytes()) {
                            Ok(()) => break,
                            Err(copyrpc::error::Error::RingFull) => {
                                ctx.poll();
                                continue;
                            }
                            Err(_) => break,
                        }
                    }
                }
            }
        }

        // === Phase 5: Complete deferred shm_spsc responses ===
        for entry in drain_flux_responses() {
            server.complete_request(ClientId(entry.client_id), entry.slot_index, entry.response);
        }
    }
}

fn complete_copyrpc_response(
    entry: CopyrpcResponseEntry,
    server: &mut shm_spsc::Server<Request, Response>,
    flux: &mut DaemonFlux,
    _pending_copyrpc_handles: &mut Vec<(
        u32,
        copyrpc::RecvHandle<'_, CopyrpcOrigin, CopyrpcOnResponse>,
    )>,
    _ctx: &CopyrpcCtx,
) {
    match entry.origin {
        CopyrpcOrigin::LocalClient {
            client_id,
            slot_index,
        } => {
            server.complete_request(ClientId(client_id), slot_index, entry.response);
        }
        CopyrpcOrigin::FluxDelegate {
            flux_from,
            flux_token,
        } => {
            reply_flux(
                flux,
                flux_from as usize,
                flux_token,
                DelegatePayload::Resp(entry.response),
            );
        }
    }
}
