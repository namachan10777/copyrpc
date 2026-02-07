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
        FLUX_RESPONSES.with(|r| {
            r.borrow_mut().push(FluxResponseEntry {
                client_id: user_data.client_id,
                slot_index: user_data.slot_index,
                response: resp,
            });
        });
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
    pub num_daemons: usize,
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

    // Setup copyrpc (all daemons when multi-node)
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

        // Create num_daemons endpoints per remote rank (full mesh)
        let num_endpoints = setup.num_daemons * setup.num_remote_ranks;
        let mut local_infos = Vec::with_capacity(num_endpoints);
        for _ in 0..num_endpoints {
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

    // Map (target_rank, target_daemon) to endpoint index
    let daemon_ep_index = |target_rank: u32, target_daemon: usize| -> usize {
        let rank_offset = if target_rank < my_rank {
            target_rank as usize
        } else {
            (target_rank - 1) as usize
        };
        rank_offset * num_daemons + target_daemon
    };

    let mut pending_shm: Vec<PendingRequest> = Vec::new();
    let mut remote_queue: Vec<(CopyrpcOrigin, Request, u32, usize)> = Vec::new();

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
                // Same rank, different daemon shard → Flux
                let _ = flux.call(
                    target_daemon,
                    DelegatePayload::Req(p.request),
                    DelegateUserData {
                        client_id: p.client_id,
                        slot_index: p.slot_index,
                    },
                );
            } else {
                // Remote rank → copyrpc (all daemons have their own endpoints)
                remote_queue.push((
                    CopyrpcOrigin {
                        client_id: p.client_id,
                        slot_index: p.slot_index,
                    },
                    p.request,
                    target_rank,
                    target_daemon,
                ));
            }
        }

        // === Phase 2: Flux poll ===
        flux.poll();

        // === Phase 3: Process Flux received requests (same-rank only) ===
        while let Some((from, token, payload)) = flux.try_recv_raw() {
            match payload {
                DelegatePayload::Req(req) => {
                    debug_assert_eq!(req.rank(), my_rank);
                    let resp = handle_local(&mut store, &req);
                    reply_flux(&mut flux, from, token, DelegatePayload::Resp(resp));
                }
                DelegatePayload::Resp(_) => {}
            }
        }

        // === Phase 4: copyrpc (all daemons when multi-node) ===
        if let Some(ctx) = ctx_ref {
            // 4a. Send queued remote requests via copyrpc
            for (origin, req, target_rank, target_daemon) in remote_queue.drain(..) {
                let remote_req = RemoteRequest { request: req };
                let ep_idx = daemon_ep_index(target_rank, target_daemon);
                loop {
                    match copyrpc_endpoints[ep_idx].call(remote_req.as_bytes(), origin) {
                        Ok(_) => break,
                        Err(copyrpc::error::Error::RingFull) => {
                            ctx.poll();
                            COPYRPC_RESPONSES.with(|r| {
                                for entry in r.borrow_mut().drain(..) {
                                    server.complete_request(
                                        ClientId(entry.origin.client_id),
                                        entry.origin.slot_index,
                                        entry.response,
                                    );
                                }
                            });
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
                let req = recv_handle.with_data(|data| RemoteRequest::from_bytes(data).request);

                debug_assert_eq!(
                    ShardedStore::owner_of(req.key(), num_daemons as u64) as usize,
                    daemon_id
                );

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
            }

            // 4d. Drain copyrpc on_response results
            COPYRPC_RESPONSES.with(|r| {
                for entry in r.borrow_mut().drain(..) {
                    server.complete_request(
                        ClientId(entry.origin.client_id),
                        entry.origin.slot_index,
                        entry.response,
                    );
                }
            });
        }

        // === Phase 5: Complete deferred shm_spsc responses from Flux ===
        FLUX_RESPONSES.with(|r| {
            for entry in r.borrow_mut().drain(..) {
                server.complete_request(ClientId(entry.client_id), entry.slot_index, entry.response);
            }
        });
    }
}
