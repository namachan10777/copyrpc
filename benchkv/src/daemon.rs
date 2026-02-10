use std::sync::atomic::{AtomicBool, Ordering};

use ipc::RequestToken;

use crate::message::*;
use crate::storage::ShardedStore;

// === Type aliases ===

/// Flux token is now a `usize` index into pending copyrpc recv handle buffer.
pub type DelegateOnResponse = fn(usize, DelegatePayload);
pub type DaemonFlux = inproc::Flux<DelegatePayload, usize, DelegateOnResponse>;

type CopyrpcOnResponse = fn(RequestToken, &[u8]);
type CopyrpcCtx = copyrpc::Context<RequestToken, CopyrpcOnResponse>;

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
    pub initial_credit: u64,
}

pub const CONNECTION_INFO_SIZE: usize = std::mem::size_of::<EndpointConnectionInfo>();

impl EndpointConnectionInfo {
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

// === Callbacks ===

pub fn on_delegate_response(pending_idx: usize, data: DelegatePayload) {
    if let DelegatePayload::Resp(resp) = data {
        FLUX_RESPONSES.with(|r| {
            r.borrow_mut().push(FluxResponseEntry {
                pending_idx,
                response: resp,
            });
        });
    }
}

fn on_copyrpc_response(token: RequestToken, data: &[u8]) {
    let remote_resp = RemoteResponse::from_bytes(data);
    COPYRPC_RESPONSES.with(|r| {
        r.borrow_mut().push(CopyrpcResponseEntry {
            token,
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

fn reply_flux(flux: &mut DaemonFlux, token: inproc::ReplyToken, payload: DelegatePayload) {
    flux.reply(token, payload);
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
    mut server: ipc::Server<Request, Response>,
    mut flux: DaemonFlux,
    copyrpc_setup: Option<CopyrpcSetup>,
    stop_flag: &AtomicBool,
    ready_barrier: &std::sync::Barrier,
) {
    let mut store = ShardedStore::new(key_range, num_daemons as u64, daemon_id as u64);

    // Setup copyrpc
    let copyrpc_ctx: Option<CopyrpcCtx>;
    let mut copyrpc_endpoints: Vec<copyrpc::Endpoint<RequestToken>> = Vec::new();
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
            .on_response(on_copyrpc_response as CopyrpcOnResponse)
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
                initial_credit: info.initial_credit,
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

    let mut remote_queue: Vec<(RequestToken, Request, u32)> = Vec::new();

    let num_daemons_u64 = num_daemons as u64;

    // Pending copyrpc recv handles waiting for Flux response
    let mut pending_copyrpc_handles: Vec<
        Option<copyrpc::RecvHandle<'_, RequestToken, CopyrpcOnResponse>>,
    > = Vec::new();
    let mut free_slots: Vec<usize> = Vec::new();

    // Adaptive ipc poll: skip ipc poll when copyrpc had no responses
    // (clients can't have new requests if no QD slots were freed).
    let mut ipc_poll_skip = 0u32;
    const IPC_POLL_INTERVAL: u32 = 4;

    while !stop_flag.load(Ordering::Relaxed) {
        // === Phase 1: copyrpc poll + recv + drain ===
        // Process RDMA completions first so ipc replies go out before ipc poll,
        // giving clients the best chance to submit new requests.
        let mut copyrpc_resp_count = 0u32;
        if let Some(ctx) = ctx_ref {
            ctx.poll();

            // copyrpc recv (incoming requests from remote nodes)
            while let Some(recv_handle) = ctx.recv() {
                let req = RemoteRequest::from_bytes(recv_handle.data()).request;
                let target_daemon = ShardedStore::owner_of(req.key(), num_daemons_u64) as usize;

                if target_daemon == daemon_id {
                    // This daemon owns the key → handle locally
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

            // Drain copyrpc on_response results → reply to ipc clients
            COPYRPC_RESPONSES.with(|r| {
                let mut r = r.borrow_mut();
                copyrpc_resp_count = r.len() as u32;
                for entry in r.drain(..) {
                    server.reply(entry.token, entry.response);
                }
            });
        }

        // === Phase 2: ipc poll + recv (conditional) ===
        // Poll ipc when: copyrpc just replied to clients (new QD capacity),
        // or we haven't polled in IPC_POLL_INTERVAL iterations (catch local traffic).
        ipc_poll_skip += 1;
        if copyrpc_resp_count > 0 || ipc_poll_skip >= IPC_POLL_INTERVAL {
            server.poll();
            ipc_poll_skip = 0;
        }

        while let Some(handle) = server.recv() {
            let req = *handle.data();
            let target_rank = req.rank();

            if target_rank == my_rank {
                debug_assert_eq!(
                    ShardedStore::owner_of(req.key(), num_daemons_u64) as usize,
                    daemon_id
                );
                let resp = handle_local(&mut store, &req);
                handle.reply(resp);
            } else {
                let token = handle.into_token();
                remote_queue.push((token, req, target_rank));
            }
        }

        // === Phase 3: copyrpc send ===
        if let Some(ctx) = ctx_ref {
            for (token, req, target_rank) in remote_queue.drain(..) {
                let remote_req = RemoteRequest { request: req };
                let ep_idx = rank_to_ep_index(target_rank);
                let mut pending = token;
                loop {
                    match copyrpc_endpoints[ep_idx].call(remote_req.as_bytes(), pending, 0u64) {
                        Ok(_) => break,
                        Err(copyrpc::error::CallError::RingFull(returned)) => {
                            pending = returned;
                            ctx.poll();
                            COPYRPC_RESPONSES.with(|r| {
                                for entry in r.borrow_mut().drain(..) {
                                    server.reply(entry.token, entry.response);
                                }
                            });
                            continue;
                        }
                        Err(_) => break,
                    }
                }
            }
        }

        // === Phase 4: Flux poll + recv ===
        flux.poll();

        while let Some((_from, flux_token, payload)) = flux.try_recv_raw() {
            if let DelegatePayload::Req(req) = payload {
                debug_assert_eq!(
                    ShardedStore::owner_of(req.key(), num_daemons_u64) as usize,
                    daemon_id
                );
                let resp = handle_local(&mut store, &req);
                reply_flux(&mut flux, flux_token, DelegatePayload::Resp(resp));
            }
        }

        // === Phase 5: Flux responses → copyrpc reply ===
        FLUX_RESPONSES.with(|r| {
            for entry in r.borrow_mut().drain(..) {
                if let Some(recv_handle) = pending_copyrpc_handles[entry.pending_idx].take() {
                    free_slots.push(entry.pending_idx);
                    let remote_resp = RemoteResponse {
                        response: entry.response,
                    };
                    if let Some(ctx) = ctx_ref {
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
                }
            }
        });
    }
}
