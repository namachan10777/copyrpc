use std::sync::atomic::{AtomicBool, Ordering};

use crate::message::*;
use crate::qd_sample::{QdCollector, QdSample};
use crate::storage::ShardedStore;

// === Type aliases ===

/// Flux token is now a `usize` index into pending copyrpc recv handle buffer.
pub type DaemonFlux = inproc::Flux<DelegatePayload, usize>;

type CopyrpcCtx = copyrpc::Context<ipc::RequestToken>;

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
    queue_depth: u32,
    mut server: ipc::Server<Request, Response>,
    mut flux: DaemonFlux,
    copyrpc_setup: Option<CopyrpcSetup>,
    stop_flag: &AtomicBool,
    ready_barrier: &std::sync::Barrier,
    qd_sample_interval: Option<u32>,
) -> Vec<QdSample> {
    let mut store = ShardedStore::new(key_range, num_daemons as u64, daemon_id as u64);

    // Setup copyrpc
    let copyrpc_ctx: Option<CopyrpcCtx>;
    let mut copyrpc_endpoints: Vec<copyrpc::Endpoint<ipc::RequestToken>> = Vec::new();
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

    // Borrow copyrpc_ctx for the entire event loop scope
    let ctx_ref = copyrpc_ctx.as_ref();

    // Map target_rank to endpoint index using my_remote_ranks ordering
    let rank_to_ep_index = |target_rank: u32| -> usize {
        my_remote_ranks
            .iter()
            .position(|&r| r == target_rank)
            .expect("target_rank not in my_remote_ranks")
    };

    let num_daemons_u64 = num_daemons as u64;

    // Pending copyrpc recv handles waiting for Flux response
    let mut pending_copyrpc_handles: Vec<Option<copyrpc::RecvHandle<'_, ipc::RequestToken>>> =
        Vec::new();
    let mut free_slots: Vec<usize> = Vec::new();

    // Adaptive ipc poll: skip ipc poll when copyrpc had no responses
    // (clients can't have new requests if no QD slots were freed).
    let mut ipc_poll_skip = 0u32;
    const IPC_POLL_INTERVAL: u32 = 4;

    // Per-client inflight tracking: skip polling clients at max QD
    let max_clients = server.max_clients() as usize;
    let mut inflight = vec![0u32; max_clients];
    let mut skip = vec![false; max_clients];

    // QD sampling
    let mut qd_collector = qd_sample_interval.map(|iv| QdCollector::new(iv, 500_000));
    let mut copyrpc_inflight_count: u32 = 0;

    // Buffer for nested ctx.poll() responses inside flux.poll() closure
    let mut nested_resp_buf: Vec<(ipc::RequestToken, Response)> = Vec::new();

    while !stop_flag.load(Ordering::Relaxed) {
        // === Phase 1: copyrpc poll + recv ===
        // Process RDMA completions first so ipc replies go out before ipc poll,
        // giving clients the best chance to submit new requests.
        let mut copyrpc_resp_count = 0u32;
        if let Some(ctx) = ctx_ref {
            ctx.poll(|token: ipc::RequestToken, data: &[u8]| {
                let resp = RemoteResponse::from_bytes(data).response;
                let cid = token.client_id().0 as usize;
                server.reply(token, resp);
                inflight[cid] -= 1;
                copyrpc_inflight_count -= 1;
                copyrpc_resp_count += 1;
            });

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
                                ctx.poll(|token: ipc::RequestToken, data: &[u8]| {
                                    let resp = RemoteResponse::from_bytes(data).response;
                                    let cid = token.client_id().0 as usize;
                                    server.reply(token, resp);
                                    inflight[cid] -= 1;
                                    copyrpc_inflight_count -= 1;
                                    copyrpc_resp_count += 1;
                                });
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
        }

        // === Phase 2: ipc poll + recv + inline copyrpc send ===
        // Poll ipc when: copyrpc just replied to clients (new QD capacity),
        // or we haven't polled in IPC_POLL_INTERVAL iterations (catch local traffic).
        ipc_poll_skip += 1;
        if copyrpc_resp_count > 0 || ipc_poll_skip >= IPC_POLL_INTERVAL {
            for i in 0..max_clients {
                skip[i] = inflight[i] >= queue_depth;
            }
            server.poll_with_skip(&skip);
            ipc_poll_skip = 0;
        }

        while let Some(handle) = server.recv() {
            let cid = handle.client_id().0 as usize;
            inflight[cid] += 1;
            let req = *handle.data();
            let target_rank = req.rank();

            if target_rank == my_rank {
                debug_assert_eq!(
                    ShardedStore::owner_of(req.key(), num_daemons_u64) as usize,
                    daemon_id
                );
                let resp = handle_local(&mut store, &req);
                handle.reply(resp);
                inflight[cid] -= 1;
            } else if let Some(ctx) = ctx_ref {
                let token = handle.into_token();
                let remote_req = RemoteRequest { request: req };
                let ep_idx = rank_to_ep_index(target_rank);
                let mut pending = token;
                loop {
                    match copyrpc_endpoints[ep_idx].call(remote_req.as_bytes(), pending, 0u64) {
                        Ok(_) => {
                            copyrpc_inflight_count += 1;
                            break;
                        }
                        Err(copyrpc::error::CallError::RingFull(returned)) => {
                            pending = returned;
                            ctx.poll(|token: ipc::RequestToken, data: &[u8]| {
                                let resp = RemoteResponse::from_bytes(data).response;
                                let cid = token.client_id().0 as usize;
                                server.reply(token, resp);
                                inflight[cid] -= 1;
                                copyrpc_inflight_count -= 1;
                            });
                            continue;
                        }
                        Err(_) => break,
                    }
                }
            }
        }

        // === QD sample point ===
        if let Some(ref mut c) = qd_collector {
            c.tick(
                copyrpc_inflight_count,
                flux.pending_count() as u32,
                inflight.iter().sum(),
                0,
            );
        }

        // === Phase 3: Flux poll + recv ===
        flux.poll(|pending_idx: usize, data: DelegatePayload| {
            if let DelegatePayload::Resp(resp) = data {
                if let Some(recv_handle) = pending_copyrpc_handles[pending_idx].take() {
                    free_slots.push(pending_idx);
                    let remote_resp = RemoteResponse { response: resp };
                    if let Some(ctx) = ctx_ref {
                        loop {
                            match recv_handle.reply(remote_resp.as_bytes()) {
                                Ok(()) => break,
                                Err(copyrpc::error::Error::RingFull) => {
                                    ctx.poll(|token: ipc::RequestToken, data: &[u8]| {
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
            }
        });

        // Process nested responses from flux.poll()
        for (token, resp) in nested_resp_buf.drain(..) {
            let cid = token.client_id().0 as usize;
            server.reply(token, resp);
            inflight[cid] -= 1;
            copyrpc_inflight_count -= 1;
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
    }

    qd_collector.map_or_else(Vec::new, |c| c.into_samples())
}
