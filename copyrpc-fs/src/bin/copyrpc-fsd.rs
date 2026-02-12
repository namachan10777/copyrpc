//! copyrpc-fsd: Filesystem daemon binary.
//!
//! Runs as MPI process. Each rank hosts `daemons_per_node` daemon threads,
//! each with its own ipc::Server and copyrpc endpoints to all remote ranks.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Duration;

use clap::Parser;
use mpi::traits::*;

use copyrpc::{ContextBuilder, EndpointConfig, RemoteEndpointInfo};
use copyrpc_fs::message::{
    FsRequest, FsResponse, RemoteCreateReq, RemoteReadReq, RemoteRequest, RemoteResponse,
    RemoteStatReq, RemoteUnlinkReq, RemoteWriteReq,
};
use copyrpc_fs::routing;
use copyrpc_fs::store::PmemStore;
use mlx5::pd::AccessFlags;

// =============================================================================
// CLI
// =============================================================================

#[derive(Parser, Debug)]
#[command(name = "copyrpc-fsd", about = "copyrpc-fs daemon")]
struct Cli {
    /// Number of daemon threads per MPI rank.
    #[arg(long, default_value_t = 1)]
    daemons_per_node: usize,

    /// Maximum chunk size in bytes (default 4 MiB).
    #[arg(long, default_value_t = 4 << 20)]
    chunk_size: usize,

    /// Pmem backing path (DevDax e.g. /dev/dax0.0, or regular file).
    /// If not set, uses /tmp/copyrpc_fs_<rank>_<daemon>.
    #[arg(long)]
    pmem_path: Option<String>,

    /// Pmem region size in bytes (only for file-backed, default 1 GiB).
    #[arg(long, default_value_t = 1 << 30)]
    pmem_size: usize,

    /// SHM path prefix for ipc servers.
    #[arg(long, default_value = "/copyrpc_fs")]
    shm_prefix: String,

    /// Maximum clients per ipc server.
    #[arg(long, default_value_t = 16)]
    max_clients: u32,

    /// IPC ring depth (queue depth).
    #[arg(long, default_value_t = 8)]
    queue_depth: u32,

    /// IPC extra buffer size per client (must be >= chunk_size + 4096).
    #[arg(long, default_value_t = (4 << 20) + 4096)]
    extra_buffer_size: u32,

    /// RDMA device index.
    #[arg(long, default_value_t = 0)]
    device_index: usize,

    /// RDMA port number.
    #[arg(long, default_value_t = 1)]
    port: u8,

    /// copyrpc ring buffer size.
    #[arg(long, default_value_t = 1 << 20)]
    ring_size: usize,
}

// =============================================================================
// Connection Info (for MPI exchange)
// =============================================================================

#[derive(Clone, Copy, Debug, Default)]
#[repr(C)]
struct EndpointConnectionInfo {
    qp_number: u32,
    packet_sequence_number: u32,
    local_identifier: u16,
    _padding: u16,
    recv_ring_addr: u64,
    recv_ring_rkey: u32,
    _padding2: u32,
    recv_ring_size: u64,
    initial_credit: u64,
}

const CONNECTION_INFO_SIZE: usize = std::mem::size_of::<EndpointConnectionInfo>();

impl EndpointConnectionInfo {
    fn to_bytes(self) -> Vec<u8> {
        unsafe {
            std::slice::from_raw_parts(&self as *const Self as *const u8, CONNECTION_INFO_SIZE)
                .to_vec()
        }
    }

    fn from_bytes(bytes: &[u8]) -> Self {
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

// =============================================================================
// MPI Helpers
// =============================================================================

fn exchange_bytes(
    world: &mpi::topology::SimpleCommunicator,
    rank: i32,
    peer: i32,
    local: &[u8],
) -> Vec<u8> {
    let mut remote = vec![0u8; local.len()];
    if rank < peer {
        world.process_at_rank(peer).send(local);
        world.process_at_rank(peer).receive_into(&mut remote);
    } else {
        world.process_at_rank(peer).receive_into(&mut remote);
        world.process_at_rank(peer).send(local);
    }
    remote
}

// =============================================================================
// Per-Daemon copyrpc Setup (mpsc channels for MPI exchange)
// =============================================================================

struct CopyrpcSetup {
    local_info_tx: std::sync::mpsc::Sender<Vec<EndpointConnectionInfo>>,
    remote_info_rx: std::sync::mpsc::Receiver<Vec<EndpointConnectionInfo>>,
    device_index: usize,
    port: u8,
    ring_size: usize,
    /// Remote ranks this daemon will connect to.
    my_remote_ranks: Vec<u32>,
}

/// Pending operation tracking for copyrpc responses.
enum PendingOp {
    Write {
        token: ipc::RequestToken,
        path_hash: u64,
        chunk_index: u32,
        offset: u32,
        len: u32,
    },
    Read(ipc::RequestToken),
    Create(ipc::RequestToken),
    Stat(ipc::RequestToken),
    Unlink(ipc::RequestToken),
}

impl PendingOp {
    fn into_token(self) -> ipc::RequestToken {
        match self {
            PendingOp::Write { token, .. }
            | PendingOp::Read(token)
            | PendingOp::Create(token)
            | PendingOp::Stat(token)
            | PendingOp::Unlink(token) => token,
        }
    }
}

fn extract_token_from_call_error(
    e: copyrpc::error::CallError<PendingOp>,
) -> Option<ipc::RequestToken> {
    match e {
        copyrpc::error::CallError::RingFull(op)
        | copyrpc::error::CallError::InsufficientCredit(op) => Some(op.into_token()),
        copyrpc::error::CallError::Other(e) => {
            eprintln!("copyrpc call error: {:?}", e);
            None
        }
    }
}

/// Per-client MR info for RDMA to/from client's extra buffer.
struct ClientMrInfo {
    mr: mlx5::pd::MemoryRegion,
}

/// Kind of pending RDMA operation in the async recv handler.
enum PendingRecvRdmaKind {
    Write {
        path_hash: u64,
        chunk_index: u32,
        offset: usize,
        len: usize,
    },
    Read,
}

/// Helper: serialize RemoteResponse to bytes.
fn remote_response_bytes(resp: &RemoteResponse) -> &[u8] {
    unsafe {
        std::slice::from_raw_parts(
            resp as *const RemoteResponse as *const u8,
            std::mem::size_of::<RemoteResponse>(),
        )
    }
}

/// Helper: update file size in metadata if this daemon is the metadata owner.
#[allow(clippy::too_many_arguments)]
fn update_file_size(
    file_metadata: &mut std::collections::HashMap<u64, copyrpc_fs::InodeHeader>,
    store: &PmemStore,
    path_hash: u64,
    chunk_index: u32,
    offset: u64,
    len: u64,
    my_global_id: usize,
    total_daemons: usize,
) {
    if routing::route(path_hash, 0, total_daemons) == my_global_id {
        let cs = store.chunk_size_for(path_hash) as u64;
        let file_end = chunk_index as u64 * cs + offset + len;
        if let Some(meta) = file_metadata.get_mut(&path_hash)
            && file_end > meta.size
        {
            meta.size = file_end;
        }
    }
}

// =============================================================================
// Daemon Thread
// =============================================================================

#[allow(clippy::too_many_arguments)]
fn run_daemon(
    daemon_id: usize,
    my_rank: u32,
    total_daemons: usize,
    daemons_per_node: usize,
    max_clients: u32,
    mut server: ipc::Server<FsRequest, FsResponse>,
    mut store: PmemStore,
    copyrpc_setup: Option<CopyrpcSetup>,
    stop_flag: &AtomicBool,
    ready_barrier: &Barrier,
) {
    // --- copyrpc setup (multi-node only) ---
    let mut copyrpc_ctx = None;
    let mut copyrpc_endpoints = Vec::new();
    let mut remote_rank_to_ep_idx = std::collections::HashMap::new();

    if let Some(setup) = copyrpc_setup {
        let ctx: copyrpc::Context<PendingOp> = ContextBuilder::new()
            .device_index(setup.device_index)
            .port(setup.port)
            .build()
            .expect("Failed to create copyrpc context");

        let ep_config = EndpointConfig {
            send_ring_size: setup.ring_size,
            recv_ring_size: setup.ring_size,
            ..EndpointConfig::default()
        };

        // Create one endpoint per remote rank
        let mut local_infos = Vec::with_capacity(setup.my_remote_ranks.len());
        let mut endpoints = Vec::with_capacity(setup.my_remote_ranks.len());

        for _ in &setup.my_remote_ranks {
            let ep = ctx.create_endpoint(&ep_config).expect("create_endpoint");
            let (info, lid, _port) = ep.local_info(ctx.lid(), ctx.port());
            local_infos.push(EndpointConnectionInfo {
                qp_number: info.qp_number,
                packet_sequence_number: 0,
                local_identifier: lid,
                _padding: 0,
                recv_ring_addr: info.recv_ring_addr,
                recv_ring_rkey: info.recv_ring_rkey,
                _padding2: 0,
                recv_ring_size: info.recv_ring_size,
                initial_credit: info.initial_credit,
            });
            endpoints.push(ep);
        }

        // Send local infos to main thread for MPI exchange
        setup
            .local_info_tx
            .send(local_infos)
            .expect("send local_infos");

        // Receive remote infos from main thread
        let remote_infos = setup.remote_info_rx.recv().expect("recv remote_infos");

        // Connect endpoints with raw RDMA enabled
        for (i, (ep, remote_info)) in endpoints.iter_mut().zip(remote_infos.iter()).enumerate() {
            let remote = RemoteEndpointInfo {
                qp_number: remote_info.qp_number,
                packet_sequence_number: remote_info.packet_sequence_number,
                local_identifier: remote_info.local_identifier,
                recv_ring_addr: remote_info.recv_ring_addr,
                recv_ring_rkey: remote_info.recv_ring_rkey,
                recv_ring_size: remote_info.recv_ring_size,
                initial_credit: remote_info.initial_credit,
            };
            ep.connect_ex(&remote, 0, setup.port, true)
                .expect("connect");
            remote_rank_to_ep_idx.insert(setup.my_remote_ranks[i], i);
        }

        copyrpc_endpoints = endpoints;
        copyrpc_ctx = Some(ctx);
    }

    // --- QPN → endpoint_idx mapping (for recv handler) ---
    let qpn_to_ep_idx: std::collections::HashMap<u32, usize> = copyrpc_endpoints
        .iter()
        .enumerate()
        .map(|(i, ep)| (ep.qpn(), i))
        .collect();

    // --- Register pmem region as MR for zero-copy RDMA ---
    let pmem_mr = copyrpc_ctx.as_ref().map(|ctx| unsafe {
        ctx.pd()
            .register(
                store.region_ptr(),
                store.region_len(),
                AccessFlags::LOCAL_WRITE | AccessFlags::REMOTE_WRITE | AccessFlags::REMOTE_READ,
            )
            .expect("Failed to register pmem region MR")
    });

    // --- Client MR registration table ---
    let mut client_mrs: Vec<Option<ClientMrInfo>> = Vec::new();

    // --- File metadata (in-memory, separate from PmemStore data) ---
    let mut file_metadata: std::collections::HashMap<u64, copyrpc_fs::InodeHeader> =
        std::collections::HashMap::new();

    // --- Pre-allocate extra buffer pointers ---
    let mut extra_buf_ptrs: Vec<Option<*mut u8>> = vec![None; max_clients as usize];

    // --- Pending RDMA recv: holds RecvHandle for deferred reply after RDMA completion ---
    struct PendingRecvRdma<'a> {
        recv_handle: copyrpc::RecvHandle<'a, PendingOp>,
        ep_idx: usize,
        kind: PendingRecvRdmaKind,
        resp: RemoteResponse,
    }

    // --- Signal ready ---
    ready_barrier.wait();

    // --- Stable borrow for RecvHandle storage across loop iterations ---
    let copyrpc_ctx_ref = copyrpc_ctx.as_ref();

    let mut pending_recv_rdma: Vec<PendingRecvRdma<'_>> = Vec::new();

    // --- Event loop ---
    let my_global_id = (my_rank as usize) * daemons_per_node + daemon_id;

    while !stop_flag.load(Ordering::Relaxed) {
        // Phase 1: copyrpc poll (process responses from remote daemons)
        if let Some(ctx) = copyrpc_ctx_ref {
            ctx.poll(|pending_op, resp_data| {
                let resp: RemoteResponse = if resp_data.len()
                    >= std::mem::size_of::<RemoteResponse>()
                {
                    unsafe { std::ptr::read_unaligned(resp_data.as_ptr() as *const RemoteResponse) }
                } else {
                    RemoteResponse { status: -1, len: 0 }
                };

                match pending_op {
                    PendingOp::Write {
                        token,
                        path_hash,
                        chunk_index,
                        offset,
                        len,
                    } => {
                        if resp.status == 0 {
                            update_file_size(
                                &mut file_metadata,
                                &store,
                                path_hash,
                                chunk_index,
                                offset as u64,
                                len as u64,
                                my_global_id,
                                total_daemons,
                            );
                            server.reply(token, FsResponse::Ok);
                        } else {
                            server.reply(token, FsResponse::Error { code: resp.status });
                        }
                    }
                    PendingOp::Create(token) | PendingOp::Unlink(token) => {
                        if resp.status == 0 {
                            server.reply(token, FsResponse::Ok);
                        } else {
                            server.reply(token, FsResponse::Error { code: resp.status });
                        }
                    }
                    PendingOp::Read(token) => {
                        if resp.status == 0 {
                            server.reply(token, FsResponse::ReadOk { len: resp.len });
                        } else {
                            server.reply(token, FsResponse::Error { code: resp.status });
                        }
                    }
                    PendingOp::Stat(token) => {
                        if resp_data.len()
                            >= std::mem::size_of::<copyrpc_fs::message::RemoteStatResponse>()
                        {
                            let stat_resp: copyrpc_fs::message::RemoteStatResponse = unsafe {
                                std::ptr::read_unaligned(resp_data.as_ptr()
                                    as *const copyrpc_fs::message::RemoteStatResponse)
                            };
                            if stat_resp.status == 0 {
                                server.reply(
                                    token,
                                    FsResponse::StatOk {
                                        header: stat_resp.header,
                                    },
                                );
                            } else if stat_resp.status == -2 {
                                server.reply(token, FsResponse::NotFound);
                            } else {
                                server.reply(
                                    token,
                                    FsResponse::Error {
                                        code: stat_resp.status,
                                    },
                                );
                            }
                        } else {
                            server.reply(token, FsResponse::Error { code: -1 });
                        }
                    }
                }
            });

            // Phase 1b: Check RDMA completions for deferred recv replies
            pending_recv_rdma.retain_mut(|pending| {
                if copyrpc_endpoints[pending.ep_idx].raw_rdma_pending() == 0 {
                    // RDMA complete — commit write + update file size + reply
                    if let PendingRecvRdmaKind::Write {
                        path_hash,
                        chunk_index,
                        offset,
                        len,
                    } = pending.kind
                    {
                        store.commit_write(path_hash, chunk_index, offset, len);
                        update_file_size(
                            &mut file_metadata,
                            &store,
                            path_hash,
                            chunk_index,
                            offset as u64,
                            len as u64,
                            my_global_id,
                            total_daemons,
                        );
                    }
                    pending
                        .recv_handle
                        .reply(remote_response_bytes(&pending.resp))
                        .ok();
                    false // remove from Vec
                } else {
                    true // keep
                }
            });

            // Phase 1c: copyrpc recv (handle requests from remote daemons)
            while let Some(req) = ctx.recv() {
                let data = req.data();
                if data.len() < std::mem::size_of::<RemoteRequest>() {
                    let resp = RemoteResponse { status: -1, len: 0 };
                    req.reply(remote_response_bytes(&resp))
                        .unwrap_or_else(|e| eprintln!("reply error: {:?}", e));
                    continue;
                }

                let remote_req: RemoteRequest =
                    unsafe { std::ptr::read_unaligned(data.as_ptr() as *const RemoteRequest) };

                let ep_idx = qpn_to_ep_idx.get(&req.qpn()).copied();

                match remote_req {
                    RemoteRequest::Write(write_req) => {
                        // Zero-copy: post RDMA READ into pmem, defer reply
                        if let (Some(idx), Some(mr)) = (ep_idx, &pmem_mr) {
                            let (dest_ptr, _cap) = store.prepare_write(
                                write_req.path_hash,
                                write_req.chunk_index,
                                write_req.offset as usize,
                            );

                            copyrpc_endpoints[idx]
                                .post_rdma_read(
                                    write_req.client_addr,
                                    write_req.client_rkey,
                                    dest_ptr as u64,
                                    mr.lkey(),
                                    write_req.len,
                                )
                                .unwrap_or_else(|e| eprintln!("RDMA READ error: {:?}", e));

                            // Non-blocking: store pending and reply after completion
                            pending_recv_rdma.push(PendingRecvRdma {
                                recv_handle: req,
                                ep_idx: idx,
                                kind: PendingRecvRdmaKind::Write {
                                    path_hash: write_req.path_hash,
                                    chunk_index: write_req.chunk_index,
                                    offset: write_req.offset as usize,
                                    len: write_req.len as usize,
                                },
                                resp: RemoteResponse {
                                    status: 0,
                                    len: write_req.len,
                                },
                            });
                        } else {
                            let resp = RemoteResponse { status: -1, len: 0 };
                            req.reply(remote_response_bytes(&resp)).ok();
                        }
                    }
                    RemoteRequest::Read(read_req) => {
                        // Zero-copy: post RDMA WRITE from pmem, defer reply
                        let (src_ptr, available) = store
                            .ptr_for_chunk_read(
                                read_req.path_hash,
                                read_req.chunk_index,
                                read_req.offset as usize,
                            )
                            .unwrap_or((std::ptr::null_mut(), 0));

                        let actual = available.min(read_req.len as usize);

                        if actual > 0
                            && !src_ptr.is_null()
                            && let (Some(idx), Some(mr)) = (ep_idx, &pmem_mr)
                        {
                            copyrpc_endpoints[idx]
                                .post_rdma_write(
                                    read_req.client_addr,
                                    read_req.client_rkey,
                                    src_ptr as u64,
                                    mr.lkey(),
                                    actual as u32,
                                )
                                .unwrap_or_else(|e| eprintln!("RDMA WRITE error: {:?}", e));

                            pending_recv_rdma.push(PendingRecvRdma {
                                recv_handle: req,
                                ep_idx: idx,
                                kind: PendingRecvRdmaKind::Read,
                                resp: RemoteResponse {
                                    status: 0,
                                    len: actual as u32,
                                },
                            });
                        } else {
                            let resp = RemoteResponse {
                                status: 0,
                                len: actual as u32,
                            };
                            req.reply(remote_response_bytes(&resp)).ok();
                        }
                    }
                    RemoteRequest::Create(create_req) => {
                        store.register_file(create_req.path_hash, create_req.chunk_size as usize);
                        file_metadata.insert(
                            create_req.path_hash,
                            copyrpc_fs::InodeHeader {
                                mode: create_req.mode,
                                chunk_size: create_req.chunk_size,
                                ..Default::default()
                            },
                        );
                        let resp = RemoteResponse { status: 0, len: 0 };
                        req.reply(remote_response_bytes(&resp)).ok();
                    }
                    RemoteRequest::Stat(stat_req) => {
                        match file_metadata.get(&stat_req.path_hash) {
                            Some(header) => {
                                let stat_resp = copyrpc_fs::message::RemoteStatResponse {
                                    status: 0,
                                    header: *header,
                                };
                                let resp_bytes = unsafe {
                                    std::slice::from_raw_parts(
                                        &stat_resp as *const copyrpc_fs::message::RemoteStatResponse
                                            as *const u8,
                                        std::mem::size_of::<copyrpc_fs::message::RemoteStatResponse>(
                                        ),
                                    )
                                };
                                req.reply(resp_bytes).ok();
                            }
                            None => {
                                let stat_resp = copyrpc_fs::message::RemoteStatResponse {
                                    status: -2, // NotFound
                                    header: copyrpc_fs::InodeHeader::default(),
                                };
                                let resp_bytes = unsafe {
                                    std::slice::from_raw_parts(
                                        &stat_resp as *const copyrpc_fs::message::RemoteStatResponse
                                            as *const u8,
                                        std::mem::size_of::<copyrpc_fs::message::RemoteStatResponse>(
                                        ),
                                    )
                                };
                                req.reply(resp_bytes).ok();
                            }
                        }
                    }
                    RemoteRequest::Unlink(unlink_req) => {
                        file_metadata.remove(&unlink_req.path_hash);
                        store.remove(unlink_req.path_hash, 0);
                        let resp = RemoteResponse { status: 0, len: 0 };
                        req.reply(remote_response_bytes(&resp)).ok();
                    }
                }
            }
        }

        // Phase 2: ipc poll + recv (handle local client requests)
        server.poll();

        // Update extra buffer pointers and register MRs for new clients
        let extra_buf_size = server.extra_buffer_size() as usize;
        for (i, slot) in extra_buf_ptrs
            .iter_mut()
            .enumerate()
            .take(max_clients as usize)
        {
            *slot = server.client_extra_buffer(ipc::ClientId(i as u32));
        }
        for (client_idx, ptr_opt) in extra_buf_ptrs.iter().enumerate() {
            while client_mrs.len() <= client_idx {
                client_mrs.push(None);
            }
            if client_mrs[client_idx].is_none()
                && let &Some(buf_ptr) = ptr_opt
                && extra_buf_size > 0
                && let Some(ctx) = copyrpc_ctx_ref
                && let Ok(mr) = unsafe {
                    ctx.pd().register(
                        buf_ptr,
                        extra_buf_size,
                        AccessFlags::LOCAL_WRITE
                            | AccessFlags::REMOTE_WRITE
                            | AccessFlags::REMOTE_READ,
                    )
                }
            {
                client_mrs[client_idx] = Some(ClientMrInfo { mr });
            }
        }

        while let Some(ipc_req) = server.recv() {
            let client_id = ipc_req.client_id();
            let client_idx = client_id.0 as usize;
            let req_data = *ipc_req.data();

            match req_data {
                FsRequest::Write {
                    path_hash,
                    chunk_index,
                    offset,
                    len,
                } => {
                    let target = routing::route(path_hash, chunk_index, total_daemons);

                    if target == my_global_id {
                        // Local write: copy from client's extra buffer data area
                        if let Some(buf_ptr) = extra_buf_ptrs.get(client_idx).copied().flatten() {
                            let data = unsafe {
                                std::slice::from_raw_parts(
                                    buf_ptr.add(copyrpc_fs::message::DATA_AREA_OFFSET),
                                    len as usize,
                                )
                            };
                            store.write(path_hash, chunk_index, offset as usize, data);
                            update_file_size(
                                &mut file_metadata,
                                &store,
                                path_hash,
                                chunk_index,
                                offset as u64,
                                len as u64,
                                my_global_id,
                                total_daemons,
                            );
                        }
                        ipc_req.reply(FsResponse::Ok);
                    } else {
                        // Remote: forward via copyrpc
                        let (target_node, _) = routing::global_to_local(target, daemons_per_node);

                        if let Some(&ep_idx) = remote_rank_to_ep_idx.get(&target_node) {
                            let token = ipc_req.into_token();
                            let client_mr = client_mrs[client_idx].as_ref();
                            let Some(mr_info) = client_mr else {
                                server.reply(token, FsResponse::Error { code: -1 });
                                continue;
                            };

                            let remote_req = RemoteRequest::Write(RemoteWriteReq {
                                path_hash,
                                chunk_index,
                                offset,
                                len,
                                client_rkey: mr_info.mr.rkey(),
                                client_addr: mr_info.mr.addr() as u64
                                    + copyrpc_fs::message::DATA_AREA_OFFSET as u64,
                            });
                            let req_bytes = unsafe {
                                std::slice::from_raw_parts(
                                    &remote_req as *const RemoteRequest as *const u8,
                                    std::mem::size_of::<RemoteRequest>(),
                                )
                            };
                            match copyrpc_endpoints[ep_idx].call(
                                req_bytes,
                                PendingOp::Write {
                                    token,
                                    path_hash,
                                    chunk_index,
                                    offset,
                                    len,
                                },
                                64,
                            ) {
                                Ok(_) => {}
                                Err(e) => {
                                    if let Some(t) = extract_token_from_call_error(e) {
                                        server.reply(t, FsResponse::Error { code: -11 });
                                    }
                                }
                            }
                        } else {
                            ipc_req.reply(FsResponse::Error { code: -2 });
                        }
                    }
                }

                FsRequest::Read {
                    path_hash,
                    chunk_index,
                    offset,
                    len,
                } => {
                    let target = routing::route(path_hash, chunk_index, total_daemons);

                    if target == my_global_id {
                        // Local read
                        if let Some(buf_ptr) = extra_buf_ptrs.get(client_idx).copied().flatten() {
                            let dst = unsafe {
                                std::slice::from_raw_parts_mut(
                                    buf_ptr.add(copyrpc_fs::message::DATA_AREA_OFFSET),
                                    len as usize,
                                )
                            };
                            let actual = store.read(path_hash, chunk_index, offset as usize, dst);
                            ipc_req.reply(FsResponse::ReadOk { len: actual as u32 });
                        } else {
                            ipc_req.reply(FsResponse::Error { code: -1 });
                        }
                    } else {
                        let (target_node, _) = routing::global_to_local(target, daemons_per_node);

                        if let Some(&ep_idx) = remote_rank_to_ep_idx.get(&target_node) {
                            let token = ipc_req.into_token();
                            let client_mr = client_mrs[client_idx].as_ref();
                            let Some(mr_info) = client_mr else {
                                server.reply(token, FsResponse::Error { code: -1 });
                                continue;
                            };

                            let remote_req = RemoteRequest::Read(RemoteReadReq {
                                path_hash,
                                chunk_index,
                                offset,
                                len,
                                client_rkey: mr_info.mr.rkey(),
                                client_addr: mr_info.mr.addr() as u64
                                    + copyrpc_fs::message::DATA_AREA_OFFSET as u64,
                            });
                            let req_bytes = unsafe {
                                std::slice::from_raw_parts(
                                    &remote_req as *const RemoteRequest as *const u8,
                                    std::mem::size_of::<RemoteRequest>(),
                                )
                            };
                            match copyrpc_endpoints[ep_idx].call(
                                req_bytes,
                                PendingOp::Read(token),
                                64,
                            ) {
                                Ok(_) => {}
                                Err(e) => {
                                    if let Some(t) = extract_token_from_call_error(e) {
                                        server.reply(t, FsResponse::Error { code: -11 });
                                    }
                                }
                            }
                        } else {
                            ipc_req.reply(FsResponse::Error { code: -2 });
                        }
                    }
                }

                FsRequest::Stat { path_hash, .. } => {
                    let target = routing::route(path_hash, 0, total_daemons);
                    if target == my_global_id {
                        match file_metadata.get(&path_hash) {
                            Some(header) => {
                                ipc_req.reply(FsResponse::StatOk { header: *header });
                            }
                            None => {
                                ipc_req.reply(FsResponse::NotFound);
                            }
                        }
                    } else {
                        let (target_node, _) = routing::global_to_local(target, daemons_per_node);
                        if let Some(&ep_idx) = remote_rank_to_ep_idx.get(&target_node) {
                            let token = ipc_req.into_token();
                            let remote_req = RemoteRequest::Stat(RemoteStatReq { path_hash });
                            let req_bytes = unsafe {
                                std::slice::from_raw_parts(
                                    &remote_req as *const RemoteRequest as *const u8,
                                    std::mem::size_of::<RemoteRequest>(),
                                )
                            };
                            let resp_size =
                                std::mem::size_of::<copyrpc_fs::message::RemoteStatResponse>();
                            match copyrpc_endpoints[ep_idx].call(
                                req_bytes,
                                PendingOp::Stat(token),
                                resp_size as u64,
                            ) {
                                Ok(_) => {}
                                Err(e) => {
                                    if let Some(t) = extract_token_from_call_error(e) {
                                        server.reply(t, FsResponse::Error { code: -11 });
                                    }
                                }
                            }
                        } else {
                            ipc_req.reply(FsResponse::Error { code: -2 });
                        }
                    }
                }

                FsRequest::Create {
                    path_hash,
                    mode,
                    chunk_size: cs,
                    ..
                } => {
                    let target = routing::route(path_hash, 0, total_daemons);
                    if target == my_global_id {
                        store.register_file(path_hash, cs as usize);
                        file_metadata.insert(
                            path_hash,
                            copyrpc_fs::InodeHeader {
                                mode,
                                chunk_size: cs,
                                ..Default::default()
                            },
                        );
                        ipc_req.reply(FsResponse::Ok);
                    } else {
                        let (target_node, _) = routing::global_to_local(target, daemons_per_node);
                        if let Some(&ep_idx) = remote_rank_to_ep_idx.get(&target_node) {
                            let token = ipc_req.into_token();
                            let remote_req = RemoteRequest::Create(RemoteCreateReq {
                                path_hash,
                                mode,
                                chunk_size: cs,
                            });
                            let req_bytes = unsafe {
                                std::slice::from_raw_parts(
                                    &remote_req as *const RemoteRequest as *const u8,
                                    std::mem::size_of::<RemoteRequest>(),
                                )
                            };
                            match copyrpc_endpoints[ep_idx].call(
                                req_bytes,
                                PendingOp::Create(token),
                                64,
                            ) {
                                Ok(_) => {}
                                Err(e) => {
                                    if let Some(t) = extract_token_from_call_error(e) {
                                        server.reply(t, FsResponse::Error { code: -11 });
                                    }
                                }
                            }
                        } else {
                            ipc_req.reply(FsResponse::Error { code: -2 });
                        }
                    }
                }

                FsRequest::Unlink { path_hash, .. } => {
                    let target = routing::route(path_hash, 0, total_daemons);
                    if target == my_global_id {
                        file_metadata.remove(&path_hash);
                        store.remove(path_hash, 0);
                        ipc_req.reply(FsResponse::Ok);
                    } else {
                        let (target_node, _) = routing::global_to_local(target, daemons_per_node);
                        if let Some(&ep_idx) = remote_rank_to_ep_idx.get(&target_node) {
                            let token = ipc_req.into_token();
                            let remote_req = RemoteRequest::Unlink(RemoteUnlinkReq { path_hash });
                            let req_bytes = unsafe {
                                std::slice::from_raw_parts(
                                    &remote_req as *const RemoteRequest as *const u8,
                                    std::mem::size_of::<RemoteRequest>(),
                                )
                            };
                            match copyrpc_endpoints[ep_idx].call(
                                req_bytes,
                                PendingOp::Unlink(token),
                                64,
                            ) {
                                Ok(_) => {}
                                Err(e) => {
                                    if let Some(t) = extract_token_from_call_error(e) {
                                        server.reply(t, FsResponse::Error { code: -11 });
                                    }
                                }
                            }
                        } else {
                            ipc_req.reply(FsResponse::Error { code: -2 });
                        }
                    }
                }

                FsRequest::Mkdir {
                    path_hash, mode, ..
                } => {
                    let target = routing::route(path_hash, 0, total_daemons);
                    if target == my_global_id {
                        file_metadata.insert(
                            path_hash,
                            copyrpc_fs::InodeHeader {
                                mode: mode | 0o040000, // S_IFDIR
                                ..Default::default()
                            },
                        );
                        ipc_req.reply(FsResponse::Ok);
                    } else {
                        ipc_req.reply(FsResponse::Error { code: -38 }); // ENOSYS
                    }
                }

                FsRequest::Readdir { .. } => {
                    ipc_req.reply(FsResponse::Error { code: -38 }); // ENOSYS
                }
            }
        }
    }
}

// =============================================================================
// Main
// =============================================================================

fn main() {
    let cli = Cli::parse();

    let universe = mpi::initialize_with_threading(mpi::Threading::Funneled);
    let Some((universe, _threading)) = universe else {
        eprintln!("Failed to initialize MPI");
        std::process::exit(1);
    };
    let world = universe.world();
    let rank = world.rank() as u32;
    let size = world.size() as u32;
    let num_daemons = cli.daemons_per_node;
    let total_daemons = num_daemons * size as usize;

    // Validate: extra_buffer_size must hold at least one full chunk + path area
    let data_capacity = cli.extra_buffer_size as usize - copyrpc_fs::message::DATA_AREA_OFFSET;
    if data_capacity < cli.chunk_size {
        eprintln!(
            "FATAL: extra_buffer_size ({}) - PATH_AREA ({}) = {} < chunk_size ({}). \
             Increase --extra-buffer-size to at least {}.",
            cli.extra_buffer_size,
            copyrpc_fs::message::DATA_AREA_OFFSET,
            data_capacity,
            cli.chunk_size,
            cli.chunk_size + copyrpc_fs::message::DATA_AREA_OFFSET,
        );
        std::process::exit(1);
    }

    eprintln!(
        "rank {}/{}: {} daemons/node, {} total",
        rank, size, num_daemons, total_daemons
    );

    // --- Create ipc servers ---
    let shm_paths: Vec<String> = (0..num_daemons)
        .map(|d| format!("{}_{}_{}", cli.shm_prefix, rank, d))
        .collect();

    let mut servers: Vec<Option<ipc::Server<FsRequest, FsResponse>>> = shm_paths
        .iter()
        .map(|path| {
            Some(
                unsafe {
                    ipc::Server::<FsRequest, FsResponse>::create(
                        path,
                        cli.max_clients,
                        cli.queue_depth,
                        cli.extra_buffer_size,
                    )
                }
                .unwrap_or_else(|e| panic!("Failed to create ipc server at {}: {:?}", path, e)),
            )
        })
        .collect();

    // --- Create PmemStores ---
    let mut stores: Vec<Option<PmemStore>> = Vec::new();
    for d in 0..num_daemons {
        let pmem_path = match &cli.pmem_path {
            Some(p) => format!("{}_{}", p, d),
            None => format!("/tmp/copyrpc_fs_{}_{}", rank, d),
        };

        let region: Box<dyn pmem::PmemRegion + Send> = if pmem_path.starts_with("/dev/dax") {
            Box::new(
                unsafe { pmem::DevDaxRegion::open(std::path::Path::new(&pmem_path)) }
                    .unwrap_or_else(|e| panic!("DevDax open {}: {:?}", pmem_path, e)),
            )
        } else {
            Box::new(
                unsafe {
                    pmem::FileRegion::create(std::path::Path::new(&pmem_path), cli.pmem_size)
                }
                .unwrap_or_else(|e| panic!("FileRegion create {}: {:?}", pmem_path, e)),
            )
        };

        stores.push(Some(PmemStore::new(region, cli.chunk_size)));
    }

    // --- copyrpc setup channels (multi-node) ---
    let mut copyrpc_local_rxs = Vec::new();
    let mut copyrpc_remote_txs = Vec::new();
    let mut copyrpc_setups: Vec<Option<CopyrpcSetup>> = Vec::new();

    if size > 1 {
        for _d in 0..num_daemons {
            let my_remote_ranks: Vec<u32> = (0..size).filter(|&r| r != rank).collect();

            let (local_tx, local_rx) = std::sync::mpsc::channel();
            let (remote_tx, remote_rx) = std::sync::mpsc::channel();
            copyrpc_local_rxs.push(local_rx);
            copyrpc_remote_txs.push(remote_tx);
            copyrpc_setups.push(Some(CopyrpcSetup {
                local_info_tx: local_tx,
                remote_info_rx: remote_rx,
                device_index: cli.device_index,
                port: cli.port,
                ring_size: cli.ring_size,
                my_remote_ranks,
            }));
        }
    } else {
        for _ in 0..num_daemons {
            copyrpc_setups.push(None);
        }
    }

    // --- Spawn daemon threads ---
    let stop_flag = Arc::new(AtomicBool::new(false));
    let ready_barrier = Arc::new(Barrier::new(num_daemons + 1));

    let mut daemon_handles = Vec::new();
    for d in 0..num_daemons {
        let server = servers[d].take().unwrap();
        let store = stores[d].take().unwrap();
        let copyrpc_setup = copyrpc_setups[d].take();
        let stop = stop_flag.clone();
        let barrier = ready_barrier.clone();
        let dpn = cli.daemons_per_node;

        let mc = cli.max_clients;
        daemon_handles.push(std::thread::spawn(move || {
            run_daemon(
                d,
                rank,
                total_daemons,
                dpn,
                mc,
                server,
                store,
                copyrpc_setup,
                &stop,
                &barrier,
            );
        }));
    }

    // --- MPI exchange for copyrpc endpoint info ---
    if size > 1 {
        let daemon_local_infos: Vec<Vec<EndpointConnectionInfo>> = copyrpc_local_rxs
            .iter()
            .map(|rx| rx.recv().expect("recv local_infos"))
            .collect();

        let daemon_remote_ranks: Vec<Vec<u32>> = (0..num_daemons)
            .map(|_| (0..size).filter(|&r| r != rank).collect())
            .collect();

        let mut daemon_remote_infos: Vec<Vec<EndpointConnectionInfo>> = (0..num_daemons)
            .map(|d| Vec::with_capacity(daemon_remote_ranks[d].len()))
            .collect();

        for peer_rank in 0..size {
            if peer_rank == rank {
                continue;
            }
            for d in 0..num_daemons {
                let peer_idx = daemon_remote_ranks[d]
                    .iter()
                    .position(|&r| r == peer_rank)
                    .unwrap();
                let send_bytes = daemon_local_infos[d][peer_idx].to_bytes();
                let recv_bytes = exchange_bytes(&world, rank as i32, peer_rank as i32, &send_bytes);
                daemon_remote_infos[d].push(EndpointConnectionInfo::from_bytes(&recv_bytes));
            }
        }

        for (d, tx) in copyrpc_remote_txs.iter().enumerate() {
            tx.send(std::mem::take(&mut daemon_remote_infos[d]))
                .expect("send remote_infos");
        }
    }

    // --- Wait for daemons to be ready ---
    ready_barrier.wait();
    world.barrier();

    eprintln!("rank {}: all daemons ready, serving...", rank);

    // --- Wait indefinitely (until killed) ---
    loop {
        std::thread::sleep(Duration::from_secs(1));
    }
}
