//! copyrpc - High-throughput RPC framework using SPSC ring buffers and RDMA WRITE+IMM.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                         Context                                  │
//! │  ┌─────────┐  ┌─────────┐  ┌────────────────────────────────┐   │
//! │  │   CQ    │  │   SRQ   │  │  Endpoint Registry             │   │
//! │  │(MonoCq) │  │ (shared)│  │  FastMap<QPN, Endpoint>        │   │
//! │  └─────────┘  └─────────┘  └────────────────────────────────┘   │
//! │                                                                  │
//! │  recv() → CQE's QPN identifies Endpoint → notify ring data       │
//! └─────────────────────────────────────────────────────────────────┘
//!                     │
//!           ┌─────────┼─────────┐
//!           ▼         ▼         ▼
//!     ┌──────────┐ ┌──────────┐ ┌──────────┐
//!     │ Endpoint │ │ Endpoint │ │ Endpoint │
//!     │  RC QP   │ │  RC QP   │ │  RC QP   │
//!     └──────────┘ └──────────┘ └──────────┘
//! ```
//!
//! - **RC QP**: Reliable 1-to-1 connections
//! - **Shared SRQ**: Context polls CQ, identifies Endpoint by CQE's QPN
//! - **No EndpointID needed**: QPN is sufficient for identification

#![allow(unsafe_op_in_unsafe_fn)]

pub mod encoding;
pub mod error;
pub mod ring;

use std::cell::{Cell, RefCell, UnsafeCell};
use std::io;

use fastmap::FastMap;
use std::marker::PhantomData;
use std::rc::Rc;

use slab::Slab;

use mlx5::cq::{CqConfig, Cqe};
use mlx5::device::Context as Mlx5Context;
use mlx5::emit_wqe;
use mlx5::mono_cq::MonoCq;
use mlx5::pd::{AccessFlags, MemoryRegion, Pd};
use mlx5::qp::{RcQpConfig, RcQpForMonoCqWithSrqAndSqCb};
use mlx5::srq::{Srq, SrqConfig};
use mlx5::transport::IbRemoteQpInfo;
use mlx5::wqe::WqeFlags;

use encoding::{
    ALIGNMENT, FLOW_METADATA_SIZE, HEADER_SIZE, WRAP_MESSAGE_COUNT, decode_flow_metadata,
    decode_header, decode_imm, encode_flow_metadata, encode_header, encode_imm, from_response_id,
    is_response, padded_message_size, to_response_id,
};
use error::{Error, Result};

/// Default ring buffer size (1 MB).
pub const DEFAULT_RING_SIZE: usize = 1 << 20;

/// Default SRQ size.
pub const DEFAULT_SRQ_SIZE: u32 = 1024;

/// Default CQ size.
pub const DEFAULT_CQ_SIZE: i32 = 4096;

/// Endpoint configuration.
#[derive(Debug, Clone)]
pub struct EndpointConfig {
    /// Send ring buffer size.
    pub send_ring_size: usize,
    /// Receive ring buffer size.
    pub recv_ring_size: usize,
    /// RC QP configuration.
    pub qp_config: RcQpConfig,
    /// Maximum response reservation (credit) in bytes.
    /// Default: send_ring_size / 4.
    pub max_resp_reservation: u64,
}

impl Default for EndpointConfig {
    fn default() -> Self {
        Self {
            send_ring_size: DEFAULT_RING_SIZE,
            recv_ring_size: DEFAULT_RING_SIZE,
            qp_config: RcQpConfig {
                max_send_wr: 256,
                max_recv_wr: 0, // Using SRQ
                max_send_sge: 1,
                max_recv_sge: 1,
                max_inline_data: 256,
                enable_scatter_to_cqe: false,
            },
            max_resp_reservation: DEFAULT_RING_SIZE as u64 / 4,
        }
    }
}

/// Remote endpoint information for connection establishment.
#[derive(Debug, Clone)]
pub struct RemoteEndpointInfo {
    /// Remote QP number.
    pub qp_number: u32,
    /// Remote PSN.
    pub packet_sequence_number: u32,
    /// Remote LID (for InfiniBand).
    pub local_identifier: u16,
    /// Remote receive ring buffer address.
    pub recv_ring_addr: u64,
    /// Remote receive ring buffer rkey.
    pub recv_ring_rkey: u32,
    /// Remote receive ring buffer size.
    pub recv_ring_size: u64,
    /// Initial credit (bytes) granted by remote for response reservation.
    pub initial_credit: u64,
}

/// Remote ring buffer information for RDMA WRITE operations.
#[derive(Debug, Clone, Copy)]
pub struct RemoteRingInfo {
    /// Remote buffer virtual address.
    pub addr: u64,
    /// Remote memory region key.
    pub rkey: u32,
    /// Remote buffer size.
    pub size: u64,
}

impl RemoteRingInfo {
    /// Create new remote ring info.
    pub fn new(addr: u64, rkey: u32, size: u64) -> Self {
        Self { addr, rkey, size }
    }
}

/// SRQ entry for tracking receive completions.
#[derive(Debug, Clone, Copy)]
pub struct SrqEntry {
    /// QPN of the QP that posted this receive.
    pub qpn: u32,
}

/// Type alias for the SQ callback used in copyrpc.
type SqCqCallback = Box<dyn Fn(Cqe, SrqEntry)>;

/// Type alias for the RC QP with SRQ used in copyrpc (for MonoCq).
type CopyrpcQp = RcQpForMonoCqWithSrqAndSqCb<SrqEntry, SqCqCallback>;

/// Buffer for send CQE completions.
/// Uses UnsafeCell to avoid RefCell overhead in the hot path.
/// Safety: Single-threaded access guaranteed (Rc, not Send/Sync).
struct SendCqeBuffer {
    entries: UnsafeCell<Vec<(Cqe, SrqEntry)>>,
}

impl SendCqeBuffer {
    fn new() -> Self {
        Self {
            entries: UnsafeCell::new(Vec::new()),
        }
    }

    fn push(&self, cqe: Cqe, entry: SrqEntry) {
        // Safety: Single-threaded access guaranteed by Rc (not Send/Sync).
        unsafe { &mut *self.entries.get() }.push((cqe, entry));
    }
}

/// Internal struct for received messages stored in recv_stack.
struct RecvMessage<U> {
    /// Endpoint that received this message.
    endpoint: Rc<UnsafeCell<EndpointInner<U>>>,
    /// QPN of the endpoint.
    qpn: u32,
    /// Call ID from the message header.
    call_id: u32,
    /// Offset in recv_ring where payload starts.
    data_offset: usize,
    /// Length of the payload.
    data_len: usize,
    /// Response allowance in bytes (from request header piggyback).
    response_allowance: u64,
}

/// RPC context managing multiple endpoints.
///
/// Type parameters:
/// - `U`: User data type associated with pending RPC calls
pub struct Context<U> {
    /// mlx5 device context.
    mlx5_ctx: Mlx5Context,
    /// Protection domain.
    pd: Pd,
    /// Shared receive queue.
    srq: Rc<Srq<SrqEntry>>,
    /// Number of recv buffers currently posted to SRQ.
    srq_posted: Cell<u32>,
    /// Maximum SRQ work requests.
    srq_max_wr: u32,
    /// Send completion queue.
    send_cq: Rc<mlx5::cq::Cq>,
    /// Receive completion queue (MonoCq for SRQ-based completion processing).
    recv_cq: Rc<MonoCq<CopyrpcQp>>,
    /// CQE buffer for send completions.
    send_cqe_buffer: Rc<SendCqeBuffer>,
    /// Registered endpoints by QPN.
    endpoints: UnsafeCell<FastMap<Rc<UnsafeCell<EndpointInner<U>>>>>,
    /// Port number (for connection establishment).
    port: u8,
    /// Local LID.
    lid: u16,
    /// Received messages stack.
    recv_stack: UnsafeCell<Vec<RecvMessage<U>>>,
    /// Marker for U.
    _marker: PhantomData<U>,
}

/// Builder for creating a Context.
pub struct ContextBuilder<U> {
    device_index: usize,
    port: u8,
    srq_config: SrqConfig,
    cq_size: i32,
    _marker: PhantomData<U>,
}

impl<U> Default for ContextBuilder<U> {
    fn default() -> Self {
        Self {
            device_index: 0,
            port: 1,
            srq_config: SrqConfig {
                max_wr: DEFAULT_SRQ_SIZE,
                max_sge: 1,
            },
            cq_size: DEFAULT_CQ_SIZE,
            _marker: PhantomData,
        }
    }
}

impl<U> ContextBuilder<U> {
    /// Create a new context builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the device index.
    pub fn device_index(mut self, index: usize) -> Self {
        self.device_index = index;
        self
    }

    /// Set the port number.
    pub fn port(mut self, port: u8) -> Self {
        self.port = port;
        self
    }

    /// Set the SRQ configuration.
    pub fn srq_config(mut self, config: SrqConfig) -> Self {
        self.srq_config = config;
        self
    }

    /// Set the CQ size.
    pub fn cq_size(mut self, size: i32) -> Self {
        self.cq_size = size;
        self
    }

    /// Build the context.
    pub fn build(self) -> io::Result<Context<U>> {
        let devices = mlx5::device::DeviceList::list()?;
        let device = devices
            .get(self.device_index)
            .ok_or_else(|| io::Error::other("device not found"))?;
        let mlx5_ctx = device.open()?;

        let port_attr = mlx5_ctx.query_port(self.port)?;
        let lid = port_attr.lid;

        let pd = mlx5_ctx.alloc_pd()?;

        let srq: Srq<SrqEntry> = pd.create_srq(&self.srq_config)?;
        let srq_max_wr = self.srq_config.max_wr;
        let srq = Rc::new(srq);

        // Pre-post recv buffers to SRQ
        for _ in 0..srq_max_wr {
            let _ = srq.post_recv(SrqEntry { qpn: 0 }, 0, 0, 0);
        }
        srq.ring_doorbell();

        let send_cq = Rc::new(mlx5_ctx.create_cq(self.cq_size, &CqConfig::default())?);

        // Create CQE buffer for send CQ
        let send_cqe_buffer = Rc::new(SendCqeBuffer::new());

        // Create MonoCq for recv (callback passed to poll at call site)
        let recv_cq = Rc::new(mlx5_ctx.create_mono_cq(self.cq_size, &CqConfig::default())?);

        Ok(Context {
            mlx5_ctx,
            pd,
            srq,
            srq_posted: Cell::new(srq_max_wr),
            srq_max_wr,
            send_cq,
            recv_cq,
            send_cqe_buffer,
            endpoints: UnsafeCell::new(FastMap::new()),
            port: self.port,
            lid,
            recv_stack: UnsafeCell::new(Vec::new()),
            _marker: PhantomData,
        })
    }
}

impl<U> Context<U> {
    /// Create a new context builder.
    pub fn builder() -> ContextBuilder<U> {
        ContextBuilder::new()
    }

    /// Get the local LID.
    pub fn lid(&self) -> u16 {
        self.lid
    }

    /// Get the port number.
    pub fn port(&self) -> u8 {
        self.port
    }

    /// Create a new endpoint.
    pub fn create_endpoint(&self, config: &EndpointConfig) -> io::Result<Endpoint<U>> {
        let inner = EndpointInner::new(
            &self.mlx5_ctx,
            &self.pd,
            &self.srq,
            &self.send_cq,
            &self.recv_cq,
            &self.send_cqe_buffer,
            config,
        )?;

        let qpn = unsafe { &*inner.get() }.qpn();

        unsafe { &mut *self.endpoints.get() }.insert(qpn, inner.clone());

        Ok(Endpoint {
            inner,
            _marker: PhantomData,
        })
    }

    /// Poll for completions and process all pending operations.
    ///
    /// This method:
    /// 1. Polls the receive CQ (MonoCq) with inline callback — no CqeBuffer
    ///    - Requests are pushed to recv_stack
    ///    - Responses invoke the on_response callback
    /// 2. Reposts recv buffers to SRQ
    /// 3. Polls the send CQ for SQ progress tracking
    /// 4. Flushes all endpoints' accumulated data
    pub fn poll(&self, mut on_response: impl FnMut(U, &[u8])) {
        // Split borrows to avoid self-reference in closure
        let recv_cq = &self.recv_cq;
        let endpoints = &self.endpoints;
        let recv_stack = &self.recv_stack;

        // 1. Poll recv CQ (MonoCq) — callback is inlined, no CqeBuffer
        let mut recv_count = 0u32;
        loop {
            let count = recv_cq.poll(|cqe, _entry| {
                // Skip error CQEs
                if cqe.opcode == mlx5::cq::CqeOpcode::ReqErr
                    || cqe.opcode == mlx5::cq::CqeOpcode::RespErr
                {
                    eprintln!(
                        "[recv_cq ERROR] qpn={}, opcode={:?}, syndrome={}",
                        cqe.qp_num, cqe.opcode, cqe.syndrome
                    );
                    return;
                }
                Self::process_cqe_static(cqe, cqe.qp_num, endpoints, &mut on_response, recv_stack);
            });
            if count == 0 {
                break;
            }
            recv_count += count as u32;
        }

        // Early return if no completions at all
        if recv_count > 0 {
            recv_cq.flush();

            // 2. Repost recvs to SRQ when below 2/3 threshold
            let posted = self.srq_posted.get().saturating_sub(recv_count);
            self.srq_posted.set(posted);

            let threshold = self.srq_max_wr * 2 / 3;
            if posted < threshold {
                let to_post = self.srq_max_wr - posted;
                for _ in 0..to_post {
                    let _ = self.srq.post_recv(SrqEntry { qpn: 0 }, 0, 0, 0);
                }
                self.srq.ring_doorbell();
                self.srq_posted.set(self.srq_max_wr);
            }
        }

        // 3. Poll send CQ for SQ progress tracking
        loop {
            let count = self.send_cq.poll();
            if count == 0 {
                break;
            }
        }
        self.send_cq.flush();

        // Clear send CQE buffer (no READ completions to process)
        {
            let entries = unsafe { &mut *self.send_cqe_buffer.entries.get() };
            entries.clear();
        }

        // 4. Flush all endpoints
        for (_, ep) in unsafe { &*self.endpoints.get() }.iter() {
            let ep_ref = unsafe { &*ep.get() };
            let _ = ep_ref.flush();
        }
    }

    /// Get the next received request from the stack.
    ///
    /// Call poll() first to populate the recv_stack.
    /// Returns `None` if no request is available.
    pub fn recv(&self) -> Option<RecvHandle<'_, U>> {
        unsafe { &mut *self.recv_stack.get() }.pop().map(|msg| {
            let data_ptr = unsafe { &*msg.endpoint.get() }.recv_ring.as_ptr();
            let data_ptr = unsafe { data_ptr.add(msg.data_offset) };
            RecvHandle {
                _lifetime: PhantomData,
                endpoint: msg.endpoint,
                qpn: msg.qpn,
                call_id: msg.call_id,
                data_ptr,
                data_len: msg.data_len,
                response_allowance: msg.response_allowance,
            }
        })
    }

    /// Process a single CQE (static version for use in poll closure).
    ///
    /// Each CQE corresponds to one WRITE+IMM which starts with flow_metadata (32B),
    /// followed by message_count messages (or a wrap if message_count == WRAP_MESSAGE_COUNT).
    /// For requests, pushes to recv_stack.
    /// For responses, invokes callback.
    #[inline(always)]
    fn process_cqe_static(
        cqe: Cqe,
        qpn: u32,
        endpoints: &UnsafeCell<FastMap<Rc<UnsafeCell<EndpointInner<U>>>>>,
        on_response: &mut impl FnMut(U, &[u8]),
        recv_stack: &UnsafeCell<Vec<RecvMessage<U>>>,
    ) {
        let endpoints = unsafe { &*endpoints.get() };
        let endpoint = match endpoints.get(qpn) {
            Some(ep) => ep,
            None => return,
        };

        // Decode the immediate value to get the offset delta
        let delta = decode_imm(cqe.imm);

        // Update recv_ring_producer
        {
            let ep = unsafe { &*endpoint.get() };
            let new_producer = ep.recv_ring_producer.get() + delta;
            ep.recv_ring_producer.set(new_producer);
        }

        // Read flow_metadata at current position
        let (ring_credit_return, credit_grant, message_count);
        {
            let ep = unsafe { &*endpoint.get() };
            let pos = ep.last_recv_pos.get();
            let recv_ring_mask = ep.recv_ring.len() as u64 - 1;
            let meta_offset = (pos & recv_ring_mask) as usize;

            unsafe {
                (ring_credit_return, credit_grant, message_count) =
                    decode_flow_metadata(ep.recv_ring[meta_offset..].as_ptr());
            }

            // Update flow control from metadata (accumulate delta)
            ep.remote_consumer_pos
                .set(ep.remote_consumer_pos.get() + ring_credit_return);
            ep.peer_credit_balance
                .set(ep.peer_credit_balance.get() + credit_grant);

            // Advance past metadata
            ep.last_recv_pos.set(pos + FLOW_METADATA_SIZE as u64);
        }

        if message_count == WRAP_MESSAGE_COUNT {
            // Wrap: skip to next ring cycle
            let ep = unsafe { &*endpoint.get() };
            let pos = ep.last_recv_pos.get();
            let recv_ring_len = ep.recv_ring.len() as u64;
            let offset = pos & (recv_ring_len - 1);
            let remaining = recv_ring_len - offset;
            let new_pos = pos + remaining;
            ep.last_recv_pos.set(new_pos);
            return;
        }

        // Process exactly message_count messages
        for _ in 0..message_count {
            let ep = unsafe { &*endpoint.get() };
            let pos = ep.last_recv_pos.get();
            let recv_ring_mask = ep.recv_ring.len() as u64 - 1;
            let header_offset = (pos & recv_ring_mask) as usize;

            let (call_id, piggyback, payload_len) =
                unsafe { decode_header(ep.recv_ring[header_offset..].as_ptr()) };

            let msg_size = padded_message_size(payload_len);
            let new_pos = pos + msg_size;
            ep.last_recv_pos.set(new_pos);

            if is_response(call_id) {
                // Response - invoke callback
                let original_call_id = from_response_id(call_id);
                let user_data =
                    unsafe { &mut *ep.pending_calls.get() }.try_remove(original_call_id as usize);

                if let Some(user_data) = user_data {
                    let data_offset = header_offset + HEADER_SIZE;
                    let data_slice = &ep.recv_ring[data_offset..data_offset + payload_len as usize];
                    on_response(user_data, data_slice);
                }
            } else {
                // Request - piggyback is response_allowance_blocks
                let response_allowance = piggyback as u64 * ALIGNMENT;

                unsafe { &mut *recv_stack.get() }.push(RecvMessage {
                    endpoint: endpoint.clone(),
                    qpn,
                    call_id,
                    data_offset: header_offset + HEADER_SIZE,
                    data_len: payload_len as usize,
                    response_allowance,
                });
            }
        }
    }
}

/// How often to signal a WQE (every N unsignaled WQEs).
/// This prevents SQ exhaustion by allowing completion processing.
const SIGNAL_INTERVAL: u32 = 64;

/// Maximum bytes that can be inlined in a write_imm WQE (inline-only, no SGE).
/// BlueFlame(256) - Ctrl(16) - RDMA(16) = 224 bytes for inline padded.
/// inline_padded_size(220) = (4 + 220 + 15) & !15 = 224. Total WQE = 256B.
const MAX_INLINE_ONLY: usize = 220;

/// Maximum bytes that can be inlined in a hybrid write_imm WQE (inline + SGE).
/// BlueFlame(256) - Ctrl(16) - RDMA(16) - SGE(16) = 208 bytes for inline padded.
/// inline_padded_size(204) = (4 + 204 + 15) & !15 = 208. Total WQE = 256B.
const MAX_INLINE_HYBRID: usize = 204;

/// Internal endpoint state.
struct EndpointInner<U> {
    /// RC Queue Pair (wrapped in Rc<RefCell>).
    qp: Rc<RefCell<CopyrpcQp>>,
    /// Send ring buffer.
    send_ring: Box<[u8]>,
    /// Send ring memory region.
    send_ring_mr: MemoryRegion,
    /// Receive ring buffer.
    recv_ring: Box<[u8]>,
    /// Receive ring memory region.
    recv_ring_mr: MemoryRegion,
    /// Remote receive ring information.
    remote_recv_ring: Cell<Option<RemoteRingInfo>>,
    /// Send ring producer position (virtual).
    send_ring_producer: Cell<u64>,
    /// Receive ring producer position (updated on receive).
    recv_ring_producer: Cell<u64>,
    /// Last received message position (consumer).
    last_recv_pos: Cell<u64>,
    /// Last recv_pos at which we notified the peer via ring_credit_return.
    last_notified_recv_pos: Cell<u64>,
    /// Remote consumer position (accumulated from ring_credit_return piggybacks).
    remote_consumer_pos: Cell<u64>,
    /// Counter for unsignaled WQEs (for periodic signaling).
    unsignaled_count: Cell<u32>,
    /// Pending calls waiting for responses. Slab index is used as call_id.
    pending_calls: UnsafeCell<Slab<U>>,
    /// Flush start position (for WQE batching).
    flush_start_pos: Cell<u64>,
    /// Response reservation (R): credits issued minus responses written (bytes).
    resp_reservation: Cell<u64>,
    /// Maximum response reservation (policy limit).
    max_resp_reservation: u64,
    /// Credit balance from peer (bytes of request we can send).
    peer_credit_balance: Cell<u64>,
    /// Current batch metadata position (virtual). Always valid after connect().
    meta_pos: Cell<u64>,
    /// Number of messages in the current batch.
    batch_message_count: Cell<u32>,
}

impl<U> EndpointInner<U> {
    fn new(
        ctx: &Mlx5Context,
        pd: &Pd,
        srq: &Rc<Srq<SrqEntry>>,
        send_cq: &Rc<mlx5::cq::Cq>,
        recv_cq: &Rc<MonoCq<CopyrpcQp>>,
        send_cqe_buffer: &Rc<SendCqeBuffer>,
        config: &EndpointConfig,
    ) -> io::Result<Rc<UnsafeCell<Self>>> {
        // Allocate ring buffers
        let send_ring_size = config.send_ring_size.next_power_of_two();
        let recv_ring_size = config.recv_ring_size.next_power_of_two();

        let mut send_ring = vec![0u8; send_ring_size].into_boxed_slice();
        let mut recv_ring = vec![0u8; recv_ring_size].into_boxed_slice();

        // Register memory regions
        let access_flags = AccessFlags::LOCAL_WRITE | AccessFlags::REMOTE_WRITE;
        let send_ring_mr =
            unsafe { pd.register(send_ring.as_mut_ptr(), send_ring.len(), access_flags)? };
        let recv_ring_mr =
            unsafe { pd.register(recv_ring.as_mut_ptr(), recv_ring.len(), access_flags)? };

        // Create QP with SRQ using MonoCq for recv
        let send_cqe_buffer_clone = send_cqe_buffer.clone();
        let sq_callback: SqCqCallback = Box::new(move |cqe, entry| {
            // Log error CQEs
            if cqe.opcode == mlx5::cq::CqeOpcode::ReqErr
                || cqe.opcode == mlx5::cq::CqeOpcode::RespErr
            {
                eprintln!(
                    "[send_cq ERROR] qpn={}, opcode={:?}, syndrome={}",
                    entry.qpn, cqe.opcode, cqe.syndrome
                );
                return;
            }
            send_cqe_buffer_clone.push(cqe, entry);
        });
        let qp = ctx
            .rc_qp_builder::<SrqEntry, SrqEntry>(pd, &config.qp_config)
            .with_srq(srq.clone())
            .sq_cq(send_cq.clone(), sq_callback)
            .rq_mono_cq(recv_cq)
            .build()?;

        let max_resp_reservation = config
            .max_resp_reservation
            .min(config.send_ring_size as u64 / 4);

        let inner = Rc::new(UnsafeCell::new(Self {
            qp,
            send_ring,
            send_ring_mr,
            recv_ring,
            recv_ring_mr,
            remote_recv_ring: Cell::new(None),
            send_ring_producer: Cell::new(0),
            recv_ring_producer: Cell::new(0),
            last_recv_pos: Cell::new(0),
            last_notified_recv_pos: Cell::new(0),
            remote_consumer_pos: Cell::new(0),
            unsignaled_count: Cell::new(0),
            pending_calls: UnsafeCell::new(Slab::new()),
            flush_start_pos: Cell::new(0),
            resp_reservation: Cell::new(0),
            max_resp_reservation,
            peer_credit_balance: Cell::new(0),
            meta_pos: Cell::new(0),
            batch_message_count: Cell::new(0),
        }));

        // SRQ recv buffers are pre-posted in Context::build() and replenished in poll()

        Ok(inner)
    }

    fn qpn(&self) -> u32 {
        self.qp.borrow().qpn()
    }

    /// Get local endpoint info for connection establishment.
    fn local_info(&self) -> LocalEndpointInfo {
        LocalEndpointInfo {
            qp_number: self.qp.borrow().qpn(),
            recv_ring_addr: self.recv_ring_mr.addr() as u64,
            recv_ring_rkey: self.recv_ring_mr.rkey(),
            recv_ring_size: self.recv_ring.len() as u64,
            initial_credit: self
                .max_resp_reservation
                .min(self.send_ring.len() as u64 / 4),
        }
    }

    /// Emit WQE for accumulated data (without doorbell).
    ///
    /// If a batch is open (meta_pos is Some), finalizes the metadata first.
    /// No split WQE needed - ensure_metadata + emit_wrap guarantee all batches
    /// stay within a single ring cycle.
    ///
    /// Returns Ok(true) if WQE was emitted, Ok(false) if nothing to emit.
    /// Low-level: emit WQE from flush_start_pos to send_ring_producer.
    /// Does NOT fill metadata or prepare next batch.
    #[inline(always)]
    fn emit_raw(&self) -> Result<bool> {
        let remote_ring = self
            .remote_recv_ring
            .get()
            .ok_or(Error::RemoteConsumerUnknown)?;

        let start = self.flush_start_pos.get();
        let end = self.send_ring_producer.get();

        if start == end {
            return Ok(false);
        }

        let delta = end - start;
        let imm = encode_imm(delta);

        let start_offset = (start & (self.send_ring.len() as u64 - 1)) as usize;
        let remote_offset = start & (remote_ring.size - 1);
        let remote_addr = remote_ring.addr + remote_offset;

        // Determine if this WQE should be signaled
        let count = self.unsignaled_count.get();
        let should_signal = count >= SIGNAL_INTERVAL;
        let qpn = self.qpn();

        {
            let qp = self.qp.borrow();
            let ctx = qp.emit_ctx().map_err(Error::Io)?;
            let delta_usize = delta as usize;

            if delta_usize <= MAX_INLINE_ONLY {
                // Fully inline: entire batch fits in BlueFlame WQE
                let inline_data = &self.send_ring[start_offset..start_offset + delta_usize];
                if should_signal {
                    emit_wqe!(
                        &ctx,
                        write_imm {
                            flags: WqeFlags::empty(),
                            remote_addr: remote_addr,
                            rkey: remote_ring.rkey,
                            imm: imm,
                            inline: inline_data,
                            signaled: SrqEntry { qpn },
                        }
                    )
                    .map_err(|e| Error::Io(e.into()))?;
                } else {
                    emit_wqe!(
                        &ctx,
                        write_imm {
                            flags: WqeFlags::empty(),
                            remote_addr: remote_addr,
                            rkey: remote_ring.rkey,
                            imm: imm,
                            inline: inline_data,
                        }
                    )
                    .map_err(|e| Error::Io(e.into()))?;
                }
            } else {
                // Hybrid: inline first MAX_INLINE_HYBRID bytes, SGE for the rest
                let inline_data = &self.send_ring[start_offset..start_offset + MAX_INLINE_HYBRID];
                let sge_offset = start_offset + MAX_INLINE_HYBRID;
                let sge_len = delta_usize - MAX_INLINE_HYBRID;
                if should_signal {
                    emit_wqe!(&ctx, write_imm {
                        flags: WqeFlags::empty(),
                        remote_addr: remote_addr,
                        rkey: remote_ring.rkey,
                        imm: imm,
                        inline: inline_data,
                        sge: { addr: self.send_ring_mr.addr() as u64 + sge_offset as u64, len: sge_len as u32, lkey: self.send_ring_mr.lkey() },
                        signaled: SrqEntry { qpn },
                    }).map_err(|e| Error::Io(e.into()))?;
                } else {
                    emit_wqe!(&ctx, write_imm {
                        flags: WqeFlags::empty(),
                        remote_addr: remote_addr,
                        rkey: remote_ring.rkey,
                        imm: imm,
                        inline: inline_data,
                        sge: { addr: self.send_ring_mr.addr() as u64 + sge_offset as u64, len: sge_len as u32, lkey: self.send_ring_mr.lkey() },
                    }).map_err(|e| Error::Io(e.into()))?;
                }
            }

            if should_signal {
                self.unsignaled_count.set(0);
            } else {
                self.unsignaled_count.set(count + 1);
            }
        }

        self.flush_start_pos.set(end);
        Ok(true)
    }

    /// Emit current message batch: fill metadata, emit WQE, prepare next batch.
    /// Returns Ok(false) if no messages to emit.
    #[inline(always)]
    fn emit_wqe(&self) -> Result<bool> {
        if self.batch_message_count.get() == 0 {
            return Ok(false);
        }
        self.fill_metadata();
        self.emit_raw()?;
        self.prepare_next_batch();
        Ok(true)
    }

    /// Emit WQE and ring doorbell.
    #[inline(always)]
    fn flush(&self) -> Result<()> {
        self.emit_wqe()?;
        self.qp.borrow().ring_sq_doorbell_bf();
        Ok(())
    }

    /// Prepare the next batch: reserve metadata space and reset batch state.
    /// Called after emit_wqe/emit_wrap and during connect().
    #[inline(always)]
    fn prepare_next_batch(&self) {
        let producer = self.send_ring_producer.get();
        self.meta_pos.set(producer);
        self.batch_message_count.set(0);
        self.send_ring_producer
            .set(producer + FLOW_METADATA_SIZE as u64);
    }

    /// Fill the current batch's flow_metadata with final values.
    #[inline(always)]
    fn fill_metadata(&self) {
        let meta_pos = self.meta_pos.get();
        let ring_size = self.send_ring.len() as u64;
        let meta_offset = (meta_pos & (ring_size - 1)) as usize;

        let ring_credit_return = self.last_recv_pos.get() - self.last_notified_recv_pos.get();
        self.last_notified_recv_pos.set(self.last_recv_pos.get());
        let credit_grant = self.compute_credit_grant();
        let message_count = self.batch_message_count.get();

        // Update resp_reservation with granted credit
        self.resp_reservation
            .set(self.resp_reservation.get() + credit_grant);

        unsafe {
            let buf_ptr = self.send_ring.as_ptr().add(meta_offset) as *mut u8;
            encode_flow_metadata(buf_ptr, ring_credit_return, credit_grant, message_count);
        }
    }

    /// Emit a wrap flow_metadata and advance producer past the ring boundary.
    /// Reclaims the pre-placed metadata placeholder, writes wrap metadata there,
    /// then prepares the next batch at the start of the new ring cycle.
    #[inline(always)]
    fn emit_wrap(&self) -> Result<()> {
        // Reclaim the pre-placed metadata placeholder
        let meta_pos = self.meta_pos.get();
        self.send_ring_producer.set(meta_pos);

        let ring_size = self.send_ring.len() as u64;
        let offset = (meta_pos & (ring_size - 1)) as usize;
        let remaining = ring_size as usize - offset;

        debug_assert!(remaining >= FLOW_METADATA_SIZE);

        let ring_credit_return = self.last_recv_pos.get() - self.last_notified_recv_pos.get();
        self.last_notified_recv_pos.set(self.last_recv_pos.get());
        let credit_grant = self.compute_credit_grant();
        self.resp_reservation
            .set(self.resp_reservation.get() + credit_grant);

        unsafe {
            let buf_ptr = self.send_ring.as_ptr().add(offset) as *mut u8;
            encode_flow_metadata(
                buf_ptr,
                ring_credit_return,
                credit_grant,
                WRAP_MESSAGE_COUNT,
            );
        }

        self.send_ring_producer.set(meta_pos + remaining as u64);
        self.emit_raw()?;
        self.prepare_next_batch();

        Ok(())
    }

    /// Compute the credit grant for the current batch.
    /// Returns bytes of credit to grant to the peer.
    #[inline(always)]
    fn compute_credit_grant(&self) -> u64 {
        let remote_ring = match self.remote_recv_ring.get() {
            Some(r) => r,
            None => return 0,
        };

        let producer = self.send_ring_producer.get();
        let consumer = self.remote_consumer_pos.get();
        let in_flight = producer.saturating_sub(consumer);
        let current_r = self.resp_reservation.get();

        // Invariant: in_flight + 2*(current_r + grant) ≤ remote_ring.size
        // grant ≤ (remote_ring.size - in_flight) / 2 - current_r
        let max_by_invariant =
            (remote_ring.size.saturating_sub(in_flight) / 2).saturating_sub(current_r);

        // Policy: don't exceed max_resp_reservation
        let max_by_policy = self.max_resp_reservation.saturating_sub(current_r);

        // Align down to ALIGNMENT
        let grant = max_by_invariant.min(max_by_policy);
        grant & !(ALIGNMENT - 1)
    }

    /// Handle wrap-around if the next write of `total_size` bytes doesn't fit
    /// in the current ring cycle. Closes the current batch if open, emits wrap.
    ///
    /// The check is based on the entire batch span from `meta_pos` (where the
    /// pre-placed metadata lives) to `producer + total_size`. This ensures the
    /// batch (metadata + messages) stays contiguous within one ring cycle.
    #[inline(always)]
    fn handle_wrap_if_needed(&self, total_size: u64) -> Result<bool> {
        let producer = self.send_ring_producer.get();
        let ring_size = self.send_ring.len() as u64;
        let meta_offset = self.meta_pos.get() & (ring_size - 1);
        let batch_span = (producer - self.meta_pos.get()) + total_size;

        // Use strict `<` to force a wrap when the batch would exactly reach the
        // ring boundary. Otherwise the next write in the same batch would start at
        // offset 0, causing the batch SGE to span the ring boundary.
        if meta_offset + batch_span < ring_size {
            return Ok(false);
        }

        // Close current batch if it has messages, then prepare_next_batch
        // so that emit_wrap reclaims the NEWLY placed metadata (not the old one).
        if self.batch_message_count.get() > 0 {
            self.fill_metadata();
            self.emit_raw()?;
            self.flush_start_pos.set(self.send_ring_producer.get());
            self.prepare_next_batch();
        }
        self.emit_wrap()?;
        Ok(true)
    }
}

/// Local endpoint information for connection establishment.
#[derive(Debug, Clone)]
pub struct LocalEndpointInfo {
    /// Local QP number.
    pub qp_number: u32,
    /// Local receive ring buffer address.
    pub recv_ring_addr: u64,
    /// Local receive ring buffer rkey.
    pub recv_ring_rkey: u32,
    /// Local receive ring buffer size.
    pub recv_ring_size: u64,
    /// Initial credit (bytes) for response reservation.
    pub initial_credit: u64,
}

/// An RPC endpoint representing a connection to a remote peer.
pub struct Endpoint<U> {
    inner: Rc<UnsafeCell<EndpointInner<U>>>,
    _marker: PhantomData<U>,
}

impl<U> Endpoint<U> {
    /// Get the QP number.
    pub fn qpn(&self) -> u32 {
        unsafe { &*self.inner.get() }.qpn()
    }

    /// Get local endpoint info for connection establishment.
    pub fn local_info(&self, ctx_lid: u16, ctx_port: u8) -> (LocalEndpointInfo, u16, u8) {
        let info = unsafe { &*self.inner.get() }.local_info();
        (info, ctx_lid, ctx_port)
    }

    /// Connect to a remote endpoint.
    pub fn connect(
        &mut self,
        remote: &RemoteEndpointInfo,
        local_psn: u32,
        port: u8,
    ) -> io::Result<()> {
        let inner = unsafe { &*self.inner.get() };

        // Set remote ring info
        inner.remote_recv_ring.set(Some(RemoteRingInfo::new(
            remote.recv_ring_addr,
            remote.recv_ring_rkey,
            remote.recv_ring_size,
        )));

        // Initialize credit:
        // - resp_reservation = initial credit we promised (capped to ring_size/4)
        //   This ensures 2*R ≤ C/2, leaving at least half the ring for messages.
        let initial_credit = inner.max_resp_reservation.min(remote.recv_ring_size / 4);
        inner.resp_reservation.set(initial_credit);

        // - peer_credit_balance = credit the remote promised us
        inner.peer_credit_balance.set(remote.initial_credit);

        // Connect the QP
        let remote_qp_info = IbRemoteQpInfo {
            qp_number: remote.qp_number,
            packet_sequence_number: remote.packet_sequence_number,
            local_identifier: remote.local_identifier,
        };

        // Use standard access flags for RDMA WRITE only (no READ needed)
        let access_flags = mlx5_sys::ibv_access_flags_IBV_ACCESS_LOCAL_WRITE
            | mlx5_sys::ibv_access_flags_IBV_ACCESS_REMOTE_WRITE;

        inner.qp.borrow_mut().connect(
            &remote_qp_info,
            port,
            local_psn,
            0, // max_rd_atomic
            0, // max_dest_rd_atomic
            access_flags,
        )?;

        // Prepare the first batch metadata placeholder
        inner.prepare_next_batch();

        Ok(())
    }

    /// Send an RPC call (async).
    ///
    /// The call is written to the send buffer. Actual transmission happens
    /// when Context::poll() is called.
    ///
    /// `response_allowance` specifies the maximum response data size (bytes).
    /// Internally padded and includes metadata overhead for flow control safety.
    ///
    /// When the response arrives, the Context's on_response callback will
    /// be invoked with the provided user_data.
    pub fn call(
        &self,
        data: &[u8],
        user_data: U,
        response_allowance: u64,
    ) -> std::result::Result<u32, error::CallError<U>> {
        let inner = unsafe { &*self.inner.get() };

        let remote_ring = inner
            .remote_recv_ring
            .get()
            .ok_or(error::CallError::Other(Error::RemoteConsumerUnknown))?;

        // Internal response_allowance includes metadata overhead for wrap safety:
        // R_i = padded_message_size(D) + FLOW_METADATA_SIZE
        let resp_msg_size = padded_message_size(response_allowance as u32);
        let internal_allowance = resp_msg_size + FLOW_METADATA_SIZE as u64;
        // Align up to ALIGNMENT
        let internal_allowance = (internal_allowance + ALIGNMENT - 1) & !(ALIGNMENT - 1);

        // Check credit balance
        if inner.peer_credit_balance.get() < internal_allowance {
            inner.flush().map_err(error::CallError::Other)?;
            return Err(error::CallError::InsufficientCredit(user_data));
        }

        // Calculate message size
        let msg_size = padded_message_size(data.len() as u32);

        // Handle wrap-around: metadata is already pre-placed by prepare_next_batch
        inner
            .handle_wrap_if_needed(msg_size)
            .map_err(error::CallError::Other)?;

        // Check available space with credit invariant:
        // (producer - consumer) + 2*R ≤ C  →  msg_size ≤ available - 2*R
        let producer = inner.send_ring_producer.get();
        let consumer = inner.remote_consumer_pos.get();
        let available = remote_ring
            .size
            .saturating_sub(producer.saturating_sub(consumer));
        let reserved = 2 * inner.resp_reservation.get();

        if available < msg_size + reserved {
            // No space: flush and return RingFull; caller should poll() and retry
            inner.flush().map_err(error::CallError::Other)?;
            return Err(error::CallError::RingFull(user_data));
        }

        // Allocate call ID from slab (index is the call_id)
        let call_id = unsafe { &mut *inner.pending_calls.get() }.insert(user_data) as u32;

        // Write message with response_allowance in piggyback field (as ALIGNMENT blocks)
        let send_offset = (producer & (inner.send_ring.len() as u64 - 1)) as usize;
        let response_allowance_blocks = (internal_allowance / ALIGNMENT) as u32;

        unsafe {
            let buf_ptr = inner.send_ring.as_ptr().add(send_offset) as *mut u8;
            encode_header(
                buf_ptr,
                call_id,
                response_allowance_blocks as u64,
                data.len() as u32,
            );
            std::ptr::copy_nonoverlapping(data.as_ptr(), buf_ptr.add(HEADER_SIZE), data.len());
        }

        // Update producer and batch count
        inner.send_ring_producer.set(producer + msg_size);
        inner
            .batch_message_count
            .set(inner.batch_message_count.get() + 1);

        // Deduct credit
        inner
            .peer_credit_balance
            .set(inner.peer_credit_balance.get() - internal_allowance);

        Ok(call_id)
    }
}

/// Handle to a received RPC request.
pub struct RecvHandle<'a, U> {
    _lifetime: PhantomData<&'a Context<U>>,
    endpoint: Rc<UnsafeCell<EndpointInner<U>>>,
    qpn: u32,
    call_id: u32,
    data_ptr: *const u8,
    data_len: usize,
    response_allowance: u64,
}

impl<U> RecvHandle<'_, U> {
    /// Zero-copy access to the request data in the receive ring buffer.
    #[inline]
    pub fn data(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.data_ptr, self.data_len) }
    }

    /// Get the QPN of the endpoint that received this request.
    pub fn qpn(&self) -> u32 {
        self.qpn
    }

    /// Get the call ID of this request.
    pub fn call_id(&self) -> u32 {
        self.call_id
    }

    /// Send a reply to this request.
    ///
    /// The reply is written to the send buffer. Actual transmission happens
    /// when Context::poll() is called.
    ///
    /// The credit-based invariant guarantees this will always have space
    /// (reply cannot fail with RingFull under normal operation).
    pub fn reply(&self, data: &[u8]) -> Result<()> {
        let inner = unsafe { &*self.endpoint.get() };

        inner
            .remote_recv_ring
            .get()
            .ok_or(Error::RemoteConsumerUnknown)?;

        // Calculate message size
        let msg_size = padded_message_size(data.len() as u32);

        // Handle wrap-around: metadata is already pre-placed by prepare_next_batch
        inner.handle_wrap_if_needed(msg_size)?;

        // Deduct response reservation (R_i from the original request)
        let resp_res = inner.resp_reservation.get();
        inner
            .resp_reservation
            .set(resp_res.saturating_sub(self.response_allowance));

        // Write response message
        let producer = inner.send_ring_producer.get();
        let send_offset = (producer & (inner.send_ring.len() as u64 - 1)) as usize;
        let response_call_id = to_response_id(self.call_id);

        unsafe {
            let buf_ptr = inner.send_ring.as_ptr().add(send_offset) as *mut u8;
            // piggyback = 0 for responses
            encode_header(buf_ptr, response_call_id, 0, data.len() as u32);
            std::ptr::copy_nonoverlapping(data.as_ptr(), buf_ptr.add(HEADER_SIZE), data.len());
        }

        // Update producer and batch count
        inner.send_ring_producer.set(producer + msg_size);
        inner
            .batch_message_count
            .set(inner.batch_message_count.get() + 1);

        Ok(())
    }
}
