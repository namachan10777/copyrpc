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

use std::cell::{Cell, RefCell};
use std::io;

use fastmap::FastMap;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};

use slab::Slab;

use mlx5::cq::{CqConfig, Cqe};
use mlx5::device::Context as Mlx5Context;
use mlx5::mono_cq::MonoCq;
use mlx5::pd::{AccessFlags, MemoryRegion, Pd};
use mlx5::qp::{RcQpConfig, RcQpForMonoCqWithSrqAndSqCb};
use mlx5::srq::{Srq, SrqConfig};
use mlx5::transport::IbRemoteQpInfo;
use mlx5::emit_wqe;
use mlx5::wqe::{WqeFlags, WriteImmParams};

use encoding::{
    HEADER_SIZE, decode_header, decode_imm, encode_header, encode_imm,
    from_response_id, is_padding_marker, is_response, padded_message_size,
    to_response_id, write_padding_marker,
};
use error::{Error, Result};
use ring::RemoteConsumer;

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
    /// Remote consumer position MR address (for RDMA READ).
    pub consumer_addr: u64,
    /// Remote consumer position MR rkey (for RDMA READ).
    pub consumer_rkey: u32,
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

/// Remote consumer position MR information for RDMA READ.
#[derive(Debug, Clone, Copy)]
struct RemoteConsumerMr {
    /// Remote consumer position address.
    addr: u64,
    /// Remote consumer position rkey.
    rkey: u32,
}

/// SRQ entry for tracking receive completions.
#[derive(Debug, Clone, Copy)]
pub struct SrqEntry {
    /// QPN of the QP that posted this receive.
    pub qpn: u32,
    /// Flag to distinguish READ completions from WRITE+IMM completions in send CQ.
    pub is_read: bool,
}

/// Type alias for the SQ callback used in copyrpc.
type SqCqCallback = Box<dyn Fn(Cqe, SrqEntry)>;

/// Type alias for the RC QP with SRQ used in copyrpc (for MonoCq).
type CopyrpcQp = RcQpForMonoCqWithSrqAndSqCb<SrqEntry, SqCqCallback>;

/// CQE buffer for storing receive completions.
/// MonoCq callback pushes CQEs here, then poll() processes them.
struct CqeBuffer {
    entries: RefCell<Vec<(Cqe, SrqEntry)>>,
}

impl CqeBuffer {
    fn new() -> Self {
        Self {
            entries: RefCell::new(Vec::new()),
        }
    }

    fn push(&self, cqe: Cqe, entry: SrqEntry) {
        self.entries.borrow_mut().push((cqe, entry));
    }

    fn drain(&self) -> Vec<(Cqe, SrqEntry)> {
        std::mem::take(&mut *self.entries.borrow_mut())
    }
}

/// Type alias for the recv MonoCq callback.
type RecvCqCallback = Box<dyn Fn(Cqe, SrqEntry)>;

/// Internal struct for received messages stored in recv_stack.
struct RecvMessage<U> {
    /// Endpoint that received this message.
    endpoint: Rc<RefCell<EndpointInner<U>>>,
    /// QPN of the endpoint.
    qpn: u32,
    /// Call ID from the message header.
    call_id: u32,
    /// Offset in recv_ring where payload starts.
    data_offset: usize,
    /// Length of the payload.
    data_len: usize,
    /// Position in the receive ring (for consumer advancement).
    recv_pos: u64,
    /// Receive ring length.
    recv_ring_len: usize,
}

/// RPC context managing multiple endpoints.
///
/// Type parameters:
/// - `U`: User data type associated with pending RPC calls
/// - `F`: Response callback type `Fn(U, &[u8])`
pub struct Context<U, F>
where
    F: Fn(U, &[u8]),
{
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
    recv_cq: Rc<MonoCq<CopyrpcQp, RecvCqCallback>>,
    /// CQE buffer for recv completions (MonoCq callback pushes here).
    cqe_buffer: Rc<CqeBuffer>,
    /// CQE buffer for send completions (for READ completion processing).
    send_cqe_buffer: Rc<CqeBuffer>,
    /// Registered endpoints by QPN.
    endpoints: RefCell<FastMap<Rc<RefCell<EndpointInner<U>>>>>,
    /// Response callback.
    on_response: F,
    /// Port number (for connection establishment).
    port: u8,
    /// Local LID.
    lid: u16,
    /// Received messages stack.
    recv_stack: RefCell<Vec<RecvMessage<U>>>,
    /// Marker for U.
    _marker: PhantomData<U>,
}

/// Builder for creating a Context.
pub struct ContextBuilder<U, F> {
    device_index: usize,
    port: u8,
    srq_config: SrqConfig,
    cq_size: i32,
    on_response: Option<F>,
    _marker: PhantomData<U>,
}

impl<U, F> Default for ContextBuilder<U, F> {
    fn default() -> Self {
        Self {
            device_index: 0,
            port: 1,
            srq_config: SrqConfig {
                max_wr: DEFAULT_SRQ_SIZE,
                max_sge: 1,
            },
            cq_size: DEFAULT_CQ_SIZE,
            on_response: None,
            _marker: PhantomData,
        }
    }
}

impl<U, F: Fn(U, &[u8])> ContextBuilder<U, F> {
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

    /// Set the response callback.
    pub fn on_response(mut self, callback: F) -> Self {
        self.on_response = Some(callback);
        self
    }

    /// Build the context.
    pub fn build(self) -> io::Result<Context<U, F>> {
        let on_response = self
            .on_response
            .ok_or_else(|| io::Error::other("on_response callback is required"))?;

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
            let _ = srq.post_recv(SrqEntry { qpn: 0, is_read: false }, 0, 0, 0);
        }
        srq.ring_doorbell();

        let send_cq = Rc::new(mlx5_ctx.create_cq(self.cq_size, &CqConfig::default())?);

        // Create CQE buffer for MonoCq callback
        let cqe_buffer = Rc::new(CqeBuffer::new());
        let cqe_buffer_for_callback = cqe_buffer.clone();

        // Create CQE buffer for send CQ (for READ completion processing)
        let send_cqe_buffer = Rc::new(CqeBuffer::new());

        // Create MonoCq for recv with callback that pushes to buffer
        let recv_callback: RecvCqCallback = Box::new(move |cqe, entry| {
            // Log error CQEs
            if cqe.opcode == mlx5::cq::CqeOpcode::ReqErr || cqe.opcode == mlx5::cq::CqeOpcode::RespErr {
                eprintln!("[recv_cq ERROR] qpn={}, opcode={:?}, syndrome={}", entry.qpn, cqe.opcode, cqe.syndrome);
                return;
            }
            cqe_buffer_for_callback.push(cqe, entry);
        });
        let recv_cq = Rc::new(mlx5_ctx.create_mono_cq(self.cq_size, recv_callback, &CqConfig::default())?);

        Ok(Context {
            mlx5_ctx,
            pd,
            srq,
            srq_posted: Cell::new(srq_max_wr),
            srq_max_wr,
            send_cq,
            recv_cq,
            cqe_buffer,
            send_cqe_buffer,
            endpoints: RefCell::new(FastMap::new()),
            on_response,
            port: self.port,
            lid,
            recv_stack: RefCell::new(Vec::new()),
            _marker: PhantomData,
        })
    }
}

impl<U, F: Fn(U, &[u8])> Context<U, F> {
    /// Create a new context builder.
    pub fn builder() -> ContextBuilder<U, F> {
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

        let qpn = inner.borrow().qpn();

        self.endpoints.borrow_mut().insert(qpn, inner.clone());

        Ok(Endpoint {
            inner,
            _marker: PhantomData,
        })
    }

    /// Poll for completions and process all pending operations.
    ///
    /// This method:
    /// 1. Polls the receive CQ (MonoCq) - callbacks push CQEs to buffer
    /// 2. Processes buffered CQEs via process_cqe
    ///    - Requests are pushed to recv_stack
    ///    - Responses invoke the on_response callback
    /// 3. Polls the send CQ to handle READ completions
    /// 4. Flushes all endpoints' accumulated data
    /// 5. Issues READ operations for endpoints that need consumer position updates
    pub fn poll(&self) {
        // 1. Poll recv CQ (MonoCq) - callbacks push CQEs to cqe_buffer
        loop {
            let count = self.recv_cq.poll();
            if count == 0 {
                break;
            }
        }
        self.recv_cq.flush();

        // 2. Process buffered CQEs
        // Use cqe.qp_num to identify the endpoint, not entry.qpn
        // (SRQ entries are returned in FIFO order, not per-endpoint)
        let cqes = self.cqe_buffer.drain();
        let num_cqes = cqes.len();

        for (cqe, _entry) in cqes {
            self.process_cqe(cqe, cqe.qp_num);
        }

        // 3. Repost recvs to SRQ when below 2/3 threshold
        let posted = self.srq_posted.get().saturating_sub(num_cqes as u32);
        self.srq_posted.set(posted);

        let threshold = self.srq_max_wr * 2 / 3;
        if posted < threshold {
            let to_post = self.srq_max_wr - posted;
            for _ in 0..to_post {
                let _ = self.srq.post_recv(SrqEntry { qpn: 0, is_read: false }, 0, 0, 0);
            }
            self.srq.ring_doorbell();
            self.srq_posted.set(self.srq_max_wr);
        }

        // 4. Poll send CQ for READ completions
        loop {
            let count = self.send_cq.poll();
            if count == 0 {
                break;
            }
        }
        self.send_cq.flush();

        // 5. Process send CQEs (READ completions)
        let send_cqes = self.send_cqe_buffer.drain();

        for (_cqe, entry) in send_cqes {
            // Only process READ completions (is_read=true)
            // WRITE+IMM completions (is_read=false) are just for SQ progress tracking
            if !entry.is_read {
                continue;
            }

            // entry.qpn identifies which endpoint's READ completed
            if let Some(ep) = self.endpoints.borrow().get(entry.qpn) {
                let ep_ref = ep.borrow();
                if ep_ref.read_inflight.get() {
                    // Read the consumer value from read_buffer
                    let consumer_value = u64::from_le_bytes(*ep_ref.read_buffer);
                    ep_ref.remote_consumer.update(consumer_value);
                    ep_ref.read_inflight.set(false);
                }
            }
        }

        // 6. Flush all endpoints and issue READ operations if needed
        for (_, ep) in self.endpoints.borrow().iter() {
            let ep_ref = ep.borrow();

            // Flush accumulated data
            let _ = ep_ref.flush();

            // Issue READ if needed or forced, and not already in-flight
            let needs = ep_ref.needs_read();
            let force = ep_ref.force_read.get();
            let inflight = ep_ref.read_inflight.get();

            if !inflight && (needs || force) {
                if let Some(remote_mr) = ep_ref.remote_consumer_mr.get() {
                    let qp = ep_ref.qp.borrow();
                    if let Ok(ctx) = qp.emit_ctx() {
                        let result = emit_wqe!(&ctx, read {
                            flags: WqeFlags::empty(),
                            remote_addr: remote_mr.addr,
                            rkey: remote_mr.rkey,
                            sge: { addr: ep_ref.read_buffer_mr.addr() as u64, len: 8, lkey: ep_ref.read_buffer_mr.lkey() },
                            signaled: SrqEntry { qpn: ep_ref.qpn(), is_read: true },
                        });

                        if result.is_ok() {
                            ep_ref.read_inflight.set(true);
                            ep_ref.force_read.set(false);
                            qp.ring_sq_doorbell();
                        }
                    }
                }
            }
        }
    }

    /// Get the next received request from the stack.
    ///
    /// Call poll() first to populate the recv_stack.
    /// Returns `None` if no request is available.
    pub fn recv(&self) -> Option<RecvHandle<'_, U, F>> {
        self.recv_stack.borrow_mut().pop().map(|msg| RecvHandle {
            ctx: self,
            endpoint: msg.endpoint,
            qpn: msg.qpn,
            call_id: msg.call_id,
            data_offset: msg.data_offset,
            data_len: msg.data_len,
            recv_pos: msg.recv_pos,
            recv_ring_len: msg.recv_ring_len,
        })
    }

    /// Process a single CQE.
    ///
    /// This is called internally when a completion is received.
    /// A single CQE may contain multiple batched messages, so we process
    /// all messages from last_recv_pos up to recv_ring_producer.
    /// For requests, pushes to recv_stack.
    /// For responses, invokes callback.
    #[inline(always)]
    fn process_cqe(&self, cqe: Cqe, qpn: u32) {
        // Skip error CQEs
        use mlx5::cq::CqeOpcode;
        if cqe.opcode == CqeOpcode::ReqErr || cqe.opcode == CqeOpcode::RespErr {
            return;
        }

        let endpoints = self.endpoints.borrow();
        let endpoint = match endpoints.get(qpn) {
            Some(ep) => ep,
            None => return,
        };

        // Decode the immediate value to get the offset delta
        let delta = decode_imm(cqe.imm);

        // Update recv_ring_producer
        {
            let endpoint_ref = endpoint.borrow();
            endpoint_ref.recv_ring_producer.set(endpoint_ref.recv_ring_producer.get() + delta);
        }

        // Process all messages in the batch
        // Loop while there are unprocessed messages (last_recv_pos < recv_ring_producer)
        loop {
            let (call_id, _piggyback, payload_len, recv_pos, header_offset, recv_ring_len, _producer) = {
                let endpoint_ref = endpoint.borrow();
                let pos = endpoint_ref.last_recv_pos.get();
                let producer = endpoint_ref.recv_ring_producer.get();

                // Check if there are more messages to process
                if pos >= producer {
                    break;
                }

                let header_offset = (pos & (endpoint_ref.recv_ring.len() as u64 - 1)) as usize;
                let header_slice = &endpoint_ref.recv_ring[header_offset..header_offset + HEADER_SIZE];

                let (call_id, piggyback, payload_len) = unsafe {
                    decode_header(header_slice.as_ptr())
                };

                endpoint_ref.remote_consumer.update(piggyback as u64);

                (call_id, piggyback, payload_len, pos, header_offset, endpoint_ref.recv_ring.len(), producer)
            };

            // Check for padding marker first
            if is_padding_marker(call_id) {
                // Padding marker: skip to ring start
                // piggyback contains the remaining bytes to skip (including header)
                let remaining = _piggyback as u64;
                let endpoint_ref = endpoint.borrow();
                // Advance last_recv_pos by remaining bytes to jump to ring start
                endpoint_ref.last_recv_pos.set(recv_pos + remaining);
                // Continue to process next message
                continue;
            }

            if is_response(call_id) {
                // This is a response - invoke callback
                let original_call_id = from_response_id(call_id);

                let endpoint_ref = endpoint.borrow();
                let user_data = endpoint_ref.pending_calls.borrow_mut().try_remove(original_call_id as usize);

                let msg_size = padded_message_size(payload_len);
                endpoint_ref.last_recv_pos.set(recv_pos + msg_size);

                if let Some(user_data) = user_data {
                    let data_offset = header_offset + HEADER_SIZE;
                    let data_slice = &endpoint_ref.recv_ring[data_offset..data_offset + payload_len as usize];
                    (self.on_response)(user_data, data_slice);
                }
            } else {
                // This is a request - push to recv_stack
                let data_offset = header_offset + HEADER_SIZE;

                // Advance last_recv_pos first
                {
                    let endpoint_ref = endpoint.borrow();
                    let msg_size = padded_message_size(payload_len);
                    endpoint_ref.last_recv_pos.set(recv_pos + msg_size);
                }

                self.recv_stack.borrow_mut().push(RecvMessage {
                    endpoint: endpoint.clone(),
                    qpn,
                    call_id,
                    data_offset,
                    data_len: payload_len as usize,
                    recv_pos,
                    recv_ring_len,
                });
            }
        }

        // Update consumer_position for RDMA READ by remote
        {
            let endpoint_ref = endpoint.borrow();
            let final_pos = endpoint_ref.last_recv_pos.get();
            endpoint_ref.consumer_position.store(final_pos, Ordering::Release);
        }
    }
}

/// How often to signal a WQE (every N unsignaled WQEs).
/// This prevents SQ exhaustion by allowing completion processing.
const SIGNAL_INTERVAL: u32 = 64;

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
    /// Send ring consumer position (from remote pigbyback).
    #[allow(dead_code)]
    send_ring_consumer: Cell<u64>,
    /// Receive ring producer position (updated on receive).
    recv_ring_producer: Cell<u64>,
    /// Last received message position (consumer).
    last_recv_pos: Cell<u64>,
    /// Remote consumer position (piggyback updates).
    remote_consumer: RemoteConsumer,
    /// Counter for unsignaled WQEs (for periodic signaling).
    unsignaled_count: Cell<u32>,
    /// Pending calls waiting for responses. Slab index is used as call_id.
    pending_calls: RefCell<Slab<U>>,
    /// Flush start position (for WQE batching).
    flush_start_pos: Cell<u64>,
    /// READ operation is in-flight.
    read_inflight: Cell<bool>,
    /// Force READ on next poll (set when RingFull occurs).
    force_read: Cell<bool>,
    /// Consumer position for RDMA READ (8-byte aligned).
    consumer_position: Box<AtomicU64>,
    /// Consumer position memory region.
    consumer_position_mr: MemoryRegion,
    /// Buffer for RDMA READ results.
    read_buffer: Box<[u8; 8]>,
    /// Read buffer memory region.
    read_buffer_mr: MemoryRegion,
    /// Remote consumer position MR information.
    remote_consumer_mr: Cell<Option<RemoteConsumerMr>>,
}

impl<U> EndpointInner<U> {
    fn new(
        ctx: &Mlx5Context,
        pd: &Pd,
        srq: &Rc<Srq<SrqEntry>>,
        send_cq: &Rc<mlx5::cq::Cq>,
        recv_cq: &Rc<MonoCq<CopyrpcQp, RecvCqCallback>>,
        send_cqe_buffer: &Rc<CqeBuffer>,
        config: &EndpointConfig,
    ) -> io::Result<Rc<RefCell<Self>>> {
        // Allocate ring buffers
        let send_ring_size = config.send_ring_size.next_power_of_two();
        let recv_ring_size = config.recv_ring_size.next_power_of_two();

        let mut send_ring = vec![0u8; send_ring_size].into_boxed_slice();
        let mut recv_ring = vec![0u8; recv_ring_size].into_boxed_slice();

        // Register memory regions
        let access_flags = AccessFlags::LOCAL_WRITE | AccessFlags::REMOTE_WRITE;
        let send_ring_mr = unsafe {
            pd.register(send_ring.as_mut_ptr(), send_ring.len(), access_flags)?
        };
        let recv_ring_mr = unsafe {
            pd.register(recv_ring.as_mut_ptr(), recv_ring.len(), access_flags)?
        };

        // Consumer position MR (for RDMA READ by remote)
        let consumer_position = Box::new(AtomicU64::new(0));
        let consumer_position_mr = unsafe {
            pd.register(
                consumer_position.as_ref() as *const AtomicU64 as *mut u8,
                std::mem::size_of::<AtomicU64>(),
                AccessFlags::LOCAL_WRITE | AccessFlags::REMOTE_READ,
            )?
        };

        // Read buffer for RDMA READ results
        let mut read_buffer = Box::new([0u8; 8]);
        let read_buffer_mr = unsafe {
            pd.register(
                read_buffer.as_mut_ptr(),
                8,
                AccessFlags::LOCAL_WRITE,
            )?
        };

        // Create QP with SRQ using MonoCq for recv
        let send_cqe_buffer_clone = send_cqe_buffer.clone();
        let sq_callback: SqCqCallback = Box::new(move |cqe, entry| {
            // Log error CQEs
            if cqe.opcode == mlx5::cq::CqeOpcode::ReqErr || cqe.opcode == mlx5::cq::CqeOpcode::RespErr {
                eprintln!("[send_cq ERROR] qpn={}, opcode={:?}, syndrome={}", entry.qpn, cqe.opcode, cqe.syndrome);
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

        let qpn = qp.borrow().qpn();

        // Register QP with MonoCq for completion dispatch
        recv_cq.register(&qp);

        let inner = Rc::new(RefCell::new(Self {
            qp,
            send_ring,
            send_ring_mr,
            recv_ring,
            recv_ring_mr,
            remote_recv_ring: Cell::new(None),
            send_ring_producer: Cell::new(0),
            send_ring_consumer: Cell::new(0),
            recv_ring_producer: Cell::new(0),
            last_recv_pos: Cell::new(0),
            remote_consumer: RemoteConsumer::new(),
            unsignaled_count: Cell::new(0),
            pending_calls: RefCell::new(Slab::new()),
            flush_start_pos: Cell::new(0),
            read_inflight: Cell::new(false),
            force_read: Cell::new(false),
            consumer_position,
            consumer_position_mr,
            read_buffer,
            read_buffer_mr,
            remote_consumer_mr: Cell::new(None),
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
            consumer_addr: self.consumer_position_mr.addr() as u64,
            consumer_rkey: self.consumer_position_mr.rkey(),
        }
    }

    /// Emit WQE for accumulated data (without doorbell).
    ///
    /// Returns Ok(true) if WQE was emitted, Ok(false) if nothing to emit.
    #[inline(always)]
    fn emit_wqe(&self) -> Result<bool> {
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
        let ring_len = self.send_ring.len();

        // Check for wrap-around: if data spans ring boundary, emit in two parts
        if start_offset + delta as usize > ring_len {
            // Part 1: from start_offset to end of ring
            let part1_len = ring_len - start_offset;
            let part1_imm = encode_imm(part1_len as u64);

            let qpn = self.qpn();
            let count = self.unsignaled_count.get();

            // Emit part 1
            {
                let qp = self.qp.borrow();
                let ctx = qp.emit_ctx().map_err(Error::Io)?;
                emit_wqe!(&ctx, write_imm {
                    flags: WqeFlags::empty(),
                    remote_addr: remote_addr,
                    rkey: remote_ring.rkey,
                    imm: part1_imm,
                    sge: { addr: self.send_ring_mr.addr() as u64 + start_offset as u64, len: part1_len as u32, lkey: self.send_ring_mr.lkey() },
                }).map_err(|e| Error::Io(e.into()))?;
            }

            // Part 2: from beginning of ring
            let part2_len = delta as usize - part1_len;
            let part2_imm = encode_imm(part2_len as u64);
            let part2_remote_addr = remote_ring.addr + ((remote_offset + part1_len as u64) & (remote_ring.size - 1));

            // Determine if part 2 should be signaled
            let should_signal = (count + 1) >= SIGNAL_INTERVAL;

            {
                let qp = self.qp.borrow();
                let ctx = qp.emit_ctx().map_err(Error::Io)?;
                if should_signal {
                    emit_wqe!(&ctx, write_imm {
                        flags: WqeFlags::empty(),
                        remote_addr: part2_remote_addr,
                        rkey: remote_ring.rkey,
                        imm: part2_imm,
                        sge: { addr: self.send_ring_mr.addr() as u64, len: part2_len as u32, lkey: self.send_ring_mr.lkey() },
                        signaled: SrqEntry { qpn, is_read: false },
                    }).map_err(|e| Error::Io(e.into()))?;
                    self.unsignaled_count.set(0);
                } else {
                    emit_wqe!(&ctx, write_imm {
                        flags: WqeFlags::empty(),
                        remote_addr: part2_remote_addr,
                        rkey: remote_ring.rkey,
                        imm: part2_imm,
                        sge: { addr: self.send_ring_mr.addr() as u64, len: part2_len as u32, lkey: self.send_ring_mr.lkey() },
                    }).map_err(|e| Error::Io(e.into()))?;
                    self.unsignaled_count.set(count + 2); // 2 WQEs emitted
                }
            }

            self.flush_start_pos.set(end);
            return Ok(true);
        }

        // Determine if this WQE should be signaled
        let count = self.unsignaled_count.get();
        let should_signal = count >= SIGNAL_INTERVAL;

        // Get QPN before borrowing qp to avoid borrow conflict
        let qpn = self.qpn();

        {
            let qp = self.qp.borrow();
            let ctx = qp.emit_ctx().map_err(Error::Io)?;
            if should_signal {
                // Signal this WQE to allow SQ progress tracking
                // Mark with is_read=false to distinguish from READ completions
                emit_wqe!(&ctx, write_imm {
                    flags: WqeFlags::empty(),
                    remote_addr: remote_addr,
                    rkey: remote_ring.rkey,
                    imm: imm,
                    sge: { addr: self.send_ring_mr.addr() as u64 + start_offset as u64, len: delta as u32, lkey: self.send_ring_mr.lkey() },
                    signaled: SrqEntry { qpn, is_read: false },
                }).map_err(|e| Error::Io(e.into()))?;
                self.unsignaled_count.set(0);
            } else {
                emit_wqe!(&ctx, write_imm {
                    flags: WqeFlags::empty(),
                    remote_addr: remote_addr,
                    rkey: remote_ring.rkey,
                    imm: imm,
                    sge: { addr: self.send_ring_mr.addr() as u64 + start_offset as u64, len: delta as u32, lkey: self.send_ring_mr.lkey() },
                }).map_err(|e| Error::Io(e.into()))?;
                self.unsignaled_count.set(count + 1);
            }
        }

        self.flush_start_pos.set(end);
        Ok(true)
    }

    /// Emit WQE and ring doorbell using direct BlueFlame.
    ///
    /// Uses the optimized `emit_write_imm_direct()` which constructs WQE
    /// using SIMD and writes directly to BlueFlame register.
    ///
    /// For wrap-around cases (data spans ring boundary), emits two WQEs
    /// sequentially via direct BlueFlame.
    #[inline(always)]
    fn flush(&self) -> Result<()> {
        let remote_ring = self
            .remote_recv_ring
            .get()
            .ok_or(Error::RemoteConsumerUnknown)?;

        let start = self.flush_start_pos.get();
        let end = self.send_ring_producer.get();

        if start == end {
            return Ok(());
        }

        let delta = end - start;
        let start_offset = (start & (self.send_ring.len() as u64 - 1)) as usize;
        let remote_offset = start & (remote_ring.size - 1);
        let remote_addr = remote_ring.addr + remote_offset;
        let ring_len = self.send_ring.len();

        // Check for wrap-around: if data spans ring boundary, emit two WQEs
        if start_offset + delta as usize > ring_len {
            // Wrap-around case: emit two WQEs directly via BlueFlame
            let part1_len = ring_len - start_offset;
            let part1_imm = encode_imm(part1_len as u64);

            let count = self.unsignaled_count.get();
            let qpn = self.qpn();
            let qp = self.qp.borrow();

            // Part 1: from start_offset to end of ring (always unsignaled)
            let params1 = WriteImmParams {
                wqe_idx: 0,
                qpn: 0,
                flags: WqeFlags::empty(),
                imm: part1_imm,
                remote_addr,
                rkey: remote_ring.rkey,
                local_addr: self.send_ring_mr.addr() as u64 + start_offset as u64,
                byte_count: part1_len as u32,
                lkey: self.send_ring_mr.lkey(),
            };
            qp.emit_write_imm_direct(params1)
                .map_err(|e| Error::Io(e.into()))?;

            // Part 2: from beginning of ring
            let part2_len = delta as usize - part1_len;
            let part2_imm = encode_imm(part2_len as u64);
            let part2_remote_addr = remote_ring.addr + ((remote_offset + part1_len as u64) & (remote_ring.size - 1));

            let params2 = WriteImmParams {
                wqe_idx: 0,
                qpn: 0,
                flags: WqeFlags::empty(),
                imm: part2_imm,
                remote_addr: part2_remote_addr,
                rkey: remote_ring.rkey,
                local_addr: self.send_ring_mr.addr() as u64,
                byte_count: part2_len as u32,
                lkey: self.send_ring_mr.lkey(),
            };

            // Signal on part 2 if needed
            let should_signal = (count + 1) >= SIGNAL_INTERVAL;
            if should_signal {
                qp.emit_write_imm_direct_signaled(params2, SrqEntry { qpn, is_read: false })
                    .map_err(|e| Error::Io(e.into()))?;
                self.unsignaled_count.set(0);
            } else {
                qp.emit_write_imm_direct(params2)
                    .map_err(|e| Error::Io(e.into()))?;
                self.unsignaled_count.set(count + 2); // 2 WQEs emitted
            }

            self.flush_start_pos.set(end);
            return Ok(());
        }

        // Hot path: single WQE, use direct BlueFlame
        let imm = encode_imm(delta);
        let count = self.unsignaled_count.get();
        let should_signal = count >= SIGNAL_INTERVAL;
        let qpn = self.qpn();

        let params = WriteImmParams {
            wqe_idx: 0, // Set by emit_write_imm_direct
            qpn: 0,     // Set by emit_write_imm_direct
            flags: WqeFlags::empty(),
            imm,
            remote_addr,
            rkey: remote_ring.rkey,
            local_addr: self.send_ring_mr.addr() as u64 + start_offset as u64,
            byte_count: delta as u32,
            lkey: self.send_ring_mr.lkey(),
        };

        let qp = self.qp.borrow();
        if should_signal {
            qp.emit_write_imm_direct_signaled(params, SrqEntry { qpn, is_read: false })
                .map_err(|e| Error::Io(e.into()))?;
            self.unsignaled_count.set(0);
        } else {
            qp.emit_write_imm_direct(params)
                .map_err(|e| Error::Io(e.into()))?;
            self.unsignaled_count.set(count + 1);
        }

        self.flush_start_pos.set(end);
        Ok(())
    }

    /// Check if we need to issue a READ for remote consumer position.
    fn needs_read(&self) -> bool {
        let remote_ring = match self.remote_recv_ring.get() {
            Some(r) => r,
            None => return false,
        };

        let producer = self.send_ring_producer.get();
        let consumer = self.remote_consumer.get();
        let in_flight = producer.wrapping_sub(consumer);
        let available = remote_ring.size.saturating_sub(in_flight);

        // Request READ when available space drops below 25% of ring size
        let threshold = remote_ring.size / 4;
        available < threshold
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
    /// Local consumer position MR address (for RDMA READ).
    pub consumer_addr: u64,
    /// Local consumer position MR rkey (for RDMA READ).
    pub consumer_rkey: u32,
}

/// An RPC endpoint representing a connection to a remote peer.
pub struct Endpoint<U> {
    inner: Rc<RefCell<EndpointInner<U>>>,
    _marker: PhantomData<U>,
}

impl<U> Endpoint<U> {
    /// Get the QP number.
    pub fn qpn(&self) -> u32 {
        self.inner.borrow().qpn()
    }

    /// Get local endpoint info for connection establishment.
    pub fn local_info(&self, ctx_lid: u16, ctx_port: u8) -> (LocalEndpointInfo, u16, u8) {
        let info = self.inner.borrow().local_info();
        (info, ctx_lid, ctx_port)
    }

    /// Connect to a remote endpoint.
    pub fn connect(&mut self, remote: &RemoteEndpointInfo, local_psn: u32, port: u8) -> io::Result<()> {
        let inner = self.inner.borrow();

        // Set remote ring info
        inner.remote_recv_ring.set(Some(RemoteRingInfo::new(
            remote.recv_ring_addr,
            remote.recv_ring_rkey,
            remote.recv_ring_size,
        )));

        // Set remote consumer MR info for RDMA READ
        inner.remote_consumer_mr.set(Some(RemoteConsumerMr {
            addr: remote.consumer_addr,
            rkey: remote.consumer_rkey,
        }));

        // Connect the QP
        let remote_qp_info = IbRemoteQpInfo {
            qp_number: remote.qp_number,
            packet_sequence_number: remote.packet_sequence_number,
            local_identifier: remote.local_identifier,
        };

        // Use standard access flags for RDMA WRITE and READ
        let access_flags = mlx5_sys::ibv_access_flags_IBV_ACCESS_LOCAL_WRITE
            | mlx5_sys::ibv_access_flags_IBV_ACCESS_REMOTE_WRITE
            | mlx5_sys::ibv_access_flags_IBV_ACCESS_REMOTE_READ;

        inner.qp.borrow_mut().connect(
            &remote_qp_info,
            port,
            local_psn,
            4,            // max_rd_atomic
            4,            // max_dest_rd_atomic
            access_flags,
        )?;

        Ok(())
    }

    /// Send an RPC call (async).
    ///
    /// The call is written to the send buffer. Actual transmission happens
    /// when Context::poll() is called.
    ///
    /// When the response arrives, the Context's on_response callback will
    /// be invoked with the provided user_data.
    pub fn call(&self, data: &[u8], user_data: U) -> Result<u32> {
        let inner = self.inner.borrow();

        let remote_ring = inner
            .remote_recv_ring
            .get()
            .ok_or(Error::RemoteConsumerUnknown)?;

        // Calculate message size
        let msg_size = padded_message_size(data.len() as u32);

        let mut producer = inner.send_ring_producer.get();
        let send_ring_size = inner.send_ring.len() as u64;
        let mut send_offset = (producer & (send_ring_size - 1)) as usize;

        // Check if write would wrap around the local send buffer
        if send_offset + msg_size as usize > inner.send_ring.len() {
            // First emit any pending WQEs before the wrap point
            inner.emit_wqe()?;

            // Write padding marker at current offset
            let remaining = inner.send_ring.len() - send_offset;
            unsafe {
                let buf_ptr = inner.send_ring.as_ptr().add(send_offset) as *mut u8;
                write_padding_marker(buf_ptr, remaining as u32);
            }
            // Advance producer past the padding (to next ring cycle start)
            producer += remaining as u64;
            inner.send_ring_producer.set(producer);

            // Emit the padding marker WQE (sends padding to remote)
            inner.emit_wqe()?;

            // Recalculate offset (should now be 0)
            send_offset = 0;
        }

        // Check if we have space in the remote ring
        let consumer = inner.remote_consumer.get();
        let available = remote_ring.size - (producer - consumer);

        if available < msg_size {
            // No space: flush and set force_read flag to trigger RDMA READ
            inner.flush()?;
            inner.force_read.set(true);
            return Err(Error::RingFull);
        }

        // Allocate call ID from slab (index is the call_id)
        let call_id = inner.pending_calls.borrow_mut().insert(user_data) as u32;

        // Prepare message in send ring
        let local_consumer = inner.last_recv_pos.get();

        unsafe {
            let buf_ptr = inner.send_ring.as_ptr().add(send_offset) as *mut u8;
            encode_header(buf_ptr, call_id, local_consumer, data.len() as u32);
            std::ptr::copy_nonoverlapping(
                data.as_ptr(),
                buf_ptr.add(HEADER_SIZE),
                data.len(),
            );
        }

        // Update producer
        inner.send_ring_producer.set(producer + msg_size);

        Ok(call_id)
    }
}

/// Handle to a received RPC request.
///
/// When dropped, advances the consumer position in the receive ring.
pub struct RecvHandle<'a, U, F>
where
    F: Fn(U, &[u8]),
{
    #[allow(dead_code)]
    ctx: &'a Context<U, F>,
    endpoint: Rc<RefCell<EndpointInner<U>>>,
    qpn: u32,
    call_id: u32,
    data_offset: usize,
    data_len: usize,
    recv_pos: u64,
    #[allow(dead_code)]
    recv_ring_len: usize,
}

impl<U, F: Fn(U, &[u8])> RecvHandle<'_, U, F> {
    /// Get the request data as a copy.
    ///
    /// Note: This returns a copy of the data because the underlying buffer
    /// is managed by RefCell and cannot be borrowed across the RecvHandle lifetime.
    pub fn data(&self) -> Vec<u8> {
        let inner = self.endpoint.borrow();
        inner.recv_ring[self.data_offset..self.data_offset + self.data_len].to_vec()
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
    pub fn reply(&self, data: &[u8]) -> Result<()> {
        let inner = self.endpoint.borrow();

        let remote_ring = inner
            .remote_recv_ring
            .get()
            .ok_or(Error::RemoteConsumerUnknown)?;

        // Calculate message size
        let msg_size = padded_message_size(data.len() as u32);

        let mut producer = inner.send_ring_producer.get();
        let send_ring_size = inner.send_ring.len() as u64;
        let mut send_offset = (producer & (send_ring_size - 1)) as usize;

        // Check if write would wrap around the local send buffer
        if send_offset + msg_size as usize > inner.send_ring.len() {
            // First emit any pending WQEs before the wrap point
            inner.emit_wqe()?;

            // Write padding marker at current offset
            let remaining = inner.send_ring.len() - send_offset;
            unsafe {
                let buf_ptr = inner.send_ring.as_ptr().add(send_offset) as *mut u8;
                write_padding_marker(buf_ptr, remaining as u32);
            }
            // Advance producer past the padding (to next ring cycle start)
            producer += remaining as u64;
            inner.send_ring_producer.set(producer);

            // Emit the padding marker WQE (sends padding to remote)
            inner.emit_wqe()?;

            // Recalculate offset (should now be 0)
            send_offset = 0;
        }

        // Check if we have space in the remote ring
        let consumer = inner.remote_consumer.get();
        let available = remote_ring.size - (producer - consumer);

        if available < msg_size {
            // No space: flush and set force_read flag to trigger RDMA READ
            inner.flush()?;
            inner.force_read.set(true);
            return Err(Error::RingFull);
        }

        // Response call_id has MSB set
        let response_call_id = to_response_id(self.call_id);

        // Prepare message in send ring
        let local_consumer = inner.last_recv_pos.get();

        unsafe {
            let buf_ptr = inner.send_ring.as_ptr().add(send_offset) as *mut u8;
            encode_header(buf_ptr, response_call_id, local_consumer, data.len() as u32);
            std::ptr::copy_nonoverlapping(
                data.as_ptr(),
                buf_ptr.add(HEADER_SIZE),
                data.len(),
            );
        }

        // Update producer
        inner.send_ring_producer.set(producer + msg_size);

        Ok(())
    }
}

impl<U, F: Fn(U, &[u8])> Drop for RecvHandle<'_, U, F> {
    fn drop(&mut self) {
        // Consumer position is already advanced in process_cqe().
        // No need to update here - doing so would cause incorrect behavior
        // when multiple messages are popped from recv_stack in reverse order.
    }
}
