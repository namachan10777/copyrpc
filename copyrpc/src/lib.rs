//! copyrpc - High-throughput RPC framework using SPSC ring buffers and RDMA WRITE+IMM.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                         Context                                  │
//! │  ┌─────────┐  ┌─────────┐  ┌────────────────────────────────┐   │
//! │  │   CQ    │  │   SRQ   │  │  Endpoint Registry             │   │
//! │  │(MonoCq) │  │ (shared)│  │  HashMap<QPN, Endpoint>        │   │
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
use std::collections::HashMap;
use std::io;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::atomic::{AtomicU32, Ordering};

use mlx5::cq::{CqConfig, Cqe};
use mlx5::device::Context as Mlx5Context;
use mlx5::pd::{AccessFlags, MemoryRegion, Pd};
use mlx5::qp::{RcQpIbWithSrq, RcQpConfig};
use mlx5::srq::{Srq, SrqConfig};
use mlx5::transport::IbRemoteQpInfo;
use mlx5::wqe::WqeFlags;

use encoding::{
    HEADER_SIZE, decode_header, decode_imm, encode_header, encode_imm,
    from_response_id, is_response, padded_message_size, to_response_id,
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

/// Callback types for CQ completions.
type SqCallback = fn(Cqe, ());
type RqCallback = fn(Cqe, SrqEntry);

/// Type alias for the RC QP with SRQ used in copyrpc.
type CopyrpcQp = RcQpIbWithSrq<(), SrqEntry, SqCallback, RqCallback>;

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
    /// Send completion queue.
    send_cq: Rc<mlx5::cq::Cq>,
    /// Receive completion queue.
    recv_cq: Rc<mlx5::cq::Cq>,
    /// Registered endpoints by QPN.
    endpoints: RefCell<HashMap<u32, Rc<RefCell<EndpointInner<U>>>>>,
    /// Response callback.
    on_response: F,
    /// Port number (for connection establishment).
    port: u8,
    /// Local LID.
    lid: u16,
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
        let srq = Rc::new(srq);

        let send_cq = Rc::new(mlx5_ctx.create_cq(self.cq_size, &CqConfig::default())?);
        let recv_cq = Rc::new(mlx5_ctx.create_cq(self.cq_size, &CqConfig::default())?);

        Ok(Context {
            mlx5_ctx,
            pd,
            srq,
            send_cq,
            recv_cq,
            endpoints: RefCell::new(HashMap::new()),
            on_response,
            port: self.port,
            lid,
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
            config,
        )?;

        let qpn = inner.qpn();
        let inner = Rc::new(RefCell::new(inner));

        self.endpoints.borrow_mut().insert(qpn, inner.clone());

        Ok(Endpoint {
            inner,
            _marker: PhantomData,
        })
    }

    /// Poll for incoming requests and process responses.
    ///
    /// For requests (call_id MSB = 0), returns a RecvHandle.
    /// For responses (call_id MSB = 1), calls the on_response callback
    /// and continues polling.
    ///
    /// Returns `None` if no request is available.
    pub fn recv(&self) -> Option<RecvHandle<'_, U, F>> {
        loop {
            // Poll the receive CQ
            let count = self.recv_cq.poll();
            if count == 0 {
                self.recv_cq.flush();
                return None;
            }
            self.recv_cq.flush();

            // Note: The CQ polling above triggers dispatch_cqe callbacks on the QP,
            // but for copyrpc we need to process completions differently.
            // This requires a redesign to use MonoCq or a custom completion handler.

            // For now, return None - the actual implementation needs
            // to capture CQE data properly through the SRQ completion callback.
            return None;
        }
    }

    /// Process a single CQE.
    ///
    /// This is called internally when a completion is received.
    #[allow(dead_code)]
    fn process_cqe(&self, cqe: Cqe) -> Option<RecvHandle<'_, U, F>> {
        let qpn = cqe.qp_num;
        let endpoints = self.endpoints.borrow();
        let endpoint = endpoints.get(&qpn)?;

        // Decode the immediate value to get the offset delta
        let delta = decode_imm(cqe.imm);

        // Get endpoint info and update positions
        let (call_id, _piggyback, payload_len, recv_pos, header_offset, recv_ring_len) = {
            let endpoint_ref = endpoint.borrow();
            endpoint_ref.recv_ring_producer.set(endpoint_ref.recv_ring_producer.get() + delta);

            let pos = endpoint_ref.last_recv_pos.get();
            let header_offset = (pos & (endpoint_ref.recv_ring.len() as u64 - 1)) as usize;
            let header_slice = &endpoint_ref.recv_ring[header_offset..header_offset + HEADER_SIZE];

            let (call_id, piggyback, payload_len) = unsafe {
                decode_header(header_slice.as_ptr())
            };

            endpoint_ref.remote_consumer.update(piggyback as u64);

            (call_id, piggyback, payload_len, pos, header_offset, endpoint_ref.recv_ring.len())
        };

        if is_response(call_id) {
            // This is a response - invoke callback
            let original_call_id = from_response_id(call_id);

            let (user_data, data_vec) = {
                let endpoint_ref = endpoint.borrow();
                let user_data = endpoint_ref.pending_calls.borrow_mut().remove(&original_call_id);

                if let Some(user_data) = user_data {
                    let data_offset = header_offset + HEADER_SIZE;
                    let data_slice = &endpoint_ref.recv_ring[data_offset..data_offset + payload_len as usize];
                    let data_vec = data_slice.to_vec();

                    let msg_size = padded_message_size(payload_len);
                    endpoint_ref.last_recv_pos.set(recv_pos + msg_size);

                    (Some(user_data), data_vec)
                } else {
                    (None, Vec::new())
                }
            };

            if let Some(user_data) = user_data {
                (self.on_response)(user_data, &data_vec);
            }

            None
        } else {
            // This is a request - return RecvHandle
            let data_offset = header_offset + HEADER_SIZE;

            Some(RecvHandle {
                ctx: self,
                endpoint: endpoint.clone(),
                qpn,
                call_id,
                data_offset,
                data_len: payload_len as usize,
                recv_pos,
                recv_ring_len,
            })
        }
    }
}

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
    /// Send ring consumer position (from remote piggyback).
    #[allow(dead_code)]
    send_ring_consumer: Cell<u64>,
    /// Receive ring producer position (updated on receive).
    recv_ring_producer: Cell<u64>,
    /// Last received message position (consumer).
    last_recv_pos: Cell<u64>,
    /// Remote consumer position (piggyback updates).
    remote_consumer: RemoteConsumer,
    /// Next call ID for requests.
    next_call_id: AtomicU32,
    /// Pending calls waiting for responses.
    pending_calls: RefCell<HashMap<u32, U>>,
}

impl<U> EndpointInner<U> {
    fn new(
        ctx: &Mlx5Context,
        pd: &Pd,
        srq: &Rc<Srq<SrqEntry>>,
        send_cq: &Rc<mlx5::cq::Cq>,
        recv_cq: &Rc<mlx5::cq::Cq>,
        config: &EndpointConfig,
    ) -> io::Result<Self> {
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

        // Create QP with SRQ
        let sq_callback: SqCallback = |_cqe, _entry| {};
        let rq_callback: RqCallback = |_cqe, _entry| {};

        let qp = ctx
            .rc_qp_builder::<(), SrqEntry>(pd, &config.qp_config)
            .with_srq(srq.clone())
            .sq_cq(send_cq.clone(), sq_callback)
            .rq_cq(recv_cq.clone(), rq_callback)
            .build()?;

        Ok(Self {
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
            next_call_id: AtomicU32::new(1),
            pending_calls: RefCell::new(HashMap::new()),
        })
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
        }
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

        // Connect the QP
        let remote_qp_info = IbRemoteQpInfo {
            qp_number: remote.qp_number,
            packet_sequence_number: remote.packet_sequence_number,
            local_identifier: remote.local_identifier,
        };

        // Use standard access flags for RDMA WRITE
        let access_flags = mlx5_sys::ibv_access_flags_IBV_ACCESS_LOCAL_WRITE
            | mlx5_sys::ibv_access_flags_IBV_ACCESS_REMOTE_WRITE;

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
    /// The call is sent asynchronously. When the response arrives,
    /// the Context's on_response callback will be invoked with the
    /// provided user_data.
    pub fn call(&self, data: &[u8], user_data: U) -> Result<u32> {
        let inner = self.inner.borrow();

        let remote_ring = inner
            .remote_recv_ring
            .get()
            .ok_or(Error::RemoteConsumerUnknown)?;

        // Calculate message size
        let msg_size = padded_message_size(data.len() as u32);

        // Check if we have space in the remote ring
        let producer = inner.send_ring_producer.get();
        let consumer = inner.remote_consumer.get();
        let available = remote_ring.size - (producer - consumer);

        if available < msg_size {
            return Err(Error::RingFull);
        }

        // Allocate call ID
        let call_id = inner.next_call_id.fetch_add(1, Ordering::Relaxed);

        // Store pending call
        inner.pending_calls.borrow_mut().insert(call_id, user_data);

        // Prepare message in send ring
        let send_offset = (producer & (inner.send_ring.len() as u64 - 1)) as usize;
        let local_consumer = inner.last_recv_pos.get();

        // Get addresses before mutable borrow
        let send_ring_addr = inner.send_ring_mr.addr() as u64;
        let send_ring_lkey = inner.send_ring_mr.lkey();

        unsafe {
            let buf_ptr = inner.send_ring.as_ptr().add(send_offset) as *mut u8;
            encode_header(buf_ptr, call_id, local_consumer, data.len() as u32);
            std::ptr::copy_nonoverlapping(
                data.as_ptr(),
                buf_ptr.add(HEADER_SIZE),
                data.len(),
            );
        }

        // Calculate remote address
        let remote_offset = producer & (remote_ring.size - 1);
        let remote_addr = remote_ring.addr + remote_offset;

        // Encode immediate value (delta from last write)
        let imm = encode_imm(msg_size);

        // Post RDMA WRITE with IMM
        {
            let mut qp = inner.qp.borrow_mut();
            qp.sq_wqe()?
                .write_imm(WqeFlags::empty(), remote_addr, remote_ring.rkey, imm)?
                .sge(
                    send_ring_addr + send_offset as u64,
                    msg_size as u32,
                    send_ring_lkey,
                )
                .finish_unsignaled()?;

            qp.ring_sq_doorbell();
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
    pub fn reply(&self, data: &[u8]) -> Result<()> {
        let inner = self.endpoint.borrow();

        let remote_ring = inner
            .remote_recv_ring
            .get()
            .ok_or(Error::RemoteConsumerUnknown)?;

        // Calculate message size
        let msg_size = padded_message_size(data.len() as u32);

        // Check if we have space in the remote ring
        let producer = inner.send_ring_producer.get();
        let consumer = inner.remote_consumer.get();
        let available = remote_ring.size - (producer - consumer);

        if available < msg_size {
            return Err(Error::RingFull);
        }

        // Response call_id has MSB set
        let response_call_id = to_response_id(self.call_id);

        // Prepare message in send ring
        let send_offset = (producer & (inner.send_ring.len() as u64 - 1)) as usize;
        let local_consumer = inner.last_recv_pos.get();

        // Get addresses before mutable borrow
        let send_ring_addr = inner.send_ring_mr.addr() as u64;
        let send_ring_lkey = inner.send_ring_mr.lkey();

        unsafe {
            let buf_ptr = inner.send_ring.as_ptr().add(send_offset) as *mut u8;
            encode_header(buf_ptr, response_call_id, local_consumer, data.len() as u32);
            std::ptr::copy_nonoverlapping(
                data.as_ptr(),
                buf_ptr.add(HEADER_SIZE),
                data.len(),
            );
        }

        // Calculate remote address
        let remote_offset = producer & (remote_ring.size - 1);
        let remote_addr = remote_ring.addr + remote_offset;

        // Encode immediate value
        let imm = encode_imm(msg_size);

        // Post RDMA WRITE with IMM
        {
            let mut qp = inner.qp.borrow_mut();
            qp.sq_wqe()?
                .write_imm(WqeFlags::empty(), remote_addr, remote_ring.rkey, imm)?
                .sge(
                    send_ring_addr + send_offset as u64,
                    msg_size as u32,
                    send_ring_lkey,
                )
                .finish_unsignaled()?;

            qp.ring_sq_doorbell();
        }

        // Update producer
        inner.send_ring_producer.set(producer + msg_size);

        Ok(())
    }
}

impl<U, F: Fn(U, &[u8])> Drop for RecvHandle<'_, U, F> {
    fn drop(&mut self) {
        // Advance consumer position
        let inner = self.endpoint.borrow();
        let msg_size = padded_message_size(self.data_len as u32);
        inner.last_recv_pos.set(self.recv_pos + msg_size);
    }
}
