#![allow(unsafe_op_in_unsafe_fn)]

use std::cell::{Cell, RefCell, UnsafeCell};
use std::io;
use std::marker::PhantomData;
use std::rc::Rc;

use fastmap::FastMap;
use slab::Slab;

use crate::encoding::{
    HEADER_SIZE, WRAP_MESSAGE_COUNT, decode_header, encode_header, from_response_id, is_response,
    padded_message_size, to_response_id,
};
use crate::error::{self, Error, Result};

use mlx5::cq::{Cq, CqConfig, Cqe, CqeOpcode};
use mlx5::dc::{DciConfig, DciWithTable, Dct, DctConfig};
use mlx5::emit_dci_wqe;
use mlx5::pd::{AccessFlags, MemoryRegion, Pd};
use mlx5::srq::{Srq, SrqConfig};
use mlx5::wqe::SubmissionError;
use mlx5::wqe::WqeFlags;
use mlx5::wqe::emit::DcAvIb;

/// Batch metadata size in bytes.
pub const BATCH_HEADER_SIZE: usize = 32;

/// Default ring buffer size (1 MB).
pub const DEFAULT_RING_SIZE: usize = 1 << 20;

/// Default SRQ size.
pub const DEFAULT_SRQ_SIZE: u32 = 1024;

/// Default CQ size.
pub const DEFAULT_CQ_SIZE: i32 = 4096;

/// Endpoint configuration.
#[derive(Debug, Clone)]
pub struct EndpointConfig {
    pub send_ring_size: usize,
    pub recv_ring_size: usize,
}

impl Default for EndpointConfig {
    fn default() -> Self {
        Self {
            send_ring_size: DEFAULT_RING_SIZE,
            recv_ring_size: DEFAULT_RING_SIZE,
        }
    }
}

/// Remote endpoint information for DC transport.
#[derive(Debug, Clone)]
pub struct RemoteEndpointInfo {
    pub dct_number: u32,
    pub dc_key: u64,
    pub local_identifier: u16,
    pub recv_ring_addr: u64,
    pub recv_ring_rkey: u32,
    pub recv_ring_size: u64,
    pub initial_credit: u64,
    pub endpoint_id: u32,
}

/// Local endpoint information for connection establishment.
#[derive(Debug, Clone)]
pub struct LocalEndpointInfo {
    pub dct_number: u32,
    pub dc_key: u64,
    pub recv_ring_addr: u64,
    pub recv_ring_rkey: u32,
    pub recv_ring_size: u64,
    pub initial_credit: u64,
    pub endpoint_id: u32,
}

#[derive(Debug, Clone, Copy)]
struct RemoteRingInfo {
    addr: u64,
    rkey: u32,
    size: u64,
}

#[derive(Debug, Clone, Copy)]
struct RemoteDctInfo {
    dctn: u32,
    dc_key: u64,
    dlid: u16,
}

#[derive(Debug, Clone, Copy)]
struct SrqEntry;

type SqCqCallback = Box<dyn Fn(Cqe, u32)>;
type DciQp = DciWithTable<u32, SqCqCallback>;

struct RecvMessage<U> {
    endpoint: Rc<UnsafeCell<EndpointInner<U>>>,
    endpoint_id: u32,
    call_id: u32,
    data_offset: usize,
    data_len: usize,
}

const DIRTY_NONE: u32 = u32::MAX;
const SIGNAL_INTERVAL_MASK: u32 = 0x7; // Signal every 8th WQE.

struct DirtyEndpointList {
    head: u32,
    tail: u32,
    next: Vec<u32>,
    in_list: Vec<bool>,
}

impl DirtyEndpointList {
    fn new() -> Self {
        Self {
            head: DIRTY_NONE,
            tail: DIRTY_NONE,
            next: Vec::new(),
            in_list: Vec::new(),
        }
    }

    fn ensure_slot(&mut self, endpoint_id: u32) {
        let idx = endpoint_id as usize;
        if idx >= self.next.len() {
            self.next.resize(idx + 1, DIRTY_NONE);
            self.in_list.resize(idx + 1, false);
        }
    }

    fn push_back(&mut self, endpoint_id: u32) {
        self.ensure_slot(endpoint_id);
        let idx = endpoint_id as usize;
        if self.in_list[idx] {
            return;
        }
        self.in_list[idx] = true;
        self.next[idx] = DIRTY_NONE;
        if self.tail == DIRTY_NONE {
            self.head = endpoint_id;
            self.tail = endpoint_id;
            return;
        }
        let tail_idx = self.tail as usize;
        self.next[tail_idx] = endpoint_id;
        self.tail = endpoint_id;
    }

    fn pop_front(&mut self) -> Option<u32> {
        if self.head == DIRTY_NONE {
            return None;
        }
        let endpoint_id = self.head;
        let idx = endpoint_id as usize;
        self.head = self.next[idx];
        if self.head == DIRTY_NONE {
            self.tail = DIRTY_NONE;
        }
        self.next[idx] = DIRTY_NONE;
        self.in_list[idx] = false;
        Some(endpoint_id)
    }

    fn is_empty(&self) -> bool {
        self.head == DIRTY_NONE
    }
}

pub struct Context<U> {
    pd: Pd,
    srq: Rc<Srq<SrqEntry>>,
    srq_posted: Cell<u32>,
    srq_max_wr: u32,
    send_cq: Rc<Cq>,
    recv_cq: Rc<Cq>,
    dci: Rc<RefCell<DciQp>>,
    dct: Dct<SrqEntry>,
    endpoints: UnsafeCell<FastMap<Rc<UnsafeCell<EndpointInner<U>>>>>,
    dirty_endpoints: UnsafeCell<DirtyEndpointList>,
    pending_send_cqes: Cell<u32>,
    recv_stack: UnsafeCell<Vec<RecvMessage<U>>>,
    next_endpoint_id: Cell<u32>,
    port: u8,
    lid: u16,
    _marker: PhantomData<U>,
}

pub struct ContextBuilder<U> {
    device_index: usize,
    port: u8,
    srq_config: SrqConfig,
    cq_size: i32,
    dci_config: DciConfig,
    dct_config: DctConfig,
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
            dci_config: DciConfig::default(),
            dct_config: DctConfig {
                dc_key: 0xC0DE_CAFE_D00D_BEEF,
            },
            _marker: PhantomData,
        }
    }
}

impl<U> ContextBuilder<U> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn device_index(mut self, index: usize) -> Self {
        self.device_index = index;
        self
    }

    pub fn port(mut self, port: u8) -> Self {
        self.port = port;
        self
    }

    pub fn srq_config(mut self, config: SrqConfig) -> Self {
        self.srq_config = config;
        self
    }

    pub fn cq_size(mut self, size: i32) -> Self {
        self.cq_size = size;
        self
    }

    pub fn dci_config(mut self, config: DciConfig) -> Self {
        self.dci_config = config;
        self
    }

    pub fn dct_config(mut self, config: DctConfig) -> Self {
        self.dct_config = config;
        self
    }

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
        for _ in 0..srq_max_wr {
            let _ = srq.post_recv(SrqEntry, 0, 0, 0);
        }
        srq.ring_doorbell();

        let send_cq = Rc::new(mlx5_ctx.create_cq(self.cq_size, &CqConfig::default())?);
        let recv_cq = Rc::new(mlx5_ctx.create_cq(self.cq_size, &CqConfig::default())?);

        let sq_callback: SqCqCallback = Box::new(|cqe, _entry| {
            if cqe.opcode == CqeOpcode::ReqErr || cqe.opcode == CqeOpcode::RespErr {
                eprintln!(
                    "[dc send_cq ERROR] qpn={}, opcode={:?}, syndrome={}",
                    cqe.qp_num, cqe.opcode, cqe.syndrome
                );
            }
        });

        let dci = mlx5_ctx
            .dci_builder::<u32>(&pd, &self.dci_config)
            .sq_cq(send_cq.clone(), sq_callback)
            .build()?;
        dci.borrow_mut().activate(self.port, 0, 4)?;

        let mut dct = mlx5_ctx
            .dct_builder(&pd, &srq, &self.dct_config)
            .recv_cq(&recv_cq)
            .build()?;
        let access = (AccessFlags::LOCAL_WRITE | AccessFlags::REMOTE_WRITE).bits();
        dct.activate(self.port, access, 4)?;

        Ok(Context {
            pd,
            srq,
            srq_posted: Cell::new(srq_max_wr),
            srq_max_wr,
            send_cq,
            recv_cq,
            dci,
            dct,
            endpoints: UnsafeCell::new(FastMap::new()),
            dirty_endpoints: UnsafeCell::new(DirtyEndpointList::new()),
            pending_send_cqes: Cell::new(0),
            recv_stack: UnsafeCell::new(Vec::new()),
            next_endpoint_id: Cell::new(1),
            port: self.port,
            lid,
            _marker: PhantomData,
        })
    }
}

impl<U> Context<U> {
    #[inline(always)]
    fn mark_endpoint_dirty(&self, ep: &Rc<UnsafeCell<EndpointInner<U>>>) {
        let ep_ref = unsafe { &*ep.get() };
        let endpoint_id = ep_ref.endpoint_id;
        unsafe { &mut *self.dirty_endpoints.get() }.push_back(endpoint_id);
    }

    pub fn builder() -> ContextBuilder<U> {
        ContextBuilder::new()
    }

    pub fn pd(&self) -> &Pd {
        &self.pd
    }

    pub fn lid(&self) -> u16 {
        self.lid
    }

    pub fn port(&self) -> u8 {
        self.port
    }

    pub fn create_endpoint(&self, config: &EndpointConfig) -> io::Result<Endpoint<U>> {
        let endpoint_id = self.next_endpoint_id.get();
        self.next_endpoint_id.set(endpoint_id + 1);

        let inner = EndpointInner::new(&self.pd, config, endpoint_id)?;
        unsafe { &mut *self.endpoints.get() }.insert(endpoint_id, inner.clone());

        Ok(Endpoint {
            inner,
            ctx: self as *const Context<U>,
            _marker: PhantomData,
        })
    }

    pub fn poll_recv(&self, mut on_response: impl FnMut(U, &[u8])) {
        let mut recv_count = 0u32;
        let endpoints = &self.endpoints;
        let recv_stack = &self.recv_stack;

        loop {
            let count = self.recv_cq.poll_raw(|cqe| {
                if cqe.opcode == CqeOpcode::ReqErr || cqe.opcode == CqeOpcode::RespErr {
                    eprintln!(
                        "[dc recv_cq ERROR] qpn={}, opcode={:?}, syndrome={}",
                        cqe.qp_num, cqe.opcode, cqe.syndrome
                    );
                    return;
                }

                let endpoint_id = cqe.imm;
                Self::process_cqe_static(endpoint_id, endpoints, &mut on_response, recv_stack);
            });
            if count == 0 {
                break;
            }
            recv_count += count as u32;
        }

        if recv_count > 0 {
            self.recv_cq.flush();

            let posted = self.srq_posted.get().saturating_sub(recv_count);
            self.srq_posted.set(posted);

            let threshold = self.srq_max_wr * 2 / 3;
            if posted < threshold {
                let to_post = self.srq_max_wr - posted;
                for _ in 0..to_post {
                    let _ = self.srq.post_recv(SrqEntry, 0, 0, 0);
                }
                self.srq.ring_doorbell();
                self.srq_posted.set(self.srq_max_wr);
            }
        }
    }

    pub fn poll(&self, on_response: impl FnMut(U, &[u8])) {
        self.poll_recv(on_response);
        self.flush_endpoints();
    }

    pub fn has_dirty_endpoints(&self) -> bool {
        !unsafe { &*self.dirty_endpoints.get() }.is_empty()
    }

    pub fn flush_endpoints(&self) {
        let mut polled_send_cqes = 0u32;
        if self.pending_send_cqes.get() > 0 {
            loop {
                let count = self.send_cq.poll();
                if count == 0 {
                    break;
                }
                polled_send_cqes = polled_send_cqes.saturating_add(count as u32);
            }
            if polled_send_cqes > 0 {
                self.send_cq.flush();
                self.pending_send_cqes.set(
                    self.pending_send_cqes
                        .get()
                        .saturating_sub(polled_send_cqes),
                );
            }
        }

        let mut emitted = false;
        let mut newly_signaled = 0u32;

        {
            let mut dci = self.dci.borrow_mut();

            loop {
                let endpoint_id = unsafe { &mut *self.dirty_endpoints.get() }.pop_front();
                let Some(endpoint_id) = endpoint_id else {
                    break;
                };
                let ep = {
                    let endpoints = unsafe { &*self.endpoints.get() };
                    endpoints.get(endpoint_id).cloned()
                };
                let Some(ep) = ep else {
                    continue;
                };
                let ep_ref = unsafe { &*ep.get() };
                match ep_ref.emit_pending(&mut dci) {
                    Ok(was_emitted) => {
                        emitted |= was_emitted;
                        newly_signaled =
                            newly_signaled.saturating_add(ep_ref.take_newly_signaled_count());
                    }
                    Err(Error::RingFull) => {
                        newly_signaled =
                            newly_signaled.saturating_add(ep_ref.take_newly_signaled_count());
                        unsafe { &mut *self.dirty_endpoints.get() }.push_back(endpoint_id);
                        // SQ/credit pressure: retry next flush cycle.
                        break;
                    }
                    Err(_) => {
                        newly_signaled =
                            newly_signaled.saturating_add(ep_ref.take_newly_signaled_count());
                        // Drop this flush attempt; next call/reply will enqueue again.
                    }
                }
            }
        }

        if newly_signaled > 0 {
            self.pending_send_cqes
                .set(self.pending_send_cqes.get().saturating_add(newly_signaled));
        }

        if emitted {
            self.dci.borrow().ring_sq_doorbell_bf();
        }
    }

    pub fn recv(&self) -> Option<RecvHandle<'_, U>> {
        unsafe { &mut *self.recv_stack.get() }.pop().map(|msg| {
            let data_ptr = unsafe { &*msg.endpoint.get() }.recv_ring.as_ptr();
            let data_ptr = unsafe { data_ptr.add(msg.data_offset) };
            RecvHandle {
                _lifetime: PhantomData,
                ctx: self,
                endpoint: msg.endpoint,
                endpoint_id: msg.endpoint_id,
                call_id: msg.call_id,
                data_ptr,
                data_len: msg.data_len,
            }
        })
    }

    #[inline(always)]
    fn process_cqe_static(
        endpoint_id: u32,
        endpoints: &UnsafeCell<FastMap<Rc<UnsafeCell<EndpointInner<U>>>>>,
        on_response: &mut impl FnMut(U, &[u8]),
        recv_stack: &UnsafeCell<Vec<RecvMessage<U>>>,
    ) {
        let endpoints = unsafe { &*endpoints.get() };
        let endpoint = match endpoints.get(endpoint_id) {
            Some(ep) => ep,
            None => return,
        };

        let (delta, ring_credit_return, message_count);
        {
            let ep = unsafe { &*endpoint.get() };
            let pos = ep.last_recv_pos.get();
            let recv_ring_mask = ep.recv_ring.len() as u64 - 1;
            let meta_offset = (pos & recv_ring_mask) as usize;

            unsafe {
                (delta, ring_credit_return, message_count) =
                    decode_batch_header(ep.recv_ring[meta_offset..].as_ptr());
            }

            ep.recv_ring_producer
                .set(ep.recv_ring_producer.get() + delta);
            ep.remote_consumer_pos
                .set(ep.remote_consumer_pos.get() + ring_credit_return);
            ep.last_recv_pos.set(pos + BATCH_HEADER_SIZE as u64);
        }

        if message_count == WRAP_MESSAGE_COUNT {
            let ep = unsafe { &*endpoint.get() };
            let pos = ep.last_recv_pos.get();
            let recv_ring_len = ep.recv_ring.len() as u64;
            let offset = pos & (recv_ring_len - 1);
            let remaining = recv_ring_len - offset;
            ep.last_recv_pos.set(pos + remaining);
            return;
        }

        for _ in 0..message_count {
            let ep = unsafe { &*endpoint.get() };
            let pos = ep.last_recv_pos.get();
            let recv_ring_mask = ep.recv_ring.len() as u64 - 1;
            let header_offset = (pos & recv_ring_mask) as usize;

            let (call_id, _piggyback, payload_len) =
                unsafe { decode_header(ep.recv_ring[header_offset..].as_ptr()) };

            let msg_size = padded_message_size(payload_len);
            ep.last_recv_pos.set(pos + msg_size);

            if is_response(call_id) {
                let original_call_id = from_response_id(call_id);
                let user_data =
                    unsafe { &mut *ep.pending_calls.get() }.try_remove(original_call_id as usize);

                if let Some(user_data) = user_data {
                    let data_offset = header_offset + HEADER_SIZE;
                    let data_slice = &ep.recv_ring[data_offset..data_offset + payload_len as usize];
                    on_response(user_data, data_slice);
                }
            } else {
                unsafe { &mut *recv_stack.get() }.push(RecvMessage {
                    endpoint: endpoint.clone(),
                    endpoint_id,
                    call_id,
                    data_offset: header_offset + HEADER_SIZE,
                    data_len: payload_len as usize,
                });
            }
        }
    }
}

struct EndpointInner<U> {
    endpoint_id: u32,
    send_ring: Box<[u8]>,
    send_ring_mr: MemoryRegion,
    recv_ring: Box<[u8]>,
    recv_ring_mr: MemoryRegion,
    remote_recv_ring: Cell<Option<RemoteRingInfo>>,
    remote_dct: Cell<Option<RemoteDctInfo>>,
    remote_endpoint_id: Cell<u32>,
    send_ring_producer: Cell<u64>,
    recv_ring_producer: Cell<u64>,
    last_recv_pos: Cell<u64>,
    last_notified_recv_pos: Cell<u64>,
    remote_consumer_pos: Cell<u64>,
    pending_calls: UnsafeCell<Slab<U>>,
    flush_start_pos: Cell<u64>,
    meta_pos: Cell<u64>,
    batch_message_count: Cell<u32>,
    wrap_pending: Cell<bool>,
    unsignaled_count: Cell<u32>,
    newly_signaled_count: Cell<u32>,
}

impl<U> EndpointInner<U> {
    fn new(pd: &Pd, config: &EndpointConfig, endpoint_id: u32) -> io::Result<Rc<UnsafeCell<Self>>> {
        let send_ring_size = config.send_ring_size.next_power_of_two();
        let recv_ring_size = config.recv_ring_size.next_power_of_two();

        let mut send_ring = vec![0u8; send_ring_size].into_boxed_slice();
        let mut recv_ring = vec![0u8; recv_ring_size].into_boxed_slice();

        let send_access_flags = AccessFlags::LOCAL_WRITE;
        let recv_access_flags = AccessFlags::LOCAL_WRITE | AccessFlags::REMOTE_WRITE;
        let send_ring_mr =
            unsafe { pd.register(send_ring.as_mut_ptr(), send_ring.len(), send_access_flags)? };
        let recv_ring_mr =
            unsafe { pd.register(recv_ring.as_mut_ptr(), recv_ring.len(), recv_access_flags)? };

        let inner = Rc::new(UnsafeCell::new(Self {
            endpoint_id,
            send_ring,
            send_ring_mr,
            recv_ring,
            recv_ring_mr,
            remote_recv_ring: Cell::new(None),
            remote_dct: Cell::new(None),
            remote_endpoint_id: Cell::new(0),
            send_ring_producer: Cell::new(0),
            recv_ring_producer: Cell::new(0),
            last_recv_pos: Cell::new(0),
            last_notified_recv_pos: Cell::new(0),
            remote_consumer_pos: Cell::new(0),
            pending_calls: UnsafeCell::new(Slab::new()),
            flush_start_pos: Cell::new(0),
            meta_pos: Cell::new(0),
            batch_message_count: Cell::new(0),
            wrap_pending: Cell::new(false),
            unsignaled_count: Cell::new(0),
            newly_signaled_count: Cell::new(0),
        }));

        Ok(inner)
    }

    fn local_info(&self, dct: &Dct<SrqEntry>, lid: u16) -> LocalEndpointInfo {
        let dct_info = dct.remote_info(lid);
        LocalEndpointInfo {
            dct_number: dct_info.dct_number,
            dc_key: dct_info.dc_key,
            recv_ring_addr: self.recv_ring_mr.addr() as u64,
            recv_ring_rkey: self.recv_ring_mr.rkey(),
            recv_ring_size: self.recv_ring.len() as u64,
            initial_credit: self.recv_ring.len() as u64 / 2,
            endpoint_id: self.endpoint_id,
        }
    }

    #[inline(always)]
    fn has_cycle_room_for(&self, msg_size: u64) -> bool {
        let producer = self.send_ring_producer.get();
        let ring_size = self.send_ring.len() as u64;
        let offset = producer & (ring_size - 1);
        offset + msg_size < ring_size
    }

    #[inline(always)]
    fn prepare_next_batch(&self) {
        let producer = self.send_ring_producer.get();
        self.meta_pos.set(producer);
        self.batch_message_count.set(0);
        self.send_ring_producer
            .set(producer + BATCH_HEADER_SIZE as u64);
    }

    #[inline(always)]
    fn fill_metadata_with(&self, message_count: u32) {
        let meta_pos = self.meta_pos.get();
        let ring_size = self.send_ring.len() as u64;
        let meta_offset = (meta_pos & (ring_size - 1)) as usize;

        let ring_credit_return = self.last_recv_pos.get() - self.last_notified_recv_pos.get();
        self.last_notified_recv_pos.set(self.last_recv_pos.get());

        unsafe {
            let buf_ptr = self.send_ring.as_ptr().add(meta_offset) as *mut u8;
            encode_batch_header(buf_ptr, 0, ring_credit_return, message_count);
        }
    }

    #[inline(always)]
    fn emit_raw(&self, dci: &mut DciQp) -> Result<bool> {
        let remote_ring = self
            .remote_recv_ring
            .get()
            .ok_or(Error::RemoteConsumerUnknown)?;
        let remote_dct = self.remote_dct.get().ok_or(Error::RemoteConsumerUnknown)?;

        let start = self.flush_start_pos.get();
        let end = self.send_ring_producer.get();
        if start == end {
            return Ok(false);
        }

        let delta = end - start;
        let start_offset = (start & (self.send_ring.len() as u64 - 1)) as usize;

        unsafe {
            patch_batch_delta(self.send_ring[start_offset..].as_ptr() as *mut u8, delta);
        }

        let remote_offset = start & (remote_ring.size - 1);
        let remote_addr = remote_ring.addr + remote_offset;
        let imm = self.remote_endpoint_id.get();

        let delta_u32 = match u32::try_from(delta) {
            Ok(v) => v,
            Err(_) => return Err(Error::RingFull),
        };
        if delta_u32 == 0 {
            return Err(Error::RingFull);
        }

        let local_addr = self.send_ring_mr.addr() as u64 + start_offset as u64;
        let emit_ctx = dci.emit_ctx().map_err(Error::Io)?;
        let av = DcAvIb::new(remote_dct.dc_key, remote_dct.dctn, remote_dct.dlid);
        let next_count = self.unsignaled_count.get().wrapping_add(1);
        let should_signal = (next_count & SIGNAL_INTERVAL_MASK) == 0;
        if should_signal {
            emit_dci_wqe!(
                &emit_ctx,
                write_imm {
                    av: av,
                    flags: WqeFlags::empty(),
                    remote_addr: remote_addr,
                    rkey: remote_ring.rkey,
                    sge: { addr: local_addr, len: delta_u32, lkey: self.send_ring_mr.lkey() },
                    imm: imm,
                    signaled: self.endpoint_id,
                }
            )
            .map_err(|e| match e {
                SubmissionError::SqFull => Error::RingFull,
                _ => Error::Io(io::Error::other(e)),
            })?;
            self.unsignaled_count.set(0);
            self.newly_signaled_count
                .set(self.newly_signaled_count.get().saturating_add(1));
        } else {
            emit_dci_wqe!(
                &emit_ctx,
                write_imm {
                    av: av,
                    flags: WqeFlags::empty(),
                    remote_addr: remote_addr,
                    rkey: remote_ring.rkey,
                    sge: { addr: local_addr, len: delta_u32, lkey: self.send_ring_mr.lkey() },
                    imm: imm,
                }
            )
            .map_err(|e| match e {
                SubmissionError::SqFull => Error::RingFull,
                _ => Error::Io(io::Error::other(e)),
            })?;
            self.unsignaled_count.set(next_count);
        }

        self.flush_start_pos.set(end);
        Ok(true)
    }

    #[inline(always)]
    fn take_newly_signaled_count(&self) -> u32 {
        let v = self.newly_signaled_count.get();
        self.newly_signaled_count.set(0);
        v
    }

    #[inline(always)]
    fn emit_wrap(&self, dci: &mut DciQp) -> Result<()> {
        let meta_pos = self.meta_pos.get();
        self.send_ring_producer.set(meta_pos);

        let ring_size = self.send_ring.len() as u64;
        let offset = (meta_pos & (ring_size - 1)) as usize;
        let remaining = ring_size as usize - offset;

        debug_assert!(remaining >= BATCH_HEADER_SIZE);

        self.fill_metadata_with(WRAP_MESSAGE_COUNT);
        self.send_ring_producer.set(meta_pos + remaining as u64);

        self.emit_raw(dci)?;
        self.prepare_next_batch();
        Ok(())
    }

    #[inline(always)]
    fn emit_pending(&self, dci: &mut DciQp) -> Result<bool> {
        let mut emitted = false;

        if self.batch_message_count.get() > 0 {
            self.fill_metadata_with(self.batch_message_count.get());
            self.emit_raw(dci)?;
            self.prepare_next_batch();
            emitted = true;
        }

        if self.wrap_pending.get() {
            self.emit_wrap(dci)?;
            self.wrap_pending.set(false);
            emitted = true;
        }

        Ok(emitted)
    }
}

pub struct Endpoint<U> {
    inner: Rc<UnsafeCell<EndpointInner<U>>>,
    ctx: *const Context<U>,
    _marker: PhantomData<U>,
}

impl<U> Endpoint<U> {
    pub fn endpoint_id(&self) -> u32 {
        unsafe { &*self.inner.get() }.endpoint_id
    }

    pub fn local_info(&self, ctx_lid: u16, _ctx_port: u8) -> (LocalEndpointInfo, u16, u8) {
        let ctx = unsafe { &*self.ctx };
        let info = unsafe { &*self.inner.get() }.local_info(&ctx.dct, ctx_lid);
        (info, ctx_lid, ctx.port)
    }

    pub fn connect(
        &mut self,
        remote: &RemoteEndpointInfo,
        _local_psn: u32,
        _port: u8,
    ) -> io::Result<()> {
        let inner = unsafe { &*self.inner.get() };

        inner.remote_recv_ring.set(Some(RemoteRingInfo {
            addr: remote.recv_ring_addr,
            rkey: remote.recv_ring_rkey,
            size: remote.recv_ring_size,
        }));

        inner.remote_dct.set(Some(RemoteDctInfo {
            dctn: remote.dct_number,
            dc_key: remote.dc_key,
            dlid: remote.local_identifier,
        }));

        inner.remote_endpoint_id.set(remote.endpoint_id);
        inner.remote_consumer_pos.set(0);

        inner.prepare_next_batch();

        Ok(())
    }

    pub fn call(
        &self,
        data: &[u8],
        user_data: U,
        _response_allowance: u64,
    ) -> std::result::Result<u32, error::CallError<U>> {
        let inner = unsafe { &*self.inner.get() };
        let ctx = unsafe { &*self.ctx };

        let remote_ring = inner
            .remote_recv_ring
            .get()
            .ok_or(error::CallError::Other(Error::RemoteConsumerUnknown))?;

        let msg_size = padded_message_size(data.len() as u32);

        if !inner.has_cycle_room_for(msg_size) {
            inner.wrap_pending.set(true);
            ctx.mark_endpoint_dirty(&self.inner);
            return Err(error::CallError::RingFull(user_data));
        }

        let producer = inner.send_ring_producer.get();
        let consumer = inner.remote_consumer_pos.get();
        let in_flight = producer.saturating_sub(consumer);
        let available = remote_ring.size.saturating_sub(in_flight);

        if available < msg_size {
            return Err(error::CallError::RingFull(user_data));
        }

        let call_id = unsafe { &mut *inner.pending_calls.get() }.insert(user_data) as u32;
        let send_offset = (producer & (inner.send_ring.len() as u64 - 1)) as usize;

        unsafe {
            let buf_ptr = inner.send_ring.as_ptr().add(send_offset) as *mut u8;
            encode_header(buf_ptr, call_id, 0, data.len() as u32);
            std::ptr::copy_nonoverlapping(data.as_ptr(), buf_ptr.add(HEADER_SIZE), data.len());
        }

        inner.send_ring_producer.set(producer + msg_size);
        inner
            .batch_message_count
            .set(inner.batch_message_count.get() + 1);

        ctx.mark_endpoint_dirty(&self.inner);

        Ok(call_id)
    }
}

pub struct RecvHandle<'a, U> {
    _lifetime: PhantomData<&'a Context<U>>,
    ctx: &'a Context<U>,
    endpoint: Rc<UnsafeCell<EndpointInner<U>>>,
    endpoint_id: u32,
    call_id: u32,
    data_ptr: *const u8,
    data_len: usize,
}

impl<U> RecvHandle<'_, U> {
    #[inline]
    pub fn data(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.data_ptr, self.data_len) }
    }

    pub fn endpoint_id(&self) -> u32 {
        self.endpoint_id
    }

    pub fn call_id(&self) -> u32 {
        self.call_id
    }

    pub fn reply(&self, data: &[u8]) -> Result<()> {
        let inner = unsafe { &*self.endpoint.get() };

        let remote_ring = inner
            .remote_recv_ring
            .get()
            .ok_or(Error::RemoteConsumerUnknown)?;

        let msg_size = padded_message_size(data.len() as u32);

        if !inner.has_cycle_room_for(msg_size) {
            inner.wrap_pending.set(true);
            self.ctx.mark_endpoint_dirty(&self.endpoint);
            return Err(Error::RingFull);
        }

        let producer = inner.send_ring_producer.get();
        let consumer = inner.remote_consumer_pos.get();
        let in_flight = producer.saturating_sub(consumer);
        let available = remote_ring.size.saturating_sub(in_flight);
        if available < msg_size {
            return Err(Error::RingFull);
        }

        let send_offset = (producer & (inner.send_ring.len() as u64 - 1)) as usize;
        let response_call_id = to_response_id(self.call_id);

        unsafe {
            let buf_ptr = inner.send_ring.as_ptr().add(send_offset) as *mut u8;
            encode_header(buf_ptr, response_call_id, 0, data.len() as u32);
            std::ptr::copy_nonoverlapping(data.as_ptr(), buf_ptr.add(HEADER_SIZE), data.len());
        }

        inner.send_ring_producer.set(producer + msg_size);
        inner
            .batch_message_count
            .set(inner.batch_message_count.get() + 1);

        self.ctx.mark_endpoint_dirty(&self.endpoint);

        Ok(())
    }
}

#[inline]
unsafe fn encode_batch_header(
    buf: *mut u8,
    delta: u64,
    ring_credit_return: u64,
    message_count: u32,
) {
    let ptr = buf as *mut u64;
    std::ptr::write(ptr, delta.to_le());
    std::ptr::write(ptr.add(1), ring_credit_return.to_le());
    let ptr32 = buf.add(16) as *mut u32;
    std::ptr::write(ptr32, message_count.to_le());
    std::ptr::write_bytes(buf.add(20), 0, 12);
}

#[inline]
unsafe fn patch_batch_delta(buf: *mut u8, delta: u64) {
    std::ptr::write(buf as *mut u64, delta.to_le());
}

#[inline]
unsafe fn decode_batch_header(buf: *const u8) -> (u64, u64, u32) {
    let ptr = buf as *const u64;
    let delta = u64::from_le(std::ptr::read(ptr));
    let ring_credit_return = u64::from_le(std::ptr::read(ptr.add(1)));
    let ptr32 = buf.add(16) as *const u32;
    let message_count = u32::from_le(std::ptr::read(ptr32));
    (delta, ring_credit_return, message_count)
}
