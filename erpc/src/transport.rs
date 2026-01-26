//! Transport layer for eRPC.
//!
//! Provides a wrapper around mlx5::ud for UD-based transport.

use std::cell::{Cell, RefCell};
use std::rc::Rc;

use mlx5::cq::{Cq, CqConfig, Cqe};
use mlx5::device::Context;
use mlx5::pd::{AddressHandle, Pd, RemoteUdQpInfo};
use mlx5::ud::{UdQpConfig, UdQpIb};
pub use mlx5::wqe::emit::UdAvIb;
use mlx5::wqe::WqeFlags;

use crate::buffer::MsgBuffer;
use crate::config::RpcConfig;
use crate::error::{Error, Result};
use crate::packet::{PktHdr, PKT_HDR_SIZE};

/// Buffer type for distinguishing request vs response buffers.
///
/// Request buffers must be retained until response is received (for retransmission).
/// Response buffers can be freed immediately on send completion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BufferType {
    /// Request buffer - keep until response received (for retransmission).
    #[default]
    Request,
    /// Response buffer - free on send completion.
    Response,
}

/// Receive completion information.
#[derive(Debug, Clone, Copy)]
pub struct RecvCompletion {
    /// Buffer index in the buffer pool.
    pub buf_idx: usize,
    /// Byte count of received data.
    pub byte_cnt: u32,
}

/// Send completion information.
#[derive(Debug, Clone, Copy)]
pub struct SendCompletion {
    /// Buffer index in the buffer pool.
    pub buf_idx: usize,
    /// Buffer type (request or response).
    pub buf_type: BufferType,
}

/// UD transport send/receive entry.
///
/// This entry is associated with each WQE and returned on completion.
#[derive(Debug, Clone, Copy)]
pub struct TransportEntry {
    /// Buffer index in the buffer pool.
    pub buf_idx: usize,
    /// Session number (for received packets).
    pub session_num: u16,
    /// Additional context (application-defined).
    pub context: u64,
    /// Buffer type (request or response).
    pub buf_type: BufferType,
}

impl Default for TransportEntry {
    fn default() -> Self {
        Self {
            buf_idx: 0,
            session_num: 0,
            context: 0,
            buf_type: BufferType::Request,
        }
    }
}

/// Remote endpoint information for UD transport.
#[derive(Debug, Clone, Copy)]
pub struct RemoteInfo {
    /// Remote QP number.
    pub qpn: u32,
    /// Remote Q_Key.
    pub qkey: u32,
    /// Remote LID (Local Identifier).
    pub lid: u16,
}

impl From<RemoteInfo> for RemoteUdQpInfo {
    fn from(info: RemoteInfo) -> Self {
        RemoteUdQpInfo {
            qpn: info.qpn,
            qkey: info.qkey,
            lid: info.lid,
        }
    }
}

/// Local endpoint information.
#[derive(Debug, Clone, Copy)]
pub struct LocalInfo {
    /// Local QP number.
    pub qpn: u32,
    /// Local Q_Key.
    pub qkey: u32,
    /// Local LID.
    pub lid: u16,
}

/// Completion callback type for send operations.
type SendCallback = Box<dyn Fn(Cqe, TransportEntry)>;

/// Completion callback type for receive operations.
type RecvCallback = Box<dyn Fn(Cqe, TransportEntry)>;

/// Completion buffers shared with callbacks.
struct CompletionBuffers {
    send_completions: Vec<SendCompletion>,
    recv_completions: Vec<RecvCompletion>,
    /// Pending response buffer indices for unsignaled sends.
    /// (send_counter, buf_idx) - counter is used to match with signaled completion.
    pending_response_bufs: Vec<(u32, usize)>,
}

/// Signaled interval - every Nth send is signaled.
/// Lower values = more CQ overhead but safer for SQ.
/// Higher values = less CQ overhead but more pending buffers.
const SIGNALED_INTERVAL: u32 = 4;

/// UD transport wrapper.
///
/// Provides a higher-level interface over mlx5::ud::UdQp for eRPC.
pub struct UdTransport {
    /// The underlying UD QP.
    qp: Rc<RefCell<UdQpIb<TransportEntry, TransportEntry, SendCallback, RecvCallback>>>,
    /// Send completion queue.
    send_cq: Rc<Cq>,
    /// Receive completion queue.
    recv_cq: Rc<Cq>,
    /// Protection domain.
    pd: Pd,
    /// Port number.
    port: u8,
    /// Local LID.
    lid: u16,
    /// MTU in bytes.
    mtu: usize,
    /// Q_Key.
    qkey: u32,
    /// Number of pending send operations.
    pending_sends: Cell<u32>,
    /// Number of pending receive operations.
    pending_recvs: Cell<u32>,
    /// Completion buffers for direct polling pattern.
    completions: Rc<RefCell<CompletionBuffers>>,
    /// Send counter for signaled interval.
    send_counter: Cell<u32>,
}

impl UdTransport {
    /// Create a new UD transport.
    pub fn new(ctx: &Context, port: u8, config: &RpcConfig) -> Result<Self> {
        // Query port attributes for LID and MTU
        let port_attr = ctx.query_port(port)?;
        let lid = port_attr.lid;
        let mtu = port_attr.active_mtu.bytes();

        // Allocate PD
        let pd = ctx.alloc_pd()?;

        // Create CQs
        let cq_config = CqConfig::default();
        let send_cq = ctx.create_cq(config.max_send_wr as i32, &cq_config)?;
        let recv_cq = ctx.create_cq(config.max_recv_wr as i32, &cq_config)?;

        let send_cq = Rc::new(send_cq);
        let recv_cq = Rc::new(recv_cq);

        // Create UD QP configuration
        let qp_config = UdQpConfig {
            max_send_wr: config.max_send_wr,
            max_recv_wr: config.max_recv_wr,
            max_send_sge: 1,
            max_recv_sge: 1,
            max_inline_data: config.max_inline_data,
            qkey: config.qkey,
        };

        // Create completion buffers shared with callbacks
        let completions = Rc::new(RefCell::new(CompletionBuffers {
            send_completions: Vec::with_capacity(config.max_send_wr as usize),
            recv_completions: Vec::with_capacity(config.max_recv_wr as usize),
            pending_response_bufs: Vec::with_capacity(config.max_send_wr as usize),
        }));

        // Create callbacks that capture completion buffers
        let send_completions = completions.clone();
        let send_callback: SendCallback = Box::new(move |_cqe, entry| {
            let mut completions = send_completions.borrow_mut();
            // The signaled WQE's send_counter is stored in entry.context
            // Release only pending buffers with counter <= this counter
            let signaled_counter = entry.context as u32;

            // Partition pending buffers: release those with counter <= signaled_counter
            let pending = std::mem::take(&mut completions.pending_response_bufs);
            let mut to_release = Vec::new();
            let mut remaining = Vec::new();
            for (counter, buf_idx) in pending {
                // Use wrapping comparison to handle counter overflow
                let diff = signaled_counter.wrapping_sub(counter);
                if diff < 0x8000_0000 {
                    // counter <= signaled_counter (within half the range)
                    to_release.push(buf_idx);
                } else {
                    // counter > signaled_counter - keep for later
                    remaining.push((counter, buf_idx));
                }
            }
            completions.pending_response_bufs = remaining;

            // Add completions for released buffers
            for buf_idx in to_release {
                completions.send_completions.push(SendCompletion {
                    buf_idx,
                    buf_type: BufferType::Response,
                });
            }
            // Add the signaled completion itself
            completions.send_completions.push(SendCompletion {
                buf_idx: entry.buf_idx,
                buf_type: entry.buf_type,
            });
        });

        let recv_completions = completions.clone();
        let recv_callback: RecvCallback = Box::new(move |cqe, entry| {
            recv_completions.borrow_mut().recv_completions.push(RecvCompletion {
                buf_idx: entry.buf_idx,
                byte_cnt: cqe.byte_cnt,
            });
        });

        // Build the UD QP
        let qp = ctx
            .ud_qp_builder::<TransportEntry, TransportEntry>(&pd, &qp_config)
            .sq_cq(send_cq.clone(), send_callback)
            .rq_cq(recv_cq.clone(), recv_callback)
            .build()?;

        // Transition QP to RTR/RTS
        {
            let mut qp_ref = qp.borrow_mut();
            qp_ref.activate(port, 0)?;
        }

        Ok(Self {
            qp,
            send_cq,
            recv_cq,
            pd,
            port,
            lid,
            mtu,
            qkey: config.qkey,
            pending_sends: Cell::new(0),
            pending_recvs: Cell::new(0),
            completions,
            send_counter: Cell::new(0),
        })
    }

    /// Get the local endpoint information.
    pub fn local_info(&self) -> LocalInfo {
        let qp = self.qp.borrow();
        LocalInfo {
            qpn: qp.qpn(),
            qkey: self.qkey,
            lid: self.lid,
        }
    }

    /// Get the Protection Domain.
    pub fn pd(&self) -> &Pd {
        &self.pd
    }

    /// Get the MTU in bytes.
    pub fn mtu(&self) -> usize {
        self.mtu
    }

    /// Get the port number.
    pub fn port(&self) -> u8 {
        self.port
    }

    /// Create an Address Handle for a remote endpoint.
    pub fn create_ah(&self, remote: &RemoteInfo) -> Result<AddressHandle> {
        let info = RemoteUdQpInfo::from(*remote);
        self.pd.create_ah(self.port, &info).map_err(Error::from)
    }

    /// Post a send operation using the emit_ud_wqe! macro approach.
    ///
    /// Uses signaled interval: only every Nth send is signaled to reduce
    /// CQ overhead while still updating SQ ci.
    /// For unsignaled Response sends, the buffer index is queued for deferred
    /// deallocation when the next signaled completion arrives.
    /// The buffer must be registered.
    pub fn post_send(
        &self,
        av: UdAvIb,
        buf: &MsgBuffer,
        entry: TransportEntry,
    ) -> Result<()> {
        let qp = self.qp.borrow();
        let lkey = buf.lkey().ok_or_else(|| {
            Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Buffer not registered",
            ))
        })?;

        // Use signaled interval: every Nth send is signaled
        // Unsignaled Response buffers are queued for deferred deallocation
        let count = self.send_counter.get();
        let should_signal = count % SIGNALED_INTERVAL == 0;
        self.send_counter.set(count.wrapping_add(1));

        let ctx = qp.emit_ctx()?;
        if should_signal {
            // Store send_counter in entry.context for completion ordering
            let mut signaled_entry = entry;
            signaled_entry.context = count as u64;
            mlx5::emit_ud_wqe!(&ctx, send {
                av: av,
                flags: WqeFlags::empty(),
                sge: { addr: buf.addr(), len: buf.len() as u32, lkey: lkey },
                signaled: signaled_entry,
            })?;
        } else {
            // Queue Response buffers for deferred deallocation with counter
            if entry.buf_type == BufferType::Response && entry.buf_idx != usize::MAX {
                self.completions.borrow_mut().pending_response_bufs.push((count, entry.buf_idx));
            }
            mlx5::emit_ud_wqe!(&ctx, send {
                av: av,
                flags: WqeFlags::empty(),
                sge: { addr: buf.addr(), len: buf.len() as u32, lkey: lkey },
            })?;
        }

        self.pending_sends.set(self.pending_sends.get() + 1);
        Ok(())
    }

    /// Post an unsignaled send operation.
    ///
    /// This variant does not generate a completion, which reduces CQ overhead.
    /// Use with caution: the buffer must remain valid until the send completes.
    pub fn post_send_unsignaled(
        &self,
        av: UdAvIb,
        buf: &MsgBuffer,
    ) -> Result<()> {
        let qp = self.qp.borrow();
        let lkey = buf.lkey().ok_or_else(|| {
            Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Buffer not registered",
            ))
        })?;

        // Use the emit context to post the WQE (unsignaled)
        let ctx = qp.emit_ctx()?;
        mlx5::emit_ud_wqe!(&ctx, send {
            av: av,
            flags: WqeFlags::empty(),
            sge: { addr: buf.addr(), len: buf.len() as u32, lkey: lkey },
        })?;

        self.pending_sends.set(self.pending_sends.get() + 1);
        Ok(())
    }

    /// Post an inline send operation.
    ///
    /// Data is copied directly into the WQE, so the source buffer can be
    /// immediately freed after this call returns.
    ///
    /// Uses signaled interval: only every Nth send is signaled to reduce
    /// CQ overhead while still updating SQ ci.
    ///
    /// # Arguments
    /// * `av` - Address vector for the destination
    /// * `data` - Data to send (copied into WQE)
    /// * `signaled` - If true, forces signaled (for explicit control)
    /// * `entry` - Entry to pass to completion callback (only used if signaled)
    pub fn post_send_inline(
        &self,
        av: UdAvIb,
        data: &[u8],
        force_signaled: bool,
        entry: TransportEntry,
    ) -> Result<()> {
        let qp = self.qp.borrow();
        let ctx = qp.emit_ctx()?;

        // Use signaled interval like post_send
        // Inline sends don't need buffer deallocation, so interval is sufficient
        let count = self.send_counter.get();
        let should_signal = force_signaled || count % SIGNALED_INTERVAL == 0;
        self.send_counter.set(count.wrapping_add(1));

        if should_signal {
            mlx5::emit_ud_wqe!(&ctx, send {
                av: av,
                flags: WqeFlags::empty(),
                inline: data,
                signaled: entry,
            })?;
        } else {
            mlx5::emit_ud_wqe!(&ctx, send {
                av: av,
                flags: WqeFlags::empty(),
                inline: data,
            })?;
        }

        self.pending_sends.set(self.pending_sends.get() + 1);
        Ok(())
    }

    /// Create a UdAvIb from an AddressHandle.
    pub fn ah_to_av(ah: &AddressHandle) -> UdAvIb {
        UdAvIb::new(ah.qpn(), ah.qkey(), ah.dlid())
    }

    /// Post a receive operation.
    ///
    /// The buffer must be registered.
    pub fn post_recv(&self, buf: &MsgBuffer, entry: TransportEntry) -> Result<()> {
        let lkey = buf.lkey().ok_or_else(|| {
            Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Buffer not registered",
            ))
        })?;
        self.post_recv_raw(buf.addr(), buf.capacity() as u32, lkey, entry)
    }

    /// Post a receive operation with raw parameters.
    ///
    /// This variant is useful when buffer info has already been extracted.
    pub fn post_recv_raw(&self, addr: u64, len: u32, lkey: u32, entry: TransportEntry) -> Result<()> {
        let qp = self.qp.borrow();
        qp.post_recv(entry, addr, len, lkey)?;
        self.pending_recvs.set(self.pending_recvs.get() + 1);
        Ok(())
    }

    /// Ring the send queue doorbell.
    pub fn ring_sq_doorbell(&self) {
        let qp = self.qp.borrow();
        qp.ring_sq_doorbell();
    }

    /// Ring the receive queue doorbell.
    pub fn ring_rq_doorbell(&self) {
        let qp = self.qp.borrow();
        qp.ring_rq_doorbell();
    }

    /// Poll for send completions.
    ///
    /// Returns a Vec of send completions with buffer indices.
    pub fn poll_send_completions(&self) -> Vec<SendCompletion> {
        let count = self.send_cq.poll();
        self.send_cq.flush();
        if count > 0 {
            let pending = self.pending_sends.get();
            self.pending_sends.set(pending.saturating_sub(count as u32));
        }

        // Drain the completion buffer
        let mut completions = self.completions.borrow_mut();
        std::mem::take(&mut completions.send_completions)
    }

    /// Poll for receive completions.
    ///
    /// Returns a Vec of receive completions with buffer indices and byte counts.
    pub fn poll_recv_completions(&self) -> Vec<RecvCompletion> {
        let count = self.recv_cq.poll();
        self.recv_cq.flush();
        if count > 0 {
            let pending = self.pending_recvs.get();
            self.pending_recvs.set(pending.saturating_sub(count as u32));
        }

        // Drain the completion buffer
        let mut completions = self.completions.borrow_mut();
        std::mem::take(&mut completions.recv_completions)
    }

    /// Poll the send CQ and dispatch completions (legacy).
    ///
    /// Returns the number of completions processed.
    #[deprecated(note = "Use poll_send_completions() instead")]
    pub fn poll_send_cq(&self) -> usize {
        self.poll_send_completions().len()
    }

    /// Poll the receive CQ and dispatch completions (legacy).
    ///
    /// Returns the number of completions processed.
    #[deprecated(note = "Use poll_recv_completions() instead")]
    pub fn poll_recv_cq(&self) -> usize {
        self.poll_recv_completions().len()
    }

    /// Poll both CQs (legacy).
    ///
    /// Returns (send_completions, recv_completions).
    #[deprecated(note = "Use poll_send_completions() and poll_recv_completions() instead")]
    pub fn poll(&self) -> (usize, usize) {
        let send_count = self.poll_send_completions().len();
        let recv_count = self.poll_recv_completions().len();
        (send_count, recv_count)
    }

    /// Get the number of pending send operations.
    pub fn pending_sends(&self) -> u32 {
        self.pending_sends.get()
    }

    /// Get the number of pending receive operations.
    pub fn pending_recvs(&self) -> u32 {
        self.pending_recvs.get()
    }

    /// Get the maximum payload size per packet (MTU - GRH - UD header).
    ///
    /// For UD, there's a 40-byte GRH that's prepended to received data.
    pub fn max_payload_per_pkt(&self) -> usize {
        // UD receive includes 40-byte GRH, so effective payload is MTU - 40
        // But for send, we can use full MTU minus our header
        self.mtu - PKT_HDR_SIZE
    }

    /// Get the QP number.
    pub fn qpn(&self) -> u32 {
        self.qp.borrow().qpn()
    }
}

/// GRH (Global Route Header) size for UD receive.
///
/// When receiving UD packets, a 40-byte GRH is prepended to the data.
pub const GRH_SIZE: usize = 40;

/// Extract the packet header from a received UD buffer.
///
/// UD receives include a 40-byte GRH prefix.
pub fn extract_pkt_hdr(buf: &MsgBuffer) -> Result<PktHdr> {
    if buf.len() < GRH_SIZE + PKT_HDR_SIZE {
        return Err(Error::BufferTooSmall {
            required: GRH_SIZE + PKT_HDR_SIZE,
            available: buf.len(),
        });
    }

    let hdr_ptr = unsafe { buf.as_ptr().add(GRH_SIZE) };
    let hdr = unsafe { PktHdr::read_from(hdr_ptr) };
    hdr.validate()?;
    Ok(hdr)
}

/// Get a pointer to the payload data in a received UD buffer.
///
/// Returns a pointer past the GRH and packet header.
pub fn get_payload_ptr(buf: &MsgBuffer) -> *const u8 {
    unsafe { buf.as_ptr().add(GRH_SIZE + PKT_HDR_SIZE) }
}

/// Get the payload length in a received UD buffer.
pub fn get_payload_len(buf: &MsgBuffer) -> usize {
    buf.len().saturating_sub(GRH_SIZE + PKT_HDR_SIZE)
}
