//! Raw DC SEND/RECV ping-pong benchmark.
//!
//! 2 MPI ranks, copyrpc completely bypassed.
//! Each rank has its own DCI (sender) + DCT/SRQ (receiver).
//! Measures baseline RDMA DC latency without any batching or ring buffer overhead.

use std::rc::Rc;
use std::time::Instant;

use mpi::collective::CommunicatorCollectives;
use mpi::point_to_point::{Destination, Source};
use mpi::topology::Communicator;

use mlx5::cq::CqConfig;
use mlx5::dc::{DciConfig, DctConfig};
use mlx5::device::DeviceList;
use mlx5::emit_dci_wqe;
use mlx5::pd::AccessFlags;
use mlx5::srq::SrqConfig;
use mlx5::wqe::WqeFlags;
use mlx5::wqe::emit::DcAvIb;

const DC_KEY: u64 = 0xDC_0000;
const SRQ_DEPTH: usize = 128;
const RECV_ENTRY_SIZE: usize = 64;
const WARMUP: u64 = 128;
const ITERATIONS: u64 = 100_000;
const SIGNAL_INTERVAL: u64 = 64;
const SRQ_DOORBELL_INTERVAL: u64 = 32;

/// Connection info exchanged via MPI.
#[derive(Clone, Copy, Debug)]
#[repr(C)]
struct DcInfo {
    dctn: u32,
    lid: u16,
    _pad: [u8; 2],
}

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

fn main() {
    let (universe, _threading) =
        mpi::initialize_with_threading(mpi::Threading::Funneled).expect("Failed to initialize MPI");
    let world = universe.world();
    let rank = world.rank();
    let size = world.size();
    assert_eq!(size, 2, "dc_pingpong requires exactly 2 MPI ranks");
    let peer = 1 - rank;
    let is_initiator = rank == 0;

    // Open RDMA device
    let device_list = DeviceList::list().expect("No RDMA devices");
    let ctx = device_list
        .iter()
        .find_map(|d| d.open().ok())
        .expect("No mlx5 device found");
    let port = 1u8;
    let port_attr = ctx.query_port(port).expect("query_port failed");
    let pd = ctx.alloc_pd().expect("alloc_pd failed");

    // Create CQs
    let send_cq = Rc::new(
        ctx.create_cq(256, &CqConfig::default())
            .expect("create send_cq"),
    );
    let recv_cq = ctx
        .create_cq(256, &CqConfig::default())
        .expect("create recv_cq");

    // Create SRQ
    let srq_config = SrqConfig {
        max_wr: SRQ_DEPTH as u32,
        max_sge: 1,
    };
    let srq: mlx5::srq::Srq<u32> = pd.create_srq(&srq_config).expect("create_srq");

    // Allocate and register receive buffer
    let mut recv_buf = vec![0u8; SRQ_DEPTH * RECV_ENTRY_SIZE];
    let recv_buf_addr = recv_buf.as_mut_ptr() as u64;
    let recv_mr = unsafe {
        pd.register(
            recv_buf.as_mut_ptr(),
            recv_buf.len(),
            AccessFlags::LOCAL_WRITE,
        )
    }
    .expect("register recv_mr");

    // Pre-post SRQ receives
    for i in 0..SRQ_DEPTH {
        let offset = (i * RECV_ENTRY_SIZE) as u64;
        srq.post_recv(
            i as u32,
            recv_buf_addr + offset,
            RECV_ENTRY_SIZE as u32,
            recv_mr.lkey(),
        )
        .expect("post_recv");
    }
    srq.ring_doorbell();

    // Create and activate DCI
    let dci_config = DciConfig::default();
    let dci = ctx
        .dci_builder::<u64>(&pd, &dci_config)
        .sq_cq(send_cq.clone(), |_cqe, _entry| {})
        .build()
        .expect("create DCI");
    dci.borrow_mut().activate(port, 0, 4).expect("activate DCI");

    // Create and activate DCT
    let dct_config = DctConfig { dc_key: DC_KEY };
    let mut dct = ctx
        .dct_builder(&pd, &srq, &dct_config)
        .recv_cq(&recv_cq)
        .build()
        .expect("create DCT");
    let access =
        (AccessFlags::LOCAL_WRITE | AccessFlags::REMOTE_WRITE | AccessFlags::REMOTE_READ).bits();
    dct.activate(port, access, 4).expect("activate DCT");

    // Exchange connection info via MPI
    let local_info = DcInfo {
        dctn: dct.dctn(),
        lid: port_attr.lid,
        _pad: [0; 2],
    };
    let local_bytes: &[u8] = unsafe {
        std::slice::from_raw_parts(
            &local_info as *const DcInfo as *const u8,
            std::mem::size_of::<DcInfo>(),
        )
    };
    let remote_bytes = exchange_bytes(&world, rank, peer, local_bytes);
    let remote_info: DcInfo = unsafe { std::ptr::read(remote_bytes.as_ptr() as *const DcInfo) };

    let av = DcAvIb::new(DC_KEY, remote_info.dctn, remote_info.lid);

    eprintln!(
        "[rank {}] local dctn=0x{:x} lid={}, remote dctn=0x{:x} lid={}",
        rank, local_info.dctn, local_info.lid, remote_info.dctn, remote_info.lid,
    );

    // Barrier before benchmark
    world.barrier();

    // Ping-pong loop (used for both warmup and measured run)
    let do_pingpong = |iterations: u64| {
        let mut srq_repost_idx = 0usize;
        let mut flush_pending = 0u64;
        for i in 0..iterations {
            if is_initiator {
                // Send (unsignaled; signal periodically to reclaim SQ)
                {
                    let dci_ref = dci.borrow();
                    let emit = dci_ref.emit_ctx().expect("emit_ctx");
                    let payload = i.to_le_bytes();
                    if i % SIGNAL_INTERVAL == SIGNAL_INTERVAL - 1 {
                        emit_dci_wqe!(
                            &emit,
                            send {
                                av: av,
                                flags: WqeFlags::empty(),
                                inline: &payload,
                                signaled: i,
                            }
                        )
                        .expect("emit send");
                    } else {
                        emit_dci_wqe!(
                            &emit,
                            send {
                                av: av,
                                flags: WqeFlags::empty(),
                                inline: &payload,
                            }
                        )
                        .expect("emit send");
                    }
                    dci_ref.ring_sq_doorbell_bf();
                }
                // Drain send CQ only on signal iterations (off critical path)
                if i % SIGNAL_INTERVAL == SIGNAL_INTERVAL - 1 {
                    while send_cq.poll() == 0 {}
                    send_cq.flush();
                }
                // Poll recv
                while recv_cq.poll_raw(|_cqe| {}) == 0 {}
                // Batch recv CQ flush
                flush_pending += 1;
                if flush_pending >= SRQ_DOORBELL_INTERVAL {
                    recv_cq.flush();
                    flush_pending = 0;
                }
                // Advance SRQ ci + re-post with round-robin buffer
                srq.process_recv_completion(0);
                let buf_idx = srq_repost_idx % SRQ_DEPTH;
                let offset = (buf_idx * RECV_ENTRY_SIZE) as u64;
                srq.post_recv(
                    buf_idx as u32,
                    recv_buf_addr + offset,
                    RECV_ENTRY_SIZE as u32,
                    recv_mr.lkey(),
                )
                .expect("re-post recv");
                srq_repost_idx += 1;
                if srq_repost_idx % SRQ_DOORBELL_INTERVAL as usize == 0 {
                    srq.ring_doorbell();
                }
            } else {
                // Poll recv
                while recv_cq.poll_raw(|_cqe| {}) == 0 {}
                flush_pending += 1;
                if flush_pending >= SRQ_DOORBELL_INTERVAL {
                    recv_cq.flush();
                    flush_pending = 0;
                }
                // Send back (unsignaled; signal periodically)
                {
                    let dci_ref = dci.borrow();
                    let emit = dci_ref.emit_ctx().expect("emit_ctx");
                    let payload = i.to_le_bytes();
                    if i % SIGNAL_INTERVAL == SIGNAL_INTERVAL - 1 {
                        emit_dci_wqe!(
                            &emit,
                            send {
                                av: av,
                                flags: WqeFlags::empty(),
                                inline: &payload,
                                signaled: i,
                            }
                        )
                        .expect("emit send");
                    } else {
                        emit_dci_wqe!(
                            &emit,
                            send {
                                av: av,
                                flags: WqeFlags::empty(),
                                inline: &payload,
                            }
                        )
                        .expect("emit send");
                    }
                    dci_ref.ring_sq_doorbell_bf();
                }
                // Drain send CQ only on signal iterations
                if i % SIGNAL_INTERVAL == SIGNAL_INTERVAL - 1 {
                    while send_cq.poll() == 0 {}
                    send_cq.flush();
                }
                // Advance SRQ ci + re-post with round-robin buffer
                srq.process_recv_completion(0);
                let buf_idx = srq_repost_idx % SRQ_DEPTH;
                let offset = (buf_idx * RECV_ENTRY_SIZE) as u64;
                srq.post_recv(
                    buf_idx as u32,
                    recv_buf_addr + offset,
                    RECV_ENTRY_SIZE as u32,
                    recv_mr.lkey(),
                )
                .expect("re-post recv");
                srq_repost_idx += 1;
                if srq_repost_idx % SRQ_DOORBELL_INTERVAL as usize == 0 {
                    srq.ring_doorbell();
                }
            }
        }
        // Final flush
        recv_cq.flush();
        srq.ring_doorbell();
        send_cq.poll();
        send_cq.flush();
    };

    // Warmup
    do_pingpong(WARMUP);
    world.barrier();

    // Measured run
    let start = Instant::now();
    do_pingpong(ITERATIONS);
    let elapsed = start.elapsed();

    if is_initiator {
        let total_us = elapsed.as_micros() as f64;
        let avg_rtt_us = total_us / ITERATIONS as f64;
        let rps = ITERATIONS as f64 / elapsed.as_secs_f64();
        eprintln!("[rank 0] DC SEND ping-pong: {} iterations", ITERATIONS,);
        eprintln!("  total: {:.1} ms", elapsed.as_secs_f64() * 1000.0);
        eprintln!("  avg RTT: {:.2} us", avg_rtt_us);
        eprintln!(
            "  throughput: {:.1}K RPS (one-way ops/s: {:.1}K)",
            rps / 1000.0,
            rps * 2.0 / 1000.0,
        );
    }
}
