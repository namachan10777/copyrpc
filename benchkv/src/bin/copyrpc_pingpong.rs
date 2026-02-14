//! copyrpc DC transport ping-pong benchmark.
//!
//! 2 MPI ranks, 1 endpoint each, QD=1.
//! Measures RTT including copyrpc batching/ring buffer overhead.

use std::cell::Cell;

use mpi::collective::CommunicatorCollectives;
use mpi::point_to_point::{Destination, Source};
use mpi::topology::Communicator;

use copyrpc::dc::{Context, ContextBuilder, EndpointConfig, RemoteEndpointInfo};

const WARMUP: u64 = 256;
const ITERATIONS: u64 = 100_000;
const RING_SIZE: usize = 1 << 20; // 1MB

#[derive(Clone, Copy, Debug, Default)]
#[repr(C)]
struct ConnInfo {
    dct_number: u32,
    local_identifier: u16,
    _pad: u16,
    dc_key: u64,
    recv_ring_addr: u64,
    recv_ring_rkey: u32,
    endpoint_id: u32,
    recv_ring_size: u64,
    initial_credit: u64,
}

const CONN_INFO_SIZE: usize = std::mem::size_of::<ConnInfo>();

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
        mpi::initialize_with_threading(mpi::Threading::Funneled).expect("MPI init");
    let world = universe.world();
    let rank = world.rank();
    let size = world.size();
    assert_eq!(size, 2, "copyrpc_pingpong requires exactly 2 MPI ranks");
    let peer = 1 - rank;
    let is_initiator = rank == 0;

    // Create copyrpc DC context
    let ctx: Context<u64> = ContextBuilder::new()
        .dci_config(mlx5::dc::DciConfig {
            max_send_wr: 256,
            max_send_sge: 1,
            max_inline_data: 256,
        })
        .srq_config(mlx5::srq::SrqConfig {
            max_wr: 1024,
            max_sge: 1,
        })
        .cq_size(1024)
        .build()
        .expect("create copyrpc context");

    let ep_config = EndpointConfig {
        send_ring_size: RING_SIZE,
        recv_ring_size: RING_SIZE,
    };
    let mut ep = ctx.create_endpoint(&ep_config).expect("create endpoint");
    let (local_info, lid, port) = ep.local_info(ctx.lid(), ctx.port());

    let conn = ConnInfo {
        dct_number: local_info.dct_number,
        local_identifier: lid,
        _pad: 0,
        dc_key: local_info.dc_key,
        recv_ring_addr: local_info.recv_ring_addr,
        recv_ring_rkey: local_info.recv_ring_rkey,
        endpoint_id: local_info.endpoint_id,
        recv_ring_size: local_info.recv_ring_size,
        initial_credit: local_info.initial_credit,
    };
    let local_bytes: &[u8] = unsafe {
        std::slice::from_raw_parts(&conn as *const ConnInfo as *const u8, CONN_INFO_SIZE)
    };
    let remote_bytes = exchange_bytes(&world, rank, peer, local_bytes);
    let remote_conn: ConnInfo =
        unsafe { std::ptr::read(remote_bytes.as_ptr() as *const ConnInfo) };

    ep.connect(
        &RemoteEndpointInfo {
            dct_number: remote_conn.dct_number,
            dc_key: remote_conn.dc_key,
            local_identifier: remote_conn.local_identifier,
            recv_ring_addr: remote_conn.recv_ring_addr,
            recv_ring_rkey: remote_conn.recv_ring_rkey,
            recv_ring_size: remote_conn.recv_ring_size,
            initial_credit: remote_conn.initial_credit,
            endpoint_id: remote_conn.endpoint_id,
        },
        0,
        port,
    )
    .expect("connect endpoint");

    eprintln!(
        "[rank {rank}] connected: local ep_id={}, remote ep_id={}",
        local_info.endpoint_id, remote_conn.endpoint_id,
    );

    world.barrier();

    let payload = [0u8; 8];
    let response_received = Cell::new(false);

    // Ping-pong function
    let do_pingpong = |iterations: u64| {
        for _i in 0..iterations {
            if is_initiator {
                // Send request
                loop {
                    match ep.call(&payload, 0u64, 0) {
                        Ok(_) => break,
                        Err(copyrpc::error::CallError::RingFull(_))
                        | Err(copyrpc::error::CallError::InsufficientCredit(_)) => {
                            ctx.poll(|_ud: u64, _data: &[u8]| {});
                        }
                        Err(e) => panic!("call failed: {:?}", e),
                    }
                }
                ctx.flush_endpoints();

                // Wait for response
                response_received.set(false);
                while !response_received.get() {
                    ctx.poll(|_ud: u64, _data: &[u8]| {
                        response_received.set(true);
                    });
                }
            } else {
                // Wait for incoming request
                loop {
                    ctx.poll(|_ud: u64, _data: &[u8]| {});
                    if let Some(recv_handle) = ctx.recv() {
                        // Reply
                        loop {
                            match recv_handle.reply(&payload) {
                                Ok(()) => break,
                                Err(copyrpc::error::Error::RingFull) => {
                                    ctx.poll(|_ud: u64, _data: &[u8]| {});
                                }
                                Err(e) => panic!("reply failed: {:?}", e),
                            }
                        }
                        ctx.flush_endpoints();
                        break;
                    }
                }
            }
        }
    };

    // Warmup
    do_pingpong(WARMUP);
    world.barrier();

    // Measured run
    let start = fastant::Instant::now();
    do_pingpong(ITERATIONS);
    let elapsed = start.elapsed();

    if is_initiator {
        let total_us = elapsed.as_micros() as f64;
        let avg_rtt_us = total_us / ITERATIONS as f64;
        let rps = ITERATIONS as f64 / elapsed.as_secs_f64();
        eprintln!("[rank 0] copyrpc DC ping-pong: {} iterations", ITERATIONS);
        eprintln!("  total: {:.1} ms", elapsed.as_secs_f64() * 1000.0);
        eprintln!("  avg RTT: {:.2} us", avg_rtt_us);
        eprintln!(
            "  throughput: {:.1}K RPS",
            rps / 1000.0,
        );
    }
}
