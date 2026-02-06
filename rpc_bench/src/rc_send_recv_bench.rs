use std::cell::RefCell;
use std::rc::Rc;
use std::time::{Duration, Instant};

use mpi::collective::CommunicatorCollectives;
use mpi::topology::Communicator;

use mlx5::cq::CqConfig;
use mlx5::device::DeviceList;
use mlx5::pd::AccessFlags;
use mlx5::qp::RcQpConfig;
use mlx5::transport::IbRemoteQpInfo;
use mlx5::wqe::WqeFlags;
use mlx5::emit_wqe;

use crate::epoch::EpochCollector;
use crate::mpi_util;
use crate::parquet_out::{self, BenchRow};
use crate::{CommonConfig, ModeCmd};

#[derive(Clone, Copy, Debug, Default)]
#[repr(C)]
struct RcConnectionInfo {
    qp_number: u32,
    packet_sequence_number: u32,
    local_identifier: u16,
    _pad: u16,
}

const RC_INFO_SIZE: usize = std::mem::size_of::<RcConnectionInfo>();

impl RcConnectionInfo {
    fn to_bytes(&self) -> Vec<u8> {
        let ptr = self as *const Self as *const u8;
        unsafe { std::slice::from_raw_parts(ptr, RC_INFO_SIZE).to_vec() }
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        assert!(bytes.len() >= RC_INFO_SIZE);
        let mut info = Self::default();
        unsafe {
            std::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                &mut info as *mut Self as *mut u8,
                RC_INFO_SIZE,
            );
        }
        info
    }
}

pub fn run(
    common: &CommonConfig,
    world: &mpi::topology::SimpleCommunicator,
    num_slots: usize,
    mode: &ModeCmd,
) -> Vec<BenchRow> {
    let rank = world.rank();
    mpi_util::set_cpu_affinity(rank as usize);

    match mode {
        ModeCmd::OneToOne {
            endpoints: _,
            inflight,
        } => {
            if world.size() != 2 {
                if rank == 0 {
                    eprintln!("rc-send one-to-one requires exactly 2 ranks");
                }
                return Vec::new();
            }
            run_one_to_one(common, world, num_slots, *inflight as usize)
        }
        ModeCmd::MultiClient { inflight: _ } => {
            if rank == 0 {
                eprintln!("rc-send multi-client: not yet implemented (use one-to-one mode)");
            }
            Vec::new()
        }
    }
}

fn open_mlx5_device(device_index: usize) -> mlx5::device::Context {
    let device_list = DeviceList::list().expect("Failed to list devices");
    let device = device_list
        .iter()
        .nth(device_index)
        .expect("Device not found");
    device.open().expect("Failed to open device")
}

const SIGNAL_INTERVAL: u32 = 64;

fn run_one_to_one(
    common: &CommonConfig,
    world: &mpi::topology::SimpleCommunicator,
    num_slots: usize,
    inflight: usize,
) -> Vec<BenchRow> {
    let rank = world.rank();
    let is_client = rank == 0;

    let ctx = open_mlx5_device(common.device_index);
    let pd = ctx.alloc_pd().expect("Failed to alloc PD");

    let slot_size = common.message_size.max(64);

    // Separate pools for send and recv
    // Use num_slots / 2 for each, or at least inflight * 2
    let send_slots = (num_slots / 2).max(inflight * 2);
    let send_total = send_slots * slot_size;
    let send_layout = std::alloc::Layout::from_size_align(send_total, 4096).unwrap();
    let send_base = unsafe { std::alloc::alloc_zeroed(send_layout) };
    assert!(!send_base.is_null());
    let send_mr = unsafe {
        pd.register(send_base, send_total, AccessFlags::LOCAL_WRITE)
            .expect("Failed to register send MR")
    };
    let send_lkey = send_mr.lkey();
    let send_free: Rc<RefCell<Vec<u32>>> =
        Rc::new(RefCell::new((0..send_slots as u32).rev().collect()));
    let send_addr = move |idx: u32| -> u64 { send_base as u64 + (idx as u64) * (slot_size as u64) };

    // Recv pool
    let recv_slots = (num_slots / 2).max(inflight * 2);
    let recv_total = recv_slots * slot_size;
    let recv_layout = std::alloc::Layout::from_size_align(recv_total, 4096).unwrap();
    let recv_base = unsafe { std::alloc::alloc_zeroed(recv_layout) };
    assert!(!recv_base.is_null());
    let recv_mr = unsafe {
        pd.register(recv_base, recv_total, AccessFlags::LOCAL_WRITE)
            .expect("Failed to register recv MR")
    };
    let recv_lkey = recv_mr.lkey();
    let recv_free: Rc<RefCell<Vec<u32>>> =
        Rc::new(RefCell::new((0..recv_slots as u32).rev().collect()));
    let recv_addr = move |idx: u32| -> u64 { recv_base as u64 + (idx as u64) * (slot_size as u64) };

    // Shared state for callbacks
    let send_completed_slots: Rc<RefCell<Vec<u32>>> = Rc::new(RefCell::new(Vec::new()));
    let recv_completed_slots: Rc<RefCell<Vec<u32>>> = Rc::new(RefCell::new(Vec::new()));

    let send_slots_cb = send_completed_slots.clone();
    let recv_slots_cb = recv_completed_slots.clone();

    let send_cq = Rc::new(
        ctx.create_cq(4096, &CqConfig::default())
            .expect("Failed to create send CQ"),
    );
    let recv_cq = Rc::new(
        ctx.create_cq(4096, &CqConfig::default())
            .expect("Failed to create recv CQ"),
    );

    let config = RcQpConfig {
        max_send_wr: inflight as u32 * 2,
        max_recv_wr: inflight as u32 * 2,
        max_send_sge: 1,
        max_recv_sge: 1,
        max_inline_data: 256,
        enable_scatter_to_cqe: false,
    };

    let qp = ctx
        .rc_qp_builder::<u32, u32>(&pd, &config)
        .sq_cq(send_cq.clone(), move |_cqe, slot_idx| {
            send_slots_cb.borrow_mut().push(slot_idx);
        })
        .rq_cq(recv_cq.clone(), move |_cqe, slot_idx| {
            recv_slots_cb.borrow_mut().push(slot_idx);
        })
        .build()
        .expect("Failed to build RC QP");

    let qpn = qp.borrow().qpn();
    let lid = ctx
        .query_port(common.port)
        .expect("Failed to query port")
        .lid;

    let local_info = RcConnectionInfo {
        qp_number: qpn,
        packet_sequence_number: 0,
        local_identifier: lid,
        _pad: 0,
    };

    let remote_bytes = mpi_util::exchange_bytes(world, rank, 1 - rank, &local_info.to_bytes());
    let remote_info = RcConnectionInfo::from_bytes(&remote_bytes);

    let access = AccessFlags::LOCAL_WRITE.bits();
    qp.borrow_mut()
        .connect(
            &IbRemoteQpInfo {
                qp_number: remote_info.qp_number,
                packet_sequence_number: remote_info.packet_sequence_number,
                local_identifier: remote_info.local_identifier,
            },
            common.port,
            0,
            1,
            1,
            access,
        )
        .expect("Failed to connect QP");

    // Pre-post recv WRs using recv pool
    for _ in 0..inflight {
        let slot_idx = recv_free.borrow_mut().pop().expect("Recv pool exhausted");
        qp.borrow()
            .post_recv(slot_idx, recv_addr(slot_idx), slot_size as u32, recv_lkey)
            .expect("Failed to post recv");
    }
    qp.borrow().ring_rq_doorbell();

    world.barrier();

    let duration = Duration::from_secs(common.duration_secs);
    let interval = Duration::from_millis(common.interval_ms);
    let msg_size = common.message_size;
    let mut all_rows = Vec::new();

    for run_idx in 0..common.runs {
        world.barrier();

        if is_client {
            let mut collector = EpochCollector::new(interval);
            let mut send_count = 0u32;
            let mut unsignaled_send_slots: Vec<u32> = Vec::new();

            // Helper: emit a send WQE using a slot from send_free
            // Returns true if sent
            let do_send = |send_count: &mut u32, unsignaled: &mut Vec<u32>| -> bool {
                let Some(slot_idx) = send_free.borrow_mut().pop() else {
                    return false;
                };
                let qp_ref = qp.borrow();
                let emit = qp_ref.emit_ctx().expect("emit_ctx failed");
                *send_count += 1;
                if *send_count % SIGNAL_INTERVAL == 0 {
                    // Signaled: on completion, all unsignaled slots before this are also done
                    emit_wqe!(&emit, send {
                        flags: WqeFlags::empty(),
                        sge: { addr: send_addr(slot_idx), len: msg_size as u32, lkey: send_lkey },
                        signaled: slot_idx,
                    })
                    .expect("emit_wqe failed");
                    // The unsignaled slots accumulated since last signal are now safe
                    // They will be freed when this signaled CQE arrives
                } else {
                    emit_wqe!(&emit, send {
                        flags: WqeFlags::empty(),
                        sge: { addr: send_addr(slot_idx), len: msg_size as u32, lkey: send_lkey },
                    })
                    .expect("emit_wqe failed");
                    unsignaled.push(slot_idx);
                }
                true
            };

            // Initial send burst
            for _ in 0..inflight {
                if !do_send(&mut send_count, &mut unsignaled_send_slots) {
                    break;
                }
            }
            qp.borrow().ring_sq_doorbell();

            let start = Instant::now();

            while start.elapsed() < duration {
                send_cq.poll();
                send_cq.flush();
                recv_cq.poll();
                recv_cq.flush();

                // Free signaled send-completed slots + all prior unsignaled
                {
                    let mut slots = send_completed_slots.borrow_mut();
                    if !slots.is_empty() {
                        for &slot_idx in slots.iter() {
                            send_free.borrow_mut().push(slot_idx);
                        }
                        // Also free unsignaled slots (they completed before the signaled one)
                        let mut sf = send_free.borrow_mut();
                        for slot_idx in unsignaled_send_slots.drain(..) {
                            sf.push(slot_idx);
                        }
                        slots.clear();
                    }
                }

                // Process recv completions (responses)
                let recv_slots_done: Vec<u32> = {
                    let mut slots = recv_completed_slots.borrow_mut();
                    std::mem::take(&mut *slots)
                };

                let new_completions = recv_slots_done.len() as u64;
                if new_completions > 0 {
                    collector.record(new_completions);

                    // Re-post recv slots
                    for &slot_idx in &recv_slots_done {
                        qp.borrow()
                            .post_recv(slot_idx, recv_addr(slot_idx), slot_size as u32, recv_lkey)
                            .expect("Failed to re-post recv");
                    }
                    qp.borrow().ring_rq_doorbell();

                    // Send new requests
                    let mut sent_any = false;
                    for _ in 0..recv_slots_done.len() {
                        if do_send(&mut send_count, &mut unsignaled_send_slots) {
                            sent_any = true;
                        } else {
                            break;
                        }
                    }
                    if sent_any {
                        qp.borrow().ring_sq_doorbell();
                    }
                }
            }

            // Drain
            let drain_deadline = Instant::now() + Duration::from_secs(2);
            while Instant::now() < drain_deadline {
                send_cq.poll();
                send_cq.flush();
                recv_cq.poll();
                recv_cq.flush();
                {
                    let mut slots = send_completed_slots.borrow_mut();
                    for &slot_idx in slots.iter() {
                        send_free.borrow_mut().push(slot_idx);
                    }
                    slots.clear();
                }
                {
                    let mut slots = recv_completed_slots.borrow_mut();
                    if slots.is_empty() {
                        break;
                    }
                    for &slot_idx in slots.iter() {
                        recv_free.borrow_mut().push(slot_idx);
                    }
                    slots.clear();
                }
            }

            collector.finish();
            let steady = collector.steady_state(common.trim);
            let rows = parquet_out::rows_from_epochs(
                "rc_send_recv",
                "1to1",
                steady,
                msg_size as u64,
                1,
                inflight as u32,
                1,
                run_idx,
            );

            if !steady.is_empty() {
                let avg_rps: f64 = rows.iter().map(|r| r.rps).sum::<f64>() / rows.len() as f64;
                eprintln!(
                    "  Run {}: avg {:.0} RPS ({} steady epochs)",
                    run_idx + 1,
                    avg_rps,
                    steady.len()
                );
            }

            all_rows.extend(rows);
        } else {
            // Server: receive requests, echo back responses
            let start = Instant::now();
            let mut send_count = 0u32;
            let mut unsignaled_send_slots: Vec<u32> = Vec::new();

            while start.elapsed() < duration {
                recv_cq.poll();
                recv_cq.flush();
                send_cq.poll();
                send_cq.flush();

                // Free signaled send-completed slots + unsignaled
                {
                    let mut slots = send_completed_slots.borrow_mut();
                    if !slots.is_empty() {
                        for &slot_idx in slots.iter() {
                            send_free.borrow_mut().push(slot_idx);
                        }
                        let mut sf = send_free.borrow_mut();
                        for slot_idx in unsignaled_send_slots.drain(..) {
                            sf.push(slot_idx);
                        }
                        slots.clear();
                    }
                }

                // Process recv completions: echo back
                let recv_slots_done: Vec<u32> = {
                    let mut slots = recv_completed_slots.borrow_mut();
                    std::mem::take(&mut *slots)
                };

                let mut sent_any = false;
                for &recv_slot_idx in &recv_slots_done {
                    // Allocate a send slot
                    let Some(send_slot_idx) = send_free.borrow_mut().pop() else {
                        // No send slots available; just re-post recv
                        qp.borrow()
                            .post_recv(
                                recv_slot_idx,
                                recv_addr(recv_slot_idx),
                                slot_size as u32,
                                recv_lkey,
                            )
                            .expect("Failed to re-post recv");
                        continue;
                    };

                    // Copy request data to send buffer
                    unsafe {
                        std::ptr::copy_nonoverlapping(
                            recv_addr(recv_slot_idx) as *const u8,
                            send_addr(send_slot_idx) as *mut u8,
                            msg_size,
                        );
                    }

                    // Re-post recv slot
                    qp.borrow()
                        .post_recv(
                            recv_slot_idx,
                            recv_addr(recv_slot_idx),
                            slot_size as u32,
                            recv_lkey,
                        )
                        .expect("Failed to re-post recv");

                    // Send response
                    let qp_ref = qp.borrow();
                    let emit = qp_ref.emit_ctx().expect("emit_ctx failed");
                    send_count += 1;
                    if send_count % SIGNAL_INTERVAL == 0 {
                        emit_wqe!(&emit, send {
                            flags: WqeFlags::empty(),
                            sge: { addr: send_addr(send_slot_idx), len: msg_size as u32, lkey: send_lkey },
                            signaled: send_slot_idx,
                        })
                        .expect("emit_wqe failed");
                    } else {
                        emit_wqe!(&emit, send {
                            flags: WqeFlags::empty(),
                            sge: { addr: send_addr(send_slot_idx), len: msg_size as u32, lkey: send_lkey },
                        })
                        .expect("emit_wqe failed");
                        unsignaled_send_slots.push(send_slot_idx);
                    }
                    sent_any = true;
                }

                if !recv_slots_done.is_empty() {
                    qp.borrow().ring_rq_doorbell();
                }
                if sent_any {
                    qp.borrow().ring_sq_doorbell();
                }
            }

            // Drain
            let drain_deadline = Instant::now() + Duration::from_secs(2);
            while Instant::now() < drain_deadline {
                recv_cq.poll();
                recv_cq.flush();
                send_cq.poll();
                send_cq.flush();
                {
                    let mut slots = send_completed_slots.borrow_mut();
                    for &slot_idx in slots.iter() {
                        send_free.borrow_mut().push(slot_idx);
                    }
                    slots.clear();
                }
                {
                    let mut slots = recv_completed_slots.borrow_mut();
                    if slots.is_empty() {
                        break;
                    }
                    for &slot_idx in slots.iter() {
                        recv_free.borrow_mut().push(slot_idx);
                    }
                    slots.clear();
                }
            }
        }
    }

    all_rows
}
