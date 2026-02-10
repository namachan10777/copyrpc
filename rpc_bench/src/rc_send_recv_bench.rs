use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use mpi::collective::CommunicatorCollectives;
use mpi::topology::Communicator;

use mlx5::cq::CqConfig;
use mlx5::device::DeviceList;
use mlx5::emit_wqe;
use mlx5::pd::AccessFlags;
use mlx5::qp::RcQpConfig;
use mlx5::transport::IbRemoteQpInfo;
use mlx5::wqe::WqeFlags;

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
    fn to_bytes(self) -> Vec<u8> {
        let ptr = &self as *const Self as *const u8;
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
    match mode {
        ModeCmd::OneToOne {
            endpoints,
            inflight,
            threads,
        } => {
            if world.size() != 2 {
                if rank == 0 {
                    eprintln!("rc-send one-to-one requires exactly 2 ranks");
                }
                return Vec::new();
            }
            let num_threads = *threads as usize;
            if num_threads > 1 {
                run_one_to_one_threaded(common, world, num_slots, *inflight as usize, num_threads)
            } else {
                run_one_to_one(
                    common,
                    world,
                    num_slots,
                    *inflight as usize,
                    *endpoints as usize,
                )
            }
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
    let device = device_list.get(device_index).expect("Device not found");
    device.open().expect("Failed to open device")
}

const SIGNAL_INTERVAL: u32 = 64;

fn run_one_to_one(
    common: &CommonConfig,
    world: &mpi::topology::SimpleCommunicator,
    num_slots: usize,
    inflight: usize,
    num_endpoints: usize,
) -> Vec<BenchRow> {
    let rank = world.rank();
    let is_client = rank == 0;
    crate::affinity::pin_thread_if_configured(
        common.affinity_mode,
        common.affinity_start,
        rank,
        1,
        0,
    );

    let signal_interval = (SIGNAL_INTERVAL as usize).min(inflight).max(1) as u32;

    let ctx = open_mlx5_device(common.device_index);
    let pd = ctx.alloc_pd().expect("Failed to alloc PD");

    let slot_size = common.message_size.max(64);
    let total_inflight = inflight * num_endpoints;

    let send_slots = (num_slots / 2).max(total_inflight * 2);
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

    let recv_slots = (num_slots / 2).max(total_inflight * 2);
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

    // Shared CQs across all endpoints
    let cq_size = (4096 * num_endpoints as i32).min(65536);
    let send_cq = Rc::new(
        ctx.create_cq(cq_size, &CqConfig::default())
            .expect("Failed to create send CQ"),
    );
    let recv_cq = Rc::new(
        ctx.create_cq(cq_size, &CqConfig::default())
            .expect("Failed to create recv CQ"),
    );

    // Per-endpoint completion tracking
    let ep_send_completed: Vec<Rc<RefCell<Vec<u32>>>> = (0..num_endpoints)
        .map(|_| Rc::new(RefCell::new(Vec::new())))
        .collect();
    let ep_recv_completed: Vec<Rc<RefCell<Vec<u32>>>> = (0..num_endpoints)
        .map(|_| Rc::new(RefCell::new(Vec::new())))
        .collect();

    let config = RcQpConfig {
        max_send_wr: inflight as u32 * 2,
        max_recv_wr: inflight as u32 * 2,
        max_send_sge: 1,
        max_recv_sge: 1,
        max_inline_data: 256,
        enable_scatter_to_cqe: false,
    };

    // Create QPs (all closures in the loop have the same type)
    let mut qps = Vec::with_capacity(num_endpoints);
    for ep in 0..num_endpoints {
        let send_cb = ep_send_completed[ep].clone();
        let recv_cb = ep_recv_completed[ep].clone();
        let qp = ctx
            .rc_qp_builder::<u32, u32>(&pd, &config)
            .sq_cq(send_cq.clone(), move |_cqe, slot_idx| {
                send_cb.borrow_mut().push(slot_idx);
            })
            .rq_cq(recv_cq.clone(), move |_cqe, slot_idx| {
                recv_cb.borrow_mut().push(slot_idx);
            })
            .build()
            .expect("Failed to build RC QP");
        qps.push(qp);
    }

    // Exchange connection info for all endpoints
    let lid = ctx
        .query_port(common.port)
        .expect("Failed to query port")
        .lid;

    let mut local_bytes = Vec::with_capacity(num_endpoints * RC_INFO_SIZE);
    for qp in &qps {
        let info = RcConnectionInfo {
            qp_number: qp.borrow().qpn(),
            packet_sequence_number: 0,
            local_identifier: lid,
            _pad: 0,
        };
        local_bytes.extend_from_slice(&info.to_bytes());
    }

    let remote_bytes = mpi_util::exchange_bytes(world, rank, 1 - rank, &local_bytes);

    let access = AccessFlags::LOCAL_WRITE.bits();
    for ep in 0..num_endpoints {
        let offset = ep * RC_INFO_SIZE;
        let remote_info =
            RcConnectionInfo::from_bytes(&remote_bytes[offset..offset + RC_INFO_SIZE]);
        qps[ep]
            .borrow_mut()
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
    }

    // Pre-post recv WRs on each QP
    for ep in 0..num_endpoints {
        for _ in 0..inflight {
            let slot_idx = recv_free.borrow_mut().pop().expect("Recv pool exhausted");
            qps[ep]
                .borrow()
                .post_recv(slot_idx, recv_addr(slot_idx), slot_size as u32, recv_lkey)
                .expect("Failed to post recv");
        }
        qps[ep].borrow().ring_rq_doorbell();
    }

    world.barrier();

    let duration = Duration::from_secs(common.duration_secs);
    let interval = Duration::from_millis(common.interval_ms);
    let msg_size = common.message_size;
    let mut all_rows = Vec::new();

    for run_idx in 0..common.runs {
        world.barrier();

        if is_client {
            let mut collector = EpochCollector::new(interval);
            let mut send_counts: Vec<u32> = vec![0; num_endpoints];
            let mut unsignaled_sends: Vec<Vec<u32>> =
                (0..num_endpoints).map(|_| Vec::new()).collect();

            // Initial send burst on each endpoint
            for ep in 0..num_endpoints {
                for _ in 0..inflight {
                    let Some(slot_idx) = send_free.borrow_mut().pop() else {
                        break;
                    };
                    let qp_ref = qps[ep].borrow();
                    let emit = qp_ref.emit_ctx().expect("emit_ctx failed");
                    send_counts[ep] += 1;
                    if send_counts[ep].is_multiple_of(signal_interval) {
                        emit_wqe!(&emit, send {
                            flags: WqeFlags::empty(),
                            sge: { addr: send_addr(slot_idx), len: msg_size as u32, lkey: send_lkey },
                            signaled: slot_idx,
                        })
                        .expect("emit_wqe failed");
                    } else {
                        emit_wqe!(&emit, send {
                            flags: WqeFlags::empty(),
                            sge: { addr: send_addr(slot_idx), len: msg_size as u32, lkey: send_lkey },
                        })
                        .expect("emit_wqe failed");
                        unsignaled_sends[ep].push(slot_idx);
                    }
                }
                qps[ep].borrow().ring_sq_doorbell();
            }

            let start = Instant::now();

            while start.elapsed() < duration {
                send_cq.poll();
                send_cq.flush();
                recv_cq.poll();
                recv_cq.flush();

                // Free send-completed slots per endpoint
                for ep in 0..num_endpoints {
                    let mut slots = ep_send_completed[ep].borrow_mut();
                    if !slots.is_empty() {
                        for &slot_idx in slots.iter() {
                            send_free.borrow_mut().push(slot_idx);
                        }
                        let mut sf = send_free.borrow_mut();
                        for slot_idx in unsignaled_sends[ep].drain(..) {
                            sf.push(slot_idx);
                        }
                        slots.clear();
                    }
                }

                // Process recv completions per endpoint
                for ep in 0..num_endpoints {
                    let recv_done: Vec<u32> = {
                        let mut slots = ep_recv_completed[ep].borrow_mut();
                        std::mem::take(&mut *slots)
                    };

                    if recv_done.is_empty() {
                        continue;
                    }

                    collector.record(recv_done.len() as u64);

                    // Re-post recv slots on this endpoint's QP
                    for &slot_idx in &recv_done {
                        qps[ep]
                            .borrow()
                            .post_recv(slot_idx, recv_addr(slot_idx), slot_size as u32, recv_lkey)
                            .expect("Failed to re-post recv");
                    }
                    qps[ep].borrow().ring_rq_doorbell();

                    // Send new requests on this endpoint
                    let mut sent_any = false;
                    for _ in 0..recv_done.len() {
                        let Some(slot_idx) = send_free.borrow_mut().pop() else {
                            break;
                        };
                        let qp_ref = qps[ep].borrow();
                        let emit = qp_ref.emit_ctx().expect("emit_ctx failed");
                        send_counts[ep] += 1;
                        if send_counts[ep].is_multiple_of(signal_interval) {
                            emit_wqe!(&emit, send {
                                flags: WqeFlags::empty(),
                                sge: { addr: send_addr(slot_idx), len: msg_size as u32, lkey: send_lkey },
                                signaled: slot_idx,
                            })
                            .expect("emit_wqe failed");
                        } else {
                            emit_wqe!(&emit, send {
                                flags: WqeFlags::empty(),
                                sge: { addr: send_addr(slot_idx), len: msg_size as u32, lkey: send_lkey },
                            })
                            .expect("emit_wqe failed");
                            unsignaled_sends[ep].push(slot_idx);
                        }
                        sent_any = true;
                    }
                    if sent_any {
                        qps[ep].borrow().ring_sq_doorbell();
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
                for ep in 0..num_endpoints {
                    let mut slots = ep_send_completed[ep].borrow_mut();
                    for &slot_idx in slots.iter() {
                        send_free.borrow_mut().push(slot_idx);
                    }
                    slots.clear();
                }
                let mut any_recv = false;
                for ep in 0..num_endpoints {
                    let mut slots = ep_recv_completed[ep].borrow_mut();
                    if !slots.is_empty() {
                        any_recv = true;
                        for &slot_idx in slots.iter() {
                            recv_free.borrow_mut().push(slot_idx);
                        }
                        slots.clear();
                    }
                }
                if !any_recv {
                    break;
                }
            }

            collector.finish();
            let steady = collector.steady_state(common.trim);
            let filtered = crate::epoch::filter_bottom_quartile(steady);
            let rows = parquet_out::rows_from_epochs(
                "rc_send_recv",
                "1to1",
                &filtered,
                msg_size as u64,
                num_endpoints as u32,
                inflight as u32,
                1,
                1,
                run_idx,
            );

            if !filtered.is_empty() {
                let avg_rps: f64 = rows.iter().map(|r| r.rps).sum::<f64>() / rows.len() as f64;
                eprintln!(
                    "  Run {}: avg {:.0} RPS ({}/{} epochs)",
                    run_idx + 1,
                    avg_rps,
                    filtered.len(),
                    steady.len()
                );
            }

            all_rows.extend(rows);
        } else {
            // Server: receive requests, echo back responses
            let start = Instant::now();
            let mut send_counts: Vec<u32> = vec![0; num_endpoints];
            let mut unsignaled_sends: Vec<Vec<u32>> =
                (0..num_endpoints).map(|_| Vec::new()).collect();

            while start.elapsed() < duration {
                recv_cq.poll();
                recv_cq.flush();
                send_cq.poll();
                send_cq.flush();

                // Free send-completed slots per endpoint
                for ep in 0..num_endpoints {
                    let mut slots = ep_send_completed[ep].borrow_mut();
                    if !slots.is_empty() {
                        for &slot_idx in slots.iter() {
                            send_free.borrow_mut().push(slot_idx);
                        }
                        let mut sf = send_free.borrow_mut();
                        for slot_idx in unsignaled_sends[ep].drain(..) {
                            sf.push(slot_idx);
                        }
                        slots.clear();
                    }
                }

                // Process recv completions per endpoint: echo back
                for ep in 0..num_endpoints {
                    let recv_done: Vec<u32> = {
                        let mut slots = ep_recv_completed[ep].borrow_mut();
                        std::mem::take(&mut *slots)
                    };

                    let mut sent_any = false;
                    for &recv_slot_idx in &recv_done {
                        let Some(send_slot_idx) = send_free.borrow_mut().pop() else {
                            qps[ep]
                                .borrow()
                                .post_recv(
                                    recv_slot_idx,
                                    recv_addr(recv_slot_idx),
                                    slot_size as u32,
                                    recv_lkey,
                                )
                                .expect("Failed to re-post recv");
                            continue;
                        };

                        unsafe {
                            std::ptr::copy_nonoverlapping(
                                recv_addr(recv_slot_idx) as *const u8,
                                send_addr(send_slot_idx) as *mut u8,
                                msg_size,
                            );
                        }

                        qps[ep]
                            .borrow()
                            .post_recv(
                                recv_slot_idx,
                                recv_addr(recv_slot_idx),
                                slot_size as u32,
                                recv_lkey,
                            )
                            .expect("Failed to re-post recv");

                        let qp_ref = qps[ep].borrow();
                        let emit = qp_ref.emit_ctx().expect("emit_ctx failed");
                        send_counts[ep] += 1;
                        if send_counts[ep].is_multiple_of(signal_interval) {
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
                            unsignaled_sends[ep].push(send_slot_idx);
                        }
                        sent_any = true;
                    }

                    if !recv_done.is_empty() {
                        qps[ep].borrow().ring_rq_doorbell();
                    }
                    if sent_any {
                        qps[ep].borrow().ring_sq_doorbell();
                    }
                }
            }

            // Drain
            let drain_deadline = Instant::now() + Duration::from_secs(2);
            while Instant::now() < drain_deadline {
                recv_cq.poll();
                recv_cq.flush();
                send_cq.poll();
                send_cq.flush();
                for ep in 0..num_endpoints {
                    let mut slots = ep_send_completed[ep].borrow_mut();
                    for &slot_idx in slots.iter() {
                        send_free.borrow_mut().push(slot_idx);
                    }
                    slots.clear();
                }
                let mut any_recv = false;
                for ep in 0..num_endpoints {
                    let mut slots = ep_recv_completed[ep].borrow_mut();
                    if !slots.is_empty() {
                        any_recv = true;
                        for &slot_idx in slots.iter() {
                            recv_free.borrow_mut().push(slot_idx);
                        }
                        slots.clear();
                    }
                }
                if !any_recv {
                    break;
                }
            }
        }
    }

    all_rows
}

fn run_one_to_one_threaded(
    common: &CommonConfig,
    world: &mpi::topology::SimpleCommunicator,
    num_slots: usize,
    inflight: usize,
    num_threads: usize,
) -> Vec<BenchRow> {
    let rank = world.rank();
    let is_client = rank == 0;

    // Phase 1: spawn workers, each creates its own device/QP/CQs/MRs
    let (info_tx, info_rx) = std::sync::mpsc::channel::<(usize, RcConnectionInfo)>();
    let (remote_txs, remote_rxs): (Vec<_>, Vec<_>) = (0..num_threads)
        .map(|_| std::sync::mpsc::channel::<RcConnectionInfo>())
        .unzip();

    let stop_flag = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(std::sync::Barrier::new(num_threads + 1));
    let completions: Vec<Arc<AtomicU64>> = (0..num_threads)
        .map(|_| Arc::new(AtomicU64::new(0)))
        .collect();

    let mut handles = Vec::with_capacity(num_threads);

    let affinity_mode = common.affinity_mode;
    let affinity_start = common.affinity_start;

    for (tid, remote_rx) in remote_rxs.into_iter().enumerate() {
        let info_tx = info_tx.clone();
        let stop = stop_flag.clone();
        let bar = barrier.clone();
        let comp = completions[tid].clone();
        let device_index = common.device_index;
        let port = common.port;
        let message_size = common.message_size;

        handles.push(std::thread::spawn(move || {
            crate::affinity::pin_thread_if_configured(affinity_mode, affinity_start, rank, num_threads, tid);

            let ctx = open_mlx5_device(device_index);
            let pd = ctx.alloc_pd().expect("Failed to alloc PD");

            let slot_size = message_size.max(64);

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
            let send_addr =
                move |idx: u32| -> u64 { send_base as u64 + (idx as u64) * (slot_size as u64) };

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
            let recv_addr =
                move |idx: u32| -> u64 { recv_base as u64 + (idx as u64) * (slot_size as u64) };

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

            let qp_config = RcQpConfig {
                max_send_wr: inflight as u32 * 2,
                max_recv_wr: inflight as u32 * 2,
                max_send_sge: 1,
                max_recv_sge: 1,
                max_inline_data: 256,
                enable_scatter_to_cqe: false,
            };

            let qp = ctx
                .rc_qp_builder::<u32, u32>(&pd, &qp_config)
                .sq_cq(send_cq.clone(), move |_cqe, slot_idx| {
                    send_slots_cb.borrow_mut().push(slot_idx);
                })
                .rq_cq(recv_cq.clone(), move |_cqe, slot_idx| {
                    recv_slots_cb.borrow_mut().push(slot_idx);
                })
                .build()
                .expect("Failed to build RC QP");

            let qpn = qp.borrow().qpn();
            let lid = ctx.query_port(port).expect("Failed to query port").lid;

            let local_info = RcConnectionInfo {
                qp_number: qpn,
                packet_sequence_number: 0,
                local_identifier: lid,
                _pad: 0,
            };

            info_tx.send((tid, local_info)).unwrap();

            let remote_info = remote_rx.recv().unwrap();

            let access = AccessFlags::LOCAL_WRITE.bits();
            qp.borrow_mut()
                .connect(
                    &IbRemoteQpInfo {
                        qp_number: remote_info.qp_number,
                        packet_sequence_number: remote_info.packet_sequence_number,
                        local_identifier: remote_info.local_identifier,
                    },
                    port,
                    0,
                    1,
                    1,
                    access,
                )
                .expect("Failed to connect QP");

            // Pre-post recv WRs
            for _ in 0..inflight {
                let slot_idx = recv_free.borrow_mut().pop().expect("Recv pool exhausted");
                qp.borrow()
                    .post_recv(slot_idx, recv_addr(slot_idx), slot_size as u32, recv_lkey)
                    .expect("Failed to post recv");
            }
            qp.borrow().ring_rq_doorbell();

            bar.wait(); // post-connect barrier

            // Run loop
            loop {
                bar.wait(); // run start
                if stop.load(Ordering::Acquire) {
                    break;
                }

                if is_client {
                    // Client: send requests, count recv completions
                    let mut send_count = 0u32;
                    let mut unsignaled_send_slots: Vec<u32> = Vec::new();
                    let msg_size = message_size;

                    let do_send = |send_count: &mut u32, unsignaled: &mut Vec<u32>| -> bool {
                        let Some(slot_idx) = send_free.borrow_mut().pop() else {
                            return false;
                        };
                        let qp_ref = qp.borrow();
                        let emit = qp_ref.emit_ctx().expect("emit_ctx failed");
                        *send_count += 1;
                        if (*send_count).is_multiple_of(SIGNAL_INTERVAL) {
                            emit_wqe!(&emit, send {
                                flags: WqeFlags::empty(),
                                sge: { addr: send_addr(slot_idx), len: msg_size as u32, lkey: send_lkey },
                                signaled: slot_idx,
                            })
                            .expect("emit_wqe failed");
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

                    for _ in 0..inflight {
                        if !do_send(&mut send_count, &mut unsignaled_send_slots) {
                            break;
                        }
                    }
                    qp.borrow().ring_sq_doorbell();

                    while !stop.load(Ordering::Relaxed) {
                        send_cq.poll();
                        send_cq.flush();
                        recv_cq.poll();
                        recv_cq.flush();

                        {
                            let mut slots = send_completed_slots.borrow_mut();
                            if !slots.is_empty() {
                                for &si in slots.iter() {
                                    send_free.borrow_mut().push(si);
                                }
                                let mut sf = send_free.borrow_mut();
                                for si in unsignaled_send_slots.drain(..) {
                                    sf.push(si);
                                }
                                slots.clear();
                            }
                        }

                        let recv_done: Vec<u32> = {
                            let mut slots = recv_completed_slots.borrow_mut();
                            std::mem::take(&mut *slots)
                        };

                        let new_completions = recv_done.len() as u64;
                        if new_completions > 0 {
                            comp.fetch_add(new_completions, Ordering::Relaxed);

                            for &si in &recv_done {
                                qp.borrow()
                                    .post_recv(si, recv_addr(si), slot_size as u32, recv_lkey)
                                    .expect("Failed to re-post recv");
                            }
                            qp.borrow().ring_rq_doorbell();

                            let mut sent_any = false;
                            for _ in 0..recv_done.len() {
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
                            for &si in slots.iter() {
                                send_free.borrow_mut().push(si);
                            }
                            slots.clear();
                        }
                        {
                            let mut slots = recv_completed_slots.borrow_mut();
                            if slots.is_empty() {
                                break;
                            }
                            for &si in slots.iter() {
                                recv_free.borrow_mut().push(si);
                            }
                            slots.clear();
                        }
                    }
                } else {
                    // Server: recv requests, echo back
                    let mut send_count = 0u32;
                    let mut unsignaled_send_slots: Vec<u32> = Vec::new();
                    let msg_size = message_size;

                    while !stop.load(Ordering::Relaxed) {
                        recv_cq.poll();
                        recv_cq.flush();
                        send_cq.poll();
                        send_cq.flush();

                        {
                            let mut slots = send_completed_slots.borrow_mut();
                            if !slots.is_empty() {
                                for &si in slots.iter() {
                                    send_free.borrow_mut().push(si);
                                }
                                let mut sf = send_free.borrow_mut();
                                for si in unsignaled_send_slots.drain(..) {
                                    sf.push(si);
                                }
                                slots.clear();
                            }
                        }

                        let recv_done: Vec<u32> = {
                            let mut slots = recv_completed_slots.borrow_mut();
                            std::mem::take(&mut *slots)
                        };

                        let mut sent_any = false;
                        for &recv_si in &recv_done {
                            let Some(send_si) = send_free.borrow_mut().pop() else {
                                qp.borrow()
                                    .post_recv(recv_si, recv_addr(recv_si), slot_size as u32, recv_lkey)
                                    .expect("Failed to re-post recv");
                                continue;
                            };

                            unsafe {
                                std::ptr::copy_nonoverlapping(
                                    recv_addr(recv_si) as *const u8,
                                    send_addr(send_si) as *mut u8,
                                    msg_size,
                                );
                            }

                            qp.borrow()
                                .post_recv(recv_si, recv_addr(recv_si), slot_size as u32, recv_lkey)
                                .expect("Failed to re-post recv");

                            let qp_ref = qp.borrow();
                            let emit = qp_ref.emit_ctx().expect("emit_ctx failed");
                            send_count += 1;
                            if send_count.is_multiple_of(SIGNAL_INTERVAL) {
                                emit_wqe!(&emit, send {
                                    flags: WqeFlags::empty(),
                                    sge: { addr: send_addr(send_si), len: msg_size as u32, lkey: send_lkey },
                                    signaled: send_si,
                                })
                                .expect("emit_wqe failed");
                            } else {
                                emit_wqe!(&emit, send {
                                    flags: WqeFlags::empty(),
                                    sge: { addr: send_addr(send_si), len: msg_size as u32, lkey: send_lkey },
                                })
                                .expect("emit_wqe failed");
                                unsignaled_send_slots.push(send_si);
                            }
                            sent_any = true;
                        }

                        if !recv_done.is_empty() {
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
                            for &si in slots.iter() {
                                send_free.borrow_mut().push(si);
                            }
                            slots.clear();
                        }
                        {
                            let mut slots = recv_completed_slots.borrow_mut();
                            if slots.is_empty() {
                                break;
                            }
                            slots.clear();
                        }
                    }
                }

                bar.wait(); // run end
            }

            // Keep MRs alive
            drop(send_mr);
            drop(recv_mr);
            unsafe {
                std::alloc::dealloc(send_base, send_layout);
                std::alloc::dealloc(recv_base, recv_layout);
            }
        }));
    }
    drop(info_tx);

    // Main thread: collect local infos
    let mut all_local_infos: Vec<Option<RcConnectionInfo>> =
        (0..num_threads).map(|_| None).collect();
    for _ in 0..num_threads {
        let (tid, info) = info_rx.recv().expect("Failed to receive info from worker");
        all_local_infos[tid] = Some(info);
    }
    let all_local_infos: Vec<RcConnectionInfo> =
        all_local_infos.into_iter().map(|o| o.unwrap()).collect();

    // Phase 2: MPI exchange
    let mut local_bytes = Vec::with_capacity(num_threads * RC_INFO_SIZE);
    for info in &all_local_infos {
        local_bytes.extend_from_slice(&info.to_bytes());
    }

    let remote_bytes = mpi_util::exchange_bytes(world, rank, 1 - rank, &local_bytes);

    for (tid, remote_tx) in remote_txs.iter().enumerate() {
        let offset = tid * RC_INFO_SIZE;
        let remote_info =
            RcConnectionInfo::from_bytes(&remote_bytes[offset..offset + RC_INFO_SIZE]);
        remote_tx.send(remote_info).unwrap();
    }

    std::thread::sleep(Duration::from_millis(10));
    world.barrier();
    barrier.wait(); // release workers

    // Phase 3: run benchmarks
    let duration = Duration::from_secs(common.duration_secs);
    let interval = Duration::from_millis(common.interval_ms);
    let mut all_rows = Vec::new();

    for run_idx in 0..common.runs {
        world.barrier();

        for c in &completions {
            c.store(0, Ordering::Release);
        }
        stop_flag.store(false, Ordering::Release);

        barrier.wait(); // start run

        if is_client {
            let mut collector = EpochCollector::new(interval);
            let start = Instant::now();

            while start.elapsed() < duration {
                std::thread::sleep(Duration::from_millis(1));
                let mut total_delta = 0u64;
                for c in &completions {
                    total_delta += c.swap(0, Ordering::Relaxed);
                }
                if total_delta > 0 {
                    collector.record(total_delta);
                }
            }

            stop_flag.store(true, Ordering::Release);
            barrier.wait(); // wait for run end

            let mut remaining = 0u64;
            for c in &completions {
                remaining += c.swap(0, Ordering::Relaxed);
            }
            if remaining > 0 {
                collector.record(remaining);
            }
            collector.finish();

            let steady = collector.steady_state(common.trim);
            let filtered = crate::epoch::filter_bottom_quartile(steady);
            let rows = parquet_out::rows_from_epochs(
                "rc_send_recv",
                "1to1",
                &filtered,
                common.message_size as u64,
                1,
                inflight as u32,
                1,
                num_threads as u32,
                run_idx,
            );

            if !filtered.is_empty() {
                let avg_rps: f64 = rows.iter().map(|r| r.rps).sum::<f64>() / rows.len() as f64;
                eprintln!(
                    "  Run {}: avg {:.0} RPS ({}/{} epochs, {} threads)",
                    run_idx + 1,
                    avg_rps,
                    filtered.len(),
                    steady.len(),
                    num_threads
                );
            }

            all_rows.extend(rows);
        } else {
            std::thread::sleep(duration);
            stop_flag.store(true, Ordering::Release);
            barrier.wait();
        }
    }

    stop_flag.store(true, Ordering::Release);
    barrier.wait();

    for h in handles {
        h.join().expect("Worker thread panicked");
    }

    all_rows
}
