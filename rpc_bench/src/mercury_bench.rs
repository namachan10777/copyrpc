use std::cell::{Cell, RefCell};
use std::ffi::{c_void, CString};
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use mpi::collective::CommunicatorCollectives;
use mpi::topology::Communicator;

use crate::epoch::EpochCollector;
use crate::mpi_util;
use crate::parquet_out::{self, BenchRow};
use crate::{CommonConfig, ModeCmd};

static MSG_SIZE: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    static COMPLETED: Cell<u64> = const { Cell::new(0) };
    static HANDLE_POOL: RefCell<Vec<mercury_sys::hg_handle_t>> = const { RefCell::new(Vec::new()) };
}

pub fn run(
    common: &CommonConfig,
    world: &mpi::topology::SimpleCommunicator,
    transport: &str,
    mode: &ModeCmd,
) -> Vec<BenchRow> {
    let rank = world.rank();
    MSG_SIZE.store(common.message_size, Ordering::Relaxed);

    match mode {
        ModeCmd::OneToOne {
            endpoints: _,
            inflight,
            threads,
        } => {
            if world.size() != 2 {
                if rank == 0 {
                    eprintln!("mercury one-to-one requires exactly 2 ranks");
                }
                return Vec::new();
            }
            if *threads > 1 && rank == 0 {
                eprintln!("mercury: multi-thread not yet supported, using 1 thread");
            }
            run_one_to_one(common, world, transport, *inflight as usize)
        }
        ModeCmd::MultiClient { inflight: _ } => {
            if rank == 0 {
                eprintln!("mercury multi-client: not yet implemented");
            }
            Vec::new()
        }
    }
}

unsafe extern "C" fn bench_proc(
    _proc_: mercury_sys::hg_proc_t,
    _data: *mut c_void,
) -> mercury_sys::hg_return_t {
    // For benchmarking, we use a no-op proc function.
    // Mercury handles the buffer management, we just need to return success.
    0 // HG_SUCCESS
}

unsafe extern "C" fn rpc_handler(handle: mercury_sys::hg_handle_t) -> mercury_sys::hg_return_t {
    unsafe {
        let msg_size = MSG_SIZE.load(Ordering::Relaxed);
        let mut input = vec![0u8; msg_size];

        let ret = mercury_sys::HG_Get_input(handle, input.as_mut_ptr() as *mut c_void);
        if ret != 0 {
            mercury_sys::HG_Destroy(handle);
            return ret;
        }

        mercury_sys::HG_Free_input(handle, input.as_mut_ptr() as *mut c_void);

        let mut output = vec![0u8; msg_size];
        mercury_sys::HG_Respond(
            handle,
            Some(respond_cb),
            handle as *mut c_void,
            output.as_mut_ptr() as *mut c_void,
        )
    }
}

unsafe extern "C" fn respond_cb(
    info: *const mercury_sys::hg_cb_info,
) -> mercury_sys::hg_return_t {
    unsafe {
        let handle = (*info).arg as mercury_sys::hg_handle_t;
        let msg_size = MSG_SIZE.load(Ordering::Relaxed);
        let mut output = vec![0u8; msg_size];

        mercury_sys::HG_Free_output(handle, output.as_mut_ptr() as *mut c_void);
        mercury_sys::HG_Destroy(handle);
        0 // HG_SUCCESS
    }
}

unsafe extern "C" fn forward_cb(
    info: *const mercury_sys::hg_cb_info,
) -> mercury_sys::hg_return_t {
    unsafe {
        let handle = (*info).arg as mercury_sys::hg_handle_t;
        let msg_size = MSG_SIZE.load(Ordering::Relaxed);
        let mut output = vec![0u8; msg_size];

        mercury_sys::HG_Get_output(handle, output.as_mut_ptr() as *mut c_void);
        mercury_sys::HG_Free_output(handle, output.as_mut_ptr() as *mut c_void);

        // Return handle to pool
        HANDLE_POOL.with(|pool| pool.borrow_mut().push(handle));

        // Count completion
        COMPLETED.with(|c| c.set(c.get() + 1));

        0 // HG_SUCCESS
    }
}

fn run_one_to_one(
    common: &CommonConfig,
    world: &mpi::topology::SimpleCommunicator,
    transport: &str,
    inflight: usize,
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

    unsafe {
        // Initialize Mercury
        let transport_cstr = CString::new(transport).expect("Invalid transport string");
        let hg_class = mercury_sys::HG_Init(
            transport_cstr.as_ptr(),
            (!is_client) as mercury_sys::hg_bool_t,
        );
        assert!(!hg_class.is_null(), "HG_Init failed");

        let hg_context = mercury_sys::HG_Context_create(hg_class);
        assert!(!hg_context.is_null(), "HG_Context_create failed");

        // Register RPC
        let rpc_name = CString::new("bench_rpc").unwrap();
        let rpc_id = if is_client {
            mercury_sys::HG_Register_name(
                hg_class,
                rpc_name.as_ptr(),
                Some(bench_proc),
                Some(bench_proc),
                None, // Client doesn't need handler
            )
        } else {
            mercury_sys::HG_Register_name(
                hg_class,
                rpc_name.as_ptr(),
                Some(bench_proc),
                Some(bench_proc),
                Some(rpc_handler),
            )
        };

        // Get self address
        let mut self_addr: mercury_sys::hg_addr_t = ptr::null_mut();
        let ret = mercury_sys::HG_Addr_self(hg_class, &mut self_addr);
        assert_eq!(ret, 0, "HG_Addr_self failed");

        // Get address string size
        let mut buf_size: mercury_sys::hg_size_t = 0;
        mercury_sys::HG_Addr_to_string(hg_class, ptr::null_mut(), &mut buf_size, self_addr);

        // Get address string
        let mut addr_buf = vec![0u8; buf_size as usize];
        mercury_sys::HG_Addr_to_string(
            hg_class,
            addr_buf.as_mut_ptr() as *mut i8,
            &mut buf_size,
            self_addr,
        );

        // Exchange addresses via MPI
        let peer = 1 - rank;
        let remote_addr_bytes =
            mpi_util::exchange_bytes_variable(world, rank, peer, &addr_buf[..buf_size as usize]);

        // Look up remote address
        let end = remote_addr_bytes
            .iter()
            .position(|&b| b == 0)
            .unwrap_or(remote_addr_bytes.len());
        let remote_cstr = CString::new(&remote_addr_bytes[..end]).unwrap();
        let mut remote_addr: mercury_sys::hg_addr_t = ptr::null_mut();

        // Wait a bit for server to be ready
        std::thread::sleep(Duration::from_millis(100));

        let ret = mercury_sys::HG_Addr_lookup2(hg_class, remote_cstr.as_ptr(), &mut remote_addr);
        assert_eq!(ret, 0, "HG_Addr_lookup2 failed");

        world.barrier();

        if is_client {
            // Client: create handles and run benchmark
            HANDLE_POOL.with(|pool| {
                let mut p = pool.borrow_mut();
                p.clear();
                for _ in 0..inflight {
                    let mut handle: mercury_sys::hg_handle_t = ptr::null_mut();
                    let ret = mercury_sys::HG_Create(hg_context, remote_addr, rpc_id, &mut handle);
                    assert_eq!(ret, 0, "HG_Create failed");
                    p.push(handle);
                }
            });

            let duration = Duration::from_secs(common.duration_secs);
            let interval = Duration::from_millis(common.interval_ms);
            let mut all_rows = Vec::new();

            for run in 0..common.runs {
                world.barrier();

                let mut collector = EpochCollector::new(interval);
                run_mercury_client_duration(
                    hg_context,
                    common.message_size,
                    inflight,
                    duration,
                    &mut collector,
                );
                collector.finish();

                let steady = collector.steady_state(common.trim);
                let filtered = crate::epoch::filter_bottom_quartile(steady);
                let timestamp = parquet_out::now_unix_secs();
                let rows = parquet_out::rows_from_epochs(
                    "mercury",
                    "1to1",
                    &filtered,
                    common.message_size as u64,
                    1,
                    inflight as u32,
                    1,
                    1,
                    run,
                    timestamp,
                );

                if !filtered.is_empty() {
                    let avg_rps: f64 =
                        rows.iter().map(|r| r.rps).sum::<f64>() / rows.len() as f64;
                    eprintln!(
                        "  Run {}: avg {:.0} RPS ({}/{} epochs)",
                        run + 1,
                        avg_rps,
                        filtered.len(),
                        steady.len()
                    );
                }

                all_rows.extend(rows);
            }

            // Cleanup handles
            HANDLE_POOL.with(|pool| {
                for handle in pool.borrow_mut().drain(..) {
                    mercury_sys::HG_Destroy(handle);
                }
            });

            mercury_sys::HG_Addr_free(hg_class, remote_addr);
            mercury_sys::HG_Addr_free(hg_class, self_addr);
            mercury_sys::HG_Context_destroy(hg_context);
            mercury_sys::HG_Finalize(hg_class);

            all_rows
        } else {
            // Server: run event loop
            let duration = Duration::from_secs(common.duration_secs);

            for _run in 0..common.runs {
                world.barrier();
                run_mercury_server_duration(hg_context, duration);
            }

            mercury_sys::HG_Addr_free(hg_class, remote_addr);
            mercury_sys::HG_Addr_free(hg_class, self_addr);
            mercury_sys::HG_Context_destroy(hg_context);
            mercury_sys::HG_Finalize(hg_class);

            Vec::new()
        }
    }
}

unsafe fn run_mercury_client_duration(
    hg_context: *mut mercury_sys::hg_context_t,
    message_size: usize,
    _inflight: usize,
    duration: Duration,
    collector: &mut EpochCollector,
) {
    unsafe {
        let mut input = vec![0xAAu8; message_size];
        let input_ptr = input.as_mut_ptr() as *mut c_void;

        COMPLETED.with(|c| c.set(0));
        let mut sent = 0u64;
        let mut completed_base = 0u64;

        // Initial fill: send all inflight requests (pool was pre-filled with inflight handles)
        HANDLE_POOL.with(|pool| {
            let mut p = pool.borrow_mut();
            let to_send = p.len();
            for _ in 0..to_send {
                if let Some(handle) = p.pop() {
                    let ret = mercury_sys::HG_Forward(
                        handle,
                        Some(forward_cb),
                        handle as *mut c_void,
                        input_ptr,
                    );
                    if ret == 0 {
                        sent += 1;
                    } else {
                        p.push(handle);
                        break;
                    }
                }
            }
        });

        let start = Instant::now();

        while start.elapsed() < duration {
            mercury_sys::HG_Progress(hg_context, 0);

            loop {
                let mut actual_count: u32 = 0;
                let ret = mercury_sys::HG_Trigger(hg_context, 0, 1, &mut actual_count);
                if ret != 0 || actual_count == 0 {
                    break;
                }
            }

            let total_completed = COMPLETED.with(|c| c.get());
            let new = total_completed - completed_base;
            if new > 0 {
                completed_base = total_completed;
                collector.record(new);

                HANDLE_POOL.with(|pool| {
                    let mut p = pool.borrow_mut();
                    let to_refill = new.min(p.len() as u64) as usize;
                    for _ in 0..to_refill {
                        if let Some(handle) = p.pop() {
                            let ret = mercury_sys::HG_Forward(
                                handle,
                                Some(forward_cb),
                                handle as *mut c_void,
                                input_ptr,
                            );
                            if ret == 0 {
                                sent += 1;
                            } else {
                                p.push(handle);
                                break;
                            }
                        }
                    }
                });
            }
        }

        // Drain phase
        let drain_deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < drain_deadline {
            mercury_sys::HG_Progress(hg_context, 0);

            loop {
                let mut actual_count: u32 = 0;
                let ret = mercury_sys::HG_Trigger(hg_context, 0, 1, &mut actual_count);
                if ret != 0 || actual_count == 0 {
                    break;
                }
            }

            let total_completed = COMPLETED.with(|c| c.get());
            if total_completed >= sent {
                break;
            }

            let new = total_completed - completed_base;
            if new > 0 {
                completed_base = total_completed;
                collector.record(new);
            }
        }
    }
}

unsafe fn run_mercury_server_duration(
    hg_context: *mut mercury_sys::hg_context_t,
    duration: Duration,
) {
    unsafe {
        let start = Instant::now();

        while start.elapsed() < duration {
            mercury_sys::HG_Progress(hg_context, 0);

            loop {
                let mut actual_count: u32 = 0;
                let ret = mercury_sys::HG_Trigger(hg_context, 0, 1, &mut actual_count);
                if ret != 0 || actual_count == 0 {
                    break;
                }
            }
        }

        // Drain phase
        let drain_deadline = Instant::now() + Duration::from_secs(2);
        let mut no_progress_count = 0;

        while Instant::now() < drain_deadline {
            mercury_sys::HG_Progress(hg_context, 0);

            let mut had_callbacks = false;
            loop {
                let mut actual_count: u32 = 0;
                let ret = mercury_sys::HG_Trigger(hg_context, 0, 1, &mut actual_count);
                if ret != 0 || actual_count == 0 {
                    break;
                }
                if actual_count > 0 {
                    had_callbacks = true;
                }
            }

            if !had_callbacks {
                no_progress_count += 1;
                if no_progress_count > 100 {
                    break;
                }
            } else {
                no_progress_count = 0;
            }
        }
    }
}
