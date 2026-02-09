use std::cell::Cell;
use std::ffi::c_void;
use std::ptr;
use std::time::{Duration, Instant};

use mpi::collective::CommunicatorCollectives;
use mpi::topology::Communicator;

use crate::epoch::EpochCollector;
use crate::mpi_util;
use crate::parquet_out::{self, BenchRow};
use crate::{CommonConfig, ModeCmd};

const AM_REQUEST: u32 = 0;
const AM_RESPONSE: u32 = 1;

thread_local! {
    static COMPLETED: Cell<u64> = const { Cell::new(0) };
}

pub fn run(
    common: &CommonConfig,
    world: &mpi::topology::SimpleCommunicator,
    mode: &ModeCmd,
) -> Vec<BenchRow> {
    let rank = world.rank();
    match mode {
        ModeCmd::OneToOne {
            endpoints: _,
            inflight,
            threads,
        } => {
            if world.size() != 2 {
                if rank == 0 {
                    eprintln!("ucx-am one-to-one requires exactly 2 ranks");
                }
                return Vec::new();
            }
            if *threads > 1 && rank == 0 {
                eprintln!(
                    "ucx-am: multi-threading not yet supported (threads > 1), using single thread"
                );
            }
            run_one_to_one(common, world, *inflight as usize)
        }
        ModeCmd::MultiClient { inflight } => {
            run_multi_client(common, world, *inflight as usize)
        }
    }
}

fn run_one_to_one(
    common: &CommonConfig,
    world: &mpi::topology::SimpleCommunicator,
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

    let (context, worker) = unsafe { init_ucx() };

    if is_client {
        unsafe {
            register_response_handler(worker);
        }
    } else {
        unsafe {
            register_request_handler(worker);
        }
    }

    let (local_addr, local_addr_len) = unsafe { get_worker_address(worker) };
    let local_bytes =
        unsafe { std::slice::from_raw_parts(local_addr as *const u8, local_addr_len) };

    let peer_rank = 1 - rank;
    let remote_bytes = mpi_util::exchange_bytes_variable(world, rank, peer_rank, local_bytes);

    unsafe {
        ucx_sys::ucp_worker_release_address(worker, local_addr);
    }

    let ep = unsafe { create_endpoint(worker, &remote_bytes) };

    world.barrier();

    let duration = Duration::from_secs(common.duration_secs);
    let interval = Duration::from_millis(common.interval_ms);
    let msg_size = common.message_size;
    let mut all_rows = Vec::new();

    for run_idx in 0..common.runs {
        world.barrier();

        if is_client {
            let mut collector = EpochCollector::new(interval);
            let buffer = vec![0u8; msg_size];

            COMPLETED.with(|c| c.set(0));

            for _ in 0..inflight {
                unsafe {
                    send_request(ep, &buffer);
                }
            }

            let start = Instant::now();
            while start.elapsed() < duration {
                unsafe {
                    ucx_sys::ucp_worker_progress(worker);
                }

                let completed = COMPLETED.with(|c| {
                    let v = c.get();
                    c.set(0);
                    v
                });

                if completed > 0 {
                    collector.record(completed);

                    for _ in 0..completed {
                        unsafe {
                            send_request(ep, &buffer);
                        }
                    }
                }
            }

            let drain_deadline = Instant::now() + Duration::from_secs(2);
            while Instant::now() < drain_deadline {
                unsafe {
                    ucx_sys::ucp_worker_progress(worker);
                }

                let completed = COMPLETED.with(|c| {
                    let v = c.get();
                    c.set(0);
                    v
                });

                if completed > 0 {
                    collector.record(completed);
                }
            }

            collector.finish();
            let steady = collector.steady_state(common.trim);
            let rows = parquet_out::rows_from_epochs(
                "ucx_am",
                "1to1",
                steady,
                msg_size as u64,
                1,
                inflight as u32,
                1,
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
            let start = Instant::now();
            while start.elapsed() < duration {
                unsafe {
                    ucx_sys::ucp_worker_progress(worker);
                }
            }

            let drain_deadline = Instant::now() + Duration::from_secs(2);
            while Instant::now() < drain_deadline {
                unsafe {
                    ucx_sys::ucp_worker_progress(worker);
                }
            }
        }
    }

    unsafe {
        ucx_sys::ucp_ep_destroy(ep);
        ucx_sys::ucp_worker_destroy(worker);
        ucx_sys::ucp_cleanup(context);
    }

    all_rows
}

fn run_multi_client(
    common: &CommonConfig,
    world: &mpi::topology::SimpleCommunicator,
    inflight: usize,
) -> Vec<BenchRow> {
    let rank = world.rank();
    let size = world.size();
    let num_clients = (size - 1) as usize;

    crate::affinity::pin_thread_if_configured(
        common.affinity_mode,
        common.affinity_start,
        rank,
        1,
        0,
    );

    let (context, worker) = unsafe { init_ucx() };

    if rank == 0 {
        unsafe {
            register_request_handler(worker);
        }

        let (local_addr, local_addr_len) = unsafe { get_worker_address(worker) };
        let local_bytes =
            unsafe { std::slice::from_raw_parts(local_addr as *const u8, local_addr_len) };

        let mut endpoints = Vec::new();
        for client_rank in 1..size {
            let remote_bytes =
                mpi_util::exchange_bytes_variable(world, rank, client_rank, local_bytes);
            let ep = unsafe { create_endpoint(worker, &remote_bytes) };
            endpoints.push(ep);
        }

        unsafe {
            ucx_sys::ucp_worker_release_address(worker, local_addr);
        }

        world.barrier();

        let duration = Duration::from_secs(common.duration_secs);

        for run_idx in 0..common.runs {
            world.barrier();

            let start = Instant::now();
            while start.elapsed() < duration {
                unsafe {
                    ucx_sys::ucp_worker_progress(worker);
                }
            }

            let drain_deadline = Instant::now() + Duration::from_secs(2);
            while Instant::now() < drain_deadline {
                unsafe {
                    ucx_sys::ucp_worker_progress(worker);
                }
            }

            if run_idx == 0 {
                eprintln!("Server run {} complete", run_idx + 1);
            }
        }

        for ep in endpoints {
            unsafe {
                ucx_sys::ucp_ep_destroy(ep);
            }
        }

        unsafe {
            ucx_sys::ucp_worker_destroy(worker);
            ucx_sys::ucp_cleanup(context);
        }

        Vec::new()
    } else {
        unsafe {
            register_response_handler(worker);
        }

        let (local_addr, local_addr_len) = unsafe { get_worker_address(worker) };
        let local_bytes =
            unsafe { std::slice::from_raw_parts(local_addr as *const u8, local_addr_len) };

        let remote_bytes = mpi_util::exchange_bytes_variable(world, rank, 0, local_bytes);

        unsafe {
            ucx_sys::ucp_worker_release_address(worker, local_addr);
        }

        let ep = unsafe { create_endpoint(worker, &remote_bytes) };

        world.barrier();

        let duration = Duration::from_secs(common.duration_secs);
        let interval = Duration::from_millis(common.interval_ms);
        let msg_size = common.message_size;
        let mut all_rows = Vec::new();

        for run_idx in 0..common.runs {
            world.barrier();

            let mut collector = EpochCollector::new(interval);
            let buffer = vec![0u8; msg_size];

            COMPLETED.with(|c| c.set(0));

            for _ in 0..inflight {
                unsafe {
                    send_request(ep, &buffer);
                }
            }

            let start = Instant::now();
            while start.elapsed() < duration {
                unsafe {
                    ucx_sys::ucp_worker_progress(worker);
                }

                let completed = COMPLETED.with(|c| {
                    let v = c.get();
                    c.set(0);
                    v
                });

                if completed > 0 {
                    collector.record(completed);

                    for _ in 0..completed {
                        unsafe {
                            send_request(ep, &buffer);
                        }
                    }
                }
            }

            let drain_deadline = Instant::now() + Duration::from_secs(2);
            while Instant::now() < drain_deadline {
                unsafe {
                    ucx_sys::ucp_worker_progress(worker);
                }

                let completed = COMPLETED.with(|c| {
                    let v = c.get();
                    c.set(0);
                    v
                });

                if completed > 0 {
                    collector.record(completed);
                }
            }

            collector.finish();
            let steady = collector.steady_state(common.trim);
            let rows = parquet_out::rows_from_epochs(
                "ucx_am",
                "multi_client",
                steady,
                msg_size as u64,
                1,
                inflight as u32,
                num_clients as u32,
                1,
                run_idx,
            );

            if !steady.is_empty() {
                let avg_rps: f64 = rows.iter().map(|r| r.rps).sum::<f64>() / rows.len() as f64;
                eprintln!(
                    "  Client {} Run {}: avg {:.0} RPS ({} steady epochs)",
                    rank,
                    run_idx + 1,
                    avg_rps,
                    steady.len()
                );
            }

            all_rows.extend(rows);
        }

        unsafe {
            ucx_sys::ucp_ep_destroy(ep);
            ucx_sys::ucp_worker_destroy(worker);
            ucx_sys::ucp_cleanup(context);
        }

        all_rows
    }
}

unsafe fn init_ucx() -> (ucx_sys::ucp_context_h, ucx_sys::ucp_worker_h) {
    let mut config: *mut ucx_sys::ucp_config_t = ptr::null_mut();
    let status = unsafe { ucx_sys::ucp_config_read(ptr::null(), ptr::null(), &mut config) };
    assert_eq!(status, ucx_sys::ucs_status_t_UCS_OK);

    let params = ucx_sys::ucp_params_t {
        field_mask: ucx_sys::ucp_params_field_UCP_PARAM_FIELD_FEATURES as u64,
        features: ucx_sys::ucp_feature_UCP_FEATURE_AM as u64,
        ..unsafe { std::mem::zeroed() }
    };

    let mut context: ucx_sys::ucp_context_h = ptr::null_mut();
    let status = unsafe {
        ucx_sys::ucp_init_version(
            ucx_sys::UCP_API_MAJOR,
            ucx_sys::UCP_API_MINOR,
            &params,
            config,
            &mut context,
        )
    };
    assert_eq!(status, ucx_sys::ucs_status_t_UCS_OK);

    unsafe { ucx_sys::ucp_config_release(config) };

    let worker_params = ucx_sys::ucp_worker_params_t {
        field_mask: ucx_sys::ucp_worker_params_field_UCP_WORKER_PARAM_FIELD_THREAD_MODE as u64,
        thread_mode: ucx_sys::ucs_thread_mode_t_UCS_THREAD_MODE_SINGLE,
        ..unsafe { std::mem::zeroed() }
    };

    let mut worker: ucx_sys::ucp_worker_h = ptr::null_mut();
    let status = unsafe { ucx_sys::ucp_worker_create(context, &worker_params, &mut worker) };
    assert_eq!(status, ucx_sys::ucs_status_t_UCS_OK);

    (context, worker)
}

unsafe fn get_worker_address(
    worker: ucx_sys::ucp_worker_h,
) -> (*mut ucx_sys::ucp_address_t, usize) {
    let mut addr: *mut ucx_sys::ucp_address_t = ptr::null_mut();
    let mut addr_len: usize = 0;
    let status = unsafe { ucx_sys::ucp_worker_get_address(worker, &mut addr, &mut addr_len) };
    assert_eq!(status, ucx_sys::ucs_status_t_UCS_OK);
    (addr, addr_len)
}

unsafe fn create_endpoint(
    worker: ucx_sys::ucp_worker_h,
    remote_addr: &[u8],
) -> ucx_sys::ucp_ep_h {
    let ep_params = ucx_sys::ucp_ep_params_t {
        field_mask: ucx_sys::ucp_ep_params_field_UCP_EP_PARAM_FIELD_REMOTE_ADDRESS as u64,
        address: remote_addr.as_ptr() as *const ucx_sys::ucp_address_t,
        ..unsafe { std::mem::zeroed() }
    };

    let mut ep: ucx_sys::ucp_ep_h = ptr::null_mut();
    let status = unsafe { ucx_sys::ucp_ep_create(worker, &ep_params, &mut ep) };
    assert_eq!(status, ucx_sys::ucs_status_t_UCS_OK);
    ep
}

unsafe extern "C" fn send_cb(
    request: *mut c_void,
    _status: ucx_sys::ucs_status_t,
    _user_data: *mut c_void,
) {
    unsafe { ucx_sys::ucp_request_free(request) };
}

unsafe fn send_request(ep: ucx_sys::ucp_ep_h, buffer: &[u8]) {
    let send_params = ucx_sys::ucp_request_param_t {
        op_attr_mask: ucx_sys::ucp_op_attr_t_UCP_OP_ATTR_FIELD_CALLBACK,
        cb: ucx_sys::ucp_request_param_t__bindgen_ty_1 {
            send: Some(send_cb),
        },
        ..unsafe { std::mem::zeroed() }
    };

    let status_ptr = unsafe {
        ucx_sys::ucp_am_send_nbx(
            ep,
            AM_REQUEST,
            ptr::null(),
            0,
            buffer.as_ptr() as *const c_void,
            buffer.len(),
            &send_params,
        )
    };

    if ucs_ptr_is_err(status_ptr) {
        panic!("ucp_am_send_nbx failed");
    }

    if status_ptr.is_null() {
        // Completed inline, nothing to free
    } else if ucs_ptr_is_ptr(status_ptr) {
        // Async request will be freed by callback
    }
}

unsafe extern "C" fn response_handler(
    _arg: *mut c_void,
    _header: *const c_void,
    _header_length: usize,
    _data: *mut c_void,
    _length: usize,
    _param: *const ucx_sys::ucp_am_recv_param_t,
) -> ucx_sys::ucs_status_t {
    COMPLETED.with(|c| c.set(c.get() + 1));
    ucx_sys::ucs_status_t_UCS_OK
}

unsafe extern "C" fn request_handler(
    _arg: *mut c_void,
    _header: *const c_void,
    _header_length: usize,
    data: *mut c_void,
    length: usize,
    param: *const ucx_sys::ucp_am_recv_param_t,
) -> ucx_sys::ucs_status_t {
    let reply_ep = unsafe { (*param).reply_ep };

    let send_params = ucx_sys::ucp_request_param_t {
        op_attr_mask: ucx_sys::ucp_op_attr_t_UCP_OP_ATTR_FIELD_CALLBACK,
        cb: ucx_sys::ucp_request_param_t__bindgen_ty_1 {
            send: Some(send_cb),
        },
        ..unsafe { std::mem::zeroed() }
    };

    let status_ptr = unsafe {
        ucx_sys::ucp_am_send_nbx(
            reply_ep,
            AM_RESPONSE,
            ptr::null(),
            0,
            data,
            length,
            &send_params,
        )
    };

    if ucs_ptr_is_err(status_ptr) {
        return ucx_sys::ucs_status_t_UCS_ERR_NO_MEMORY;
    }

    ucx_sys::ucs_status_t_UCS_OK
}

unsafe fn register_response_handler(worker: ucx_sys::ucp_worker_h) {
    let handler_param = ucx_sys::ucp_am_handler_param_t {
        field_mask: (ucx_sys::ucp_am_handler_param_field_UCP_AM_HANDLER_PARAM_FIELD_ID
            | ucx_sys::ucp_am_handler_param_field_UCP_AM_HANDLER_PARAM_FIELD_CB
            | ucx_sys::ucp_am_handler_param_field_UCP_AM_HANDLER_PARAM_FIELD_FLAGS) as u64,
        id: AM_RESPONSE,
        cb: Some(response_handler),
        flags: ucx_sys::ucp_am_cb_flags_UCP_AM_FLAG_WHOLE_MSG,
        ..unsafe { std::mem::zeroed() }
    };

    let status = unsafe { ucx_sys::ucp_worker_set_am_recv_handler(worker, &handler_param) };
    assert_eq!(status, ucx_sys::ucs_status_t_UCS_OK);
}

unsafe fn register_request_handler(worker: ucx_sys::ucp_worker_h) {
    let handler_param = ucx_sys::ucp_am_handler_param_t {
        field_mask: (ucx_sys::ucp_am_handler_param_field_UCP_AM_HANDLER_PARAM_FIELD_ID
            | ucx_sys::ucp_am_handler_param_field_UCP_AM_HANDLER_PARAM_FIELD_CB
            | ucx_sys::ucp_am_handler_param_field_UCP_AM_HANDLER_PARAM_FIELD_FLAGS) as u64,
        id: AM_REQUEST,
        cb: Some(request_handler),
        flags: ucx_sys::ucp_am_cb_flags_UCP_AM_FLAG_WHOLE_MSG,
        ..unsafe { std::mem::zeroed() }
    };

    let status = unsafe { ucx_sys::ucp_worker_set_am_recv_handler(worker, &handler_param) };
    assert_eq!(status, ucx_sys::ucs_status_t_UCS_OK);
}

fn ucs_ptr_is_err(ptr: ucx_sys::ucs_status_ptr_t) -> bool {
    (ptr as usize) >= (ucx_sys::ucs_status_t_UCS_ERR_LAST as usize).wrapping_neg()
}

fn ucs_ptr_is_ptr(ptr: ucx_sys::ucs_status_ptr_t) -> bool {
    let p = ptr as usize;
    p != 0 && !ucs_ptr_is_err(ptr)
}
