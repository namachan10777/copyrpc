//! Minimal safe wrapper around ucx_sys for UCP Active Messages.
//!
//! # Example
//!
//! ```no_run
//! use benchkv::ucx_am::{UcpContext, UcpWorker, UcpEndpoint};
//! use ucx_sys::ucs_status_t;
//!
//! // Initialize context
//! let ctx = UcpContext::new()?;
//!
//! // Create workers
//! let worker = UcpWorker::new(&ctx)?;
//!
//! // Exchange addresses (e.g., via MPI or sockets)
//! let my_addr = worker.address()?;
//! let remote_addr = /* receive from peer */;
//!
//! // Register AM handler (ID 0)
//! unsafe extern "C" fn am_handler(
//!     arg: *mut std::ffi::c_void,
//!     header: *const std::ffi::c_void,
//!     header_length: usize,
//!     data: *mut std::ffi::c_void,
//!     length: usize,
//!     param: *const ucx_sys::ucp_am_recv_param_t,
//! ) -> ucs_status_t {
//!     // Handle received message
//!     ucx_sys::ucs_status_t_UCS_OK as ucs_status_t
//! }
//! worker.set_am_handler(0, Some(am_handler), std::ptr::null_mut())?;
//!
//! // Create endpoint
//! let ep = UcpEndpoint::new(&worker, &remote_addr)?;
//!
//! // Send message
//! let header = b"header";
//! let data = b"payload";
//! ep.am_send_nbx(0, header, data)?;
//!
//! // Progress loop
//! while worker.progress() > 0 { }
//! # Ok::<(), std::io::Error>(())
//! ```

use std::ffi::c_void;
use std::io;
use std::mem::MaybeUninit;
use std::ptr;

// Re-export key types from ucx_sys
use ucx_sys::{
    ucp_address_t, ucp_am_handler_param_t, ucp_am_recv_callback_t, ucp_context_h, ucp_ep_h,
    ucp_ep_params_t, ucp_params_t, ucp_worker_h, ucp_worker_params_t, ucs_status_ptr_t,
    ucs_status_t,
};

// Constants
const UCS_OK: ucs_status_t = ucx_sys::ucs_status_t_UCS_OK as ucs_status_t;
const UCP_API_MAJOR: u32 = ucx_sys::UCP_API_MAJOR;
const UCP_API_MINOR: u32 = ucx_sys::UCP_API_MINOR;
const UCP_FEATURE_AM: u64 = ucx_sys::ucp_feature_UCP_FEATURE_AM as u64;
const UCP_PARAM_FIELD_FEATURES: u64 =
    ucx_sys::ucp_params_field_UCP_PARAM_FIELD_FEATURES as u64;
const UCP_WORKER_PARAM_FIELD_THREAD_MODE: u64 =
    ucx_sys::ucp_worker_params_field_UCP_WORKER_PARAM_FIELD_THREAD_MODE as u64;
const UCP_EP_PARAM_FIELD_REMOTE_ADDRESS: u64 =
    ucx_sys::ucp_ep_params_field_UCP_EP_PARAM_FIELD_REMOTE_ADDRESS as u64;
const UCP_AM_HANDLER_PARAM_FIELD_ID: u64 =
    ucx_sys::ucp_am_handler_param_field_UCP_AM_HANDLER_PARAM_FIELD_ID as u64;
const UCP_AM_HANDLER_PARAM_FIELD_CB: u64 =
    ucx_sys::ucp_am_handler_param_field_UCP_AM_HANDLER_PARAM_FIELD_CB as u64;
const UCP_AM_HANDLER_PARAM_FIELD_ARG: u64 =
    ucx_sys::ucp_am_handler_param_field_UCP_AM_HANDLER_PARAM_FIELD_ARG as u64;
const UCS_THREAD_MODE_SINGLE: u32 = ucx_sys::ucs_thread_mode_t_UCS_THREAD_MODE_SINGLE;

/// Convert ucs_status_t to io::Result
fn status_to_result(status: ucs_status_t) -> io::Result<()> {
    if status == UCS_OK {
        Ok(())
    } else {
        let msg = unsafe {
            let ptr = ucx_sys::ucs_status_string(status);
            if ptr.is_null() {
                format!("UCX error: {}", status)
            } else {
                std::ffi::CStr::from_ptr(ptr)
                    .to_string_lossy()
                    .into_owned()
            }
        };
        Err(io::Error::other(msg))
    }
}

/// Check if status pointer is an error (inline function, not a UCX API)
fn ucs_ptr_is_err(ptr: ucs_status_ptr_t) -> bool {
    (ptr as usize) >= (usize::MAX - 127)
}

/// Extract status from error pointer (inline function, not a UCX API)
fn ucs_ptr_raw_status(ptr: ucs_status_ptr_t) -> ucs_status_t {
    ptr as ucs_status_t
}

/// UCP context wrapper.
pub struct UcpContext {
    handle: ucp_context_h,
}

impl UcpContext {
    /// Initialize UCP with Active Message feature.
    pub fn new() -> io::Result<Self> {
        let params = ucp_params_t {
            field_mask: UCP_PARAM_FIELD_FEATURES,
            features: UCP_FEATURE_AM,
            request_size: 0,
            request_init: None,
            request_cleanup: None,
            tag_sender_mask: 0,
            mt_workers_shared: 0,
            estimated_num_eps: 0,
            estimated_num_ppn: 0,
            name: ptr::null(),
        };

        let mut context: MaybeUninit<ucp_context_h> = MaybeUninit::uninit();
        let status = unsafe {
            ucx_sys::ucp_init_version(
                UCP_API_MAJOR,
                UCP_API_MINOR,
                &params as *const _,
                ptr::null(), // config
                context.as_mut_ptr(),
            )
        };

        status_to_result(status)?;

        Ok(Self {
            handle: unsafe { context.assume_init() },
        })
    }

    /// Get the raw context handle (for creating workers).
    pub fn handle(&self) -> ucp_context_h {
        self.handle
    }
}

impl Drop for UcpContext {
    fn drop(&mut self) {
        unsafe {
            ucx_sys::ucp_cleanup(self.handle);
        }
    }
}

/// UCP worker wrapper.
pub struct UcpWorker {
    handle: ucp_worker_h,
}

impl UcpWorker {
    /// Create a worker from a context.
    pub fn new(ctx: &UcpContext) -> io::Result<Self> {
        let params = ucp_worker_params_t {
            field_mask: UCP_WORKER_PARAM_FIELD_THREAD_MODE,
            thread_mode: UCS_THREAD_MODE_SINGLE,
            cpu_mask: unsafe { std::mem::zeroed() },
            events: 0,
            user_data: ptr::null_mut(),
            event_fd: 0,
            flags: 0,
            name: ptr::null(),
            am_alignment: 0,
            client_id: 0,
        };

        let mut worker: MaybeUninit<ucp_worker_h> = MaybeUninit::uninit();
        let status = unsafe {
            ucx_sys::ucp_worker_create(ctx.handle(), &params as *const _, worker.as_mut_ptr())
        };

        status_to_result(status)?;

        Ok(Self {
            handle: unsafe { worker.assume_init() },
        })
    }

    /// Get the worker's address for connection exchange.
    pub fn address(&self) -> io::Result<Vec<u8>> {
        let mut addr_ptr: *mut ucp_address_t = ptr::null_mut();
        let mut addr_len: usize = 0;

        let status = unsafe {
            ucx_sys::ucp_worker_get_address(self.handle, &mut addr_ptr, &mut addr_len)
        };

        status_to_result(status)?;

        let addr_vec = unsafe {
            std::slice::from_raw_parts(addr_ptr as *const u8, addr_len).to_vec()
        };

        unsafe {
            ucx_sys::ucp_worker_release_address(self.handle, addr_ptr);
        }

        Ok(addr_vec)
    }

    /// Drive progress on the worker.
    pub fn progress(&self) -> u32 {
        unsafe { ucx_sys::ucp_worker_progress(self.handle) }
    }

    /// Set an Active Message receive handler.
    ///
    /// # Arguments
    /// * `am_id` - Active message ID (0-31)
    /// * `callback` - Receive callback function
    /// * `arg` - User data pointer passed to callback
    pub fn set_am_handler(
        &self,
        am_id: u32,
        callback: ucp_am_recv_callback_t,
        arg: *mut c_void,
    ) -> io::Result<()> {
        let params = ucp_am_handler_param_t {
            field_mask: UCP_AM_HANDLER_PARAM_FIELD_ID
                | UCP_AM_HANDLER_PARAM_FIELD_CB
                | UCP_AM_HANDLER_PARAM_FIELD_ARG,
            id: am_id,
            flags: 0,
            cb: callback,
            arg,
        };

        let status =
            unsafe { ucx_sys::ucp_worker_set_am_recv_handler(self.handle, &params as *const _) };

        status_to_result(status)
    }

    /// Get the raw worker handle (for creating endpoints).
    pub fn handle(&self) -> ucp_worker_h {
        self.handle
    }
}

impl Drop for UcpWorker {
    fn drop(&mut self) {
        unsafe {
            ucx_sys::ucp_worker_destroy(self.handle);
        }
    }
}

/// UCP endpoint wrapper.
pub struct UcpEndpoint {
    handle: ucp_ep_h,
}

impl UcpEndpoint {
    /// Create an endpoint to a remote worker.
    ///
    /// # Arguments
    /// * `worker` - Local worker
    /// * `remote_addr` - Remote worker address (from `UcpWorker::address()`)
    pub fn new(worker: &UcpWorker, remote_addr: &[u8]) -> io::Result<Self> {
        let params = ucp_ep_params_t {
            field_mask: UCP_EP_PARAM_FIELD_REMOTE_ADDRESS,
            address: remote_addr.as_ptr() as *const ucp_address_t,
            err_mode: 0,
            err_handler: unsafe { std::mem::zeroed() },
            user_data: ptr::null_mut(),
            flags: 0,
            sockaddr: unsafe { std::mem::zeroed() },
            conn_request: ptr::null_mut(),
            name: ptr::null(),
            local_sockaddr: unsafe { std::mem::zeroed() },
        };

        let mut ep: MaybeUninit<ucp_ep_h> = MaybeUninit::uninit();
        let status =
            unsafe { ucx_sys::ucp_ep_create(worker.handle(), &params as *const _, ep.as_mut_ptr()) };

        status_to_result(status)?;

        Ok(Self {
            handle: unsafe { ep.assume_init() },
        })
    }

    /// Send an active message (non-blocking extended API).
    ///
    /// # Arguments
    /// * `am_id` - Active message ID (must match receiver's handler)
    /// * `header` - Header data (optional, can be empty)
    /// * `data` - Payload data (optional, can be empty)
    ///
    /// # Returns
    /// `Ok(())` if send completed inline, `Err` otherwise.
    /// For simplicity, this wrapper only supports inline completion.
    pub fn am_send_nbx(&self, am_id: u32, header: &[u8], data: &[u8]) -> io::Result<()> {
        let header_ptr = if header.is_empty() {
            ptr::null()
        } else {
            header.as_ptr() as *const c_void
        };

        let data_ptr = if data.is_empty() {
            ptr::null()
        } else {
            data.as_ptr() as *const c_void
        };

        let param: ucx_sys::ucp_request_param_t = unsafe { std::mem::zeroed() };

        let status_ptr = unsafe {
            ucx_sys::ucp_am_send_nbx(
                self.handle,
                am_id,
                header_ptr,
                header.len(),
                data_ptr,
                data.len(),
                &param as *const _,
            )
        };

        // Check if send completed inline (status == UCS_OK)
        if status_ptr.is_null() {
            // NULL means UCS_OK (inline completion)
            Ok(())
        } else if ucs_ptr_is_err(status_ptr) {
            // Error pointer
            let status = ucs_ptr_raw_status(status_ptr);
            status_to_result(status)
        } else {
            // Request pointer — send is in progress, free the request
            unsafe {
                ucx_sys::ucp_request_free(status_ptr);
            }
            Ok(())
        }
    }

}

impl Drop for UcpEndpoint {
    fn drop(&mut self) {
        unsafe {
            ucx_sys::ucp_ep_destroy(self.handle);
        }
    }
}
