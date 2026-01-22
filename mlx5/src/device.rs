//! Device enumeration and context management.
//!
//! This module provides access to mlx5 RDMA devices via the mlx5dv API.

use std::rc::Rc;
use std::{io, mem::MaybeUninit, ops::Deref, ptr::NonNull};

use crate::types::{Gid, LinkLayer, PciAtomicCaps};

/// An RDMA device.
///
/// Represents a single InfiniBand or RoCE device in the system.
/// Use [`DeviceList::list()`] to enumerate available devices.
pub struct Device {
    device: NonNull<mlx5_sys::ibv_device>,
}

/// A list of available RDMA devices.
///
/// Obtained via [`DeviceList::list()`]. The list owns references to all
/// devices and will free them when dropped.
///
/// # Example
/// ```ignore
/// let devices = DeviceList::list()?;
/// for device in devices.iter() {
///     let ctx = device.open()?;
///     // use context...
/// }
/// ```
pub struct DeviceList {
    list: NonNull<*mut mlx5_sys::ibv_device>,
    list_ref: Box<[Device]>,
}

impl DeviceList {
    /// Get a list of available RDMA devices.
    ///
    /// # Errors
    /// Returns an error if no devices are found or if the query fails.
    pub fn list() -> io::Result<Self> {
        unsafe {
            let mut num_devices = MaybeUninit::uninit();
            let list = mlx5_sys::ibv_get_device_list(num_devices.as_mut_ptr());
            let Some(list) = NonNull::new(list) else {
                return Err(io::Error::last_os_error());
            };
            let len = num_devices.assume_init() as usize;
            let list_ref = (0..len)
                .map(|i| Device {
                    device: NonNull::new_unchecked(*list.as_ptr().add(i)),
                })
                .collect();
            Ok(Self { list, list_ref })
        }
    }
}

impl Drop for DeviceList {
    fn drop(&mut self) {
        unsafe {
            mlx5_sys::ibv_free_device_list(self.list.as_ptr());
        }
    }
}

impl Deref for DeviceList {
    type Target = [Device];
    fn deref(&self) -> &Self::Target {
        &self.list_ref
    }
}

/// Internal context structure holding the raw ibv_context pointer.
///
/// This is wrapped in Rc to ensure proper resource lifetime management.
pub(crate) struct ContextInner {
    ctx: NonNull<mlx5_sys::ibv_context>,
}

impl Drop for ContextInner {
    fn drop(&mut self) {
        unsafe {
            mlx5_sys::ibv_close_device(self.ctx.as_ptr());
        }
    }
}

/// An opened RDMA device context.
///
/// The context is required for creating RDMA resources such as Protection Domains,
/// Queue Pairs, Completion Queues, etc.
///
/// Created via [`Device::open()`]. The device will be closed when the context is dropped.
/// This type uses `Rc` internally and can be cheaply cloned.
#[derive(Clone)]
pub struct Context(Rc<ContextInner>);

impl Device {
    /// Open the device with mlx5dv_open_device.
    ///
    /// Opens an RDMA device context with mlx5 provider attributes.
    /// This is required before creating any RDMA resources.
    ///
    /// # Errors
    /// Returns an error if the device cannot be opened.
    pub fn open(&self) -> io::Result<Context> {
        unsafe {
            let mut attr: mlx5_sys::mlx5dv_context_attr = std::mem::zeroed();
            let ctx = mlx5_sys::mlx5dv_open_device(self.device.as_ptr(), &mut attr);
            NonNull::new(ctx).map_or(Err(io::Error::last_os_error()), |ctx| {
                Ok(Context(Rc::new(ContextInner { ctx })))
            })
        }
    }
}

impl Context {
    /// Get the raw ibv_context pointer.
    pub(crate) fn as_ptr(&self) -> *mut mlx5_sys::ibv_context {
        self.0.ctx.as_ptr()
    }

    /// Query standard ibverbs device attributes.
    ///
    /// Returns device attributes defined by the ibverbs API, including:
    /// - Maximum supported QPs, CQs, MRs, PDs
    /// - Maximum work requests, scatter/gather entries
    /// - Device capabilities flags
    /// - Physical port count
    ///
    /// Note: The maximum values returned are upper limits. Actual available
    /// resources may be limited by machine configuration, host memory,
    /// user permissions, and resources already in use.
    ///
    /// # Errors
    /// Returns an error if the query fails.
    pub fn query_ibv_device(&self) -> io::Result<crate::types::DeviceAttr> {
        unsafe {
            let mut attrs: MaybeUninit<crate::types::DeviceAttr> = MaybeUninit::uninit();
            let ret = mlx5_sys::ibv_query_device(
                self.as_ptr(),
                attrs.as_mut_ptr() as *mut mlx5_sys::ibv_device_attr,
            );
            if ret != 0 {
                return Err(io::Error::from_raw_os_error(-ret));
            }
            Ok(attrs.assume_init())
        }
    }

    /// Query mlx5-specific device attributes.
    ///
    /// Returns HW device-specific information important for data-path
    /// that isn't provided by [`query_ibv_device()`](Self::query_ibv_device).
    ///
    /// The returned [`Mlx5DeviceAttr`](crate::types::Mlx5DeviceAttr) includes:
    /// - `version`: Format version of internal hardware structures
    /// - `flags`: Device capability flags (CQE version, MPW support, etc.)
    /// - `comp_mask`: Indicates which optional fields are valid
    /// - `cqe_comp_caps`: CQE compression capabilities
    /// - `sw_parsing_caps`: Software parsing capabilities
    /// - `striding_rq_caps`: Striding RQ capabilities
    /// - `max_dynamic_bfregs`: Max blue-flame registers that can be dynamically allocated
    /// - `dci_streams_caps`: DCI streams capabilities
    /// - And more mlx5-specific capabilities
    ///
    /// # Errors
    /// Returns an error if the query fails.
    pub fn query_mlx5_device(&self) -> io::Result<crate::types::Mlx5DeviceAttr> {
        unsafe {
            let mut attrs: MaybeUninit<crate::types::Mlx5DeviceAttr> = MaybeUninit::zeroed();
            // Set comp_mask to request all optional fields
            let attrs_ptr = attrs.as_mut_ptr() as *mut mlx5_sys::mlx5dv_context;
            (*attrs_ptr).comp_mask =
                (mlx5_sys::mlx5dv_context_comp_mask_MLX5DV_CONTEXT_MASK_CQE_COMPRESION
                    | mlx5_sys::mlx5dv_context_comp_mask_MLX5DV_CONTEXT_MASK_SWP
                    | mlx5_sys::mlx5dv_context_comp_mask_MLX5DV_CONTEXT_MASK_STRIDING_RQ
                    | mlx5_sys::mlx5dv_context_comp_mask_MLX5DV_CONTEXT_MASK_TUNNEL_OFFLOADS
                    | mlx5_sys::mlx5dv_context_comp_mask_MLX5DV_CONTEXT_MASK_DYN_BFREGS
                    | mlx5_sys::mlx5dv_context_comp_mask_MLX5DV_CONTEXT_MASK_CLOCK_INFO_UPDATE
                    | mlx5_sys::mlx5dv_context_comp_mask_MLX5DV_CONTEXT_MASK_DC_ODP_CAPS
                    | mlx5_sys::mlx5dv_context_comp_mask_MLX5DV_CONTEXT_MASK_DCI_STREAMS)
                    as u64;
            let ret = mlx5_sys::mlx5dv_query_device(self.as_ptr(), attrs_ptr);
            if ret != 0 {
                return Err(io::Error::from_raw_os_error(-ret));
            }
            Ok(attrs.assume_init())
        }
    }

    /// Query port attributes.
    ///
    /// Returns attributes for the specified port, including:
    /// - Port state (down, init, armed, active)
    /// - MTU settings
    /// - LID (Local Identifier)
    /// - Link layer type (InfiniBand or Ethernet/RoCE)
    ///
    /// # Arguments
    /// * `port_num` - Port number (1-based)
    ///
    /// # Errors
    /// Returns an error if the query fails.
    pub fn query_port(&self, port_num: u8) -> io::Result<crate::types::PortAttr> {
        unsafe {
            let mut attrs: MaybeUninit<crate::types::PortAttr> = MaybeUninit::uninit();
            let ret = mlx5_sys::ibv_query_port_ex(
                self.as_ptr(),
                port_num,
                attrs.as_mut_ptr() as *mut mlx5_sys::ibv_port_attr,
            );
            if ret != 0 {
                return Err(io::Error::from_raw_os_error(-ret));
            }
            Ok(attrs.assume_init())
        }
    }

    /// Query GID (Global Identifier) for a port.
    ///
    /// GIDs are used for RoCE addressing. Each port has a GID table containing
    /// multiple GIDs (typically one per IP address configured on the port).
    ///
    /// # Arguments
    /// * `port_num` - Port number (1-based)
    /// * `index` - GID table index (0-based)
    ///
    /// # Returns
    /// The GID at the specified index, or an error if the query fails.
    ///
    /// # NOTE: RoCE support is untested (IB-only hardware environment)
    ///
    /// # Example
    /// ```ignore
    /// // Query the first GID on port 1
    /// let gid = ctx.query_gid(1, 0)?;
    /// println!("GID: {:?}", gid);
    /// ```
    pub fn query_gid(&self, port_num: u8, index: u8) -> io::Result<Gid> {
        unsafe {
            let mut gid: MaybeUninit<mlx5_sys::ibv_gid> = MaybeUninit::zeroed();
            let ret = mlx5_sys::ibv_query_gid(
                self.as_ptr(),
                port_num,
                index as i32,
                gid.as_mut_ptr(),
            );
            if ret != 0 {
                return Err(io::Error::from_raw_os_error(-ret));
            }
            let gid = gid.assume_init();
            Ok(Gid::from_raw(gid.raw))
        }
    }

    /// Check if the port is using RoCE (Ethernet link layer).
    ///
    /// # Arguments
    /// * `port_num` - Port number (1-based)
    ///
    /// # Returns
    /// `true` if the port is using Ethernet (RoCE), `false` otherwise.
    ///
    /// # NOTE: RoCE support is untested (IB-only hardware environment)
    pub fn is_roce(&self, port_num: u8) -> io::Result<bool> {
        let port_attr = self.query_port(port_num)?;
        Ok(LinkLayer::from(port_attr.link_layer) == LinkLayer::Ethernet)
    }

    /// Check if the port is using InfiniBand link layer.
    ///
    /// # Arguments
    /// * `port_num` - Port number (1-based)
    ///
    /// # Returns
    /// `true` if the port is using InfiniBand, `false` otherwise.
    pub fn is_infiniband(&self, port_num: u8) -> io::Result<bool> {
        let port_attr = self.query_port(port_num)?;
        Ok(LinkLayer::from(port_attr.link_layer) == LinkLayer::InfiniBand)
    }

    /// Query extended device attributes including TM (Tag Matching) capabilities.
    ///
    /// Returns the TM capabilities of the device:
    /// - `max_num_tags`: Maximum number of tagged buffers in matching list
    /// - `max_ops`: Maximum number of outstanding tag operations
    /// - `max_sge`: Maximum number of SGEs in a tagged buffer
    ///
    /// Returns `None` if the query fails or TM is not supported.
    pub fn query_tm_caps(&self) -> Option<TmCaps> {
        unsafe {
            let mut attr: MaybeUninit<mlx5_sys::ibv_device_attr_ex> = MaybeUninit::zeroed();
            let ret = mlx5_sys::ibv_query_device_ex_ex(
                self.as_ptr(),
                std::ptr::null(),
                attr.as_mut_ptr(),
            );
            if ret != 0 {
                return None;
            }
            let attr = attr.assume_init();
            Some(TmCaps {
                max_rndv_hdr_size: attr.tm_caps.max_rndv_hdr_size,
                max_num_tags: attr.tm_caps.max_num_tags,
                flags: attr.tm_caps.flags,
                max_ops: attr.tm_caps.max_ops,
                max_sge: attr.tm_caps.max_sge,
            })
        }
    }

    /// Query PCI Atomic capabilities.
    ///
    /// Returns the PCI atomic operation capabilities of the device, indicating
    /// which sizes of atomic operations are supported:
    /// - `fetch_add`: Supported sizes for Fetch-and-Add (bitmask)
    /// - `swap`: Supported sizes for Swap (bitmask)
    /// - `compare_swap`: Supported sizes for Compare-and-Swap (bitmask)
    ///
    /// Each bitmask has bit N set if 2^N byte operations are supported.
    /// For example, `fetch_add = 0xC` means both 4-byte (bit 2) and 8-byte (bit 3)
    /// Fetch-and-Add are supported.
    ///
    /// Note: Masked atomic operations (32-bit and 64-bit extended atomics) require
    /// device support. ConnectX-5 and later generally support these operations.
    ///
    /// Returns `None` if the query fails or PCI atomics are not supported.
    ///
    /// # Example
    /// ```ignore
    /// if let Some(caps) = ctx.query_pci_atomic_caps() {
    ///     if caps.supports_cas_32() {
    ///         println!("32-bit masked CAS supported");
    ///     }
    ///     if caps.supports_cas_64() {
    ///         println!("64-bit masked CAS supported");
    ///     }
    /// }
    /// ```
    pub fn query_pci_atomic_caps(&self) -> Option<PciAtomicCaps> {
        unsafe {
            let mut attr: MaybeUninit<mlx5_sys::ibv_device_attr_ex> = MaybeUninit::zeroed();
            let ret = mlx5_sys::ibv_query_device_ex_ex(
                self.as_ptr(),
                std::ptr::null(),
                attr.as_mut_ptr(),
            );
            if ret != 0 {
                return None;
            }
            let attr = attr.assume_init();
            Some(PciAtomicCaps {
                fetch_add: attr.pci_atomic_caps.fetch_add,
                swap: attr.pci_atomic_caps.swap,
                compare_swap: attr.pci_atomic_caps.compare_swap,
            })
        }
    }
}

/// Tag Matching capabilities.
#[derive(Debug, Clone, Copy)]
pub struct TmCaps {
    /// Maximum size of rendezvous request header.
    pub max_rndv_hdr_size: u32,
    /// Maximum number of tagged buffers in a TM-SRQ matching list.
    pub max_num_tags: u32,
    /// Capability flags.
    pub flags: u32,
    /// Maximum number of outstanding list operations.
    pub max_ops: u32,
    /// Maximum number of SGEs in a tagged buffer.
    pub max_sge: u32,
}
