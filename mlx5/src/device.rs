use std::{io, mem::MaybeUninit, ops::Deref, ptr::NonNull};

pub struct Device {
    device: NonNull<mlx5_sys::ibv_device>,
}

pub struct DeviceList {
    list: NonNull<*mut mlx5_sys::ibv_device>,
    list_ref: Box<[Device]>,
}

impl DeviceList {
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

pub struct Context {
    ctx: NonNull<mlx5_sys::ibv_context>,
}

impl Device {
    /// Open the device with mlx5dv_open_device.
    pub fn open(&self) -> io::Result<Context> {
        unsafe {
            let mut attr: mlx5_sys::mlx5dv_context_attr = std::mem::zeroed();
            let ctx = mlx5_sys::mlx5dv_open_device(self.device.as_ptr(), &mut attr);
            NonNull::new(ctx).map_or(Err(io::Error::last_os_error()), |ctx| Ok(Context { ctx }))
        }
    }
}

impl Drop for Context {
    fn drop(&mut self) {
        unsafe {
            mlx5_sys::ibv_close_device(self.ctx.as_ptr());
        }
    }
}

impl Context {
    /// Query ibverbs device attributes.
    pub fn query_ibv_device(&self) -> io::Result<mlx5_sys::ibv_device_attr> {
        unsafe {
            let mut attrs: MaybeUninit<mlx5_sys::ibv_device_attr> = MaybeUninit::uninit();
            let ret = mlx5_sys::ibv_query_device(self.ctx.as_ptr(), attrs.as_mut_ptr());
            if ret != 0 {
                return Err(io::Error::from_raw_os_error(-ret));
            }
            Ok(attrs.assume_init())
        }
    }

    /// Query mlx5 device attributes.
    pub fn query_mlx5_device(&self) -> io::Result<mlx5_sys::mlx5dv_context> {
        unsafe {
            let mut attrs: MaybeUninit<mlx5_sys::mlx5dv_context> = MaybeUninit::zeroed();
            let ret = mlx5_sys::mlx5dv_query_device(self.ctx.as_ptr(), attrs.as_mut_ptr());
            if ret != 0 {
                return Err(io::Error::from_raw_os_error(-ret));
            }
            Ok(attrs.assume_init())
        }
    }
}
