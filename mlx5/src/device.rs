use std::{io, mem::MaybeUninit, ops::Deref, ptr::NonNull};

pub struct Device {
    device: NonNull<ibverbs_sys::ibv_device>,
}

pub struct DeviceList {
    list: NonNull<*mut ibverbs_sys::ibv_device>,
    list_ref: Box<[Device]>,
}

impl DeviceList {
    pub fn list() -> io::Result<Self> {
        unsafe {
            let mut num_devices = MaybeUninit::uninit();
            let list = ibverbs_sys::ibv_get_device_list(num_devices.as_mut_ptr());
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
            ibverbs_sys::ibv_free_device_list(self.list.as_ptr());
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
    ctx: NonNull<ibverbs_sys::ibv_context>,
}

impl Device {
    pub fn open(&self) -> io::Result<Context> {
        unsafe {
            let ctx = ibverbs_sys::ibv_open_device(self.device.as_ptr());
            NonNull::new(ctx).map_or(Err(io::Error::last_os_error()), |ctx| Ok(Context { ctx }))
        }
    }
}

impl Drop for Context {
    fn drop(&mut self) {
        unsafe {
            ibverbs_sys::ibv_close_device(self.ctx.as_ptr());
        }
    }
}
