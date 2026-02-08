mod flush;
mod devdax;
mod file;

#[allow(clippy::len_without_is_empty)]
pub trait PmemRegion {
    /// Raw pointer to the start of the region (for RDMA MR registration)
    fn as_ptr(&self) -> *mut u8;
    /// Size of the region in bytes
    fn len(&self) -> usize;
}

impl PmemRegion for devdax::DevDaxRegion {
    fn as_ptr(&self) -> *mut u8 {
        devdax::DevDaxRegion::as_ptr(self)
    }

    fn len(&self) -> usize {
        devdax::DevDaxRegion::len(self)
    }
}

impl PmemRegion for file::FileRegion {
    fn as_ptr(&self) -> *mut u8 {
        file::FileRegion::as_ptr(self)
    }

    fn len(&self) -> usize {
        file::FileRegion::len(self)
    }
}

pub use flush::{flush, drain, persist, FlushMethod, detect_flush_method};
pub use devdax::DevDaxRegion;
pub use file::FileRegion;
