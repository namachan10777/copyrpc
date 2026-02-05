//! Serial trait for types that can be safely transmitted through channels.

/// Marker trait for types that can be safely transmitted through the channel.
///
/// # Safety
/// Types implementing this trait must be `Copy` to ensure they can be safely
/// transmitted without ownership issues.
pub unsafe trait Serial: Copy {}

// Implement Serial for common types
unsafe impl Serial for u8 {}
unsafe impl Serial for u16 {}
unsafe impl Serial for u32 {}
unsafe impl Serial for u64 {}
unsafe impl Serial for u128 {}
unsafe impl Serial for usize {}
unsafe impl Serial for i8 {}
unsafe impl Serial for i16 {}
unsafe impl Serial for i32 {}
unsafe impl Serial for i64 {}
unsafe impl Serial for i128 {}
unsafe impl Serial for isize {}
unsafe impl Serial for f32 {}
unsafe impl Serial for f64 {}
unsafe impl Serial for bool {}
unsafe impl Serial for char {}
unsafe impl<T: Copy> Serial for Option<T> {}
unsafe impl<T: Copy, E: Copy> Serial for Result<T, E> {}
unsafe impl<T: Copy, const N: usize> Serial for [T; N] {}
unsafe impl<A: Copy, B: Copy> Serial for (A, B) {}
unsafe impl<A: Copy, B: Copy, C: Copy> Serial for (A, B, C) {}
unsafe impl<A: Copy, B: Copy, C: Copy, D: Copy> Serial for (A, B, C, D) {}
