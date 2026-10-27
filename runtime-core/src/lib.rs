pub mod bytes;
pub mod channels;
#[cfg(feature = "simulation")]
pub mod deterministic_testing;
pub mod io_driver;
pub mod macros;
pub mod net;
pub mod runtime;
pub mod task;
#[cfg(feature = "tracing")]
pub mod tracing;

pub use macros::scoped_tls;
pub use task::Handle;

/// Re-exports used by `define_buf_ring!`-generated code. Not part of the
/// public API; callers should not rely on items here directly.
#[doc(hidden)]
pub mod __private {
    pub use io_uring::IoUring;
    pub use io_uring_rb::RingBuffer;
}
