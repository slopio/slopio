use io_uring::squeue::Entry;

pub mod files;
pub mod io_traits;
pub mod sockets;

pub use io_traits::{IoBuf, Slice};

/// A token for a single-shot io_uring operation.
#[derive(Debug, Clone, Copy)]
pub struct OneshotToken(pub(crate) u64);

/// A token for a multishot io_uring operation.
#[derive(Debug, Clone, Copy)]
pub struct MultishotToken(pub(crate) u64);

impl OneshotToken {
    /// Raw u64 value for io_uring user_data.
    pub(crate) fn raw(self) -> u64 {
        self.0
    }
}

impl MultishotToken {
    /// Raw u64 value for io_uring user_data.
    pub(crate) fn raw(self) -> u64 {
        self.0
    }
}

#[allow(drop_bounds)]
trait IoOP: Drop {
    fn register_to_driver(&mut self);

    fn driver_cleanup(&mut self);

    fn to_entry(&self) -> Entry;
}
