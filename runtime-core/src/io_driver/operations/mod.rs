use io_uring::squeue::Entry;

pub(crate) mod fd;
pub mod files;
pub mod fixed_buf;
pub mod io_traits;
pub mod sockets;

pub use fixed_buf::FixedBufIndex;
pub use io_traits::{IoBuf, IoBufMut, Slice};

/// What a failed completion says about the multishot op that produced it. The
/// kernel ended the op either way; this is whether re-arming it is worth
/// anything, and when.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Continuation {
    /// Re-arm now: the failure was transient in itself.
    Continue,
    /// Re-arm once the condition clears. Nothing here knows when that is —
    /// the exhausted resource is refilled by another holder, not by polling.
    ContinueLater,
    /// The op is over; a re-arm would fail the same way.
    NotContinue,
}

/// A token for a single-shot io_uring operation.
#[derive(Debug, Clone, Copy)]
pub struct OneshotToken(pub(crate) u64);

/// A token for a multishot io_uring operation.
#[derive(Debug, Clone, Copy)]
pub struct MultishotToken(pub(crate) u64);

impl OneshotToken {
    /// Raw u64 value for io_uring user_data: slab index plus buffer-ring bits.
    pub(crate) fn raw(self) -> u64 {
        self.0
    }

    pub(crate) fn index(self) -> usize {
        crate::io_driver::UserData::token_index(self.0)
    }
}

impl MultishotToken {
    /// Raw u64 value for io_uring user_data: slab index plus buffer-ring bits.
    pub(crate) fn raw(self) -> u64 {
        self.0
    }

    pub(crate) fn index(self) -> usize {
        crate::io_driver::UserData::token_index(self.0)
    }
}

#[allow(drop_bounds)]
trait IoOP: Drop {
    fn register_to_driver(&mut self);

    fn driver_cleanup(&mut self);

    fn to_entry(&self) -> Entry;
}
