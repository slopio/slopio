//! Types for io_uring pre-registered ("fixed") buffers.

/// Index of a buffer previously registered with the io_uring instance via
/// `register_buffers`. Passed to `*Fixed` opcodes to identify which
/// registered slot the kernel should read from or write to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FixedBufIndex(u16);

impl FixedBufIndex {
    pub const fn new(index: u16) -> Self {
        Self(index)
    }

    pub const fn get(self) -> u16 {
        self.0
    }
}
