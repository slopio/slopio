//! Buffer traits for io_uring submission.
//!
//! io_uring may keep accessing a buffer after the future that submitted the
//! SQE is dropped (e.g. when a cancel races the original op). For that reason
//! ops that take buffers via these traits require ownership (or `'static`
//! borrow): the runtime can then hold the buffer alive via the keepalive
//! mechanism until the cancelled CQE arrives, even after the user's future
//! has been dropped.
//!
//! Borrowed-buffer APIs (e.g. `socket_send` taking `&'a [u8]`) remain
//! available for the common await-to-completion pattern, but are unsound to
//! drop mid-flight if the underlying memory might be freed before the kernel
//! finishes the op.

use std::ops::{Bound, RangeBounds};

/// A buffer the kernel may read from during an io_uring op.
///
/// # Safety
/// - `stable_ptr` must return the same pointer for the lifetime of `self`,
///   regardless of moves of `self`. (`Vec<u8>` and `Box<[u8]>` satisfy this
///   because their data lives on the heap; the pointer is stored *inside*
///   `Self` but points *outside* it.)
/// - The first `bytes_init` bytes starting at `stable_ptr` must be valid to
///   read for as long as `self` is held.
/// - `'static` is required so the buffer can be type-erased into
///   `Box<dyn Any>` for the cancellation keepalive path.
pub unsafe trait IoBuf: Unpin + Sized + 'static {
    /// Whether this buffer's memory needs to be kept alive past the user's
    /// future when an op is cancelled mid-flight.
    ///
    /// Set to `false` for buffers whose backing memory lives forever
    /// regardless of the value being dropped — e.g. `&'static [u8]`, whose
    /// bytes live in the binary's `.rodata`. The runtime then skips the
    /// type-erasure `Box<dyn Any>` allocation on cancel for these types.
    ///
    /// Defaults to `true` (the safe assumption): the runtime keeps the
    /// buffer alive until the cancelled CQE arrives.
    const NEEDS_KEEPALIVE: bool = true;

    /// Pointer to the first byte the kernel may read.
    fn stable_ptr(&self) -> *const u8;
    /// Number of initialized bytes the kernel may read from `stable_ptr`.
    fn bytes_init(&self) -> usize;

    /// Take ownership of `self` and return a sub-range as a [`Slice`] that
    /// itself implements [`IoBuf`]. Use to submit only part of a buffer:
    ///
    /// ```ignore
    /// let buf = vec![0u8; 4096];
    /// let (result, slice) = socket_send_owned(&sock, buf.slice(0..1024)).await;
    /// let buf = slice.into_inner();   // get the Vec back
    /// ```
    ///
    /// Panics if `range` falls outside `0..bytes_init()`.
    fn slice(self, range: impl RangeBounds<usize>) -> Slice<Self> {
        let begin = match range.start_bound() {
            Bound::Included(&n) => n,
            Bound::Excluded(&n) => n + 1,
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(&n) => n + 1,
            Bound::Excluded(&n) => n,
            Bound::Unbounded => self.bytes_init(),
        };
        let cap = self.bytes_init();
        assert!(
            begin <= end && end <= cap,
            "slice {begin}..{end} out of bounds (bytes_init = {cap})"
        );
        Slice {
            buf: self,
            begin,
            end,
        }
    }
}

// SAFETY: `Vec`'s data is heap-allocated; `as_ptr()` returns the same
// pointer until the `Vec` is dropped or reallocates. We never call any
// mutating method on `&self`.
unsafe impl IoBuf for Vec<u8> {
    fn stable_ptr(&self) -> *const u8 {
        self.as_ptr()
    }
    fn bytes_init(&self) -> usize {
        self.len()
    }
}

// SAFETY: `Box<[u8]>` has fixed-size heap storage; `as_ptr()` is stable for
// the lifetime of the Box.
unsafe impl IoBuf for Box<[u8]> {
    fn stable_ptr(&self) -> *const u8 {
        self.as_ptr()
    }
    fn bytes_init(&self) -> usize {
        self.len()
    }
}

// SAFETY: `&'static [u8]` points at memory with the `'static` lifetime —
// never freed. Pointer is trivially stable.
unsafe impl IoBuf for &'static [u8] {
    const NEEDS_KEEPALIVE: bool = false;
    fn stable_ptr(&self) -> *const u8 {
        (*self).as_ptr()
    }
    fn bytes_init(&self) -> usize {
        self.len()
    }
}

// SAFETY: same as `&'static [u8]`. Lets byte-string literals (`b"foo"`,
// of type `&'static [u8; N]`) be passed without an explicit `.as_slice()`.
unsafe impl<const N: usize> IoBuf for &'static [u8; N] {
    const NEEDS_KEEPALIVE: bool = false;
    fn stable_ptr(&self) -> *const u8 {
        self.as_ptr()
    }
    fn bytes_init(&self) -> usize {
        N
    }
}

/// A sub-range view over an [`IoBuf`]. Produced by [`IoBuf::slice`].
///
/// Itself implements `IoBuf` so it can be passed to any owned-buffer op.
/// Use [`Slice::into_inner`] to recover the underlying buffer.
pub struct Slice<B: IoBuf> {
    buf: B,
    begin: usize,
    end: usize,
}

impl<B: IoBuf> Slice<B> {
    /// Offset within the underlying buffer where this slice begins.
    pub fn begin(&self) -> usize {
        self.begin
    }
    /// Offset within the underlying buffer where this slice ends.
    pub fn end(&self) -> usize {
        self.end
    }
    /// Length of this slice in bytes (`end - begin`).
    pub fn len(&self) -> usize {
        self.end - self.begin
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Recover the underlying buffer, discarding the slice metadata.
    pub fn into_inner(self) -> B {
        self.buf
    }
}

// SAFETY: `stable_ptr` is `self.buf.stable_ptr().add(self.begin)`. The
// underlying `buf`'s pointer is stable by `IoBuf`'s contract; `begin` is
// fixed at construction. `end - begin` (the new `bytes_init`) is checked
// at construction to fit within `buf.bytes_init()`.
unsafe impl<B: IoBuf> IoBuf for Slice<B> {
    // Propagate from the inner buffer: slicing static memory still gives
    // static memory; slicing owned heap still gives owned heap.
    const NEEDS_KEEPALIVE: bool = B::NEEDS_KEEPALIVE;

    fn stable_ptr(&self) -> *const u8 {
        unsafe { self.buf.stable_ptr().add(self.begin) }
    }
    fn bytes_init(&self) -> usize {
        self.end - self.begin
    }
}
