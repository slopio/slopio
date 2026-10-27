//! Byte buffers for one worker thread.
//!
//! [`Bytes`] is an immutable view: cloning and slicing it shares the memory
//! instead of copying it. [`BytesMut`] is a growable buffer you write into,
//! then [`freeze`](BytesMut::freeze) into a `Bytes` without copying.
//!
//! A `Bytes` can sit on three kinds of memory:
//! - static data, from [`Bytes::from_static`];
//! - the heap, from [`Bytes::copy_from_slice`] or a frozen `BytesMut`;
//! - a buffer-ring slot, from a received [`BorrowedBuffer`]. The slot goes back
//!   to its ring when the last view on it is dropped, so a small slice kept
//!   for long holds a whole slot: copy out what you keep
//!   ([`Bytes::copy_from_slice`]), and watch [`BufRingSpec::usage`].
//!
//! Neither type is `Send`: references are counted without atomics, and ring
//! slots belong to the worker that received them. To hand data to another
//! worker, copy it out (`to_vec()`).
//!
//! ```compile_fail
//! fn send<T: Send>() {}
//! send::<runtime_core::bytes::Bytes>();
//! ```
//!
//! [`BufRingSpec::usage`]: crate::io_driver::BufRingSpec::usage

mod bytes_mut;
mod impls;
mod origin;

use std::mem;
use std::ops::{Bound, Deref, RangeBounds};
use std::ptr::NonNull;

pub use bytes_mut::BytesMut;
use origin::Origin;

use crate::io_driver::{BorrowedBuffer, BufRingSpec};

/// An immutable, cheaply cloned view into shared bytes. See the [module docs](self).
pub struct Bytes {
    ptr: NonNull<u8>,
    len: usize,
    origin: Origin,
}

impl Bytes {
    /// An empty `Bytes`; does not allocate.
    pub const fn new() -> Self {
        Self {
            ptr: NonNull::dangling(),
            len: 0,
            origin: Origin::STATIC,
        }
    }

    /// A view on static data; does not allocate or count references.
    pub const fn from_static(data: &'static [u8]) -> Self {
        Self {
            ptr: unsafe { NonNull::new_unchecked(data.as_ptr().cast_mut()) },
            len: data.len(),
            origin: Origin::STATIC,
        }
    }

    /// Copy `data` into a new heap block.
    pub fn copy_from_slice(data: &[u8]) -> Self {
        BytesMut::from(data).freeze()
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// A view on `range` of this one, sharing its memory.
    ///
    /// Panics if `range` is out of bounds.
    pub fn slice(&self, range: impl RangeBounds<usize>) -> Self {
        let begin = match range.start_bound() {
            Bound::Included(&n) => n,
            Bound::Excluded(&n) => n + 1,
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(&n) => n + 1,
            Bound::Excluded(&n) => n,
            Bound::Unbounded => self.len,
        };
        assert!(
            begin <= end && end <= self.len,
            "slice {begin}..{end} out of bounds (len = {})",
            self.len
        );
        if begin == end {
            return Self::new();
        }
        self.origin.retain();
        Self {
            ptr: unsafe { self.ptr.add(begin) },
            len: end - begin,
            origin: self.origin,
        }
    }

    /// The view that covers `subset`, a slice borrowed from this `Bytes`.
    ///
    /// Panics if `subset` is not inside this view.
    pub fn slice_ref(&self, subset: &[u8]) -> Self {
        if subset.is_empty() {
            return Self::new();
        }
        let start = self.ptr.as_ptr() as usize;
        let sub = subset.as_ptr() as usize;
        assert!(
            sub >= start && sub + subset.len() <= start + self.len,
            "slice_ref: subset is not inside this Bytes"
        );
        let offset = sub - start;
        self.slice(offset..offset + subset.len())
    }

    /// Split off and return `[0, at)`; `self` keeps `[at, len)`.
    ///
    /// Panics if `at > len`.
    pub fn split_to(&mut self, at: usize) -> Self {
        assert!(
            at <= self.len,
            "split_to({at}) past the end (len = {})",
            self.len
        );
        if at == self.len {
            return mem::take(self);
        }
        if at == 0 {
            return Self::new();
        }
        self.origin.retain();
        let head = Self {
            ptr: self.ptr,
            len: at,
            origin: self.origin,
        };
        self.ptr = unsafe { self.ptr.add(at) };
        self.len -= at;
        head
    }

    /// Split off and return `[at, len)`; `self` keeps `[0, at)`.
    ///
    /// Panics if `at > len`.
    pub fn split_off(&mut self, at: usize) -> Self {
        assert!(
            at <= self.len,
            "split_off({at}) past the end (len = {})",
            self.len
        );
        if at == 0 {
            return mem::take(self);
        }
        if at == self.len {
            return Self::new();
        }
        self.origin.retain();
        let tail = Self {
            ptr: unsafe { self.ptr.add(at) },
            len: self.len - at,
            origin: self.origin,
        };
        self.len = at;
        tail
    }

    /// Keep the first `len` bytes. Truncating to 0 lets go of the memory.
    pub fn truncate(&mut self, len: usize) {
        if len == 0 {
            self.clear();
        } else if len < self.len {
            self.len = len;
        }
    }

    /// Empty the view and let go of its memory.
    pub fn clear(&mut self) {
        *self = Self::new();
    }

    /// Drop the first `n` bytes. Consuming everything lets go of the memory.
    ///
    /// Panics if `n > len`.
    pub fn advance(&mut self, n: usize) {
        assert!(
            n <= self.len,
            "advance({n}) past the end (len = {})",
            self.len
        );
        if n == self.len {
            self.clear();
        } else {
            self.ptr = unsafe { self.ptr.add(n) };
            self.len -= n;
        }
    }

    /// No other view shares this memory, so [`try_into_mut`](Self::try_into_mut) would succeed.
    pub fn is_unique(&self) -> bool {
        self.origin.is_unique()
    }

    /// Turn the only view on some memory into a `BytesMut` without copying,
    /// e.g. to unmask or decrypt in place. Gives `self` back if it is shared
    /// or static.
    ///
    /// The result can grow into what is left of a heap block after this view;
    /// a ring slot has no room to grow in place.
    pub fn try_into_mut(self) -> Result<BytesMut, Self> {
        if self.is_empty() {
            return Ok(BytesMut::new());
        }
        if !self.origin.is_unique() {
            return Err(self);
        }
        let cap = match self.origin.unique_heap() {
            Some((start, cap)) => cap - unsafe { self.ptr.offset_from_unsigned(start) },
            None => self.len,
        };
        let this = mem::ManuallyDrop::new(self);
        Ok(unsafe { BytesMut::from_raw_parts(this.ptr, this.len, cap, this.origin) })
    }

    fn take_array<const N: usize>(&mut self) -> [u8; N] {
        assert!(
            N <= self.len,
            "read of {N} bytes past the end (len = {})",
            self.len
        );
        let array = unsafe { self.ptr.cast::<[u8; N]>().read_unaligned() };
        self.advance(N);
        array
    }

    impls::get_methods!();
}

impl Clone for Bytes {
    fn clone(&self) -> Self {
        self.origin.retain();
        Self {
            ptr: self.ptr,
            len: self.len,
            origin: self.origin,
        }
    }
}

impl Drop for Bytes {
    fn drop(&mut self) {
        self.origin.release();
    }
}

impl Default for Bytes {
    fn default() -> Self {
        Self::new()
    }
}

impl Deref for Bytes {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
}

impl From<&'static [u8]> for Bytes {
    fn from(data: &'static [u8]) -> Self {
        Self::from_static(data)
    }
}

impl From<&'static str> for Bytes {
    fn from(data: &'static str) -> Self {
        Self::from_static(data.as_bytes())
    }
}

impl From<BytesMut> for Bytes {
    fn from(buf: BytesMut) -> Self {
        buf.freeze()
    }
}

/// Keeps the slot out of its ring until the last view on it is dropped.
impl<S: BufRingSpec> From<BorrowedBuffer<S>> for Bytes {
    fn from(buf: BorrowedBuffer<S>) -> Self {
        let (ptr, len, bid) = buf.into_raw_parts();
        if len == 0 {
            S::recycle_bid(bid);
            return Self::new();
        }
        Self {
            ptr,
            len,
            origin: Origin::ring::<S>(bid),
        }
    }
}

// SAFETY: the memory is heap, static or a ring slot, none of which moves with
// `self`, and stays valid while `self` holds its reference.
unsafe impl crate::io_driver::operations::IoBuf for Bytes {
    fn stable_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }
    fn bytes_init(&self) -> usize {
        self.len
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clones_and_slices_share_one_block() {
        let a = Bytes::copy_from_slice(b"hello world");
        let b = a.clone();
        let world = a.slice(6..);
        assert_eq!(world, "world");
        assert_eq!(world.as_ptr(), unsafe { a.as_ptr().add(6) });
        assert!(!a.is_unique());
        drop((a, b));
        assert!(world.is_unique());
    }

    #[test]
    fn split_to_and_split_off_cover_the_whole_view() {
        let mut b = Bytes::copy_from_slice(b"head:body:tail");
        let head = b.split_to(5);
        let tail = b.split_off(5);
        assert_eq!(
            (&head[..], &b[..], &tail[..]),
            (&b"head:"[..], &b"body:"[..], &b"tail"[..])
        );
        assert_eq!(b.split_to(5), "body:");
        assert!(b.is_empty());
    }

    #[test]
    fn empty_results_hold_no_memory() {
        let b = Bytes::copy_from_slice(b"abc");
        let empty = b.slice(1..1);
        assert!(b.is_unique());
        drop(empty);
        let mut c = b.clone();
        c.advance(3);
        assert!(b.is_unique(), "a fully consumed view lets go");
    }

    #[test]
    fn slice_ref_maps_a_borrow_back_to_a_view() {
        let b = Bytes::copy_from_slice(b"GET /index.html HTTP/1.1");
        let path = b.split(|&c| c == b' ').nth(1).unwrap();
        let path = b.slice_ref(path);
        assert_eq!(path, "/index.html");
        assert!(!b.is_unique());
    }

    #[test]
    #[should_panic(expected = "not inside")]
    fn slice_ref_rejects_foreign_memory() {
        let b = Bytes::copy_from_slice(b"abc");
        b.slice_ref(b"abc");
    }

    #[test]
    fn static_bytes_are_never_unique() {
        let b = Bytes::from_static(b"static");
        assert!(!b.is_unique());
        let b = b.try_into_mut().unwrap_err();
        assert_eq!(b, "static");
    }

    #[test]
    fn try_into_mut_gives_the_rest_of_the_block() {
        let mut buf = BytesMut::with_capacity(100);
        buf.extend_from_slice(b"0123456789");
        let mut frozen = buf.freeze();
        let head = frozen.split_to(4);
        assert!(frozen.clone().try_into_mut().is_err());
        drop(head);
        let mut m = frozen.try_into_mut().unwrap();
        assert_eq!(m, "456789");
        assert_eq!(m.capacity(), 96);
        m[0] = b'x';
        assert_eq!(m, "x56789");
    }

    #[test]
    fn cursor_reads_in_both_byte_orders() {
        let mut out = BytesMut::new();
        out.put_u8(0xab);
        out.put_u16(0x0102);
        out.put_u32_le(0x0304_0506);
        out.put_uint(0x07_0809, 3);
        out.put_i64(-2);
        out.put_f32(1.5);
        let mut b = out.freeze();
        assert_eq!(b.get_u8(), 0xab);
        assert_eq!(b.get_u16(), 0x0102);
        assert_eq!(b.get_u32_le(), 0x0304_0506);
        assert_eq!(b.get_uint(3), 0x07_0809);
        assert_eq!(b.get_i64(), -2);
        assert_eq!(b.get_f32(), 1.5);
        assert!(b.is_empty());
    }

    #[test]
    #[should_panic(expected = "past the end")]
    fn a_short_read_panics() {
        Bytes::from_static(b"abc").get_u32();
    }

    #[test]
    fn debug_escapes_like_a_byte_string() {
        let b = Bytes::from_static(b"a\"\n\x00");
        assert_eq!(format!("{b:?}"), r#"b"a\"\n\x00""#);
    }

    #[test]
    fn works_as_a_map_key_looked_up_by_slice() {
        let mut map = std::collections::HashMap::new();
        map.insert(Bytes::copy_from_slice(b"content-type"), 1);
        assert_eq!(map.get(&b"content-type"[..]), Some(&1));
    }
}
