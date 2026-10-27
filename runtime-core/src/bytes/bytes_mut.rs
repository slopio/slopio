use std::fmt;
use std::mem::{self, MaybeUninit};
use std::ops::{Deref, DerefMut};
use std::ptr::{self, NonNull};

use super::Bytes;
use super::origin::Origin;
use crate::io_driver::operations::{IoBuf, IoBufMut};

const MIN_GROWTH: usize = 64;

/// A growable buffer that owns `[0, capacity)` of its memory exclusively,
/// though split-off parts may share the same block. See the [module docs](super).
pub struct BytesMut {
    ptr: NonNull<u8>,
    len: usize,
    cap: usize,
    origin: Origin,
}

impl BytesMut {
    /// An empty buffer; does not allocate.
    pub const fn new() -> Self {
        Self {
            ptr: NonNull::dangling(),
            len: 0,
            cap: 0,
            origin: Origin::STATIC,
        }
    }

    pub fn with_capacity(cap: usize) -> Self {
        if cap == 0 {
            return Self::new();
        }
        let (origin, ptr) = Origin::alloc(cap);
        Self {
            ptr,
            len: 0,
            cap,
            origin,
        }
    }

    /// `len` zero bytes.
    pub fn zeroed(len: usize) -> Self {
        let mut buf = Self::with_capacity(len);
        buf.put_bytes(0, len);
        buf
    }

    /// # Safety
    /// `[ptr, ptr + cap)` is writable, belongs to this view alone and lies in
    /// memory `origin` keeps alive; `[ptr, ptr + len)` is initialized; the
    /// caller hands over one reference on `origin`.
    pub(super) unsafe fn from_raw_parts(
        ptr: NonNull<u8>,
        len: usize,
        cap: usize,
        origin: Origin,
    ) -> Self {
        Self {
            ptr,
            len,
            cap,
            origin,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Bytes this buffer can hold without reallocating.
    pub fn capacity(&self) -> usize {
        self.cap
    }

    /// Make room for at least `additional` more bytes.
    ///
    /// When no other view shares the block, this first takes back space freed
    /// by dropped split-off parts, and moves the data to the front of the block
    /// if that is enough; otherwise it allocates and copies.
    #[inline]
    pub fn reserve(&mut self, additional: usize) {
        if additional > self.cap - self.len {
            self.reserve_slow(additional);
        }
    }

    #[inline(never)]
    fn reserve_slow(&mut self, additional: usize) {
        let needed = self.len.checked_add(additional).expect("capacity overflow");
        if let Some((start, total)) = self.origin.unique_heap() {
            let offset = unsafe { self.ptr.offset_from_unsigned(start) };
            self.cap = total - offset;
            if self.cap >= needed {
                return;
            }
            // Moving costs no more than the copy a growth would make.
            if total >= needed && offset >= self.len {
                unsafe { ptr::copy_nonoverlapping(self.ptr.as_ptr(), start.as_ptr(), self.len) };
                self.ptr = start;
                self.cap = total;
                return;
            }
            if offset == 0 {
                let cap = needed.max(total * 2);
                let (origin, ptr) = unsafe { self.origin.grow(cap) };
                self.origin = origin;
                self.ptr = ptr;
                self.cap = cap;
                return;
            }
        }
        let cap = needed.max(self.cap * 2).max(MIN_GROWTH);
        let (origin, ptr) = Origin::alloc(cap);
        unsafe { ptr::copy_nonoverlapping(self.ptr.as_ptr(), ptr.as_ptr(), self.len) };
        mem::replace(&mut self.origin, origin).release();
        self.ptr = ptr;
        self.cap = cap;
    }

    pub fn extend_from_slice(&mut self, data: &[u8]) {
        self.reserve(data.len());
        unsafe {
            ptr::copy_nonoverlapping(data.as_ptr(), self.ptr.as_ptr().add(self.len), data.len());
        }
        self.len += data.len();
    }

    /// Append `n` copies of `byte`.
    pub fn put_bytes(&mut self, byte: u8, n: usize) {
        self.reserve(n);
        unsafe { self.ptr.as_ptr().add(self.len).write_bytes(byte, n) };
        self.len += n;
    }

    /// Grow with `byte` or truncate to `len`.
    pub fn resize(&mut self, len: usize, byte: u8) {
        if len > self.len {
            self.put_bytes(byte, len - self.len);
        } else {
            self.len = len;
        }
    }

    /// Keep the first `len` bytes; the capacity stays.
    pub fn truncate(&mut self, len: usize) {
        self.len = self.len.min(len);
    }

    /// Empty the buffer; the capacity stays.
    pub fn clear(&mut self) {
        self.len = 0;
    }

    /// # Safety
    /// `len <= capacity()` and the first `len` bytes are initialized.
    pub unsafe fn set_len(&mut self, len: usize) {
        debug_assert!(len <= self.cap);
        self.len = len;
    }

    /// The writable tail `[len, capacity)`; commit with [`set_len`](Self::set_len).
    pub fn spare_capacity_mut(&mut self) -> &mut [MaybeUninit<u8>] {
        unsafe {
            std::slice::from_raw_parts_mut(
                self.ptr.as_ptr().add(self.len).cast(),
                self.cap - self.len,
            )
        }
    }

    /// Drop the first `n` bytes; their space is reclaimed by a later `reserve`.
    ///
    /// Panics if `n > len`.
    pub fn advance(&mut self, n: usize) {
        assert!(
            n <= self.len,
            "advance({n}) past the end (len = {})",
            self.len
        );
        self.ptr = unsafe { self.ptr.add(n) };
        self.len -= n;
        self.cap -= n;
    }

    /// Split off and return `[0, at)`; `self` keeps `[at, capacity)`.
    ///
    /// Panics if `at > len`.
    pub fn split_to(&mut self, at: usize) -> Self {
        assert!(
            at <= self.len,
            "split_to({at}) past the end (len = {})",
            self.len
        );
        self.origin.retain();
        let head = Self {
            ptr: self.ptr,
            len: at,
            cap: at,
            origin: self.origin,
        };
        self.ptr = unsafe { self.ptr.add(at) };
        self.len -= at;
        self.cap -= at;
        head
    }

    /// Split off and return `[at, capacity)`; `self` keeps `[0, at)`.
    ///
    /// Panics if `at > capacity`.
    pub fn split_off(&mut self, at: usize) -> Self {
        assert!(
            at <= self.cap,
            "split_off({at}) past the capacity ({})",
            self.cap
        );
        self.origin.retain();
        let tail = Self {
            ptr: unsafe { self.ptr.add(at) },
            len: self.len.saturating_sub(at),
            cap: self.cap - at,
            origin: self.origin,
        };
        self.len = self.len.min(at);
        self.cap = at;
        tail
    }

    /// Split off and return the filled part; `self` keeps the spare capacity.
    pub fn split(&mut self) -> Self {
        self.split_to(self.len)
    }

    /// Append `other`: without copying if it was split off right after `self`,
    /// by copying otherwise.
    pub fn unsplit(&mut self, other: Self) {
        if self.is_empty() {
            *self = other;
        } else if self.origin == other.origin
            && self.len == self.cap
            && unsafe { self.ptr.add(self.len) } == other.ptr
        {
            self.len += other.len;
            self.cap += other.cap;
        } else {
            self.extend_from_slice(&other);
        }
    }

    /// Turn the filled part into a `Bytes` without copying.
    pub fn freeze(self) -> Bytes {
        if self.is_empty() {
            return Bytes::new();
        }
        let this = mem::ManuallyDrop::new(self);
        Bytes {
            ptr: this.ptr,
            len: this.len,
            origin: this.origin,
        }
    }

    /// Append `value`'s low `nbytes` bytes, big-endian.
    ///
    /// Panics if `nbytes > 8`.
    pub fn put_uint(&mut self, value: u64, nbytes: usize) {
        assert!(nbytes <= 8, "put_uint of {nbytes} bytes");
        self.extend_from_slice(&value.to_be_bytes()[8 - nbytes..]);
    }

    pub fn put_slice(&mut self, data: &[u8]) {
        self.extend_from_slice(data);
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

    super::impls::get_methods!();
    super::impls::put_methods!();
}

impl Drop for BytesMut {
    fn drop(&mut self) {
        self.origin.release();
    }
}

impl Default for BytesMut {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for BytesMut {
    /// A copy into a new block.
    fn clone(&self) -> Self {
        Self::from(&self[..])
    }
}

impl Deref for BytesMut {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
}

impl DerefMut for BytesMut {
    fn deref_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }
}

impl AsMut<[u8]> for BytesMut {
    fn as_mut(&mut self) -> &mut [u8] {
        self
    }
}

impl From<&[u8]> for BytesMut {
    fn from(data: &[u8]) -> Self {
        let mut buf = Self::with_capacity(data.len());
        buf.extend_from_slice(data);
        buf
    }
}

impl From<&str> for BytesMut {
    fn from(data: &str) -> Self {
        Self::from(data.as_bytes())
    }
}

impl fmt::Write for BytesMut {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        self.extend_from_slice(s.as_bytes());
        Ok(())
    }
}

impl<'a> Extend<&'a u8> for BytesMut {
    fn extend<I: IntoIterator<Item = &'a u8>>(&mut self, iter: I) {
        let iter = iter.into_iter();
        self.reserve(iter.size_hint().0);
        for &byte in iter {
            self.put_u8(byte);
        }
    }
}

// SAFETY: the block does not move with `self`, and `reserve` needs `&mut self`,
// which the op holds for as long as the kernel reads.
unsafe impl IoBuf for BytesMut {
    fn stable_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }
    fn bytes_init(&self) -> usize {
        self.len
    }
}

/// The kernel appends into the spare capacity: `reserve` before a read, since
/// a full buffer reads 0 bytes.
// SAFETY: `[len, capacity)` is writable and belongs to this view alone.
unsafe impl IoBufMut for BytesMut {
    fn stable_mut_ptr(&mut self) -> *mut u8 {
        unsafe { self.ptr.as_ptr().add(self.len) }
    }
    fn bytes_total(&self) -> usize {
        self.cap - self.len
    }
    unsafe fn set_init(&mut self, n: usize) {
        self.len += n;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grows_from_nothing() {
        let mut buf = BytesMut::new();
        assert_eq!(buf.capacity(), 0);
        buf.extend_from_slice(b"abc");
        assert!(buf.capacity() >= MIN_GROWTH);
        for i in 0..1000u32 {
            buf.put_u32(i);
        }
        assert_eq!(buf.len(), 3 + 4000);
        assert_eq!(&buf[..3], b"abc");
        assert_eq!(&buf[buf.len() - 4..], 999u32.to_be_bytes());
    }

    #[test]
    fn split_then_freeze_shares_the_block() {
        let mut buf = BytesMut::with_capacity(64);
        buf.extend_from_slice(b"frame-1");
        let first = buf.split().freeze();
        buf.extend_from_slice(b"frame-2");
        let second = buf.split().freeze();
        assert_eq!(
            (first.clone(), second.clone()),
            (
                Bytes::from_static(b"frame-1"),
                Bytes::from_static(b"frame-2")
            )
        );
        assert_eq!(second.as_ptr(), unsafe { first.as_ptr().add(7) });
        assert_eq!(buf.capacity(), 64 - 14);
    }

    #[test]
    fn reserve_takes_back_space_once_split_parts_are_gone() {
        let mut buf = BytesMut::with_capacity(64);
        buf.extend_from_slice(&[1; 40]);
        let frame = buf.split_to(32);
        let block = frame.as_ptr();
        drop(frame);
        // 8 bytes left at offset 32: moving them to the front makes room.
        buf.reserve(50);
        assert_eq!(buf.as_ptr(), block);
        assert_eq!(buf.capacity(), 64);
        assert_eq!(buf, [1; 8]);
    }

    #[test]
    fn reserve_copies_while_another_view_holds_the_block() {
        let mut buf = BytesMut::with_capacity(16);
        buf.extend_from_slice(b"0123456789");
        let kept = buf.split_to(4).freeze();
        buf.reserve(100);
        assert!(buf.capacity() >= 106);
        assert_eq!(buf, "456789");
        assert_eq!(kept, "0123");
    }

    #[test]
    fn reserve_reallocates_a_sole_block_from_its_start() {
        let mut buf = BytesMut::with_capacity(8);
        buf.extend_from_slice(b"abcdefgh");
        buf.reserve(1);
        assert_eq!(buf.capacity(), 16);
        assert_eq!(buf, "abcdefgh");
    }

    #[test]
    fn unsplit_rejoins_neighbours_without_copying() {
        let mut buf = BytesMut::with_capacity(32);
        buf.extend_from_slice(b"hello world");
        let start = buf.as_ptr();
        let world = buf.split_off(5);
        assert_eq!(buf.capacity(), 5);
        buf.unsplit(world);
        assert_eq!(buf, "hello world");
        assert_eq!(buf.as_ptr(), start);
        assert_eq!(buf.capacity(), 32);
    }

    #[test]
    fn unsplit_copies_strangers() {
        let mut a = BytesMut::from("abc");
        a.unsplit(BytesMut::from("def"));
        assert_eq!(a, "abcdef");
    }

    #[test]
    fn advance_then_truncate_and_resize() {
        let mut buf = BytesMut::from("xxheader");
        buf.advance(2);
        assert_eq!(buf, "header");
        buf.truncate(4);
        buf.resize(6, b'!');
        assert_eq!(buf, "head!!");
    }

    #[test]
    fn spare_capacity_then_set_len() {
        let mut buf = BytesMut::with_capacity(8);
        buf.spare_capacity_mut()[..3].copy_from_slice(&[MaybeUninit::new(7); 3]);
        unsafe { buf.set_len(3) };
        assert_eq!(buf, [7, 7, 7]);
    }

    #[test]
    fn as_a_read_target_it_appends() {
        let mut buf = BytesMut::from("ab");
        buf.reserve(4);
        let free = buf.bytes_total();
        unsafe {
            buf.stable_mut_ptr()
                .copy_from_nonoverlapping(b"cd".as_ptr(), 2);
            buf.set_init(2);
        }
        assert_eq!(buf, "abcd");
        assert_eq!(buf.bytes_total(), free - 2);
    }

    #[test]
    fn fmt_write_appends_text() {
        use std::fmt::Write;
        let mut buf = BytesMut::new();
        write!(buf, "content-length: {}\r\n", 42).unwrap();
        assert_eq!(buf, "content-length: 42\r\n");
    }
}
