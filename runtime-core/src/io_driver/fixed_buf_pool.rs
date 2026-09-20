//! Pre-registered ("fixed") send-buffer pool.
//!
//! Allocates a contiguous region of `count * size` bytes and registers it
//! with the io_uring instance on the current worker via
//! [`IODriver::register_fixed_buffers`] as one single region. Slot `i` covers
//! bytes `[i * size, (i + 1) * size)` within it, addressed by pointer/length
//! inside [`SLAB_BUF_INDEX`].
//!
//! One region, not one per slot: an SQE carries a single `buf_index`, so
//! per-slot registration would `EFAULT` any gather send spanning two slots.
//!
//! The pool is `!Send + !Sync` — registration belongs to one ring on one
//! thread. Take a slot with [`FixedSendBufPool::try_acquire`], which never
//! waits: it hands out a slot or reports the pool empty, and what to do then
//! is the caller's policy (fall back to a copying send, shed the request,
//! retry later). The slot is returned **automatically** when the [`PooledBuf`]
//! is dropped, so it cannot be leaked by forgetting to release.
//!
//! # Ownership / lifetime contract
//!
//! A [`PooledBuf`] references its pool through a raw pointer rather than an
//! owning handle, so acquiring and releasing a slot costs nothing beyond a
//! pointer copy. The caller upholds one invariant in exchange:
//!
//! > The pool must outlive every `PooledBuf` it hands out — including any
//! > buffer parked inside a send future's cancel keepalive — and, once the
//! > first slot has been acquired, the pool value must not move.
//!
//! Both are satisfied by keeping the pool behind an `Rc`/`Box` that lives for
//! the worker's duration (as the examples and tests do): the heap address is
//! stable, and the pool outlives all in-flight sends. Dropping the pool while
//! a send is still in flight is a use-after-free.

use std::cell::RefCell;
use std::io;
use std::ptr::NonNull;

use crate::io_driver::{IO_DRIVER, operations::FixedBufIndex, operations::IoBuf};

/// The pool owns the whole registration (`register_buffers` replaces the set)
/// and registers one region, so the slab is always index 0.
pub const SLAB_BUF_INDEX: FixedBufIndex = FixedBufIndex::new(0);

pub struct FixedSendBufPool {
    /// Free-list, used as a stack: release pushes to the back, `try_acquire`
    /// pops from the back. LIFO keeps the just-released slot hot in cache.
    free: RefCell<Vec<u16>>,
    /// Owns the backing memory. `Box<[u8]>` is fixed-size by construction, so
    /// the heap allocation never moves — the iovecs the kernel registered stay
    /// valid until `Drop` calls unregister.
    #[allow(dead_code)]
    storage: Box<[u8]>,
    /// Cached `storage.as_mut_ptr()`. Stable for the pool's lifetime.
    base_ptr: *mut u8,
    count: u16,
    size: u32,
}

impl FixedSendBufPool {
    /// Allocate `count` buffers of `size` bytes each and register them with
    /// the io_uring instance on the current thread.
    pub fn new(count: u16, size: u32) -> io::Result<Self> {
        assert!(count > 0, "FixedSendBufPool: count must be > 0");
        assert!(size > 0, "FixedSendBufPool: size must be > 0");

        let total = count as usize * size as usize;
        let mut storage: Box<[u8]> = vec![0u8; total].into_boxed_slice();
        let base_ptr = storage.as_mut_ptr();

        // The whole slab as one region; slots are offsets inside it.
        let iovec = libc::iovec {
            iov_base: base_ptr as *mut _,
            iov_len: total,
        };

        // SAFETY: `storage` is owned by `Self` as a `Box<[u8]>`; the heap
        // allocation cannot move and the iovec stays valid until `Drop` calls
        // unregister.
        IO_DRIVER.with(|d| unsafe { d.register_fixed_buffers(std::slice::from_ref(&iovec)) })?;

        let free: Vec<u16> = (0..count).rev().collect();
        Ok(Self {
            free: RefCell::new(free),
            storage,
            base_ptr,
            count,
            size,
        })
    }

    /// Take a free slot if one is available right now, `None` otherwise.
    ///
    /// The pool never parks a task: on a pool shared by every connection on
    /// the thread, waiting makes one connection's stall everyone's stall, and
    /// only the caller knows whether to fall back to a copying send, shed the
    /// work or retry later.
    pub fn try_acquire(&self) -> Option<PooledBuf> {
        let slot = self.free.borrow_mut().pop()?;
        Some(PooledBuf {
            pool: NonNull::from(self),
            slot_base: self.slot_ptr(slot),
            cap: self.size,
            slot,
            write_pos: 0,
        })
    }

    /// Number of buffers in the pool.
    pub fn capacity(&self) -> usize {
        self.count as usize
    }

    /// Slots free right now — `capacity()` minus the live `PooledBuf`s.
    ///
    /// Exact, not an estimate: the pool is single-threaded, so the count can
    /// only change at an `.await` point in the caller's own code. Sizing a
    /// batch off it and taking that many slots without awaiting in between is
    /// sound.
    pub fn available(&self) -> usize {
        self.free.borrow().len()
    }

    /// No slot is free; the next [`Self::try_acquire`] returns `None`.
    pub fn is_exhausted(&self) -> bool {
        self.free.borrow().is_empty()
    }

    /// Size of each buffer in bytes.
    pub fn buffer_size(&self) -> usize {
        self.size as usize
    }

    fn slot_ptr(&self, slot: u16) -> *mut u8 {
        unsafe { self.base_ptr.add(slot as usize * self.size as usize) }
    }

    /// Hand `slot` back to the free-list.
    fn release(&self, slot: u16) {
        self.free.borrow_mut().push(slot);
    }
}

impl Drop for FixedSendBufPool {
    fn drop(&mut self) {
        // Tell the kernel to drop its references before `storage` is freed.
        // Per the lifetime contract no `PooledBuf` (hence no in-flight op) is
        // referencing the registration at this point. Best-effort: if
        // unregister fails (e.g. ring already gone) there is nothing useful to
        // do here.
        let _ = IO_DRIVER.with(|d| d.unregister_fixed_buffers());
    }
}

/// A buffer slot borrowed from a [`FixedSendBufPool`].
///
/// Acts as a writable cursor: `Write` advances `len()` and refuses bytes past
/// the slot's `capacity`. `as_slice` exposes the written prefix to hand to the
/// kernel via the slot's `buf_index`.
///
/// The slot is returned to its pool automatically when the `PooledBuf` is
/// dropped. A send future (`socket_write_fixed`/`_zc`) takes ownership of the
/// `PooledBuf` and hands it back on completion; if such a future is dropped
/// mid-flight it stashes the `PooledBuf` until the kernel signals it is done,
/// so the slot is never reused while still being read.
///
/// Also an [`IoBuf`], so it can go to any owned-buffer op — that is the
/// fallback path when the pool is drained on the other side of the connection
/// and the payload has to move through a plain `socket_send`. Those ops submit
/// a plain pointer/length: correct, but the kernel pins the pages per request
/// instead of using the registration. Use `socket_write_fixed`/`_zc` to keep
/// the point of the pool.
///
/// Holds a raw pointer to its pool; see the module-level lifetime contract.
pub struct PooledBuf {
    pool: NonNull<FixedSendBufPool>,
    /// Cached `base_ptr + index * size`. Avoids re-deriving the slot pointer
    /// on every `Write` call.
    slot_base: *mut u8,
    cap: u32,
    /// Free-list slot, not a kernel buffer index — the slab is one region.
    slot: u16,
    write_pos: u32,
}

impl PooledBuf {
    /// Shared by every slot; `ptr()`/`len()` select the slot inside it.
    pub fn buf_index(&self) -> FixedBufIndex {
        SLAB_BUF_INDEX
    }

    /// Pointer to the start of the slot (offset 0, not the write cursor).
    pub fn ptr(&self) -> *const u8 {
        self.slot_base
    }

    /// Number of bytes written so far.
    pub fn len(&self) -> usize {
        self.write_pos as usize
    }

    pub fn is_empty(&self) -> bool {
        self.write_pos == 0
    }

    /// Full slot capacity in bytes.
    pub fn capacity(&self) -> usize {
        self.cap as usize
    }

    /// View the bytes written so far.
    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.slot_base, self.write_pos as usize) }
    }

    /// Mutable view of the unwritten tail (`[write_pos..capacity]`).
    pub fn remaining_mut(&mut self) -> &mut [u8] {
        let written = self.write_pos as usize;
        unsafe {
            std::slice::from_raw_parts_mut(self.slot_base.add(written), self.cap as usize - written)
        }
    }

    /// Commit `n` bytes written through [`Self::remaining_mut`].
    ///
    /// The companion `remaining_mut` needs to be useful: `io::Write` is the
    /// only other way to move the cursor and it copies, so a caller that fills
    /// the slot in place — serializing a response body straight into it, then
    /// writing the frame header in front once the length is known — has no way
    /// to say how far it got.
    ///
    /// Safe rather than `unsafe`: the slot is zero-initialized at pool
    /// construction, so committing bytes the caller never wrote transmits
    /// stale zeros, not uninitialized memory.
    ///
    /// Panics if `n` runs past the slot — the kernel would otherwise be handed
    /// a length beyond the registered slot.
    pub fn advance(&mut self, n: usize) {
        let free = self.cap as usize - self.write_pos as usize;
        assert!(n <= free, "advance({n}) past the slot's {free} free bytes");
        self.write_pos += n as u32;
    }

    /// Rewind the write cursor to 0 for reuse without giving the slot back.
    pub fn reset(&mut self) {
        self.write_pos = 0;
    }
}

// SAFETY: `slot_base` points into the pool's `Box<[u8]>`, outside `Self`, so
// it survives moves of the `PooledBuf` and stays valid as long as the buffer
// is held (the pool outlives it, per the module contract). The first
// `write_pos` bytes are written — and the whole slab is zeroed at construction,
// so even bytes committed through `advance` without being written are init.
// `NEEDS_KEEPALIVE` stays `true`: on a cancelled op the runtime boxes the
// `PooledBuf` until the terminal CQE, which is exactly when the slot may go
// back to the free-list.
unsafe impl IoBuf for PooledBuf {
    fn stable_ptr(&self) -> *const u8 {
        self.slot_base
    }
    fn bytes_init(&self) -> usize {
        self.write_pos as usize
    }
}

impl io::Write for PooledBuf {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let dst = self.remaining_mut();
        let n = dst.len().min(buf.len());
        dst[..n].copy_from_slice(&buf[..n]);
        self.write_pos += n as u32;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Drop for PooledBuf {
    fn drop(&mut self) {
        // SAFETY: the pool outlives every PooledBuf it issued (module contract).
        unsafe { self.pool.as_ref().release(self.slot) };
    }
}
