//! Pre-registered ("fixed") send-buffer pool.
//!
//! Allocates a contiguous region of `count * size` bytes and registers it
//! with the io_uring instance on the current worker via
//! [`IODriver::register_fixed_buffers`]. Slot `i` covers bytes
//! `[i * size, (i + 1) * size)` and corresponds to [`FixedBufIndex::new(i)`].
//!
//! The pool is `!Send + !Sync` — registration belongs to one ring on one
//! thread. Acquire with [`FixedSendBufPool::acquire`] (async, waits when
//! the pool is empty); release with [`FixedSendBufPool::release`] or
//! [`FixedSendBufPool::release_index`] (manual — there is no auto-release
//! on `PooledBuf` drop).

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::future::Future;
use std::io;
use std::marker::PhantomData;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use crate::io_driver::{IO_DRIVER, operations::FixedBufIndex};

pub struct FixedSendBufPool {
    inner: RefCell<PoolInner>,
    /// Owns the backing memory. `Box<[u8]>` is fixed-size by construction,
    /// so the heap allocation never moves — the iovecs the kernel registered
    /// stay valid until `Drop` calls unregister.
    #[allow(dead_code)]
    storage: Box<[u8]>,
    /// Cached `storage.as_mut_ptr()`. Stable for the pool's lifetime.
    base_ptr: *mut u8,
    count: u16,
    size: u32,
}

struct PoolInner {
    /// Free-list, used as a stack: `release` pushes to the back, `acquire`
    /// pops from the back. LIFO keeps the just-released slot hot in cache.
    free: Vec<u16>,
    /// Cells let `Acquire::drop` mark a queued waiter as cancelled without
    /// scanning the deque: release pops slots and skips cells whose waker
    /// has been cleared.
    waiters: VecDeque<Rc<Cell<Option<Waker>>>>,
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

        let iovecs: Vec<libc::iovec> = (0..count as usize)
            .map(|i| libc::iovec {
                iov_base: unsafe { base_ptr.add(i * size as usize) } as *mut _,
                iov_len: size as usize,
            })
            .collect();

        // SAFETY: `storage` is owned by `Self` as a `Box<[u8]>`; the heap
        // allocation cannot move and iovecs stay valid until `Drop` calls
        // unregister.
        IO_DRIVER.with(|d| unsafe { d.register_fixed_buffers(&iovecs) })?;

        let free: Vec<u16> = (0..count).rev().collect();
        Ok(Self {
            inner: RefCell::new(PoolInner {
                free,
                waiters: VecDeque::new(),
            }),
            storage,
            base_ptr,
            count,
            size,
        })
    }

    /// Wait for a free buffer slot. Resolves immediately if one is
    /// available; otherwise parks the task until `release` frees one.
    pub fn acquire(&self) -> Acquire<'_> {
        Acquire {
            pool: self,
            waker_slot: None,
        }
    }

    /// Hand `buf` back to the free-list and wake the next waiter (if any).
    pub fn release(&self, buf: PooledBuf<'_>) {
        self.release_index(buf.index);
    }

    /// Hand `index` back to the free-list directly. Used by the cancel
    /// keepalive path, where the original `PooledBuf` no longer exists but
    /// its slot still needs to be reclaimed.
    pub fn release_index(&self, index: FixedBufIndex) {
        let waker_to_wake = {
            let mut inner = self.inner.borrow_mut();
            inner.free.push(index.get());
            loop {
                match inner.waiters.pop_front() {
                    Some(slot) => {
                        if let Some(w) = slot.take() {
                            break Some(w);
                        }
                        // Cancelled waiter, skip.
                    }
                    None => break None,
                }
            }
        };
        if let Some(w) = waker_to_wake {
            w.wake();
        }
    }

    /// Number of buffers in the pool.
    pub fn capacity(&self) -> usize {
        self.count as usize
    }

    /// Size of each buffer in bytes.
    pub fn buffer_size(&self) -> usize {
        self.size as usize
    }

    /// Base pointer for slot `index`. Stable for the pool's lifetime.
    fn slot_ptr(&self, index: u16) -> *mut u8 {
        unsafe { self.base_ptr.add(index as usize * self.size as usize) }
    }
}

impl Drop for FixedSendBufPool {
    fn drop(&mut self) {
        // Tell the kernel to drop its references before `storage` is freed.
        // Best-effort: if unregister fails (e.g. ring already gone) we have
        // nothing useful to do with the error here.
        let _ = IO_DRIVER.with(|d| d.unregister_fixed_buffers());
    }
}

/// A buffer slot borrowed from a [`FixedSendBufPool`].
///
/// Acts as a writable cursor: `Write` advances `len()` and refuses bytes
/// past the slot's `capacity`. `as_slice` exposes the written prefix to
/// hand to the kernel via the slot's `buf_index`.
///
/// Released by calling [`FixedSendBufPool::release`] — there is no
/// automatic release on drop.
pub struct PooledBuf<'p> {
    /// Cached `pool.base_ptr + index * size`. Avoids re-deriving the slot
    /// pointer (and re-entering the pool) on every `Write` call. Tied to the
    /// pool's lifetime via `_pool`.
    slot_base: *mut u8,
    cap: u32,
    index: FixedBufIndex,
    write_pos: u32,
    _pool: PhantomData<&'p FixedSendBufPool>,
}

impl<'p> PooledBuf<'p> {
    pub fn buf_index(&self) -> FixedBufIndex {
        self.index
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
            std::slice::from_raw_parts_mut(
                self.slot_base.add(written),
                self.cap as usize - written,
            )
        }
    }

    /// Rewind the write cursor to 0 for reuse without giving the slot back.
    pub fn reset(&mut self) {
        self.write_pos = 0;
    }
}

impl io::Write for PooledBuf<'_> {
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

/// Future returned by [`FixedSendBufPool::acquire`].
pub struct Acquire<'p> {
    pool: &'p FixedSendBufPool,
    waker_slot: Option<Rc<Cell<Option<Waker>>>>,
}

impl<'p> Future for Acquire<'p> {
    type Output = PooledBuf<'p>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut inner = self.pool.inner.borrow_mut();
        if let Some(idx) = inner.free.pop() {
            drop(inner);
            // Drop our queued waker slot (if any) so release won't try to
            // wake a future that's already completed.
            if let Some(slot) = self.waker_slot.take() {
                slot.set(None);
            }
            return Poll::Ready(PooledBuf {
                slot_base: self.pool.slot_ptr(idx),
                cap: self.pool.size,
                index: FixedBufIndex::new(idx),
                write_pos: 0,
                _pool: PhantomData,
            });
        }
        match &self.waker_slot {
            Some(slot) => {
                // If a release already took our waker (cell is None) but a
                // racing acquirer grabbed the freed slot before us, our cell
                // is no longer in `waiters` — re-enqueue, otherwise this task
                // would never be woken again. If the stored waker already
                // matches `cx.waker()`, reuse it and skip the atomic-refcount
                // clone.
                let old = slot.take();
                let needs_enqueue = old.is_none();
                let to_store = match old {
                    Some(w) if w.will_wake(cx.waker()) => w,
                    _ => cx.waker().clone(),
                };
                slot.set(Some(to_store));
                if needs_enqueue {
                    inner.waiters.push_back(slot.clone());
                }
            }
            None => {
                let slot = Rc::new(Cell::new(Some(cx.waker().clone())));
                inner.waiters.push_back(slot.clone());
                drop(inner);
                self.waker_slot = Some(slot);
            }
        }
        Poll::Pending
    }
}

impl<'p> Drop for Acquire<'p> {
    fn drop(&mut self) {
        if let Some(slot) = self.waker_slot.take() {
            // Mark this waiter cancelled; release will skip it.
            slot.set(None);
        }
    }
}
