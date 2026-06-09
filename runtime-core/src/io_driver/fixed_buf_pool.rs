//! Pre-registered ("fixed") send-buffer pool.
//!
//! Allocates a contiguous region of `count * size` bytes and registers it
//! with the io_uring instance on the current worker via
//! [`IODriver::register_fixed_buffers`]. Slot `i` covers bytes
//! `[i * size, (i + 1) * size)` and corresponds to [`FixedBufIndex::new(i)`].
//!
//! The pool is `!Send + !Sync` — registration belongs to one ring on one
//! thread. Acquire with [`FixedSendBufPool::acquire`] (async, waits when the
//! pool is empty); the slot is returned **automatically** when the
//! [`PooledBuf`] is dropped, so it cannot be leaked by forgetting to release.
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

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::ptr::NonNull;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use crate::io_driver::{IO_DRIVER, operations::FixedBufIndex};

pub struct FixedSendBufPool {
    state: RefCell<PoolState>,
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

struct PoolState {
    /// Free-list, used as a stack: release pushes to the back, acquire pops
    /// from the back. LIFO keeps the just-released slot hot in cache.
    free: Vec<u16>,
    /// Cells let `Acquire::drop` mark a queued waiter as cancelled without
    /// scanning the deque: release pops slots and skips cells whose waker has
    /// been cleared.
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
        // allocation cannot move and the iovecs stay valid until `Drop` calls
        // unregister.
        IO_DRIVER.with(|d| unsafe { d.register_fixed_buffers(&iovecs) })?;

        let free: Vec<u16> = (0..count).rev().collect();
        Ok(Self {
            state: RefCell::new(PoolState {
                free,
                waiters: VecDeque::new(),
            }),
            storage,
            base_ptr,
            count,
            size,
        })
    }

    /// Wait for a free buffer slot. Resolves immediately if one is available;
    /// otherwise parks the task until a `PooledBuf` is dropped.
    pub fn acquire(&self) -> Acquire {
        Acquire {
            pool: NonNull::from(self),
            waker_slot: None,
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

    fn slot_ptr(&self, index: u16) -> *mut u8 {
        unsafe { self.base_ptr.add(index as usize * self.size as usize) }
    }

    /// Hand `index` back to the free-list and wake the next live waiter.
    fn release(&self, index: FixedBufIndex) {
        let waker_to_wake = {
            let mut state = self.state.borrow_mut();
            state.free.push(index.get());
            loop {
                match state.waiters.pop_front() {
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
/// Holds a raw pointer to its pool; see the module-level lifetime contract.
pub struct PooledBuf {
    pool: NonNull<FixedSendBufPool>,
    /// Cached `base_ptr + index * size`. Avoids re-deriving the slot pointer
    /// on every `Write` call.
    slot_base: *mut u8,
    cap: u32,
    index: FixedBufIndex,
    write_pos: u32,
}

impl PooledBuf {
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
        unsafe { self.pool.as_ref().release(self.index) };
    }
}

/// Future returned by [`FixedSendBufPool::acquire`].
pub struct Acquire {
    pool: NonNull<FixedSendBufPool>,
    waker_slot: Option<Rc<Cell<Option<Waker>>>>,
}

impl Future for Acquire {
    type Output = PooledBuf;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: the pool outlives this future (module contract).
        let pool = unsafe { self.pool.as_ref() };
        let mut state = pool.state.borrow_mut();
        if let Some(idx) = state.free.pop() {
            drop(state);
            // Drop our queued waker slot (if any) so release won't try to wake
            // a future that's already completed.
            if let Some(slot) = self.waker_slot.take() {
                slot.set(None);
            }
            return Poll::Ready(PooledBuf {
                pool: self.pool,
                slot_base: pool.slot_ptr(idx),
                cap: pool.size,
                index: FixedBufIndex::new(idx),
                write_pos: 0,
            });
        }
        match &self.waker_slot {
            Some(slot) => {
                // If a release already took our waker (cell is None) but a
                // racing acquirer grabbed the freed slot before us, our cell is
                // no longer in `waiters` — re-enqueue, otherwise this task would
                // never be woken again. If the stored waker already matches
                // `cx.waker()`, reuse it and skip the atomic-refcount clone.
                let old = slot.take();
                let needs_enqueue = old.is_none();
                let to_store = match old {
                    Some(w) if w.will_wake(cx.waker()) => w,
                    _ => cx.waker().clone(),
                };
                slot.set(Some(to_store));
                if needs_enqueue {
                    state.waiters.push_back(slot.clone());
                }
            }
            None => {
                let slot = Rc::new(Cell::new(Some(cx.waker().clone())));
                state.waiters.push_back(slot.clone());
                drop(state);
                self.waker_slot = Some(slot);
            }
        }
        Poll::Pending
    }
}

impl Drop for Acquire {
    fn drop(&mut self) {
        if let Some(slot) = self.waker_slot.take() {
            // Mark this waiter cancelled; release will skip it.
            slot.set(None);
        }
    }
}
