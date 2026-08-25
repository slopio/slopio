use std::{
    cell::{Cell, UnsafeCell},
    marker::PhantomData,
    mem::MaybeUninit,
    sync::atomic::Ordering,
};

pub mod buffer;
mod buffer_pool;
mod mapped_ring;

use io_uring::{IoUring, types::BufRingEntry};

use crate::{buffer::Buffer, buffer_pool::BufferPool, mapped_ring::MmapedRing};

type BufferId = u16;

/// Flags for the anonymous mappings a buffer ring is made of.
///
/// `MAP_POPULATE` prefaults the pages, so the first buffer handed out does not
/// take a fault. It is only a hint about when the memory is paged in, never
/// about what the mapping is — so a simulated build drops it and gets the same
/// ring, which is what lets Miri run the simulation suites: it accepts nothing
/// beyond a bare `MAP_PRIVATE|MAP_ANONYMOUS`.
pub(crate) const MAP_FLAGS: rustix::mm::MapFlags = if cfg!(feature = "simulation") {
    rustix::mm::MapFlags::PRIVATE
} else {
    rustix::mm::MapFlags::PRIVATE.union(rustix::mm::MapFlags::POPULATE)
};

/// Helper to get the current tail value from a ring buffer
unsafe fn get_tail(ring_ptr: *const BufRingEntry) -> u16 {
    unsafe {
        let tail_ptr = BufRingEntry::tail(ring_ptr) as *const std::sync::atomic::AtomicU16;
        (*tail_ptr).load(Ordering::Acquire)
    }
}

/// Helper to set the tail value on a ring buffer
///
/// Takes `*mut` where reading takes `*const`, because the write has to inherit
/// write permission from whatever the caller derived the pointer from. Handed a
/// pointer that came through a shared reborrow, the store is undefined
/// behaviour whatever the tail itself allows.
unsafe fn set_tail(ring_ptr: *mut BufRingEntry, new_tail: u16) {
    unsafe {
        let tail_ptr =
            BufRingEntry::tail(ring_ptr).cast_mut() as *const std::sync::atomic::AtomicU16;
        (*tail_ptr).store(new_tail, Ordering::Release);
    }
}

/// Sets up a ring entry at the given tail position
unsafe fn setup_ring_entry<const BUFFER_SIZE: u32, const RING_SIZE: u16>(
    ring_ptr: *mut BufRingEntry,
    tail: u16,
    addr: u64,
    bid: u16,
) {
    unsafe {
        let idx = (tail as usize) & ((RING_SIZE - 1) as usize);
        let entry = ring_ptr.add(idx);
        (*entry).set_addr(addr);
        (*entry).set_len(BUFFER_SIZE);
        (*entry).set_bid(bid);
    }
}

pub struct RingBuffer<const BUFFER_SIZE: u32, const RING_SIZE: u16> {
    buffer_pool: UnsafeCell<BufferPool<BUFFER_SIZE, RING_SIZE>>,
    mapped_ring: UnsafeCell<MmapedRing>,
    id: u16,
    /// How many entries have been handed out, for a ring consumed in
    /// userspace.
    ///
    /// The shared ring carries only a tail, written when a buffer is published
    /// or recycled; a kernel keeps its consumption cursor privately and never
    /// exposes it. So does whoever takes its place — see [`take`](Self::take).
    /// Unused when the ring is registered, since then the kernel keeps its own.
    consumed: Cell<u16>,
}

impl<const BUFFER_SIZE: u32, const RING_SIZE: u16> RingBuffer<BUFFER_SIZE, RING_SIZE> {
    pub fn group_id(&self) -> u16 {
        self.id
    }

    pub fn new(ring: &IoUring, flags: u16, buffer_group_id: u16) -> std::io::Result<Self> {
        Self::build(buffer_group_id, |slice| unsafe {
            ring.submitter().register_buf_ring_with_flags(
                slice.as_ptr() as _,
                RING_SIZE as _,
                buffer_group_id,
                flags,
            )
        })
    }

    /// The same ring, without publishing it to the kernel.
    ///
    /// Registration is the *only* thing a buffer ring needs a live io_uring
    /// for: the ring itself and its buffer pool are ordinary anonymous
    /// mappings, and every other operation — handing out pointers by `bid`,
    /// recycling, the tail updates — is plain userspace memory traffic. Skip
    /// the registration and the identical structure serves a simulated ring,
    /// which reads the tail and fills buffers exactly as the kernel would.
    pub fn new_unregistered(buffer_group_id: u16) -> std::io::Result<Self> {
        Self::build(buffer_group_id, |_| Ok(()))
    }

    fn build(
        buffer_group_id: u16,
        register: impl FnOnce(&[MaybeUninit<BufRingEntry>]) -> std::io::Result<()>,
    ) -> std::io::Result<Self> {
        assert!(BUFFER_SIZE.is_power_of_two());
        assert!(RING_SIZE.is_power_of_two());

        let mut mmaped_ring: MmapedRing = MmapedRing::build(RING_SIZE as _)?;
        let slice = mmaped_ring.as_slice();

        register(slice)?;

        let bp = BufferPool::<BUFFER_SIZE, RING_SIZE>::new()?;

        for (bid, slot) in slice.iter_mut().enumerate() {
            let entry = slot.write(unsafe { std::mem::zeroed() });
            entry.set_addr(bp.ptr_for_bid(bid as _) as _);
            entry.set_bid(bid as u16);
            entry.set_len(BUFFER_SIZE);
        }

        unsafe {
            set_tail(slice.as_mut_ptr().cast::<BufRingEntry>(), RING_SIZE);
        }

        Ok(RingBuffer {
            buffer_pool: UnsafeCell::new(bp),
            mapped_ring: UnsafeCell::new(mmaped_ring),
            id: buffer_group_id,
            consumed: Cell::new(0),
        })
    }

    /// Take the next buffer the ring has published, the way a kernel would.
    ///
    /// For a ring nobody registered: something has to play the consumer, and
    /// the entries say where each buffer is and which id it carries.
    ///
    /// `None` once the ring is drained — every buffer handed out and none
    /// recycled — which is the `-ENOBUFS` a real ring reports.
    pub fn take(&self) -> Option<(BufferId, std::ptr::NonNull<u8>)> {
        let ring = unsafe { &*self.mapped_ring.get() };
        let ring_ptr = ring.inner().as_ptr();
        let tail = unsafe { get_tail(ring_ptr) };
        let head = self.consumed.get();

        let outstanding = tail.wrapping_sub(head);
        assert!(
            outstanding <= RING_SIZE,
            "buffer group {}: {outstanding} buffers published into a ring of {RING_SIZE}. \
             A buffer was recycled twice, and the second push overwrote an entry that was \
             still live — two operations now share one buffer.",
            self.id,
        );
        if outstanding == 0 {
            return None;
        }

        let idx = (head as usize) & ((RING_SIZE - 1) as usize);
        // SAFETY: `idx` is masked into the mapping, which holds `RING_SIZE`
        // initialized entries from `build` onwards.
        let entry = unsafe { &*ring_ptr.add(idx) };
        let (bid, addr) = (entry.bid(), entry.addr());

        self.consumed.set(head.wrapping_add(1));
        std::ptr::NonNull::new(addr as *mut u8).map(|ptr| (bid, ptr))
    }

    pub fn get_buffer(&self, bid: BufferId, len: usize) -> Option<Buffer<BUFFER_SIZE>> {
        let inner = unsafe { &*self.buffer_pool.get() };
        if len > BUFFER_SIZE as usize {
            return None;
        }
        inner.get(bid).map(|ptr| Buffer {
            bid,
            ptr,
            len,
            _not_send_sync: PhantomData,
        })
    }

    /// Resolve a buffer id to its raw pointer. Returns `None` if `bid` is
    /// out of range for this ring.
    pub fn ptr_for_bid(&self, bid: BufferId) -> Option<std::ptr::NonNull<u8>> {
        let inner = unsafe { &*self.buffer_pool.get() };
        inner.get(bid)
    }

    ///recycles a buffer in the ring, use this only once on a buffer when you are done
    pub fn recycle_buffer(&self, buffer: &Buffer<BUFFER_SIZE>) {
        let ring = unsafe { &*self.mapped_ring.get() };

        unsafe {
            let ring_ptr = ring.inner().as_ptr();
            let tail = get_tail(ring_ptr);
            setup_ring_entry::<BUFFER_SIZE, RING_SIZE>(
                ring_ptr,
                tail,
                buffer.ptr.as_ptr() as u64,
                buffer.bid,
            );
            set_tail(ring_ptr, tail.wrapping_add(1));
        }
    }

    /// Recycle by buffer id, without holding a `Buffer` handle. Used by the
    /// runtime's cancel paths and by typed `BorrowedBuffer<S>`'s drop, which
    /// only retain the raw bid.
    pub fn recycle_bid(&self, bid: BufferId) {
        let inner = unsafe { &*self.buffer_pool.get() };
        let ptr = inner.get(bid).expect("invalid bid");
        let ring = unsafe { &*self.mapped_ring.get() };

        unsafe {
            let ring_ptr = ring.inner().as_ptr();
            let tail = get_tail(ring_ptr);
            setup_ring_entry::<BUFFER_SIZE, RING_SIZE>(
                ring_ptr,
                tail,
                ptr.as_ptr() as u64,
                bid,
            );
            set_tail(ring_ptr, tail.wrapping_add(1));
        }
    }
}
