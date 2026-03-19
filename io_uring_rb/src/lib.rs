use std::{cell::UnsafeCell, marker::PhantomData, sync::atomic::Ordering};

pub mod buffer;
mod buffer_pool;
mod mapped_ring;

use io_uring::{IoUring, types::BufRingEntry};

use crate::{buffer::Buffer, buffer_pool::BufferPool, mapped_ring::MmapedRing};

type BufferId = u16;

/// Helper to get the current tail value from a ring buffer
unsafe fn get_tail(ring_ptr: *const BufRingEntry) -> u16 {
    unsafe {
        let tail_ptr = BufRingEntry::tail(ring_ptr) as *const std::sync::atomic::AtomicU16;
        (*tail_ptr).load(Ordering::Acquire)
    }
}

/// Helper to set the tail value on a ring buffer
unsafe fn set_tail(ring_ptr: *const BufRingEntry, new_tail: u16) {
    unsafe {
        let tail_ptr = BufRingEntry::tail(ring_ptr) as *const std::sync::atomic::AtomicU16;
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
}

impl<const BUFFER_SIZE: u32, const RING_SIZE: u16> RingBuffer<BUFFER_SIZE, RING_SIZE> {
    pub fn group_id(&self) -> u16 {
        self.id
    }

    pub fn new(ring: &IoUring, flags: u16, buffer_group_id: u16) -> std::io::Result<Self> {
        assert!(BUFFER_SIZE.is_power_of_two());
        assert!(RING_SIZE.is_power_of_two());

        let mut mmaped_ring: MmapedRing = MmapedRing::build(RING_SIZE as _)?;
        let slice = mmaped_ring.as_slice();

        unsafe {
            ring.submitter().register_buf_ring_with_flags(
                slice.as_ptr() as _,
                RING_SIZE as _,
                buffer_group_id,
                flags,
            )?
        };

        let bp = BufferPool::<BUFFER_SIZE, RING_SIZE>::new()?;

        for (bid, slot) in slice.iter_mut().enumerate() {
            let entry = slot.write(unsafe { std::mem::zeroed() });
            entry.set_addr(bp.ptr_for_bid(bid as _) as _);
            entry.set_bid(bid as u16);
            entry.set_len(BUFFER_SIZE);
        }

        unsafe {
            set_tail(slice.as_ptr() as *const BufRingEntry, RING_SIZE);
        }

        Ok(RingBuffer {
            buffer_pool: UnsafeCell::new(bp),
            mapped_ring: UnsafeCell::new(mmaped_ring),
            id: buffer_group_id,
        })
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
}
