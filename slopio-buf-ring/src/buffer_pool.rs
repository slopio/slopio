use std::ptr::{NonNull, null_mut};

use rustix::mm::{ProtFlags, mmap_anonymous};

use crate::{BufferId, MAP_FLAGS};

pub struct BufferPool<const BUFFER_SIZE: u32, const POOL_SIZE: u32> {
    ptr: *mut u8,
}

impl<const BUFFER_SIZE: u32, const POOL_SIZE: u32> BufferPool<BUFFER_SIZE, POOL_SIZE> {
    const BYTES: usize = BUFFER_SIZE as usize * POOL_SIZE as usize;

    pub fn new() -> std::io::Result<Self> {
        let ptr = unsafe {
            mmap_anonymous(
                null_mut(),
                Self::BYTES,
                ProtFlags::READ | ProtFlags::WRITE,
                MAP_FLAGS,
            )?
        };
        Ok(Self { ptr: ptr.cast() })
    }

    /// Returns the pointer offset for a given buffer id
    fn buffer_offset(&self, bid: u16) -> *mut u8 {
        unsafe { self.ptr.add(bid as usize * BUFFER_SIZE as usize) }
    }

    ///gets the pointer to the buffer of index bid (read-only)
    pub fn get(&self, bid: u16) -> Option<NonNull<u8>> {
        if bid as u32 >= POOL_SIZE {
            return None;
        }
        NonNull::new(self.buffer_offset(bid))
    }

    ///for building purpose
    pub(crate) fn ptr_for_bid(&self, bid: BufferId) -> *mut u8 {
        assert!((bid as u32) < POOL_SIZE);
        self.buffer_offset(bid)
    }
}

impl<const BUFFER_SIZE: u32, const POOL_SIZE: u32> Drop for BufferPool<BUFFER_SIZE, POOL_SIZE> {
    fn drop(&mut self) {
        unsafe {
            let _ = rustix::mm::munmap(self.ptr.cast(), Self::BYTES);
        }
    }
}
