use std::{mem::MaybeUninit, ptr::NonNull};

use io_uring::types::BufRingEntry;
use rustix::mm::{MapFlags, ProtFlags, mmap_anonymous};

pub struct MmapedRing {
    ptr: NonNull<BufRingEntry>,
    len: usize,
}

impl Drop for MmapedRing {
    fn drop(&mut self) {
        unsafe {
            let _ = rustix::mm::munmap(
                self.ptr.as_ptr().cast(),
                self.len * size_of::<BufRingEntry>(),
            );
        }
    }
}

impl MmapedRing {
    pub fn build(len: usize) -> std::io::Result<Self> {
        let ptr = Self::map(len)?;
        Ok(Self::new(ptr, len))
    }

    fn new(ptr: NonNull<BufRingEntry>, len: usize) -> Self {
        Self { ptr, len }
    }

    fn map(ring_size: usize) -> std::io::Result<NonNull<BufRingEntry>> {
        let mmaped_ring = unsafe {
            mmap_anonymous(
                core::ptr::null_mut(),
                ring_size * size_of::<io_uring::types::BufRingEntry>(),
                ProtFlags::READ | ProtFlags::WRITE,
                MapFlags::PRIVATE | MapFlags::POPULATE,
            )
        }?;

        unsafe {
            *(BufRingEntry::tail(mmaped_ring.cast::<BufRingEntry>()).cast_mut()) = 0;
        }
        Ok(unsafe { NonNull::new_unchecked(mmaped_ring) }.cast())
    }

    pub fn as_slice(&mut self) -> &mut [MaybeUninit<BufRingEntry>] {
        unsafe { core::slice::from_raw_parts_mut(self.ptr.as_ptr().cast(), self.len) }
    }

    pub fn inner(&self) -> NonNull<BufRingEntry> {
        self.ptr
    }
}
