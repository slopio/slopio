//! The process allocator: mimalloc, called the way its fast path wants.
//!
//! `mimalloc::MiMalloc` sends every request through `mi_malloc_aligned`. In
//! v3 that reaches the fast path only for power-of-two sizes: any other size
//! is over-allocated into a larger size class, so the page of its own class
//! never fills and the miss repeats on every call. A 384-byte task cost ~150
//! instructions to allocate instead of ~30, and took a 448-byte block.
//!
//! Every mimalloc block is a whole number of words, in a page whose first
//! block is 16-aligned, so any block is word-aligned: only a stricter
//! alignment needs the aligned calls.

use std::alloc::{GlobalAlloc, Layout};

use libmimalloc_sys as mi;

pub(crate) struct Mimalloc;

/// What every block is aligned to.
const WORD: usize = size_of::<usize>();

unsafe impl GlobalAlloc for Mimalloc {
    #[inline]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = match layout.align() {
            align if align <= WORD => unsafe { mi::mi_malloc(layout.size()) },
            align => unsafe { mi::mi_malloc_aligned(layout.size(), align) },
        };
        p.cast()
    }

    #[inline]
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let p = match layout.align() {
            align if align <= WORD => unsafe { mi::mi_zalloc(layout.size()) },
            align => unsafe { mi::mi_zalloc_aligned(layout.size(), align) },
        };
        p.cast()
    }

    #[inline]
    unsafe fn dealloc(&self, ptr: *mut u8, _layout: Layout) {
        unsafe { mi::mi_free(ptr.cast()) }
    }

    #[inline]
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let p = match layout.align() {
            align if align <= WORD => unsafe { mi::mi_realloc(ptr.cast(), new_size) },
            align => unsafe { mi::mi_realloc_aligned(ptr.cast(), new_size, align) },
        };
        p.cast()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::ffi::c_void;

    // Declared by libmimalloc-sys only under its `extended` feature; the
    // symbol is always there.
    unsafe extern "C" {
        fn mi_usable_size(p: *const c_void) -> usize;
    }

    const SIZES: [usize; 10] = [1, 7, 8, 24, 100, 224, 384, 1000, 5000, 100_000];
    const ALIGNS: [usize; 8] = [1, 2, 4, 8, 16, 64, 512, 4096];

    #[test]
    fn every_block_has_its_alignment() {
        for size in SIZES {
            for align in ALIGNS {
                let layout = Layout::from_size_align(size, align).unwrap();
                let p = unsafe { Mimalloc.alloc(layout) };
                assert!(!p.is_null());
                assert_eq!(p.addr() % align, 0, "size {size}, align {align}");
                unsafe { Mimalloc.dealloc(p, layout) };
            }
        }
    }

    #[test]
    fn zeroed_blocks_are_zero() {
        for size in SIZES {
            for align in ALIGNS {
                let layout = Layout::from_size_align(size, align).unwrap();
                let p = unsafe { Mimalloc.alloc_zeroed(layout) };
                assert_eq!(p.addr() % align, 0, "size {size}, align {align}");
                let block = unsafe { std::slice::from_raw_parts(p, size) };
                assert!(block.iter().all(|&b| b == 0), "size {size}, align {align}");
                unsafe { Mimalloc.dealloc(p, layout) };
            }
        }
    }

    #[test]
    fn realloc_keeps_contents_and_alignment() {
        for align in ALIGNS {
            let layout = Layout::from_size_align(100, align).unwrap();
            let p = unsafe { Mimalloc.alloc(layout) };
            (0..100).for_each(|i| unsafe { p.add(i).write(i as u8) });

            let grown = unsafe { Mimalloc.realloc(p, layout, 10_000) };
            assert_eq!(grown.addr() % align, 0, "align {align}");
            (0..100).for_each(|i| assert_eq!(unsafe { grown.add(i).read() }, i as u8));

            let grown_layout = Layout::from_size_align(10_000, align).unwrap();
            let shrunk = unsafe { Mimalloc.realloc(grown, grown_layout, 50) };
            assert_eq!(shrunk.addr() % align, 0, "align {align}");
            (0..50).for_each(|i| assert_eq!(unsafe { shrunk.add(i).read() }, i as u8));

            let shrunk_layout = Layout::from_size_align(50, align).unwrap();
            unsafe { Mimalloc.dealloc(shrunk, shrunk_layout) };
        }
    }

    /// Size classes in mimalloc's own spacing, which through
    /// `mi_malloc_aligned` came out of the next one up: 256 bytes for 224,
    /// 448 for 384.
    #[test]
    fn word_aligned_sizes_take_their_own_class() {
        for size in [224, 384] {
            let layout = Layout::from_size_align(size, WORD).unwrap();
            let p = unsafe { Mimalloc.alloc(layout) };
            assert_eq!(unsafe { mi_usable_size(p.cast()) }, size);
            unsafe { Mimalloc.dealloc(p, layout) };
        }
    }
}
