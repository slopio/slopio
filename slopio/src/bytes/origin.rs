//! Who owns the memory behind a view, and how its references are counted.

use std::alloc::{self, Layout};
use std::any::TypeId;
use std::cell::{Cell, RefCell};
use std::ptr::{self, NonNull};

use crate::io_driver::BufRingSpec;

/// Null for static memory, a `Header` for a heap block, a `SlotRef` tagged with
/// `RING_TAG` for a buffer-ring slot.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct Origin(*mut ());

const RING_TAG: usize = 1;

/// Prefix of every heap block; the data follows it.
#[repr(C)]
struct Header {
    refs: Cell<usize>,
    cap: usize,
}

const DATA_OFFSET: usize = size_of::<Header>();

/// Reference count of one ring slot; 8-aligned so the low bit is free for the tag.
#[repr(C, align(8))]
struct SlotRef {
    refs: Cell<u32>,
    bid: u16,
    bgid: u8,
}

struct RingRefs {
    spec: TypeId,
    slots: Box<[SlotRef]>,
    recycle: fn(u16),
}

thread_local! {
    static RINGS: [RefCell<Option<RingRefs>>; 4] = const { [const { RefCell::new(None) }; 4] };
}

enum Kind {
    Static,
    Heap(NonNull<Header>),
    Ring(NonNull<SlotRef>),
}

fn heap_layout(cap: usize) -> Layout {
    DATA_OFFSET
        .checked_add(cap)
        .and_then(|size| Layout::from_size_align(size, align_of::<Header>()).ok())
        .expect("capacity overflow")
}

impl Origin {
    pub(super) const STATIC: Origin = Origin(ptr::null_mut());

    /// A new heap block of `cap` data bytes, held once.
    pub(super) fn alloc(cap: usize) -> (Origin, NonNull<u8>) {
        let layout = heap_layout(cap);
        let header = unsafe { alloc::alloc(layout) }.cast::<Header>();
        let Some(header) = NonNull::new(header) else {
            alloc::handle_alloc_error(layout)
        };
        unsafe {
            header.write(Header {
                refs: Cell::new(1),
                cap,
            })
        };
        (Origin(header.as_ptr().cast()), data_of(header))
    }

    /// The first reference to slot `bid` of ring `S`, which the caller took off a completion.
    pub(super) fn ring<S: BufRingSpec>(bid: u16) -> Origin {
        RINGS.with(|rings| {
            let mut entry = rings
                .get(S::BGID as usize)
                .expect("bgid must be below 4")
                .borrow_mut();
            let refs = match &mut *entry {
                Some(refs) if refs.spec == TypeId::of::<S>() => refs,
                other => {
                    // Another spec had this bgid on this thread; none of its slots may still be viewed.
                    if let Some(old) = other {
                        assert!(
                            old.slots.iter().all(|slot| slot.refs.get() == 0),
                            "bgid {} reused while a view still holds one of its slots",
                            S::BGID,
                        );
                    }
                    other.insert(RingRefs {
                        spec: TypeId::of::<S>(),
                        slots: (0..S::POOL_SIZE)
                            .map(|bid| SlotRef {
                                refs: Cell::new(0),
                                bid: bid as u16,
                                bgid: S::BGID as u8,
                            })
                            .collect(),
                        recycle: S::recycle_bid,
                    })
                }
            };
            let slot = &refs.slots[bid as usize];
            debug_assert_eq!(slot.refs.get(), 0, "slot {bid} handed out twice");
            slot.refs.set(1);
            Origin(
                ptr::from_ref(slot)
                    .cast_mut()
                    .cast::<()>()
                    .map_addr(|a| a | RING_TAG),
            )
        })
    }

    fn kind(self) -> Kind {
        if self.0.is_null() {
            Kind::Static
        } else if self.0.addr() & RING_TAG != 0 {
            Kind::Ring(unsafe { NonNull::new_unchecked(self.0.map_addr(|a| a & !RING_TAG).cast()) })
        } else {
            Kind::Heap(unsafe { NonNull::new_unchecked(self.0.cast()) })
        }
    }

    /// One more view on the same memory.
    #[inline]
    pub(super) fn retain(self) {
        match self.kind() {
            Kind::Static => {}
            Kind::Heap(h) => {
                let refs = unsafe { &h.as_ref().refs };
                if refs.get() == usize::MAX {
                    std::process::abort();
                }
                refs.set(refs.get() + 1);
            }
            Kind::Ring(s) => {
                let refs = unsafe { &s.as_ref().refs };
                if refs.get() == u32::MAX {
                    std::process::abort();
                }
                refs.set(refs.get() + 1);
            }
        }
    }

    /// One view fewer; the last one frees the block or recycles the slot.
    #[inline]
    pub(super) fn release(self) {
        match self.kind() {
            Kind::Static => {}
            Kind::Heap(h) => {
                let header = unsafe { h.as_ref() };
                let left = header.refs.get() - 1;
                header.refs.set(left);
                if left == 0 {
                    let layout = heap_layout(header.cap);
                    unsafe { alloc::dealloc(h.as_ptr().cast(), layout) };
                }
            }
            Kind::Ring(s) => {
                let slot = unsafe { s.as_ref() };
                let left = slot.refs.get() - 1;
                slot.refs.set(left);
                if left == 0 {
                    recycle(slot.bgid, slot.bid);
                }
            }
        }
    }

    pub(super) fn is_unique(self) -> bool {
        match self.kind() {
            Kind::Static => false,
            Kind::Heap(h) => unsafe { h.as_ref() }.refs.get() == 1,
            Kind::Ring(s) => unsafe { s.as_ref() }.refs.get() == 1,
        }
    }

    /// Data start and capacity of a heap block no other view holds.
    pub(super) fn unique_heap(self) -> Option<(NonNull<u8>, usize)> {
        match self.kind() {
            Kind::Heap(h) => {
                let header = unsafe { h.as_ref() };
                (header.refs.get() == 1).then(|| (data_of(h), header.cap))
            }
            _ => None,
        }
    }

    /// Reallocate a block to `cap` data bytes, keeping its contents.
    ///
    /// # Safety
    /// `self` is a heap block no other view holds.
    pub(super) unsafe fn grow(self, cap: usize) -> (Origin, NonNull<u8>) {
        let Kind::Heap(h) = self.kind() else {
            unreachable!("grow on a block that is not on the heap")
        };
        let old = heap_layout(unsafe { h.as_ref() }.cap);
        let new = heap_layout(cap);
        let header = unsafe { alloc::realloc(h.as_ptr().cast(), old, new.size()) }.cast::<Header>();
        let Some(header) = NonNull::new(header) else {
            alloc::handle_alloc_error(new)
        };
        unsafe { (*header.as_ptr()).cap = cap };
        (Origin(header.as_ptr().cast()), data_of(header))
    }
}

fn data_of(header: NonNull<Header>) -> NonNull<u8> {
    unsafe { header.cast::<u8>().add(DATA_OFFSET) }
}

fn recycle(bgid: u8, bid: u16) {
    let recycle = RINGS.with(|rings| {
        rings[bgid as usize]
            .borrow()
            .as_ref()
            .expect("a ring slot outlived its table")
            .recycle
    });
    recycle(bid);
}
