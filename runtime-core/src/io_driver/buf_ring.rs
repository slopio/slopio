//! Statically-typed provided-buffer rings.
//!
//! Each ring is a zero-sized type implementing [`BufRingSpec`], created by
//! the [`define_buf_ring!`](crate::define_buf_ring) macro. The set of rings
//! a runtime should install is expressed as a [`BufRingSet`] (a single spec,
//! a tuple of specs, or the empty tuple `()` for "no rings").
//!
//! The set is passed as a type parameter to the runtime entry point, which
//! initializes one `RingBuffer` per spec per worker thread.

use std::marker::PhantomData;
use std::ptr::NonNull;

use crate::io_driver::{IODriver, RingHandle};

/// A single provided-buffer ring known at compile time.
///
/// Implementors are produced by [`define_buf_ring!`](crate::define_buf_ring).
/// Each implementor is a zero-sized type that uniquely identifies one ring;
/// the const fields describe its kernel-side layout.
pub trait BufRingSpec: 'static + Sized {
    /// io_uring buffer group id. Must be unique across all specs installed
    /// in a single runtime.
    const BGID: u16;
    /// Size of each buffer in the ring, in bytes.
    const BUFFER_SIZE: u32;
    /// Number of buffers in the ring.
    const RING_SIZE: u16;

    /// Build the underlying `RingBuffer` and stash it in this spec's
    /// per-thread cell. Called once per worker thread at runtime startup.
    fn init(ring: RingHandle<'_>);

    /// Construct a typed `BorrowedBuffer<Self>` from a completion-event
    /// `(bid, len)` pair. The returned buffer recycles to this spec's ring
    /// on drop.
    fn make_buffer(bid: u16, len: usize) -> BorrowedBuffer<Self>;

    /// Recycle buffer `bid` back to this spec's ring, without holding a
    /// `BorrowedBuffer`. Used by cancel-cleanup paths and by `Drop`.
    fn recycle_bid(bid: u16);
}

/// A buffer borrowed from a registered provided-buffer ring `S`.
///
/// Recycled to `S`'s ring on drop. `!Send + !Sync` because the underlying
/// pointer references thread-local kernel memory.
pub struct BorrowedBuffer<S: BufRingSpec> {
    ptr: NonNull<u8>,
    len: usize,
    bid: u16,
    _not_send_sync: PhantomData<*const ()>,
    _spec: PhantomData<S>,
}

impl<S: BufRingSpec> BorrowedBuffer<S> {
    /// # Safety
    /// `ptr`/`len`/`bid` must come from a kernel completion event for ring `S`.
    /// The caller transfers ownership of the buffer slot to the returned value;
    /// the slot is recycled when this `BorrowedBuffer` is dropped.
    pub unsafe fn from_raw_parts(ptr: NonNull<u8>, len: usize, bid: u16) -> Self {
        Self {
            ptr,
            len,
            bid,
            _not_send_sync: PhantomData,
            _spec: PhantomData,
        }
    }

    pub fn bid(&self) -> u16 {
        self.bid
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl<S: BufRingSpec> AsRef<[u8]> for BorrowedBuffer<S> {
    fn as_ref(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
}

impl<S: BufRingSpec> AsMut<[u8]> for BorrowedBuffer<S> {
    fn as_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }
}

impl<S: BufRingSpec> Drop for BorrowedBuffer<S> {
    fn drop(&mut self) {
        S::recycle_bid(self.bid);
    }
}

/// A composition of zero or more `BufRingSpec`s, installed together by the
/// runtime entry point.
///
/// Implemented for:
/// - `()` — install no rings
/// - any `S: BufRingSpec` — install a single ring
/// - tuples of arity 1..=12 of `BufRingSpec` — install several
pub trait BufRingSet {
    /// Initialize every ring in this set against `ring` on the current thread.
    fn init_all(ring: RingHandle<'_>);

    /// Register a `(bgid, recycle_fn)` entry for every ring in this set into
    /// `driver`'s cancel-fallback table. The driver uses this when a cancel
    /// completion delivers a buffer the type-erased cancel path needs to
    /// recycle.
    fn register_fallbacks(driver: &IODriver);
}

impl BufRingSet for () {
    fn init_all(_: RingHandle<'_>) {}
    fn register_fallbacks(_: &IODriver) {}
}

impl<A: BufRingSpec> BufRingSet for A {
    fn init_all(ring: RingHandle<'_>) {
        A::init(ring);
    }
    fn register_fallbacks(driver: &IODriver) {
        driver.register_recycler(A::BGID, A::recycle_bid);
    }
}

macro_rules! impl_buf_ring_set_tuple {
    ( $( $t:ident ),+ $(,)? ) => {
        impl< $( $t: BufRingSpec ),+ > BufRingSet for ( $( $t, )+ ) {
            fn init_all(ring: RingHandle<'_>) {
                $( <$t as BufRingSpec>::init(ring); )+
            }
            fn register_fallbacks(driver: &IODriver) {
                $( driver.register_recycler(<$t as BufRingSpec>::BGID, <$t as BufRingSpec>::recycle_bid); )+
            }
        }
    };
}

impl_buf_ring_set_tuple!(A);
impl_buf_ring_set_tuple!(A, B);
impl_buf_ring_set_tuple!(A, B, C);
impl_buf_ring_set_tuple!(A, B, C, D);
impl_buf_ring_set_tuple!(A, B, C, D, E);
impl_buf_ring_set_tuple!(A, B, C, D, E, F);
impl_buf_ring_set_tuple!(A, B, C, D, E, F, G);
impl_buf_ring_set_tuple!(A, B, C, D, E, F, G, H);
impl_buf_ring_set_tuple!(A, B, C, D, E, F, G, H, I);
impl_buf_ring_set_tuple!(A, B, C, D, E, F, G, H, I, J);
impl_buf_ring_set_tuple!(A, B, C, D, E, F, G, H, I, J, K);
impl_buf_ring_set_tuple!(A, B, C, D, E, F, G, H, I, J, K, L);

/// Declares a provided-buffer ring as a zero-sized type implementing
/// [`BufRingSpec`].
///
/// The generated type can be passed to the runtime entry point (alone or as
/// part of a tuple) and used as a type parameter on IO operations.
///
/// # Example
///
/// ```rust,ignore
/// runtime::define_buf_ring!(pub SmallRecv, bgid = 0, buffer_size = 4096, ring_size = 8192);
///
/// runtime::main_thread_with::<SmallRecv, _>(async {
///     let mr = MultiRecv::<SmallRecv>::new(fd);
///     // ...
/// });
/// ```
///
/// `bgid` must be unique across every spec installed in the same runtime.
/// `buffer_size` and `ring_size` must both be powers of two.
#[macro_export]
macro_rules! define_buf_ring {
    (
        $vis:vis $name:ident,
        bgid = $bgid:literal,
        buffer_size = $bsize:literal,
        ring_size = $rsize:literal $(,)?
    ) => {
        $vis struct $name;

        const _: () = {
            ::std::thread_local! {
                static RING: ::std::cell::OnceCell<
                    $crate::__private::RingBuffer<$bsize, $rsize>
                > = const { ::std::cell::OnceCell::new() };
            }

            impl $crate::io_driver::BufRingSpec for $name {
                const BGID: u16 = $bgid;
                const BUFFER_SIZE: u32 = $bsize;
                const RING_SIZE: u16 = $rsize;

                fn init(ring: $crate::io_driver::RingHandle<'_>) {
                    RING.with(|cell| {
                        let rb = match ring {
                            $crate::io_driver::RingHandle::Uring(ring) => {
                                <$crate::__private::RingBuffer<$bsize, $rsize>>::new(
                                    ring, 0, $bgid,
                                )
                            }
                            // Registration is the only step that needs a kernel;
                            // the mapping and its buffers are ordinary memory.
                            $crate::io_driver::RingHandle::Simulated => {
                                <$crate::__private::RingBuffer<$bsize, $rsize>>::new_unregistered(
                                    $bgid,
                                )
                            }
                        }
                        .expect("buf ring init failed");
                        if cell.set(rb).is_err() {
                            panic!(concat!(
                                "buf ring `", stringify!($name),
                                "` already initialized on this thread",
                            ));
                        }
                    });
                }

                fn make_buffer(
                    bid: u16,
                    len: usize,
                ) -> $crate::io_driver::BorrowedBuffer<Self> {
                    RING.with(|cell| {
                        let rb = cell.get().expect(concat!(
                            "buf ring `", stringify!($name),
                            "` not initialized on this thread",
                        ));
                        let ptr = rb.ptr_for_bid(bid).expect("invalid bid");
                        unsafe {
                            $crate::io_driver::BorrowedBuffer::from_raw_parts(
                                ptr, len, bid,
                            )
                        }
                    })
                }

                fn recycle_bid(bid: u16) {
                    RING.with(|cell| {
                        let rb = cell.get().expect(concat!(
                            "buf ring `", stringify!($name),
                            "` not initialized on this thread",
                        ));
                        rb.recycle_bid(bid);
                    });
                }
            }
        };
    };
}
