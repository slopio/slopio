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

use crate::io_driver::{BufRingHooks, IODriver, RingHandle};

/// Hands out the next buffer of one group, as `(bid, pointer, capacity)`.
///
/// A plain function pointer because a ring is reached by `bgid` — a number off
/// the SQE — while the buffers live in a per-spec thread-local keyed by type.
pub type BufRingTake = fn() -> Option<(u16, NonNull<u8>, u32)>;

/// Build this spec's ring, registering it with whichever backend the driver is
/// on.
///
/// The branch lives here rather than in [`define_buf_ring!`] because the macro
/// expands into the caller's crate, where a `cfg` on *this* crate's features
/// would mean nothing. `take` is ignored unless the ring is simulated.
pub fn install_buf_ring<const BUFFER_SIZE: u32, const RING_SIZE: u16, const POOL_SIZE: u32>(
    ring: RingHandle<'_>,
    bgid: u16,
    take: BufRingTake,
) -> std::io::Result<io_uring_rb::RingBuffer<BUFFER_SIZE, RING_SIZE, POOL_SIZE>> {
    let _ = take;
    match ring {
        RingHandle::Uring(ring) => io_uring_rb::RingBuffer::new(ring, 0, bgid),
        // Registration is the only step that needs a kernel; the mapping and
        // its buffers are ordinary memory. Hand the simulator a way to consume
        // the ring, since no kernel will.
        #[cfg(feature = "simulation")]
        RingHandle::Simulated(handle) => {
            handle.register_buf_ring(bgid, take, BUFFER_SIZE);
            io_uring_rb::RingBuffer::new_unregistered(bgid)
        }
    }
}

/// Hand an already-built ring to a new driver on the same thread.
///
/// Only a simulated run may: its ring was reset when the previous run ended,
/// and the new simulator needs `take` to consume it. A real ring is never
/// installed twice.
pub fn reinstall_buf_ring(
    ring: RingHandle<'_>,
    bgid: u16,
    take: BufRingTake,
    capacity: u32,
    name: &str,
) {
    let _ = (bgid, take, capacity);
    match ring {
        RingHandle::Uring(_) => panic!("buf ring `{name}` already initialized on this thread"),
        #[cfg(feature = "simulation")]
        RingHandle::Simulated(handle) => handle.register_buf_ring(bgid, take, capacity),
    }
}

/// Put a ring back as it was built, once a simulated run has ended.
#[doc(hidden)]
pub fn reset_buf_ring<const BUFFER_SIZE: u32, const RING_SIZE: u16, const POOL_SIZE: u32>(
    rb: &io_uring_rb::RingBuffer<BUFFER_SIZE, RING_SIZE, POOL_SIZE>,
) {
    #[cfg(feature = "simulation")]
    rb.reset();
    #[cfg(not(feature = "simulation"))]
    {
        let _ = rb;
        unreachable!("only a simulated run resets its buf rings");
    }
}

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
    /// Number of buffers the kernel can see at once.
    const RING_SIZE: u16;
    /// Number of buffers in all; those past `RING_SIZE` wait in a reserve and
    /// enter the ring as it empties.
    const POOL_SIZE: u32;

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

    /// Account one buffer consumed by the kernel. Called by the driver, once
    /// per completion that carries a buffer.
    #[doc(hidden)]
    fn note_consumed();

    /// Move reserve buffers into the ring's free entries. Called by the driver
    /// before it submits.
    #[doc(hidden)]
    fn refill();

    /// Where this ring's buffers are right now, on the current thread.
    fn usage() -> BufRingUsage;

    /// Put this thread's ring back as `init` built it. Called when a simulated
    /// run ends; panics if a buffer is still out.
    #[doc(hidden)]
    fn reset();
}

/// A snapshot of where a ring's buffers are.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BufRingUsage {
    /// Available to the kernel.
    pub in_kernel: usize,
    /// Free, waiting for room in the ring.
    pub reserved: usize,
    /// Handed out and not given back yet.
    pub in_flight: usize,
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

// Only a pointer, a length and a bid; the spec is a ZST marker.
impl<S: BufRingSpec> Unpin for BorrowedBuffer<S> {}

// SAFETY: ring memory never moves and outlives the borrow; the slot is recycled
// only on drop, which the keepalive defers past a cancelled op's terminal CQE.
unsafe impl<S: BufRingSpec> crate::io_driver::operations::IoBuf for BorrowedBuffer<S> {
    fn stable_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }
    fn bytes_init(&self) -> usize {
        self.len
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

    /// Tell `driver` about every ring in this set: it accounts their
    /// consumption, refills them, and recycles on the type-erased cancel path.
    fn register_fallbacks(driver: &IODriver);

    /// Reset every ring in this set, so the next simulated run on this thread
    /// starts from fresh rings.
    #[cfg(feature = "simulation")]
    fn reset_all();
}

fn hooks<S: BufRingSpec>() -> BufRingHooks {
    BufRingHooks {
        consumed: S::note_consumed,
        refill: S::refill,
        recycle: S::recycle_bid,
    }
}

impl BufRingSet for () {
    fn init_all(_: RingHandle<'_>) {}
    fn register_fallbacks(_: &IODriver) {}
    #[cfg(feature = "simulation")]
    fn reset_all() {}
}

impl<A: BufRingSpec> BufRingSet for A {
    fn init_all(ring: RingHandle<'_>) {
        A::init(ring);
    }
    fn register_fallbacks(driver: &IODriver) {
        driver.register_buf_ring(A::BGID, hooks::<A>());
    }
    #[cfg(feature = "simulation")]
    fn reset_all() {
        A::reset();
    }
}

macro_rules! impl_buf_ring_set_tuple {
    ( $( $t:ident ),+ $(,)? ) => {
        impl< $( $t: BufRingSpec ),+ > BufRingSet for ( $( $t, )+ ) {
            fn init_all(ring: RingHandle<'_>) {
                $( <$t as BufRingSpec>::init(ring); )+
            }
            fn register_fallbacks(driver: &IODriver) {
                $( driver.register_buf_ring(<$t as BufRingSpec>::BGID, hooks::<$t>()); )+
            }
            #[cfg(feature = "simulation")]
            fn reset_all() {
                $( <$t as BufRingSpec>::reset(); )+
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
///     let mr = MultiRecv::<SmallRecv>::new(socket);
///     // ...
/// });
/// ```
///
/// `bgid` must be below 4 and unique across every spec installed in the same
/// runtime. `buffer_size` and `ring_size` must both be powers of two.
///
/// `pool_size` is optional: it reserves more buffers than the ring shows the
/// kernel at once, and the extra ones enter the ring as it empties. Defaults to
/// `ring_size`.
#[macro_export]
macro_rules! define_buf_ring {
    (
        $vis:vis $name:ident,
        bgid = $bgid:literal,
        buffer_size = $bsize:literal,
        ring_size = $rsize:literal $(,)?
    ) => {
        $crate::define_buf_ring!(
            $vis $name,
            bgid = $bgid,
            buffer_size = $bsize,
            ring_size = $rsize,
            pool_size = $rsize,
        );
    };
    (
        $vis:vis $name:ident,
        bgid = $bgid:literal,
        buffer_size = $bsize:literal,
        ring_size = $rsize:literal,
        pool_size = $psize:literal $(,)?
    ) => {
        $vis struct $name;

        const _: () = {
            // The ring rides in two bits of every op's user_data.
            assert!($bgid < 4, "bgid must be below 4");
            assert!($psize >= $rsize, "pool_size must be at least ring_size");
            assert!($psize <= 1 << 16, "pool_size must fit 16-bit buffer ids");
        };

        const _: () = {
            ::std::thread_local! {
                static RING: ::std::cell::OnceCell<
                    $crate::__private::RingBuffer<$bsize, $rsize, $psize>
                > = const { ::std::cell::OnceCell::new() };
            }

            fn with_ring<R>(
                f: impl FnOnce(&$crate::__private::RingBuffer<$bsize, $rsize, $psize>) -> R,
            ) -> R {
                RING.with(|cell| {
                    f(cell.get().expect(concat!(
                        "buf ring `", stringify!($name),
                        "` not initialized on this thread",
                    )))
                })
            }

            impl $crate::io_driver::BufRingSpec for $name {
                const BGID: u16 = $bgid;
                const BUFFER_SIZE: u32 = $bsize;
                const RING_SIZE: u16 = $rsize;
                const POOL_SIZE: u32 = $psize;

                fn init(ring: $crate::io_driver::RingHandle<'_>) {
                    fn take() -> ::core::option::Option<(u16, ::core::ptr::NonNull<u8>, u32)> {
                        RING.with(|cell| {
                            let rb = cell.get()?;
                            rb.take().map(|(bid, ptr)| (bid, ptr, $bsize))
                        })
                    }
                    RING.with(|cell| {
                        if cell.get().is_some() {
                            $crate::io_driver::reinstall_buf_ring(
                                ring, $bgid, take, $bsize, stringify!($name),
                            );
                            return;
                        }
                        let rb = $crate::io_driver::install_buf_ring::<$bsize, $rsize, $psize>(
                            ring, $bgid, take,
                        )
                        .expect("buf ring init failed");
                        // Checked empty just above.
                        let _ = cell.set(rb);
                    });
                }

                fn make_buffer(
                    bid: u16,
                    len: usize,
                ) -> $crate::io_driver::BorrowedBuffer<Self> {
                    with_ring(|rb| {
                        let ptr = rb.ptr_for_bid(bid).expect("invalid bid");
                        unsafe {
                            $crate::io_driver::BorrowedBuffer::from_raw_parts(
                                ptr, len, bid,
                            )
                        }
                    })
                }

                fn recycle_bid(bid: u16) {
                    with_ring(|rb| rb.recycle_bid(bid));
                }

                fn note_consumed() {
                    with_ring(|rb| rb.note_consumed());
                }

                fn refill() {
                    with_ring(|rb| {
                        rb.refill();
                    });
                }

                fn reset() {
                    with_ring(|rb| $crate::io_driver::reset_buf_ring(rb));
                }

                fn usage() -> $crate::io_driver::BufRingUsage {
                    with_ring(|rb| $crate::io_driver::BufRingUsage {
                        in_kernel: rb.in_kernel() as usize,
                        reserved: rb.reserved(),
                        in_flight: rb.in_flight(),
                    })
                }
            }
        };
    };
}
