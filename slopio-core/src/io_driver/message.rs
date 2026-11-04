use std::ptr::NonNull;
use std::sync::Arc;

/// One end of a peer message: a worker thread, plus an optional slab slot on
/// that worker.
///
/// The `slab` names a suspended op parked on that thread waiting to be matched
/// — a request awaiting its reply. It is `None` for fire-and-forget traffic
/// that expects no reply, so the slot is only carried when it is actually
/// needed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Endpoint {
    pub thread: u16,
    pub slab: Option<u32>,
}

impl Endpoint {
    /// A bare endpoint with no reply slot (fire-and-forget).
    #[inline]
    pub const fn new(thread: u16) -> Self {
        Self { thread, slab: None }
    }

    /// An endpoint whose `slab` points at a parked op awaiting a reply.
    #[inline]
    pub const fn with_slab(thread: u16, slab: u32) -> Self {
        Self {
            thread,
            slab: Some(slab),
        }
    }
}

/// Routing metadata carried alongside every peer message, split by role.
///
/// `sender` is who produced the message and where a reply should be routed;
/// `receiver` is who it is addressed to. To turn a request into its reply, swap
/// the two ends. Each side's slab is independent and optional.
#[derive(Clone, Copy, Debug, Default)]
pub struct Header {
    pub sender: Endpoint,
    pub receiver: Endpoint,
}

impl Header {
    #[inline]
    pub const fn new(sender: Endpoint, receiver: Endpoint) -> Self {
        Self { sender, receiver }
    }

    /// The routing header for a reply to a message with this header: the two
    /// ends swap, so the reply travels back to the original sender and, if it
    /// carried a reply slab, lands in that slot.
    #[inline]
    pub const fn swapped(&self) -> Self {
        Self {
            sender: self.receiver,
            receiver: self.sender,
        }
    }
}

// `header` must sit at offset 0 so `erase` can cast `Arc<Message<T>>` down to a
// `NonNull<Header>` and back; `repr(C)` on `Message<T>` guarantees that.
// `Arc::into_raw` hands out a pointer to the `Message<T>` itself — not to the
// enclosing `ArcInner` — so the offset-0 cast holds for the `Arc` layout too.
// The `Header`/`Endpoint` types stay `repr(Rust)` — the allocation never leaves
// the process, so no stable wire layout is needed and `Option<u32>` is free to
// use.
#[repr(C)]
pub struct Message<T> {
    pub header: Header,
    pub payload: T,
}

impl<T> Message<T> {
    pub fn new(payload: T) -> Arc<Self> {
        Arc::new(Message {
            header: Header::default(),
            payload,
        })
    }

    /// Like [`new`](Self::new) but stamps the routing metadata up front, for the
    /// request/reply path.
    pub fn with_header(header: Header, payload: T) -> Arc<Self> {
        Arc::new(Message { header, payload })
    }
}

impl<T: Send + 'static> Message<T> {
    /// Consume one strong reference and hand it to the wire as a thin pointer.
    ///
    /// The reference is *transferred*, not duplicated: the caller keeps whatever
    /// clones it still holds, and the receiver's [`into_typed`] reclaims exactly
    /// the one given up here. Clone before erasing to keep a handle on the
    /// payload, or to fan the same allocation out to several peers.
    ///
    /// Only `Send` is required. Erasing with a clone still alive is what makes
    /// two threads able to hold a `&T` at once, and that — not the crossing
    /// itself — is what `T: Sync` would cover. The bound is deliberately not
    /// imposed here: the move-only path never needs it, and a caller that does
    /// share a `!Sync` payload is expected to arrange its own discipline around
    /// it (interior-mutability-free data, a handoff protocol, its own locking).
    ///
    /// [`into_typed`]: ErasedMessage::into_typed
    ///
    /// # Safety
    /// The erased reference is meant to cross to another thread. If any clone
    /// of `self` outlives this call, the payload is then shared between that
    /// thread and this one, which must be sound for `T` — always so for a
    /// `T: Sync`. Dropping every clone first, the plain move, discharges it.
    pub unsafe fn erase(self: Arc<Self>) -> ErasedMessage {
        // SAFETY-adjacent: `Arc::into_raw` is never null, and `repr(C)` puts
        // `header` at offset 0 so the downcast to `Header` is in-bounds.
        let ptr = Arc::into_raw(self) as *mut Header;
        ErasedMessage {
            ptr: unsafe { NonNull::new_unchecked(ptr) },
        }
    }
}

/// Must not be dropped while erased: it carries no destructor, so dropping it
/// as-is leaks the strong reference it holds. Consume it via
/// [`into_typed`](ErasedMessage::into_typed) to recover the concrete
/// `Arc<Message<T>>`, which restores normal refcount handling.
#[derive(Debug)]
pub struct ErasedMessage {
    ptr: NonNull<Header>,
}

unsafe impl Send for ErasedMessage {}

impl ErasedMessage {
    #[inline]
    pub fn header(&self) -> &Header {
        unsafe { self.ptr.as_ref() }
    }

    #[inline]
    pub fn into_raw(self) -> u64 {
        self.ptr.as_ptr() as u64
    }

    /// # Safety
    /// `raw` must be a non-null pointer from a prior `into_raw` that has not yet
    /// been reconstructed. Reconstructing it twice releases one strong reference
    /// too many, which can free the allocation while clones still point at it.
    #[inline]
    pub unsafe fn from_raw(raw: u64) -> Self {
        ErasedMessage {
            ptr: NonNull::new(raw as *mut Header).expect("null message pointer"),
        }
    }

    /// Reclaim the strong reference this pointer stands for.
    ///
    /// The returned `Arc` is not necessarily unique — the sender may have kept
    /// clones. Use [`Arc::into_inner`] to take ownership of the payload when it
    /// is (it returns `None` otherwise), or read through `&*msg` when sharing is
    /// expected.
    ///
    /// # Safety
    /// `T` must be the type the sender allocated the message with.
    #[inline]
    pub unsafe fn into_typed<T: 'static>(self) -> Arc<Message<T>> {
        unsafe { Arc::from_raw(self.ptr.as_ptr() as *const Message<T>) }
    }
}
