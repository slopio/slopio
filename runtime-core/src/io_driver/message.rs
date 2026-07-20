use std::ptr::NonNull;

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

// `header` must sit at offset 0 so `erase` can cast `Box<Message<T>>` down to a
// `NonNull<Header>` and back; `repr(C)` on `Message<T>` guarantees that. The
// `Header`/`Endpoint` types stay `repr(Rust)` — the allocation never leaves the
// process, so no stable wire layout is needed and `Option<u32>` is free to use.
#[repr(C)]
pub struct Message<T> {
    pub header: Header,
    pub payload: T,
}

impl<T: Send + 'static> Message<T> {
    pub fn new(payload: T) -> Box<Self> {
        Box::new(Message {
            header: Header::default(),
            payload,
        })
    }

    /// Like [`new`](Self::new) but stamps the routing metadata up front, for the
    /// request/reply path.
    pub fn with_header(header: Header, payload: T) -> Box<Self> {
        Box::new(Message { header, payload })
    }

    pub fn erase(self: Box<Self>) -> ErasedMessage {
        ErasedMessage {
            ptr: NonNull::from(Box::leak(self)).cast::<Header>(),
        }
    }
}

/// Must not be dropped while erased: it carries no destructor, so dropping it
/// as-is leaks. Consume it via [`into_typed`](ErasedMessage::into_typed) to
/// recover the concrete `Box<Message<T>>`, which frees it.
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
    /// been reconstructed. Reconstructing it twice double-frees.
    #[inline]
    pub unsafe fn from_raw(raw: u64) -> Self {
        ErasedMessage {
            ptr: NonNull::new(raw as *mut Header).expect("null message pointer"),
        }
    }

    /// # Safety
    /// `T` must be the type the sender allocated the message with.
    #[inline]
    pub unsafe fn into_typed<T: 'static>(self) -> Box<Message<T>> {
        unsafe { Box::from_raw(self.ptr.as_ptr() as *mut Message<T>) }
    }
}
