use std::ptr::NonNull;

#[repr(C)]
pub struct Header {
    pub src_thread: u16,
    pub dst_thread: u16,
    pub src_slab: u32,
}

#[repr(C)]
pub struct Message<T> {
    pub header: Header,
    pub payload: T,
}

impl<T: Send + 'static> Message<T> {
    pub fn new(payload: T) -> Box<Self> {
        Box::new(Message {
            header: Header {
                src_thread: 0,
                dst_thread: 0,
                src_slab: 0,
            },
            payload,
        })
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
