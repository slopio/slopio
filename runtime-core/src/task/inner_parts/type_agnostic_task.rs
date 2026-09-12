use std::{ptr::NonNull, task::Waker};

use crate::runtime::TASK_LIST;
use crate::task::inner_parts::{Metadata, UNLISTED};
#[cfg(feature = "tracing")]
use crate::tracing::swap_current_task_tracing;

#[derive(Debug)]
pub struct TypeAgnosticTask {
    ptr: NonNull<Metadata>,
}

impl Copy for TypeAgnosticTask {}

impl Clone for TypeAgnosticTask {
    fn clone(&self) -> Self {
        *self
    }
}

impl TypeAgnosticTask {
    #[must_use]
    pub fn new(ptr: NonNull<Metadata>) -> Self {
        Self { ptr }
    }

    fn header(&self) -> &Metadata {
        unsafe { self.ptr.as_ref() }
    }

    pub fn detach(self) {
        let vtable = self.header().vtable;
        unsafe { (vtable.detach)(self.ptr) }
    }

    /// Marks the task killed and queues it; the scheduler drops and frees it on the next pop.
    #[cold]
    pub fn kill(self) {
        self.header().detached_mode.set(true);
        self.abort();
    }

    /// Marks the task killed and queues it; the box stays until its handle is dropped.
    pub(crate) fn abort(self) {
        let header = self.header();
        header.killed.set(true);
        if header.queued.get() {
            return;
        }
        header.queued.set(true);
        TASK_LIST.with(|ctx| ctx.push(self));
    }

    pub(crate) fn set_live(&self, index: u32) {
        self.header().live.set(index);
    }

    pub(crate) fn unlist(&self) {
        self.header().live.set(UNLISTED);
    }

    pub fn poll(self) {
        let header = unsafe { &mut *self.ptr.as_ptr() };
        #[cfg(feature = "tracing")]
        let prev = swap_current_task_tracing(&mut header.tracing);
        unsafe { (header.vtable.poll)(self.ptr) };
        #[cfg(feature = "tracing")]
        swap_current_task_tracing(prev);
    }

    ///# Safety
    pub unsafe fn get_ouput(self, dst: *mut (), waker: &Waker) {
        let vtable = self.header().vtable;
        unsafe { (vtable.get_output)(self.ptr, dst, waker) };
    }
}
