use std::{ptr::NonNull, task::Waker};

use crate::{task::inner_parts::Metadata, tracing::set_current_task_tracing};

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

    pub fn free(self) {
        let vtable = self.header().vtable;
        unsafe { (vtable.free)(self.ptr) }
    }

    pub fn detach(self) {
        let vtable = self.header().vtable;
        unsafe { (vtable.detach)(self.ptr) }
    }

    pub fn poll(self) {
        let header = unsafe { &mut *self.ptr.as_ptr() };
        set_current_task_tracing(&mut header.tracing);
        unsafe { (header.vtable.poll)(self.ptr) }
    }

    pub fn get_ouput(self, dst: *mut (), waker: &Waker) {
        let vtable = self.header().vtable;
        unsafe { (vtable.get_output)(self.ptr, dst, waker) };
    }
}
