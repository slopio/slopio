use std::{ptr::NonNull, task::Waker};

use crate::task::inner_parts::Metadata;

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
        println!(">>{}", self.header().counter.get());
        if self.header().counter.get() == 1 {
            self.free();
        } else {
            self.decrement_count();
            self.set_detached();
        }
    }

    fn set_detached(&self) {
        self.header().detached.replace(true);
    }

    fn decrement_count(&self) {
        self.header().counter.update(|c| c - 1);
    }

    pub(crate) fn increment_count(&self) {
        self.header().counter.update(|c| c + 1);
    }

    pub fn poll(self) {
        self.decrement_count();
        let vtable = self.header().vtable;
        unsafe { (vtable.poll)(self.ptr) }
    }

    pub fn get_ouput(self, dst: *mut (), waker: &Waker) {
        let vtable = self.header().vtable;
        unsafe { (vtable.get_output)(self.ptr, dst, waker) };
    }
}
