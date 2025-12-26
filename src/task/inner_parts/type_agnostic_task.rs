use std::{ptr::NonNull, task::Waker};

use crate::task::inner_parts::TypeInformation;

#[derive(Debug)]
pub struct TypeAgnosticTask {
    ptr: NonNull<TypeInformation>,
}

impl Copy for TypeAgnosticTask {}

impl Clone for TypeAgnosticTask {
    fn clone(&self) -> Self {
        *self
    }
}

impl TypeAgnosticTask {
    #[must_use]
    pub fn new(ptr: NonNull<TypeInformation>) -> Self {
        Self { ptr }
    }

    fn header(&self) -> &TypeInformation {
        unsafe { self.ptr.as_ref() }
    }

    pub fn free(self) {
        let vtable = self.header().vtable;
        unsafe { (vtable.free)(self.ptr) }
    }

    pub fn poll(self) {
        let vtable = self.header().vtable;
        unsafe { (vtable.poll)(self.ptr) }
    }

    pub fn get_ouput(self, dst: *mut (), waker: &Waker) {
        let vtable = self.header().vtable;
        unsafe { (vtable.get_output)(self.ptr, dst, waker) };
    }
}
