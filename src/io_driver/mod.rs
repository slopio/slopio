use std::{cell::UnsafeCell, rc::Rc, task::Waker};

use io_uring::{IoUring, squeue::Entry};
use slab::Slab;

use crate::scoped_thread_local;
pub mod demo;
mod operations;

#[derive(Default)]
pub struct IODriver {
    slab: Rc<UnsafeCell<Slab<UringOperation>>>,
    ring: Ring,
}

impl Default for Ring {
    fn default() -> Self {
        Self {
            inner: IoUring::new(1024).unwrap(),
        }
    }
}

scoped_thread_local!(pub(crate) static IO_DRIVER: IODriver);

pub struct OpOp;

impl IODriver {
    pub fn with<R>(&self, f: impl FnOnce() -> R) -> R {
        IO_DRIVER.set(self, f)
    }

    pub fn push_op(&mut self, entry: &mut Entry) {
        let slab = unsafe { &mut *self.slab.get() };
        let user_data = slab.insert(UringOperation::Init) as u64;
        entry.set_user_data(user_data);
        self.ring.add(entry);
    }

    pub fn progress(&self, id: usize, waker: &Waker) -> std::task::Poll<()> {
        let slab = unsafe { &mut *self.slab.get() };
        let entry = slab.get_mut(id).unwrap();
        match entry {
            UringOperation::Init => {
                *entry = UringOperation::Sent(waker.clone());
                std::task::Poll::Pending
            }
            UringOperation::Sent(waker) => {
                waker.wake_by_ref();
                std::task::Poll::Ready(())
            }
        }
    }

    pub fn tick(&self) {
        let slab = unsafe { &mut *self.slab.get() };
        for _ in slab {}
    }

    pub fn submit_op(&self, _op: OpOp) -> usize {
        let slab = unsafe { &mut *self.slab.get() };
        slab.insert(UringOperation::Init)
    }

    fn complete(&self, slab_index: usize) {
        let slab = unsafe { &mut *self.slab.get() };
        let entry = slab.get_mut(slab_index).unwrap();
        match entry.completion() {
            CompletionEnd::ToBeDeleted => {
                slab.remove(slab_index);
            }
            CompletionEnd::Persistant => {}
        }
    }
}

enum UringOperation {
    Init,
    Sent(Waker),
}

impl UringOperation {
    fn completion(&mut self) -> CompletionEnd {
        CompletionEnd::ToBeDeleted
    }
}

#[allow(dead_code)]
enum CompletionEnd {
    ToBeDeleted,
    Persistant,
}

struct Ring {
    inner: IoUring,
}

impl Ring {
    pub(crate) fn add(&mut self, entry: &Entry) {
        unsafe {
            self.inner.submission().push(entry).unwrap();
        }
    }
}
