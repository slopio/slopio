use std::{cell::UnsafeCell, rc::Rc, task::Waker};

use io_uring::{
    IoUring,
    cqueue::Entry,
    squeue::{self},
};
use slab::Slab;

use crate::{io_driver::operations::IoDriverToken, scoped_thread_local};
pub mod operations;

#[derive(Default)]
pub struct IODriver {
    slab: Rc<UnsafeCell<Slab<UringOperation>>>,
    ring: UnsafeCell<Ring>,
}

impl Default for Ring {
    fn default() -> Self {
        Self {
            inner: IoUring::new(1024).unwrap(),
        }
    }
}

scoped_thread_local!(pub(crate) static IO_DRIVER: IODriver);

impl IODriver {
    pub fn with<R>(&self, f: impl FnOnce() -> R) -> R {
        IO_DRIVER.set(self, f)
    }

    pub fn push_op(&self, mut entry: squeue::Entry) -> IoDriverToken {
        let slab = unsafe { &mut *self.slab.get() };
        let ring_inner = unsafe { &mut *self.ring.get() };
        let user_data = slab.insert(UringOperation::Init) as IoDriverToken;
        entry.set_user_data(user_data);
        ring_inner.add(&entry);
        user_data
    }

    pub fn poll_op(&self, id: usize, waker: &Waker) -> std::task::Poll<OpMetadata> {
        let slab = unsafe { &mut *self.slab.get() };
        let entry = slab.get_mut(id).unwrap();
        match entry {
            UringOperation::Init => {
                *entry = UringOperation::Sent(waker.clone());
                std::task::Poll::Pending
            }
            UringOperation::Sent(waker) => {
                waker.wake_by_ref();
                std::task::Poll::Pending
            }
            UringOperation::Ended(e) => std::task::Poll::Ready(*e),
        }
    }

    pub fn submit(&self) {
        let ring = unsafe { &mut *self.ring.get() };
        ring.inner.submit().unwrap();
    }

    pub fn process_completion_queue(&self) {
        let ring = unsafe { &mut *self.ring.get() };
        for c_event in ring.inner.completion() {
            self.process_completion(c_event);
        }
    }

    fn process_completion(&self, entry: Entry) {
        let slab = unsafe { &mut *self.slab.get() };
        let slab_index = entry.user_data() as usize;
        let slab_entry = slab.get_mut(slab_index).unwrap();
        match slab_entry {
            UringOperation::Init => unreachable!("submitted entry should't be in init"), //sure?
            UringOperation::Sent(waker) => {
                waker.wake_by_ref();
            }
            UringOperation::Ended(_) => {
                unreachable!("should not be in ended state");
            }
        }
        std::mem::swap(
            slab_entry,
            &mut UringOperation::Ended(OpMetadata {
                result: entry.result(),
                flags: entry.flags(),
            }),
        );
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct OpMetadata {
    result: i32,
    flags: u32,
}

#[derive(Debug)]
enum UringOperation {
    Init,
    Sent(Waker),
    Ended(OpMetadata),
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
    pub(crate) fn add(&mut self, entry: &squeue::Entry) {
        unsafe {
            self.inner.submission().push(entry).unwrap();
        }
    }
}
