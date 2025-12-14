use std::{cell::UnsafeCell, mem, rc::Rc, task::Waker};

use slab::Slab;

use crate::scoped_thread_local;
pub mod demo;

#[derive(Default)]
pub struct IODriver {
    slab: Rc<UnsafeCell<Slab<UringOperation>>>,
}

scoped_thread_local!(pub(crate) static IO_DRIVER: IODriver);

struct OpCode;

impl IODriver {
    pub fn with<R>(&self, f: impl FnOnce() -> R) -> R {
        IO_DRIVER.set(self, f)
    }

    pub fn poll(&self, id: usize, waker: &Waker) -> std::task::Poll<()> {
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
        for i in slab {}
    }

    pub fn submit_op(&self, op: OpCode) -> usize {
        let slab = unsafe { &mut *self.slab.get() };
        //TODO turn insert op in submission queue.
        slab.insert(UringOperation::Init)
    }

    pub fn complete(&self, slab_index: usize) {
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

enum CompletionEnd {
    ToBeDeleted,
    Persistant,
}
