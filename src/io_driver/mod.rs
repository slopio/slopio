use std::{cell::UnsafeCell, rc::Rc, task::Waker};

use io_uring::{
    IoUring,
    cqueue::Entry,
    opcode,
    squeue::{self},
};
use slab::Slab;

use crate::{io_driver::operations::IoDriverToken, scoped_thread_local};
pub mod operations;

#[derive(Default)]
pub struct IODriver {
    slab: Rc<UnsafeCell<Slab<OpState>>>,
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
        let user_data = slab.insert(OpState::Init) as IoDriverToken;
        entry.set_user_data(user_data);
        ring_inner.add(&entry);
        user_data
    }

    fn poll_op(&self, id: usize, waker: &Waker) -> std::task::Poll<OpMetadata> {
        let slab = unsafe { &mut *self.slab.get() };
        let entry = slab.get_mut(id).unwrap();
        let mut id_to_delete = None;
        let res = match entry {
            OpState::Init => {
                *entry = OpState::Sent(waker.clone());
                std::task::Poll::Pending
            }
            OpState::Sent(waker) => {
                waker.wake_by_ref();
                std::task::Poll::Pending
            }
            OpState::Ended(e) => {
                id_to_delete = Some(id);
                std::task::Poll::Ready(*e)
            }
            OpState::CancellationAsked => unimplemented!("should not poll this"),
            _ => {
                unreachable!("this should never get polled")
            }
        };
        if let Some(id) = id_to_delete {
            slab.remove(id);
        }
        res
    }

    pub(crate) fn submit(&self) {
        let ring = unsafe { &mut *self.ring.get() };
        ring.inner.submit().unwrap();
    }

    pub(crate) fn process_completion_queue(&self) {
        let ring = unsafe { &mut *self.ring.get() };
        for c_event in ring.inner.completion() {
            self.handle_completion(c_event);
        }
    }

    pub fn cancel_op(&self, id: usize) {
        let ring_inner = unsafe { &mut *self.ring.get() };
        let slab = unsafe { &mut *self.slab.get() };
        let op_to_cancel = slab.get_mut(id).unwrap();
        //TODO manage marked for deletion
        if let OpState::Ended(_) = op_to_cancel {
            *op_to_cancel = OpState::CancellationAsked;
        } else {
            let cancel_id = slab.insert(OpState::CancellationOperation(id));
            let cancel_op = opcode::AsyncCancel::new(cancel_id as u64)
                .build()
                .user_data(cancel_id as u64);
            ring_inner.add(&cancel_op);
        }
    }

    fn handle_completion(&self, entry: Entry) {
        let slab = unsafe { &mut *self.slab.get() };
        let slab_index = entry.user_data() as usize;
        let slab_entry = slab.get_mut(slab_index).unwrap();
        let mut index_to_remove = None;
        match slab_entry {
            OpState::Init => unreachable!("submitted entry should't be in init"),
            OpState::Sent(waker) => {
                waker.wake_by_ref();
                std::mem::swap(
                    slab_entry,
                    &mut OpState::Ended(OpMetadata {
                        result: entry.result(),
                        flags: entry.flags(),
                    }),
                );
            }
            OpState::Ended(_) => {
                unreachable!("polled ended op");
            }
            OpState::CancellationAsked => {
                match -entry.result() {
                    libc::ECANCELED => {
                        println!("successful cancellation")
                    }
                    n if n <= 0 => {
                        //TODO solve buffer issues?
                        println!("was not cancelled");
                    }
                    _ => unreachable!(),
                };
                index_to_remove = Some(slab_index);
            }
            OpState::CancellationOperation(cancelled_op_index) => match -entry.result() {
                0 => index_to_remove = Some(slab_index),
                libc::ECANCELED => {
                    //todo warn
                    println!("cancelled operations could not be found");
                    index_to_remove = Some(*cancelled_op_index);
                }
                libc::EALREADY => {
                    unreachable!("operation was already cancelled");
                }
                _ => {}
            },
        }
        if let Some(index) = index_to_remove {
            slab.remove(index);
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct OpMetadata {
    result: i32,
    flags: u32,
}

#[derive(Debug)]
enum OpState {
    Init,
    Sent(Waker),
    Ended(OpMetadata),
    CancellationAsked,
    CancellationOperation(usize),
}

impl OpState {
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
