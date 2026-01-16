use std::{
    cell::{Cell, UnsafeCell},
    collections::VecDeque,
    os::fd::RawFd,
    rc::Rc,
    task::Waker,
};

use io_uring::{
    IoUring,
    cqueue::Entry,
    opcode,
    squeue::{self},
    types::Fd,
};
use io_uring_buf_ring::{BorrowedBuffer, IoUringBufRing};
use slab::Slab;

use crate::{io_driver::operations::IoDriverToken, scoped_thread_local};
pub mod operations;

const MULTI_OP_FLAG: u64 = 1u64 << 63;

const QUEUE_PAUSE_THRESHOLD: usize = 64;

#[derive(Default)]
pub struct IODriver {
    ring: UnsafeCell<Ring>,
    slab: Rc<UnsafeCell<Slab<OpState>>>,
    multishot_slab: Rc<UnsafeCell<Slab<MultiOpState>>>,
    shutting_down: Cell<bool>,
}

#[derive(Clone, Copy, Debug)]
struct MultishotCompletionEvent {
    pub result: i32,
    pub flags: u32,
}

enum MultiOpState {
    Init {
        fd: i32,
    },
    Sent {
        waker: Waker,
        woken_up: bool,
        fd: i32,
        queue: VecDeque<MultishotCompletionEvent>,
    },
    Paused {
        waker: Waker,
        fd: i32,
        queue: VecDeque<MultishotCompletionEvent>,
    },
}

impl MultiOpState {
    fn wake(&mut self) {
        match self {
            MultiOpState::Init { .. } => {}
            MultiOpState::Sent {
                waker, woken_up, ..
            } => {
                waker.wake_by_ref();
                *woken_up = true;
            }
            MultiOpState::Paused { waker, .. } => {
                waker.wake_by_ref();
            }
        }
    }

    // fn clean(&mut self, w: Waker) {
    //     match self {
    //         MultiOpState::Init { .. } => {}
    //         MultiOpState::Sent {
    //             waker,
    //             woken_up,
    //             queue,
    //             ..
    //         } => {
    //             queue.clear();
    //             *waker = w;
    //             *woken_up = false;
    //         }
    //         MultiOpState::Paused { .. } => {}
    //     }
    // }
}

impl Default for Ring {
    fn default() -> Self {
        let mut builder = IoUring::builder();
        let ring = builder
            .setup_coop_taskrun()
            .setup_single_issuer()
            .setup_defer_taskrun()
            .build(4096)
            .unwrap();
        let buf_ring = IoUringBufRing::new_with_flags(&ring, 1024, 0, 1 << 14, 0).unwrap();
        Self {
            buffer_ring: buf_ring,
            inner: { ring },
        }
    }
}

scoped_thread_local!(pub(crate) static IO_DRIVER: IODriver);

impl IODriver {
    pub fn shutdown(&self) -> bool {
        self.shutting_down.get()
    }

    pub fn with<R>(&self, f: impl FnOnce() -> R) -> R {
        IO_DRIVER.set(self, f)
    }

    pub fn get_buffer(&self, id: u16, len: usize) -> BorrowedBuffer<'static, Vec<u8>> {
        unsafe {
            let ring_inner = &mut *self.ring.get();
            std::mem::transmute(ring_inner.buffer_ring.get_buf(id, len).unwrap())
        }
    }

    pub fn push_multishot_op(&self, mut entry: squeue::Entry, fd: i32) -> IoDriverToken {
        let slab_multishot = unsafe { &mut *self.multishot_slab.get() };
        let ring_inner = unsafe { &mut *self.ring.get() };
        let user_data = slab_multishot.insert(MultiOpState::Init { fd }) as IoDriverToken;
        entry.set_user_data(user_data | MULTI_OP_FLAG);
        ring_inner.add(&entry);
        user_data
    }

    pub fn push_op(&self, mut entry: squeue::Entry) -> IoDriverToken {
        let slab = unsafe { &mut *self.slab.get() };
        let ring_inner = unsafe { &mut *self.ring.get() };
        let user_data = slab.insert(OpState::Init) as IoDriverToken;
        entry.set_user_data(user_data);
        ring_inner.add(&entry);
        user_data
    }

    pub fn push_signal_op(&self, fd: RawFd) -> IoDriverToken {
        let slab = unsafe { &mut *self.slab.get() };
        let ring_inner = unsafe { &mut *self.ring.get() };
        let mut sig_data = Box::new([0u8; 8]);
        let ptr = sig_data.as_mut_ptr();
        let user_data = slab.insert(OpState::Signal(sig_data)) as IoDriverToken;
        let mut entry = opcode::Read::new(Fd(fd), ptr, 8).build();
        entry.set_user_data(user_data);
        ring_inner.add(&entry);
        user_data
    }

    fn poll_op_multishot(
        &self,
        id: usize,
        waker: &Waker,
        poller_queue: &mut VecDeque<MultishotCompletionEvent>,
    ) -> std::task::Poll<()> {
        //if you care about the order of received data, call only if poller_queue is empty
        let slab_multishot = unsafe { &mut *self.multishot_slab.get() };
        let entry = unsafe { slab_multishot.get_unchecked_mut(id) };
        match entry {
            MultiOpState::Init { fd } => {
                let fd_copy = *fd;
                *entry = MultiOpState::Sent {
                    waker: waker.clone(),
                    woken_up: false,
                    fd: fd_copy,
                    queue: VecDeque::new(),
                };
                std::task::Poll::Pending
            }
            MultiOpState::Sent {
                queue: provision,
                waker: existant_waker,
                ..
            } => {
                if !provision.is_empty() {
                    std::mem::swap(poller_queue, provision);
                    std::task::Poll::Ready(())
                } else {
                    *existant_waker = waker.clone();
                    std::task::Poll::Pending
                }
            }
            MultiOpState::Paused {
                waker, queue, fd, ..
            } => {
                println!("resuming");
                self.resume_multirecv(id, *fd);
                std::mem::swap(poller_queue, queue);
                *entry = MultiOpState::Sent {
                    waker: waker.clone(),
                    woken_up: false,
                    fd: *fd,
                    queue: std::mem::take(queue),
                };
                std::task::Poll::Pending
            }
        }
    }

    fn poll_op(&self, id: usize, waker: &Waker) -> std::task::Poll<OpMetadata> {
        let slab = unsafe { &mut *self.slab.get() };
        let entry = unsafe { slab.get_unchecked_mut(id) };
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

    pub(crate) fn has_pending_submissions(&self) -> bool {
        let ring = unsafe { &mut *self.ring.get() };
        !ring.inner.submission().is_empty()
    }

    pub(crate) fn submit_without_wait(&self) {
        let ring = unsafe { &mut *self.ring.get() };
        match ring.inner.submit() {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {
                // EINTR is normal when a signal interrupts the syscall, just continue
            }
            Err(e) => {
                panic!("io_uring submit failed: {:?}", e);
            }
        }
    }

    pub(crate) fn submit_and_wait(&self) {
        let ring = unsafe { &mut *self.ring.get() };
        match ring.inner.submit_and_wait(1) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {
                // EINTR is normal when a signal interrupts the syscall, just continue
            }
            Err(e) => {
                // For other errors, panic
                panic!("io_uring submit_and_wait failed: {:?}", e);
            }
        }
    }

    pub(crate) fn process_completion_queue(&self) {
        let ring = unsafe { &mut *self.ring.get() };
        for c_event in ring.inner.completion() {
            if c_event.user_data() & MULTI_OP_FLAG != 0 {
                self.handle_completion_multishot(c_event);
            } else {
                self.handle_completion(c_event)
            }
        }
    }

    pub fn cancel_op(&self, id: usize) {
        let ring_inner = unsafe { &mut *self.ring.get() };
        let slab = unsafe { &mut *self.slab.get() };
        let op_to_cancel = unsafe { slab.get_unchecked_mut(id) };
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

    pub fn cancel_multishot_op(&self, id: usize) {
        let ring_inner = unsafe { &mut *self.ring.get() };
        let slab = unsafe { &mut *self.slab.get() };
        let cancel_id = slab.insert(OpState::Init);
        let user_data_to_cancel = (id as u64) | MULTI_OP_FLAG;
        let cancel_op = opcode::AsyncCancel::new(user_data_to_cancel)
            .build()
            .user_data(cancel_id as u64);
        ring_inner.add(&cancel_op);
    }

    pub fn resume_multirecv(&self, id: usize, fd: i32) {
        let ring_inner = unsafe { &mut *self.ring.get() };
        let entry = opcode::RecvMulti::new(Fd(fd), 0)
            .build()
            .user_data((id as u64) | MULTI_OP_FLAG);
        ring_inner.add(&entry);
    }

    fn handle_completion_multishot(&self, entry: Entry) {
        let index = entry.user_data() & !MULTI_OP_FLAG;
        let slab_multi = unsafe { &mut *self.multishot_slab.get() };
        let multi = unsafe { slab_multi.get_unchecked_mut(index as usize) };
        match multi {
            MultiOpState::Init { .. } => unimplemented!("init"),
            MultiOpState::Sent {
                woken_up, queue, ..
            } => {
                queue.push_back(MultishotCompletionEvent {
                    result: entry.result(),
                    flags: entry.flags(),
                });
                let should_wake = !*woken_up;
                let should_pause = queue.len() >= QUEUE_PAUSE_THRESHOLD;

                if should_wake {
                    multi.wake();
                }

                // Check if we need to pause due to queue overflow
                if should_pause {
                    // Transition to Paused state
                    if let MultiOpState::Sent {
                        waker, fd, queue, ..
                    } = multi
                    {
                        let paused_state = MultiOpState::Paused {
                            waker: waker.clone(),
                            fd: *fd,
                            queue: std::mem::take(queue),
                        };
                        *multi = paused_state;

                        // Cancel the multishot operation
                        self.cancel_multishot_op(index as usize);
                    }
                }
            }
            MultiOpState::Paused { queue, .. } => {
                // Still receiving events while paused (race condition)
                // Just queue them
                queue.push_back(MultishotCompletionEvent {
                    result: entry.result(),
                    flags: entry.flags(),
                });
            }
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
                *slab_entry = OpState::Ended(OpMetadata {
                    result: entry.result(),
                    flags: entry.flags(),
                });
            }
            OpState::Ended(_) => {
                unreachable!("polled ended op");
            }
            OpState::CancellationAsked => {
                //todo
                match -entry.result() {
                    libc::ECANCELED => {}
                    n if n <= 0 => {}
                    _ => unreachable!(),
                };
                index_to_remove = Some(slab_index);
            }
            OpState::CancellationOperation(cancelled_op_index) => match -entry.result() {
                0 => index_to_remove = Some(slab_index),
                libc::ECANCELED => {
                    //todo warn
                    index_to_remove = Some(*cancelled_op_index);
                }
                libc::EALREADY => {
                    unreachable!("operation was already cancelled");
                }
                _ => {}
            },
            OpState::Signal(_arr) => {
                // let value = u64::from_ne_bytes(**arr);
                self.shutting_down.replace(true);
            }
        }
        if let Some(index) = index_to_remove {
            slab.remove(index);
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct OpMetadata {
    result: i32,
    #[allow(dead_code)]
    flags: u32,
}

#[allow(dead_code)]
#[derive(Debug)]
struct Event {}

#[derive(Debug)]
enum OpState {
    Init,
    Sent(Waker),
    Ended(OpMetadata),
    CancellationAsked,
    CancellationOperation(usize),
    Signal(Box<[u8; 8]>),
}

struct Ring {
    inner: IoUring,
    buffer_ring: IoUringBufRing<Vec<u8>>,
}

impl Ring {
    pub(crate) fn add(&mut self, entry: &squeue::Entry) {
        unsafe {
            self.inner.submission().push(entry).unwrap();
        }
    }
}
