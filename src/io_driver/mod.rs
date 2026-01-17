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
use libc::ECANCELED;
use slab::Slab;

use crate::{io_driver::operations::IoDriverToken, scoped_thread_local};
pub mod operations;

const MULTI_OP_FLAG: u64 = 1u64 << 63;
const CANCEL_OP_FLAG: u64 = 1u64 << 62;
pub const RING_BUFFER_SIZE: usize = 10 << 12;

const QUEUE_PAUSE_THRESHOLD: usize = 54;

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

impl From<Entry> for MultishotCompletionEvent {
    fn from(value: Entry) -> Self {
        Self {
            result: value.result(),
            flags: value.flags(),
        }
    }
}

struct MultiOpQueue {
    inner: VecDeque<MultishotCompletionEvent>,
}

enum QueueInsertEvent {
    Ok,
    TooFull,
    WasEmpty,
}

impl MultiOpQueue {
    fn has_content(&self) -> bool {
        !self.inner.is_empty()
    }

    fn push(&mut self, element: MultishotCompletionEvent) -> QueueInsertEvent {
        self.inner.push_back(element);
        println!("len: {} {}", self.inner.len(), element.result);
        match self.inner.len() {
            1 => QueueInsertEvent::WasEmpty,
            k if k > QUEUE_PAUSE_THRESHOLD => QueueInsertEvent::TooFull,
            _ => QueueInsertEvent::Ok,
        }
    }

    fn pop(&mut self) -> Option<MultishotCompletionEvent> {
        self.inner.pop_front()
    }
}

impl Default for MultiOpQueue {
    fn default() -> Self {
        Self {
            inner: VecDeque::with_capacity(QUEUE_PAUSE_THRESHOLD + 10),
        }
    }
}

impl MultiOpQueue {}

enum MultiOpState {
    Init {
        fd: i32,
    },
    Sent {
        waker: Waker,
        // woken_up: bool,
        fd: i32,
        queue: MultiOpQueue,
    },
    TringToPause {
        waker: Waker,
        fd: i32,
        queue: MultiOpQueue,
        last_cqe: bool,
        ecancelled: bool,
    },
    Paused {
        waker: Waker,
        fd: i32,
        queue: MultiOpQueue,
    },
}

impl MultiOpState {
    fn transition_init_to_sent(&mut self, waker: &Waker) {
        println!("init to send");
        if let MultiOpState::Init { fd } = self {
            *self = MultiOpState::Sent {
                waker: waker.clone(),
                fd: *fd,
                queue: Default::default(),
            };
        }
    }
    fn transition_to_paused(&mut self) {
        println!("to paused");
        if let MultiOpState::TringToPause {
            waker, fd, queue, ..
        } = self
        {
            *self = MultiOpState::Paused {
                waker: waker.clone(),
                fd: *fd,
                queue: std::mem::take(queue),
            }
        }
    }

    fn transition_to_trying_to_pause(&mut self) {
        println!("trying to pause");
        if let MultiOpState::Sent { waker, fd, queue } = self {
            *self = Self::TringToPause {
                waker: waker.clone(),
                fd: *fd,
                queue: std::mem::take(queue),
                last_cqe: false,
                ecancelled: false,
            };
        } else {
            unreachable!()
        }
    }

    fn wake(&self) {
        match self {
            MultiOpState::Init { .. } => {}
            MultiOpState::Sent { waker, .. } => {
                waker.wake_by_ref();
                // *woken_up = true;
            }
            MultiOpState::Paused { waker, .. } => {
                waker.wake_by_ref();
            }
            MultiOpState::TringToPause { waker, .. } => {
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
        let buf_ring = IoUringBufRing::new_with_flags(&ring, 1024, 0, RING_BUFFER_SIZE, 0).unwrap();
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
        poller_queue: &mut MultiOpQueue,
    ) -> std::task::Poll<()> {
        //if you care about the order of received data, call only if poller_queue is empty
        let slab_multishot = unsafe { &mut *self.multishot_slab.get() };
        let multishot_op = unsafe { slab_multishot.get_unchecked_mut(id) };
        match multishot_op {
            MultiOpState::Init { .. } => {
                multishot_op.transition_init_to_sent(waker);
                std::task::Poll::Pending
            }
            MultiOpState::Sent {
                queue: provision,
                waker: existant_waker,
                ..
            } => {
                if provision.has_content() {
                    std::mem::swap(poller_queue, provision);
                    std::task::Poll::Ready(())
                } else {
                    *existant_waker = waker.clone();
                    std::task::Poll::Pending
                }
            }
            MultiOpState::TringToPause {
                waker, fd, queue, ..
            } => std::task::Poll::Pending,
            MultiOpState::Paused {
                waker, queue, fd, ..
            } => todo!(),
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
            if c_event.user_data() & CANCEL_OP_FLAG != 0 {
                continue; //we don't treat cancel completion events
            }
            if c_event.user_data() & MULTI_OP_FLAG != 0 {
                self.handle_completion_multishot(c_event);
            } else {
                self.handle_completion(c_event)
            }
        }
        println!("WOW");
    }

    pub fn send_multiop_cancel_event(&self, id: usize) {
        let ring_inner = unsafe { &mut *self.ring.get() };
        let cancel_op = opcode::AsyncCancel::new(id as u64)
            .build()
            .user_data(id as u64 | MULTI_OP_FLAG | CANCEL_OP_FLAG);
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
        let slab_index = (entry.user_data() & !MULTI_OP_FLAG) as usize;
        let slab_multi = unsafe { &mut *self.multishot_slab.get() };
        let multi_op = unsafe { slab_multi.get_unchecked_mut(slab_index) };
        match multi_op {
            MultiOpState::Init { .. } => unimplemented!("should not be in init state now."),
            MultiOpState::Sent { queue, .. } => match queue.push(entry.into()) {
                QueueInsertEvent::Ok => {}
                QueueInsertEvent::TooFull => {
                    self.send_multiop_cancel_event(slab_index);
                    multi_op.transition_to_trying_to_pause();
                }
                QueueInsertEvent::WasEmpty => {
                    multi_op.wake();
                }
            },
            MultiOpState::TringToPause {
                queue,
                ecancelled,
                last_cqe,
                ..
            } => {
                if io_uring::cqueue::more(entry.flags()) {
                    *last_cqe = true;
                }
                if entry.result() == -ECANCELED {
                    *ecancelled = true;
                }
                queue.push(entry.into());
                if *ecancelled && *last_cqe {
                    multi_op.transition_to_paused();
                }
            }
            MultiOpState::Paused { queue, .. } => {
                queue.push(entry.into());
                eprintln!("receiving while paused?");
            }
        }
    }

    fn handle_completion(&self, entry: Entry) {
        let slab = unsafe { &mut *self.slab.get() };
        let slab_index = entry.user_data() as usize;
        let slab_entry = slab.get_mut(slab_index).unwrap();
        let index_to_remove = None;
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
