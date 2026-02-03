use std::{
    cell::{Cell, UnsafeCell},
    collections::VecDeque,
    os::fd::RawFd,
    rc::Rc,
    task::Waker,
    time::Duration,
};

use io_uring::{
    IoUring,
    cqueue::Entry,
    opcode, squeue,
    types::{Fd, Timespec},
};
use io_uring_rb::{RingBuffer, buffer::Buffer};
use slab::Slab;

use crate::{
    io_driver::{
        completion_events::{
            CANCEL_OP_FLAG, CompletionEvent, Event, MULTI_OP_FLAG, MultiProcessResult,
            SingleProcessResult, process_multi, process_single,
        },
        operations::IoDriverToken,
    },
    scoped_thread_local,
};
mod completion_events;
pub mod operations;

pub const RING_SIZE: u16 = 1024;
pub const BUFFER_SIZE: u32 = 4096;

const QUEUE_PAUSE_THRESHOLD: usize = 2;

#[derive(Default)]
pub struct IODriver {
    ring: UnsafeCell<Ring>,
    slab: Rc<UnsafeCell<Slab<SingleOpState>>>,
    multishot_slab: Rc<UnsafeCell<Slab<MultiOpState>>>,
    shutting_down: Cell<bool>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct MultishotCompletionEvent {
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

#[derive(Debug)]
struct MultiOpQueue {
    inner: VecDeque<MultishotCompletionEvent>,
}

enum QueueInsertEvent {
    Ok,
    TooFull,
    WasEmpty,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MultiPollResult {
    Pending,
    Ready,
    NeedsResubmit,
}

impl MultiOpQueue {
    fn has_content(&self) -> bool {
        !self.inner.is_empty()
    }

    fn push(&mut self, element: MultishotCompletionEvent) -> QueueInsertEvent {
        self.inner.push_back(element);
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
        if let MultiOpState::Init { fd } = self {
            *self = MultiOpState::Sent {
                waker: waker.clone(),
                fd: *fd,
                queue: Default::default(),
            };
        }
    }
    fn transition_to_paused(&mut self) {
        println!("paused");
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

    fn transition_paused_to_sent(&mut self, new_waker: &Waker) {
        println!("restarted");
        if let MultiOpState::Paused { fd, queue, .. } = self {
            *self = MultiOpState::Sent {
                waker: new_waker.clone(),
                fd: *fd,
                queue: std::mem::take(queue),
            };
        }
    }

    fn transition_to_trying_to_pause(&mut self) {
        println!("try pausing");
        match self {
            MultiOpState::Init { .. } => unreachable!("pause a non init ?"),
            MultiOpState::Sent { waker, fd, queue } => {
                *self = Self::TringToPause {
                    waker: waker.clone(),
                    fd: *fd,
                    queue: std::mem::take(queue),
                    last_cqe: false,
                    ecancelled: false,
                };
            }
            MultiOpState::TringToPause { .. } => unreachable!("already"),
            MultiOpState::Paused { .. } => todo!(),
        }
    }

    fn wake(&self) {
        match self {
            MultiOpState::Init { .. } => {}
            MultiOpState::Sent { waker, .. } => {
                waker.wake_by_ref();
            }
            MultiOpState::Paused { waker, .. } => {
                waker.wake_by_ref();
            }
            MultiOpState::TringToPause { waker, .. } => {
                waker.wake_by_ref();
            }
        }
    }
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
        let buf_ring = io_uring_rb::RingBuffer::new(&ring, 0, 0).unwrap();

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

    fn process_cqe(&self, completion_event: CompletionEvent) {
        match completion_event.event {
            Event::MultiShot(data) => {
                let clean_index = completion_event.clean_slab_index();
                let slab = unsafe { &mut *self.multishot_slab.get() };
                let slab_entry = unsafe { slab.get_unchecked_mut(clean_index) };
                let user_data = completion_event.slab_index as u64;

                match process_multi(
                    slab_entry,
                    MultishotCompletionEvent {
                        result: data.result,
                        flags: data.flags,
                    },
                    user_data,
                ) {
                    MultiProcessResult::Queued => {}
                    MultiProcessResult::WakeConsumer => {}
                    MultiProcessResult::PauseRequired(user_data) => {
                        self.send_multiop_cancel_event(user_data);
                    }
                    MultiProcessResult::FullyPaused => {}
                    MultiProcessResult::InvalidState(msg) => {
                        eprintln!("multishot CQE invalid state: {}", msg);
                    }
                }
            }
            Event::SingleShot(data) => {
                let slab = unsafe { &mut *self.slab.get() };
                let slab_entry = unsafe { slab.get_unchecked_mut(completion_event.slab_index) };

                match process_single(slab_entry, data) {
                    SingleProcessResult::Completed => {}
                    SingleProcessResult::Shutdown => {
                        self.shutting_down.replace(true);
                    }
                    SingleProcessResult::InvalidState(msg) => {
                        eprintln!("singleshot CQE invalid state: {}", msg);
                    }
                    SingleProcessResult::CancelledRecycling { id, len } => {
                        let b = self.get_buffer(id, len as usize);
                        self.recycle_buffer(&b);
                        slab.remove(completion_event.slab_index); //will never be polled
                    }
                }
            }
        }
    }

    pub fn with<R>(&self, f: impl FnOnce() -> R) -> R {
        IO_DRIVER.set(self, f)
    }

    fn get_buffer(&self, id: u16, len: usize) -> Buffer<BUFFER_SIZE> {
        let ring_inner = unsafe { &*self.ring.get() };
        ring_inner.buffer_ring.get_buffer(id, len).unwrap()
    }

    fn recycle_buffer(&self, buffer: &Buffer<BUFFER_SIZE>) {
        println!("recyling");
        let ring_inner = unsafe { &*self.ring.get() };
        ring_inner.buffer_ring.recycle_buffer(&buffer);
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
        let user_data = slab.insert(SingleOpState::Init) as IoDriverToken;
        entry.set_user_data(user_data);
        ring_inner.add(&entry);
        user_data
    }

    pub fn push_wakeup_timeout(&self, duration: Duration) {
        let ms = duration.as_millis();
        let time = Box::leak(Box::new(Timespec::new().nsec((ms * 1_500_000) as u32)));
        let entry = io_uring::opcode::Timeout::new(time)
            .count(1)
            .build()
            .user_data(CANCEL_OP_FLAG);
        let ring_inner = unsafe { &mut *self.ring.get() };
        ring_inner.add(&entry);
    }

    pub fn push_signal_op(&self, fd: RawFd) -> IoDriverToken {
        let slab = unsafe { &mut *self.slab.get() };
        let ring_inner = unsafe { &mut *self.ring.get() };
        let mut sig_data = Box::new([0u8; 8]);
        let ptr = sig_data.as_mut_ptr();
        let user_data = slab.insert(SingleOpState::Signal(sig_data)) as IoDriverToken;
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
    ) -> MultiPollResult {
        let slab_multishot = unsafe { &mut *self.multishot_slab.get() };
        let multishot_op = unsafe { slab_multishot.get_unchecked_mut(id) };
        match multishot_op {
            MultiOpState::Init { .. } => {
                multishot_op.transition_init_to_sent(waker);
                MultiPollResult::Pending
            }
            MultiOpState::Sent {
                queue: provision,
                waker: existant_waker,
                ..
            }
            | MultiOpState::TringToPause {
                waker: existant_waker,
                queue: provision,
                ..
            } => {
                if provision.has_content() {
                    std::mem::swap(poller_queue, provision);
                    MultiPollResult::Ready
                } else {
                    *existant_waker = waker.clone();
                    MultiPollResult::Pending
                }
            }
            MultiOpState::Paused { queue, .. } => {
                std::mem::swap(poller_queue, queue);
                MultiPollResult::NeedsResubmit
            }
        }
    }

    fn poll_op(&self, id: usize, waker: &Waker) -> std::task::Poll<OpMetadata> {
        let slab = unsafe { &mut *self.slab.get() };
        let entry = unsafe { slab.get_unchecked_mut(id) };
        let mut id_to_delete = None;
        let res = match entry {
            SingleOpState::Init => {
                *entry = SingleOpState::Sent(waker.clone());
                std::task::Poll::Pending
            }
            SingleOpState::Sent(waker) => {
                waker.wake_by_ref();
                std::task::Poll::Pending
            }
            SingleOpState::Ended(e) => {
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

    pub(crate) fn submit_and_wait(&self) {
        let ring = unsafe { &mut *self.ring.get() };
        match ring.inner.submit_and_wait(1) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => {
                panic!("io_uring submit_and_wait failed: {:?}", e);
            }
        }
    }

    pub(crate) fn process_completion_queue(&self) {
        let ring = unsafe { &mut *self.ring.get() };
        for c_event in ring.inner.completion() {
            if let Some(completion_event) = CompletionEvent::from_entry(c_event) {
                self.process_cqe(completion_event);
            }
        }
    }

    pub fn cancel_single_op(&self, id: usize) {
        let slab = unsafe { &mut *self.slab.get() };
        *slab.get_mut(id).unwrap() = SingleOpState::Cancelled;
        self.send_singleop_cancel_event(id as u64);
    }

    fn send_singleop_cancel_event(&self, id: u64) {
        let ring_inner = unsafe { &mut *self.ring.get() };
        let cancel_op = opcode::AsyncCancel::new(id)
            .build()
            .user_data(id | CANCEL_OP_FLAG);
        ring_inner.add(&cancel_op);
    }

    pub fn send_multiop_cancel_event(&self, id: u64) {
        let ring_inner = unsafe { &mut *self.ring.get() };
        let cancel_op = opcode::AsyncCancel::new(id)
            .build()
            .user_data(id | MULTI_OP_FLAG | CANCEL_OP_FLAG);
        ring_inner.add(&cancel_op);
    }

    pub fn resubmit_multishot(&self, id: usize, mut entry: squeue::Entry, waker: &Waker) -> bool {
        let slab = unsafe { &mut *self.multishot_slab.get() };
        let op = match slab.get_mut(id) {
            Some(op) => op,
            None => return false,
        };

        if !matches!(op, MultiOpState::Paused { .. }) {
            return false;
        }

        op.transition_paused_to_sent(waker);

        entry = entry.user_data((id as u64) | MULTI_OP_FLAG);

        let ring = unsafe { &mut *self.ring.get() };
        ring.add(&entry);

        true
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct OpMetadata {
    result: i32,
    #[allow(dead_code)]
    flags: u32,
}

#[derive(Debug)]
enum SingleOpState {
    Init,
    Sent(Waker),
    Ended(OpMetadata),
    Signal(Box<[u8; 8]>),
    Cancelled,
}

struct Ring {
    inner: IoUring,
    buffer_ring: RingBuffer<BUFFER_SIZE, RING_SIZE>,
}

impl Ring {
    pub(crate) fn add(&mut self, entry: &squeue::Entry) {
        unsafe {
            self.inner.submission().push(entry).unwrap();
        }
    }
}
