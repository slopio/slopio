use std::{
    cell::{Cell, UnsafeCell},
    collections::VecDeque,
    os::fd::RawFd,
    rc::Rc,
    task::Waker,
    time::Duration,
};

use crate::{
    define_buffer_rings,
    io_driver::{
        completion_events::{
            CompletionEvent, Event, MultiProcessResult, SingleProcessResult, process_multi,
            process_single,
        },
        operations::{MultishotToken, OneshotToken},
    },
    scoped_thread_local,
};
use io_uring::{
    IoUring,
    cqueue::Entry,
    opcode, squeue,
    types::{Fd, Timespec},
};
use slab::Slab;
mod completion_events;
pub mod operations;
mod sending_buffers;
mod user_data;

pub(crate) use user_data::UserData;

pub use sending_buffers::{SEND_BUFFER_SIZE, SendingBuffer, SendingBufferPool};

include!(concat!(env!("OUT_DIR"), "/ring_config.rs"));

const QUEUE_PAUSE_THRESHOLD: usize = 2;

#[derive(Default)]
pub struct IODriver {
    ring: UnsafeCell<Ring>,
    slab: Rc<UnsafeCell<OneshotSlab>>,
    multishot_slab: Rc<UnsafeCell<MultishotSlab>>,
    sending_buffers: SendingBufferPool,
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
    Cancelled {
        bgid: u16,
        last_cqe: bool,
        ecancelled: bool,
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
        if let MultiOpState::Paused { fd, queue, .. } = self {
            *self = MultiOpState::Sent {
                waker: new_waker.clone(),
                fd: *fd,
                queue: std::mem::take(queue),
            };
        }
    }

    fn transition_to_trying_to_pause(&mut self) {
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
            MultiOpState::Cancelled { .. } => {}
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
            MultiOpState::Cancelled { .. } => {}
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
        let buffer_rings = MultiRing::new(&ring);

        Self {
            buffer_rings,
            inner: ring,
            pending: 0,
            wakeup_timeout: Timespec::new(),
        }
    }
}

scoped_thread_local!(pub static IO_DRIVER: IODriver);

impl IODriver {
    pub fn shutdown(&self) -> bool {
        self.shutting_down.get()
    }

    fn process_cqe(&self, completion_event: CompletionEvent) {
        let index = completion_event.slab_index;
        match completion_event.event {
            Event::MultiShot(cqe_data) => {
                let slab = unsafe { &mut *self.multishot_slab.get() };
                let slab_entry = unsafe { slab.get_unchecked_mut_raw(index) };

                match process_multi(
                    slab_entry,
                    MultishotCompletionEvent {
                        result: cqe_data.result,
                        flags: cqe_data.flags,
                    },
                    index as u64,
                ) {
                    MultiProcessResult::Queued => {}
                    MultiProcessResult::WakeConsumer => {}
                    MultiProcessResult::PauseRequired(clean_index) => {
                        self.send_multiop_cancel_event(
                            UserData::multi(clean_index).into_bits(),
                        );
                    }
                    MultiProcessResult::FullyPaused => {}
                    MultiProcessResult::CancelComplete => {
                        slab.remove_raw(index);
                    }
                    MultiProcessResult::CancelledRecycling {
                        buffer_id,
                        len,
                        bgid,
                    } => {
                        drop(self.get_buffer(bgid, buffer_id, len));
                        // Check if cancel is now complete
                        if let MultiOpState::Cancelled {
                            ecancelled,
                            last_cqe,
                            ..
                        } = slab_entry
                            && *ecancelled
                            && *last_cqe
                        {
                            slab.remove_raw(index);
                        }
                    }
                    MultiProcessResult::InvalidState(_msg) => {}
                }
            }
            Event::SingleShot(cqe_data) => {
                let slab = unsafe { &mut *self.slab.get() };
                let slab_entry = unsafe { slab.get_unchecked_mut_raw(index) };

                match process_single(slab_entry, cqe_data) {
                    SingleProcessResult::Completed => {}
                    SingleProcessResult::CancelComplete => {
                        slab.remove_raw(index);
                    }
                    SingleProcessResult::Shutdown => {
                        self.shutting_down.replace(true);
                    }
                    SingleProcessResult::InvalidState(_msg) => {}
                    SingleProcessResult::CancelledRecycling { id, len, bgid } => {
                        let b = self.get_buffer(bgid, id, len as usize);
                        self.recycle_buffer(&b);
                        slab.remove_raw(index);
                    }
                }
            }
            Event::SendBuffer(result) => {
                if result < 0 {
                    eprintln!(
                        "sendmsg failed: {}",
                        std::io::Error::from_raw_os_error(-result)
                    );
                }
                self.sending_buffers.release(index);
            }
        }
    }

    pub fn with<R>(&self, f: impl FnOnce() -> R) -> R {
        IO_DRIVER.set(self, f)
    }

    pub(crate) fn get_buffer(&self, bgid: u16, id: u16, len: usize) -> BorrowedBuffer {
        let ring_inner = unsafe { &*self.ring.get() };
        ring_inner.buffer_rings.get_buffer(bgid, id, len)
    }

    pub(crate) fn recycle_buffer(&self, buffer: &BorrowedBuffer) {
        let ring_inner = unsafe { &*self.ring.get() };
        ring_inner.buffer_rings.recycle_buffer(buffer);
    }

    pub fn push_multishot_op(&self, mut entry: squeue::Entry, fd: i32) -> MultishotToken {
        let slab = unsafe { &mut *self.multishot_slab.get() };
        let ring_inner = unsafe { &mut *self.ring.get() };
        let token = slab.insert(MultiOpState::Init { fd });
        entry.set_user_data(UserData::multi(token.raw()).into_bits());
        ring_inner.add(&entry);
        token
    }

    pub fn push_anon_op(&self, entry: squeue::Entry) {
        let ring_inner = unsafe { &mut *self.ring.get() };
        ring_inner.add(&entry)
    }

    pub fn push_op(&self, mut entry: squeue::Entry) -> OneshotToken {
        let slab = unsafe { &mut *self.slab.get() };
        let ring_inner = unsafe { &mut *self.ring.get() };
        let token = slab.insert(SingleOpState::Init);
        entry.set_user_data(UserData::single(token.raw()).into_bits());
        ring_inner.add(&entry);
        token
    }

    pub fn push_wakeup_timeout(&self, duration: Duration) {
        let ring_inner = unsafe { &mut *self.ring.get() };
        ring_inner.wakeup_timeout = Timespec::from(duration);
        let entry = io_uring::opcode::Timeout::new(&ring_inner.wakeup_timeout)
            .count(1)
            .build()
            .user_data(UserData::ignored_only().into_bits());
        ring_inner.add(&entry);
    }

    pub fn push_signal_op(&self, fd: RawFd) -> OneshotToken {
        let slab = unsafe { &mut *self.slab.get() };
        let ring_inner = unsafe { &mut *self.ring.get() };
        let mut sig_data = Box::new([0u8; 8]);
        let ptr = sig_data.as_mut_ptr();
        let token = slab.insert(SingleOpState::Signal(sig_data));
        let mut entry = opcode::Read::new(Fd(fd), ptr, 8).build();
        entry.set_user_data(UserData::single(token.raw()).into_bits());
        ring_inner.add(&entry);
        token
    }

    fn poll_op_multishot(
        &self,
        token: MultishotToken,
        waker: &Waker,
        poller_queue: &mut MultiOpQueue,
    ) -> MultiPollResult {
        let slab = unsafe { &mut *self.multishot_slab.get() };
        let multishot_op = unsafe { slab.get_unchecked_mut(token) };
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
            MultiOpState::Cancelled { .. } => MultiPollResult::Pending,
        }
    }

    pub fn poll_op(&self, token: OneshotToken, waker: &Waker) -> std::task::Poll<OpMetadata> {
        let slab = unsafe { &mut *self.slab.get() };
        let entry = unsafe { slab.get_unchecked_mut(token) };
        let mut to_delete = None;
        let res = match entry {
            SingleOpState::Init => {
                *entry = SingleOpState::Sent(waker.clone());
                std::task::Poll::Pending
            }
            SingleOpState::Sent(existing) => {
                existing.clone_from(waker);
                std::task::Poll::Pending
            }
            SingleOpState::Ended(e) => {
                to_delete = Some(token);
                std::task::Poll::Ready(*e)
            }
            _ => {
                unreachable!("this should never get polled")
            }
        };
        if let Some(token) = to_delete {
            slab.remove(token);
        }
        res
    }

    pub(crate) fn has_pending_sqes(&self) -> bool {
        let ring = unsafe { &*self.ring.get() };
        ring.pending > 0
    }

    pub(crate) fn submit(&self) {
        let ring = unsafe { &mut *self.ring.get() };
        ring.pending = 0;
        match ring.inner.submit() {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => {
                panic!("io_uring submit failed: {:?}", e);
            }
        }
    }

    pub(crate) fn submit_and_wait(&self) {
        let ring = unsafe { &mut *self.ring.get() };
        ring.pending = 0;
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

    /// Cancel a single-shot operation. `bgid` must be `Some` if the operation
    /// used provided buffers (so the buffer can be recycled on late completion),
    /// or `None` for operations that manage their own buffers.
    pub fn cancel_single_op(&self, token: OneshotToken, bgid: Option<u16>) {
        self.cancel_single_op_keep(token, bgid, None);
    }

    /// Cancel a single-shot operation, transferring ownership of `keepalive`
    /// to the driver. The kernel may still be reading from memory referenced
    /// by the in-flight SQE; `keepalive` is dropped only when the cancelled
    /// op's CQE arrives.
    pub fn cancel_single_op_keep(
        &self,
        token: OneshotToken,
        bgid: Option<u16>,
        keepalive: Option<Box<dyn std::any::Any>>,
    ) {
        let slab = unsafe { &mut *self.slab.get() };
        *slab.get_mut(token).unwrap() = SingleOpState::Cancelled {
            bgid,
            keepalive: keepalive.map(CancelKeepalive),
        };
        self.send_singleop_cancel_event(token.raw());
    }

    fn send_singleop_cancel_event(&self, raw: u64) {
        let ring_inner = unsafe { &mut *self.ring.get() };
        let cancel_op = opcode::AsyncCancel::new(raw)
            .build()
            .user_data(UserData::cancel_echo(raw).into_bits());
        ring_inner.add(&cancel_op);
    }

    pub fn cancel_multishot_op(&self, token: MultishotToken, bgid: u16) {
        let slab = unsafe { &mut *self.multishot_slab.get() };
        if let Some(entry) = slab.get_mut(token) {
            // Recycle kernel buffers from the old state's queue before dropping it.
            let old = std::mem::replace(
                entry,
                MultiOpState::Cancelled {
                    bgid,
                    last_cqe: false,
                    ecancelled: false,
                },
            );
            self.recycle_multiop_queue(old, bgid);
            self.send_multiop_cancel_event(UserData::multi(token.raw()).into_bits());
        }
    }

    fn recycle_multiop_queue(&self, state: MultiOpState, bgid: u16) {
        let mut queue = match state {
            MultiOpState::Sent { queue, .. }
            | MultiOpState::TringToPause { queue, .. }
            | MultiOpState::Paused { queue, .. } => queue,
            _ => return,
        };
        while let Some(event) = queue.pop() {
            if event.result > 0 {
                if let Some(buffer_id) = io_uring::cqueue::buffer_select(event.flags) {
                    drop(self.get_buffer(bgid, buffer_id, event.result as usize));
                }
            }
        }
    }

    pub fn send_multiop_cancel_event(&self, raw: u64) {
        let ring_inner = unsafe { &mut *self.ring.get() };
        let cancel_op = opcode::AsyncCancel::new(raw)
            .build()
            .user_data(UserData::cancel_echo(raw).into_bits());
        ring_inner.add(&cancel_op);
    }

    pub fn resubmit_multishot(
        &self,
        token: MultishotToken,
        mut entry: squeue::Entry,
        waker: &Waker,
    ) -> bool {
        let slab = unsafe { &mut *self.multishot_slab.get() };
        let op = match slab.get_mut(token) {
            Some(op) => op,
            None => return false,
        };

        if !matches!(op, MultiOpState::Paused { .. }) {
            return false;
        }

        op.transition_paused_to_sent(waker);

        entry = entry.user_data(UserData::multi(token.raw()).into_bits());

        let ring = unsafe { &mut *self.ring.get() };
        ring.add(&entry);

        true
    }

    /// Returns a reference to the sending buffer pool.
    pub fn sending_buffers(&self) -> &SendingBufferPool {
        &self.sending_buffers
    }
}

#[derive(Debug, Clone, Copy)]
pub struct OpMetadata {
    pub result: i32,
    #[allow(dead_code)]
    pub flags: u32,
}

struct CancelKeepalive(#[allow(dead_code)] Box<dyn std::any::Any>);

impl std::fmt::Debug for CancelKeepalive {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CancelKeepalive(..)")
    }
}

#[derive(Debug)]
enum SingleOpState {
    Init,
    Sent(Waker),
    Ended(OpMetadata),
    Signal(Box<[u8; 8]>),
    /// The operation was cancelled. `bgid` is set when the op used provided
    /// buffers so a late-arriving completion can recycle the buffer.
    /// `keepalive` owns memory the kernel may still be reading; it is dropped
    /// when the cancelled op's CQE arrives.
    Cancelled {
        bgid: Option<u16>,
        #[allow(dead_code)]
        keepalive: Option<CancelKeepalive>,
    },
}

/// Type-safe slab for single-shot operations, keyed by `OneshotToken`.
struct OneshotSlab(Slab<SingleOpState>);

impl Default for OneshotSlab {
    fn default() -> Self {
        Self(Slab::new())
    }
}

impl OneshotSlab {
    fn insert(&mut self, val: SingleOpState) -> OneshotToken {
        OneshotToken(self.0.insert(val) as u64)
    }

    fn get_mut(&mut self, token: OneshotToken) -> Option<&mut SingleOpState> {
        self.0.get_mut(token.raw() as usize)
    }

    /// # Safety
    /// The token must refer to an occupied slot.
    unsafe fn get_unchecked_mut(&mut self, token: OneshotToken) -> &mut SingleOpState {
        unsafe { self.0.get_unchecked_mut(token.raw() as usize) }
    }

    fn remove(&mut self, token: OneshotToken) -> SingleOpState {
        self.0.remove(token.raw() as usize)
    }

    /// Access by raw index from a CQE user_data.
    unsafe fn get_unchecked_mut_raw(&mut self, index: usize) -> &mut SingleOpState {
        unsafe { self.0.get_unchecked_mut(index) }
    }

    fn remove_raw(&mut self, index: usize) -> SingleOpState {
        self.0.remove(index)
    }
}

/// Type-safe slab for multishot operations, keyed by `MultishotToken`.
struct MultishotSlab(Slab<MultiOpState>);

impl Default for MultishotSlab {
    fn default() -> Self {
        Self(Slab::new())
    }
}

impl MultishotSlab {
    fn insert(&mut self, val: MultiOpState) -> MultishotToken {
        MultishotToken(self.0.insert(val) as u64)
    }

    fn get_mut(&mut self, token: MultishotToken) -> Option<&mut MultiOpState> {
        self.0.get_mut(token.raw() as usize)
    }

    /// # Safety
    /// The token must refer to an occupied slot.
    unsafe fn get_unchecked_mut(&mut self, token: MultishotToken) -> &mut MultiOpState {
        unsafe { self.0.get_unchecked_mut(token.raw() as usize) }
    }

    fn remove(&mut self, token: MultishotToken) -> MultiOpState {
        self.0.remove(token.raw() as usize)
    }

    /// Access by raw index from a CQE user_data.
    unsafe fn get_unchecked_mut_raw(&mut self, index: usize) -> &mut MultiOpState {
        unsafe { self.0.get_unchecked_mut(index) }
    }

    fn remove_raw(&mut self, index: usize) -> MultiOpState {
        self.0.remove(index)
    }
}

struct Ring {
    inner: IoUring,
    buffer_rings: MultiRing,
    pending: u32,
    wakeup_timeout: Timespec,
}

impl Ring {
    pub(crate) fn add(&mut self, entry: &squeue::Entry) {
        unsafe {
            self.inner.submission().push(entry).unwrap();
        }
        self.pending += 1;
    }
}
