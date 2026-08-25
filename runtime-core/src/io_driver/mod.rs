use std::{
    cell::{Cell, UnsafeCell},
    collections::VecDeque,
    os::fd::{AsRawFd, RawFd},
    rc::Rc,
    sync::Arc,
    task::Waker,
    time::Duration,
};

#[cfg(feature = "simulation")]
use crate::deterministic_testing::FakeRing;
#[cfg(feature = "simulation")]
use crate::deterministic_testing::FakeRingHandle;
use crate::runtime::peers::PeerTable;

use crate::{
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
    IoUring, opcode, squeue,
    types::{Fd, Timespec},
};
use slab::Slab;
mod buf_ring;
mod completion_events;
mod fixed_buf_pool;
mod message;
pub mod operations;
mod sending_buffers;
mod user_data;

pub(crate) use user_data::{
    IO_MULTI, IO_REPLY_SIGNAL, IO_SEND_BUFFER, MsgPtrUserData, PeerUserData, TAG_IGNORED, TAG_IO,
    TAG_MESSAGE, TAG_MSG_PTR, UserData,
};

pub use message::{Endpoint, ErasedMessage, Header, Message};

pub use buf_ring::{BorrowedBuffer, BufRingSet, BufRingSpec, BufRingTake, install_buf_ring};
pub use fixed_buf_pool::{Acquire as FixedBufAcquire, FixedSendBufPool, PooledBuf};
pub use sending_buffers::{SEND_BUFFER_SIZE, SendingBuffer, SendingBufferPool};

pub const DEFAULT_QUEUE_SIZE: usize = 32;

#[derive(Default)]
pub struct IODriver {
    ring: UnsafeCell<Ring>,
    slab: Rc<UnsafeCell<OneshotSlab>>,
    multishot_slab: Rc<UnsafeCell<MultishotSlab>>,
    sending_buffers: SendingBufferPool,
    shutting_down: Cell<bool>,
    /// Cancel-path recycler fallbacks, keyed by bgid. The hot path uses
    /// the typed `S::recycle_bid` instead; this table is only consulted
    /// when a cancelled completion delivers a buffer at a point where the
    /// originating spec type has already been erased.
    recyclers: UnsafeCell<smallvec::SmallVec<[(u16, fn(u16)); 4]>>,
    /// Shared, process-wide registry of every worker's ring/event fds. Used
    /// to target MSG_RING at peer rings. `None` under `main_thread`.
    peers: Option<Arc<PeerTable>>,
    /// Messages delivered by peers via MSG_RING, drained by `recv_from_peer`.
    peer_inbox: UnsafeCell<PeerInbox>,
    /// Heap-allocated, type-erased messages delivered by peers via MSG_RING,
    /// drained by `recv_msg_from_peer`.
    msg_inbox: UnsafeCell<MsgInbox>,
    /// Slots this worker reserved for replies to requests it sent. A peer's
    /// reply names its slot in `receiver.slab`; delivery routes it here and
    /// wakes the awaiting future instead of the generic `msg_inbox`.
    reply_slab: UnsafeCell<Slab<ReplySlot>>,
}

/// A reserved slot awaiting the reply to a request this worker sent out. Its
/// index rides in the request's `sender.slab` and comes back in the reply's
/// `receiver.slab`.
enum ReplySlot {
    /// No reply yet; wake this waker (if any) when it lands.
    Waiting(Option<Waker>),
    /// The reply arrived and is parked here until the awaiting future takes it.
    Delivered(ErasedMessage),
    /// A pointer-free wakeup landed: the peer answered by naming this slot
    /// instead of sending it a payload. Carries the CQE `res` as a status.
    Signalled(i32),
    /// The awaiting future was dropped, but a reply may still be racing toward
    /// this slot. The slot stays reserved — so its index is not handed to a new
    /// request — until that late reply lands.
    ///
    /// `Some(glue)` for a message-kind request: it is the only carrier of the
    /// concrete payload type, which the driver otherwise cannot recover to drop
    /// the `Arc`. `None` for a signal-kind one — a wakeup owns nothing, so
    /// absorbing it is just releasing the slot.
    Cancelled(Option<fn(ErasedMessage)>),
}

/// A message received from another worker via MSG_RING.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PeerMessage {
    /// Caller-defined 62-bit payload (from the SQE `user_data`).
    pub payload: u64,
    /// Secondary payload (from the CQE `res`).
    pub data: i32,
}

#[derive(Default)]
struct PeerInbox {
    queue: VecDeque<PeerMessage>,
    waker: Option<Waker>,
}

#[derive(Default)]
struct MsgInbox {
    queue: VecDeque<ErasedMessage>,
    waker: Option<Waker>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct MultishotCompletionEvent {
    pub result: i32,
    pub flags: u32,
}

#[derive(Debug)]
struct MultiOpQueue {
    inner: VecDeque<MultishotCompletionEvent>,
    pause_threshold: usize,
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
    fn new(pause_threshold: usize) -> Self {
        Self {
            inner: VecDeque::with_capacity(pause_threshold + 10),
            pause_threshold,
        }
    }

    fn has_content(&self) -> bool {
        !self.inner.is_empty()
    }

    fn push(&mut self, element: MultishotCompletionEvent) -> QueueInsertEvent {
        self.inner.push_back(element);
        match self.inner.len() {
            1 => QueueInsertEvent::WasEmpty,
            k if k > self.pause_threshold => QueueInsertEvent::TooFull,
            _ => QueueInsertEvent::Ok,
        }
    }

    fn pop(&mut self) -> Option<MultishotCompletionEvent> {
        self.inner.pop_front()
    }
}

impl Default for MultiOpQueue {
    fn default() -> Self {
        Self::new(DEFAULT_QUEUE_SIZE)
    }
}

enum MultiOpState {
    Init {
        fd: i32,
        queue_size: usize,
    },
    Sent {
        waker: Waker,
        // woken_up: bool,
        fd: i32,
        queue: MultiOpQueue,
        /// Set when a CQE without `IORING_CQE_F_MORE` was queued: the kernel
        /// op terminated on its own (EOF, error) and no further CQE will
        /// arrive for this token. Cancellation must then reap the slab entry
        /// directly instead of waiting for a CQE that will never come.
        last_cqe: bool,
    },
    TringToPause {
        waker: Waker,
        fd: i32,
        queue: MultiOpQueue,
        last_cqe: bool,
    },
    Paused {
        waker: Waker,
        fd: i32,
        queue: MultiOpQueue,
    },
    Cancelled {
        bgid: u16,
        last_cqe: bool,
        /// Memory the kernel may still touch (e.g. a UDP `msghdr` Box) for
        /// in-flight CQEs after cancel is submitted. Dropped when the slab
        /// entry is reaped on `CancelComplete`.
        #[allow(dead_code)]
        keepalive: Option<CancelKeepalive>,
    },
    /// Cancelled multishot accept. Separate from `Cancelled` because for
    /// accept-style ops `event.result > 0` means "a freshly-accepted fd",
    /// not a buffer index. The driver closes these fds as they arrive
    /// instead of trying to recycle them as buffers.
    AcceptCancelled {
        last_cqe: bool,
    },
}

impl MultiOpState {
    fn transition_init_to_sent(&mut self, waker: &Waker) {
        if let MultiOpState::Init { fd, queue_size } = self {
            *self = MultiOpState::Sent {
                waker: waker.clone(),
                fd: *fd,
                queue: MultiOpQueue::new(*queue_size),
                last_cqe: false,
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
                // A fresh SQE is about to be submitted for this token.
                last_cqe: false,
            };
        }
    }

    fn transition_to_trying_to_pause(&mut self) {
        match self {
            MultiOpState::Init { .. } => unreachable!("pause a non init ?"),
            MultiOpState::Sent {
                waker,
                fd,
                queue,
                last_cqe,
            } => {
                *self = Self::TringToPause {
                    waker: waker.clone(),
                    fd: *fd,
                    queue: std::mem::take(queue),
                    last_cqe: *last_cqe,
                };
            }
            MultiOpState::TringToPause { .. } => unreachable!("already"),
            MultiOpState::Paused { .. } => todo!(),
            MultiOpState::Cancelled { .. } => {}
            MultiOpState::AcceptCancelled { .. } => {}
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
            MultiOpState::AcceptCancelled { .. } => {}
        }
    }
}

impl Ring {
    /// A ring backed by the simulator rather than the kernel.
    #[cfg(feature = "simulation")]
    fn simulated(fake: FakeRing) -> Self {
        Self {
            inner: RingBackend::Fake(fake),
            pending: 0,
            wakeup_timeout: Timespec::new(),
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

        Self {
            inner: RingBackend::Uring(ring),
            pending: 0,
            wakeup_timeout: Timespec::new(),
        }
    }
}

scoped_thread_local!(pub static IO_DRIVER: IODriver);

impl IODriver {
    /// A driver over a simulated ring.
    ///
    /// Everything above the ring — slabs, wakers, the multishot state machine,
    /// buffer recycling — is exactly what `Default` builds for production, so a
    /// test exercises the real driver rather than a variant of it.
    #[cfg(feature = "simulation")]
    pub fn simulated(fake: FakeRing) -> Self {
        Self {
            ring: UnsafeCell::new(Ring::simulated(fake)),
            ..Default::default()
        }
    }

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
                    MultiProcessResult::AcceptCancelComplete => {
                        slab.remove_raw(index);
                    }
                    MultiProcessResult::CancelledRecycling { buffer_id, bgid } => {
                        self.recycle_by_bgid(bgid, buffer_id);
                        // Check if cancel is now complete
                        if let MultiOpState::Cancelled { last_cqe, .. } = slab_entry
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
                    SingleProcessResult::LinkCompleted => {
                        slab.remove_raw(index);
                    }
                    SingleProcessResult::Shutdown => {
                        self.shutting_down.replace(true);
                    }
                    SingleProcessResult::InvalidState(_msg) => {}
                    SingleProcessResult::CancelledRecycling { id, bgid } => {
                        self.recycle_by_bgid(bgid, id);
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
            Event::PeerMessage { payload, data } => {
                // No slab entry — `index` is unused for peer messages.
                self.deliver_peer_message(payload, data);
            }
            Event::TypedMessage(msg) => {
                // No slab entry — we own the erased allocation and hand it off.
                self.route_msg(msg);
            }
            Event::ReplySignal { status } => {
                // `index` is the reply slot the peer named; nothing is owned.
                self.route_signal(index as u32, status);
            }
        }
    }

    pub fn with<R>(&self, f: impl FnOnce() -> R) -> R {
        IO_DRIVER.set(self, f)
    }

    /// Install every ring in `R` against this driver's io_uring instance,
    /// registering each spec's recycler into the cancel-fallback table.
    /// Must be called exactly once per worker thread, before any IO op that
    /// uses provided buffers is submitted.
    pub fn init_buf_rings<R: BufRingSet>(&self) {
        let ring_inner = unsafe { &*self.ring.get() };
        R::init_all(ring_inner.handle());
        R::register_fallbacks(self);
    }

    /// Register a set of pre-allocated buffers with the kernel for use with
    /// `*Fixed` opcodes. The caller is responsible for keeping the memory
    /// referenced by `iovecs` valid until the ring is dropped or
    /// [`unregister_fixed_buffers`](Self::unregister_fixed_buffers) is called.
    ///
    /// # Safety
    /// Each `iov_base`/`iov_len` must point to memory that stays valid for
    /// the registration's lifetime.
    pub unsafe fn register_fixed_buffers(&self, iovecs: &[libc::iovec]) -> std::io::Result<()> {
        let ring_inner = unsafe { &*self.ring.get() };
        match &ring_inner.inner {
            RingBackend::Uring(ring) => unsafe { ring.submitter().register_buffers(iovecs) },
            // Nothing to tell: a simulated ring reads the same memory the
            // caller already owns, registered or not.
            #[cfg(feature = "simulation")]
            RingBackend::Fake(_) => Ok(()),
        }
    }

    /// Tell the kernel to drop its references to the previously-registered
    /// fixed buffer set. Safe to call without a prior registration.
    pub fn unregister_fixed_buffers(&self) -> std::io::Result<()> {
        let ring_inner = unsafe { &*self.ring.get() };
        match &ring_inner.inner {
            RingBackend::Uring(ring) => ring.submitter().unregister_buffers(),
            #[cfg(feature = "simulation")]
            RingBackend::Fake(_) => Ok(()),
        }
    }

    pub(crate) fn register_recycler(&self, bgid: u16, f: fn(u16)) {
        let recyclers = unsafe { &mut *self.recyclers.get() };
        recyclers.push((bgid, f));
    }

    /// Recycle buffer `bid` back to the ring identified by `bgid`. Used by
    /// cancel cleanup, which doesn't carry the spec type through the slab.
    pub(crate) fn recycle_by_bgid(&self, bgid: u16, bid: u16) {
        let recyclers = unsafe { &*self.recyclers.get() };
        for &(g, f) in recyclers.iter() {
            if g == bgid {
                f(bid);
                return;
            }
        }
        panic!("no recycler registered for bgid {bgid}");
    }

    /// Number of multishot slab entries currently held by the driver.
    /// Diagnostic accessor, used by leak regression tests.
    pub fn multishot_ops_in_flight(&self) -> usize {
        let slab = unsafe { &*self.multishot_slab.get() };
        slab.len()
    }

    pub fn push_multishot_op(
        &self,
        mut entry: squeue::Entry,
        fd: i32,
        queue_size: usize,
    ) -> MultishotToken {
        let slab = unsafe { &mut *self.multishot_slab.get() };
        let ring_inner = unsafe { &mut *self.ring.get() };
        let token = slab.insert(MultiOpState::Init { fd, queue_size });
        entry.set_user_data(UserData::multi(token.raw()).into_bits());
        ring_inner.add(&entry);
        token
    }

    pub fn push_anon_op(&self, entry: squeue::Entry) {
        let ring_inner = unsafe { &mut *self.ring.get() };
        ring_inner.add(&entry)
    }

    /// Attach the shared peer table to this driver. Called once at
    /// construction by [`RuntimeInner::with_peers`](crate::runtime::RuntimeInner::with_peers).
    pub fn attach_peers(&mut self, peers: Arc<PeerTable>) {
        self.peers = Some(peers);
    }

    /// Raw fd of the underlying io_uring, usable as a MSG_RING target from
    /// other threads (the fd table is shared process-wide).
    pub fn ring_fd(&self) -> RawFd {
        let ring = unsafe { &*self.ring.get() };
        match &ring.inner {
            RingBackend::Uring(ring) => ring.as_raw_fd(),
            #[cfg(feature = "simulation")]
            RingBackend::Fake(ring) => ring.ring_fd(),
        }
    }

    /// Publish our ring fd into the shared table, then rendezvous on the
    /// startup barrier. No-op when no peer table is attached (`main_thread`).
    pub fn publish_self(&self, thread_id: usize) {
        if let Some(peers) = &self.peers {
            peers.publish(thread_id, self.ring_fd());
        }
    }

    /// Post a MSG_RING to peer `thread_id`: looks up its ring fd in the shared
    /// table and pushes the op onto our own ring. The peer receives a
    /// [`PeerMessage`] carrying `payload` and `data` in its inbox.
    pub fn send_to_peer(&self, thread_id: usize, data: i32, payload: u64) {
        let peers = self
            .peers
            .as_ref()
            .expect("send_to_peer called outside a multi-threaded runtime");
        let fd = peers
            .slot(thread_id)
            .ring_fd()
            .expect("peer ring fd unpublished; the startup barrier guarantees it");
        self.push_msg_ring(fd, data, PeerUserData::message(payload).into_bits());
    }

    /// Wake reply slot `slot` on peer `thread_id` without allocating anything:
    /// the slot index travels in the `user_data` and `status` in the CQE `res`.
    ///
    /// The counterpart of [`send_erased_to_peer`](Self::send_erased_to_peer)
    /// for a reply that carries no payload — because the requester already
    /// holds it. Nothing is owned by the wire, so a wakeup aimed at a slot that
    /// no longer exists is simply dropped.
    pub fn send_signal_to_peer(&self, thread_id: usize, slot: u32, status: i32) {
        let peers = self
            .peers
            .as_ref()
            .expect("send_signal_to_peer called outside a multi-threaded runtime");
        let fd = peers
            .slot(thread_id)
            .ring_fd()
            .expect("peer ring fd unpublished; the startup barrier guarantees it");
        self.push_msg_ring(fd, status, UserData::reply_signal(slot as u64).into_bits());
    }

    /// Post a heap-allocated, type-erased message to peer `thread_id`. The
    /// strong reference held by `msg` passes to the wire: the pointer travels in
    /// the receiver's `user_data`, and the peer reclaims that reference in its
    /// message inbox. Whether it is the *last* reference depends on what the
    /// sender kept — see [`Message::erase`].
    pub fn send_erased_to_peer(&self, thread_id: usize, msg: ErasedMessage) {
        let peers = self
            .peers
            .as_ref()
            .expect("send_msg_to_peer called outside a multi-threaded runtime");
        let fd = peers
            .slot(thread_id)
            .ring_fd()
            .expect("peer ring fd unpublished; the startup barrier guarantees it");
        self.push_msg_ring(fd, 0, MsgPtrUserData::for_msg(msg.into_raw()).into_bits());
    }

    /// Low-level MSG_RING: deliver a CQE on `target_ring_fd` carrying `data`
    /// and `peer_user_data`. Our own send completion is marked ignored.
    fn push_msg_ring(&self, target_ring_fd: RawFd, data: i32, peer_user_data: u64) {
        let entry = opcode::MsgRingData::new(Fd(target_ring_fd), data, peer_user_data, None)
            .build()
            .user_data(UserData::ignored_only().into_bits());
        self.push_anon_op(entry);
    }

    /// Push an inbound peer message and wake any pending `recv_from_peer`.
    fn deliver_peer_message(&self, payload: u64, data: i32) {
        let inbox = unsafe { &mut *self.peer_inbox.get() };
        inbox.queue.push_back(PeerMessage { payload, data });
        if let Some(waker) = inbox.waker.take() {
            waker.wake();
        }
    }

    /// Poll for the next peer message. Returns `Ready` immediately if the
    /// inbox is non-empty; otherwise registers `waker` and returns `Pending`.
    pub fn poll_peer_inbox(&self, waker: &Waker) -> std::task::Poll<PeerMessage> {
        let inbox = unsafe { &mut *self.peer_inbox.get() };
        match inbox.queue.pop_front() {
            Some(msg) => std::task::Poll::Ready(msg),
            None => {
                inbox.waker = Some(waker.clone());
                std::task::Poll::Pending
            }
        }
    }

    /// Route an inbound typed message. If it names an existing reply slot
    /// (`receiver.slab`), hand it to that slot and wake the awaiting future;
    /// otherwise fall back to the generic inbox drained by
    /// `recv_msg_from_peer`. A named-but-missing slot (the awaiter was
    /// dropped) also falls back rather than leaking silently.
    fn route_msg(&self, msg: ErasedMessage) {
        if let Some(slab_index) = msg.header().receiver.slab {
            let slab = unsafe { &mut *self.reply_slab.get() };
            if let Some(slot) = slab.get_mut(slab_index as usize) {
                match slot {
                    ReplySlot::Waiting(waker) => {
                        let waker = waker.take();
                        *slot = ReplySlot::Delivered(msg);
                        if let Some(waker) = waker {
                            waker.wake();
                        }
                    }
                    ReplySlot::Cancelled(Some(drop_glue)) => {
                        // The awaiter walked away and left this glue behind: free
                        // the late reply in its concrete type and release the
                        // slot, now safe to reuse.
                        let drop_glue = *drop_glue;
                        slab.remove(slab_index as usize);
                        drop_glue(msg);
                    }
                    ReplySlot::Cancelled(None) => {
                        // Signal-kind slot: no glue was stashed because a wakeup
                        // owns nothing, so this message cannot be freed here.
                        debug_assert!(false, "message delivered to a signal reply slot");
                        self.push_msg_inbox(msg);
                    }
                    ReplySlot::Delivered(_) | ReplySlot::Signalled(_) => {
                        debug_assert!(false, "second reply delivered to one slot");
                        self.push_msg_inbox(msg);
                    }
                }
                return;
            }
        }
        self.push_msg_inbox(msg);
    }

    /// Route an inbound pointer-free wakeup to the reply slot it names.
    ///
    /// There is no generic-inbox fallback, and there needs to be none: a wakeup
    /// carries no payload, so a slot that is gone (or was never reserved) leaves
    /// nothing to leak or misdeliver — dropping it is complete.
    fn route_signal(&self, slot: u32, status: i32) {
        let slab = unsafe { &mut *self.reply_slab.get() };
        let Some(entry) = slab.get_mut(slot as usize) else {
            return;
        };
        match entry {
            ReplySlot::Waiting(waker) => {
                let waker = waker.take();
                *entry = ReplySlot::Signalled(status);
                if let Some(waker) = waker {
                    waker.wake();
                }
            }
            ReplySlot::Cancelled(_) => {
                // The awaiter walked away; the slot was held only so its index
                // could not be reused while this wakeup was in flight. Nothing
                // is owned, so releasing it is all there is to do.
                slab.remove(slot as usize);
            }
            ReplySlot::Delivered(_) | ReplySlot::Signalled(_) => {
                debug_assert!(false, "second reply delivered to one slot");
            }
        }
    }

    /// Append a typed message to the generic inbox and wake any pending
    /// `recv_msg_from_peer`.
    fn push_msg_inbox(&self, msg: ErasedMessage) {
        let inbox = unsafe { &mut *self.msg_inbox.get() };
        inbox.queue.push_back(msg);
        if let Some(waker) = inbox.waker.take() {
            waker.wake();
        }
    }

    /// Reserve a reply slot for a request about to go out. The returned index
    /// is what the request carries in `sender.slab`; the peer echoes it back in
    /// the reply's `receiver.slab`.
    pub fn reserve_reply_slot(&self) -> u32 {
        let slab = unsafe { &mut *self.reply_slab.get() };
        let index = slab.insert(ReplySlot::Waiting(None));
        index.try_into().expect("reply slab index exceeds u32")
    }

    /// Poll a reserved reply slot: `Ready` once the peer's reply has landed
    /// (removing the slot), otherwise parks `waker` and returns `Pending`.
    pub fn poll_reply_slot(&self, index: u32, waker: &Waker) -> std::task::Poll<ErasedMessage> {
        let slab = unsafe { &mut *self.reply_slab.get() };
        match slab.get_mut(index as usize) {
            Some(ReplySlot::Delivered(_)) => {
                let ReplySlot::Delivered(msg) = slab.remove(index as usize) else {
                    unreachable!("slot was Delivered")
                };
                std::task::Poll::Ready(msg)
            }
            Some(slot @ ReplySlot::Waiting(_)) => {
                *slot = ReplySlot::Waiting(Some(waker.clone()));
                std::task::Poll::Pending
            }
            Some(ReplySlot::Signalled(_)) => {
                unreachable!("a signal answered a slot awaiting a message")
            }
            Some(ReplySlot::Cancelled(_)) => {
                unreachable!("polled a reply slot after its future was cancelled")
            }
            None => panic!("polled a reply slot that no longer exists"),
        }
    }

    /// Poll a reserved reply slot for a pointer-free wakeup: `Ready(status)`
    /// once the peer has signalled it (removing the slot), otherwise parks
    /// `waker` and returns `Pending`.
    ///
    /// The signal-kind counterpart of
    /// [`poll_reply_slot`](Self::poll_reply_slot). A slot is answered one way or
    /// the other for the whole of its life — which side it is on is fixed by the
    /// requester's choice of API — so a message landing here is a protocol
    /// error, and unlike the type cast it replaces, it is *caught* rather than
    /// silently reinterpreted.
    pub fn poll_reply_signal(&self, index: u32, waker: &Waker) -> std::task::Poll<i32> {
        let slab = unsafe { &mut *self.reply_slab.get() };
        match slab.get_mut(index as usize) {
            Some(ReplySlot::Signalled(_)) => {
                let ReplySlot::Signalled(status) = slab.remove(index as usize) else {
                    unreachable!("slot was Signalled")
                };
                std::task::Poll::Ready(status)
            }
            Some(slot @ ReplySlot::Waiting(_)) => {
                *slot = ReplySlot::Waiting(Some(waker.clone()));
                std::task::Poll::Pending
            }
            Some(ReplySlot::Delivered(_)) => {
                unreachable!("a message answered a slot awaiting a signal")
            }
            Some(ReplySlot::Cancelled(_)) => {
                unreachable!("polled a reply slot after its future was cancelled")
            }
            None => panic!("polled a reply slot that no longer exists"),
        }
    }

    /// Release a reply slot whose awaiting future was dropped.
    ///
    /// If a reply had already been delivered but never taken, free it now via
    /// `drop_glue` (which knows the concrete type) and remove the slot. If no
    /// reply has arrived yet, keep the slot reserved as `Cancelled`, stashing
    /// `drop_glue` so the eventual late reply is freed — and the slot released —
    /// in [`route_msg`](Self::route_msg). Holding the
    /// slot occupied until then stops its index from being reused while a reply
    /// is still in flight toward it.
    pub fn abandon_reply_slot(&self, index: u32, drop_glue: fn(ErasedMessage)) {
        let slab = unsafe { &mut *self.reply_slab.get() };
        match slab.get_mut(index as usize) {
            Some(ReplySlot::Delivered(_)) => {
                let ReplySlot::Delivered(msg) = slab.remove(index as usize) else {
                    unreachable!("slot was Delivered")
                };
                drop_glue(msg);
            }
            Some(slot @ ReplySlot::Waiting(_)) => {
                *slot = ReplySlot::Cancelled(Some(drop_glue));
            }
            Some(ReplySlot::Signalled(_)) => {
                // Nothing to free: a wakeup owns nothing.
                slab.remove(index as usize);
            }
            Some(ReplySlot::Cancelled(_)) => {
                debug_assert!(false, "reply slot abandoned twice");
            }
            None => {}
        }
    }

    /// Release a signal-kind reply slot whose awaiting future was dropped.
    ///
    /// The signal-kind counterpart of
    /// [`abandon_reply_slot`](Self::abandon_reply_slot), and simpler for the
    /// same reason throughout: a wakeup carries no allocation, so an already
    /// delivered one is discarded by releasing the slot, and no drop glue has to
    /// be stashed for a late one. The slot is still kept as `Cancelled` while a
    /// wakeup may be racing toward it, so its index cannot be reused and resolve
    /// somebody else's future.
    pub fn abandon_signal_slot(&self, index: u32) {
        let slab = unsafe { &mut *self.reply_slab.get() };
        match slab.get_mut(index as usize) {
            Some(ReplySlot::Signalled(_)) => {
                slab.remove(index as usize);
            }
            Some(slot @ ReplySlot::Waiting(_)) => {
                *slot = ReplySlot::Cancelled(None);
            }
            Some(ReplySlot::Delivered(_)) => {
                debug_assert!(false, "message parked in a signal reply slot");
            }
            Some(ReplySlot::Cancelled(_)) => {
                debug_assert!(false, "reply slot abandoned twice");
            }
            None => {}
        }
    }

    /// Poll for the next typed message. Returns `Ready` immediately if the
    /// inbox is non-empty; otherwise registers `waker` and returns `Pending`.
    pub fn poll_msg_inbox(&self, waker: &Waker) -> std::task::Poll<ErasedMessage> {
        let inbox = unsafe { &mut *self.msg_inbox.get() };
        match inbox.queue.pop_front() {
            Some(msg) => std::task::Poll::Ready(msg),
            None => {
                inbox.waker = Some(waker.clone());
                std::task::Poll::Pending
            }
        }
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
            MultiOpState::AcceptCancelled { .. } => MultiPollResult::Pending,
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
            SingleOpState::Notif {
                waker: existing, ..
            } => {
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
        match &mut ring.inner {
            RingBackend::Uring(ring) => match ring.submit() {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => {
                    panic!("io_uring submit failed: {:?}", e);
                }
            },
            #[cfg(feature = "simulation")]
            RingBackend::Fake(ring) => ring.submit(),
        }
    }

    pub(crate) fn submit_and_wait(&self) {
        let ring = unsafe { &mut *self.ring.get() };
        ring.pending = 0;
        match &mut ring.inner {
            RingBackend::Uring(ring) => match ring.submit_and_wait(1) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => {
                    panic!("io_uring submit_and_wait failed: {:?}", e);
                }
            },
            #[cfg(feature = "simulation")]
            RingBackend::Fake(ring) => ring.submit_and_wait(),
        }
    }

    pub(crate) fn process_completion_queue(&self) {
        let ring = unsafe { &mut *self.ring.get() };
        match &mut ring.inner {
            RingBackend::Uring(ring) => {
                for c_event in ring.completion() {
                    let decoded = CompletionEvent::from_parts(
                        c_event.user_data(),
                        c_event.result(),
                        c_event.flags(),
                    );
                    if let Some(completion_event) = decoded {
                        self.process_cqe(completion_event);
                    }
                }
            }
            #[cfg(feature = "simulation")]
            RingBackend::Fake(ring) => {
                while let Some(c_event) = ring.pop_completion() {
                    let decoded = CompletionEvent::from_parts(
                        c_event.user_data,
                        c_event.result,
                        c_event.flags,
                    );
                    if let Some(completion_event) = decoded {
                        self.process_cqe(completion_event);
                    }
                }
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
        // Only transition to Cancelled (and stash the keepalive) if the cancel
        // SQE was actually submitted. For ops already in `Ended` state,
        // `send_singleop_cancel_event` reaps the slab entry directly and
        // returns false — the keepalive then drops at end-of-scope.
        if self.send_singleop_cancel_event(token) {
            let slab = unsafe { &mut *self.slab.get() };
            *slab.get_mut(token).unwrap() = SingleOpState::Cancelled {
                bgid,
                keepalive: keepalive.map(CancelKeepalive),
            };
        }
    }

    /// Submit an async cancel SQE for `token`, returning `true` if a cancel
    /// was actually needed. If the op is already in `Ended` state (CQE arrived
    /// before this cancel call), the slab entry is reaped directly and `false`
    /// is returned — no cancel SQE is sent.
    fn send_singleop_cancel_event(&self, token: OneshotToken) -> bool {
        let slab = unsafe { &mut *self.slab.get() };
        if matches!(slab.get_mut(token), Some(SingleOpState::Ended(_))) {
            slab.remove(token);
            return false;
        }
        let raw = token.raw();
        let ring_inner = unsafe { &mut *self.ring.get() };
        let cancel_op = opcode::AsyncCancel::new(raw)
            .build()
            .user_data(UserData::cancel_echo(raw).into_bits());
        ring_inner.add(&cancel_op);
        true
    }

    pub fn cancel_multishot_op(&self, token: MultishotToken, bgid: u16) {
        self.cancel_multishot_op_keep(token, bgid, None);
    }

    /// Cancel a multishot op, transferring ownership of `keepalive` to the
    /// driver. The kernel may still write to memory referenced by the op
    /// (e.g. a UDP recvmsg's msghdr) until the cancelled CQE arrives;
    /// `keepalive` is dropped only at that point.
    pub fn cancel_multishot_op_keep(
        &self,
        token: MultishotToken,
        bgid: u16,
        keepalive: Option<Box<dyn std::any::Any>>,
    ) {
        let slab = unsafe { &mut *self.multishot_slab.get() };
        let Some(entry) = slab.get_mut(token) else {
            return;
        };

        // If the kernel op already terminated, no CQE will ever arrive for
        // this token again: an `AsyncCancel` would complete with -ENOENT on
        // its own (suppressed) user_data, and a `Cancelled` entry would wait
        // forever — leaking the slab slot and the keepalive on every
        // peer-closed connection. Reap directly instead, mirroring the
        // oneshot `Ended` fast path. This covers `Sent` after a terminal CQE
        // (EOF/error) and `Paused` (pausing works by cancelling, so a paused
        // op is already fully stopped in the kernel).
        if matches!(
            entry,
            MultiOpState::Sent { last_cqe: true, .. } | MultiOpState::Paused { .. }
        ) {
            let old = slab.remove(token);
            self.recycle_multiop_queue(old, bgid);
            return;
        }

        // Already cancelled: replacing the entry would reset its completion
        // flags and orphan it.
        if matches!(
            entry,
            MultiOpState::Cancelled { .. } | MultiOpState::AcceptCancelled { .. }
        ) {
            return;
        }

        // A pause already has a cancel SQE in flight; don't submit a second.
        let pause_cancel_in_flight = matches!(entry, MultiOpState::TringToPause { .. });

        // Recycle kernel buffers from the old state's queue before dropping it.
        let old = std::mem::replace(
            entry,
            MultiOpState::Cancelled {
                bgid,
                last_cqe: false,
                keepalive: keepalive.map(CancelKeepalive),
            },
        );
        self.recycle_multiop_queue(old, bgid);
        if !pause_cancel_in_flight {
            self.send_multiop_cancel_event(UserData::multi(token.raw()).into_bits());
        }
    }

    /// Cancel a multishot accept. The driver transitions the slab entry to
    /// `AcceptCancelled`, closes any already-accepted fds still queued from
    /// before cancel, and submits the cancel SQE. Subsequent accept CQEs
    /// arriving during the cancel window have their fds closed in
    /// `process_multi`'s `AcceptCancelled` arm.
    pub(crate) fn cancel_multishot_op_accept(&self, token: MultishotToken) {
        let slab = unsafe { &mut *self.multishot_slab.get() };
        let Some(entry) = slab.get_mut(token) else {
            return;
        };

        // Same terminal-op fast path as `cancel_multishot_op_keep`: no CQE
        // will arrive for an already-terminated op, so reap directly.
        if matches!(
            entry,
            MultiOpState::Sent { last_cqe: true, .. } | MultiOpState::Paused { .. }
        ) {
            let old = slab.remove(token);
            Self::close_queued_fds(old);
            return;
        }

        if matches!(
            entry,
            MultiOpState::Cancelled { .. } | MultiOpState::AcceptCancelled { .. }
        ) {
            return;
        }

        let pause_cancel_in_flight = matches!(entry, MultiOpState::TringToPause { .. });

        let old = std::mem::replace(entry, MultiOpState::AcceptCancelled { last_cqe: false });
        // Close any fds the kernel queued before we cancelled.
        Self::close_queued_fds(old);
        if !pause_cancel_in_flight {
            self.send_multiop_cancel_event(UserData::multi(token.raw()).into_bits());
        }
    }

    fn close_queued_fds(state: MultiOpState) {
        let mut queue = match state {
            MultiOpState::Sent { queue, .. }
            | MultiOpState::TringToPause { queue, .. }
            | MultiOpState::Paused { queue, .. } => queue,
            _ => return,
        };
        while let Some(event) = queue.pop() {
            if event.result > 0 {
                unsafe {
                    libc::close(event.result);
                }
            }
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
                    self.recycle_by_bgid(bgid, buffer_id);
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
    /// Zero-copy send: the completion CQE arrived with `IORING_CQE_F_MORE`
    /// set, so the byte count is known but the kernel may still be reading
    /// the buffer. We stash the result and wait for the `IORING_CQE_F_NOTIF`
    /// notification before waking — only then is the buffer safe to release.
    Notif { result: i32, waker: Waker },
    Ended(OpMetadata),
    Signal(Box<[u8; 8]>),
    /// Slot for a non-terminal op in a link chain. The `u64` is the slab
    /// id of the next link, so a cancel walker can traverse the chain and
    /// reap slab entries. No waker — the result is dropped on completion.
    LinkedSent(u64),
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

    fn len(&self) -> usize {
        self.0.len()
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
    inner: RingBackend,
    pending: u32,
    wakeup_timeout: Timespec,
}

/// Where submissions go and completions come from.
///
/// An enum rather than a type parameter on purpose: the set is closed — a real
/// io_uring and a simulated one — and the driver is reached through a scoped
/// thread-local of concrete type, which a type parameter would force onto every
/// operation future and all 82 of the driver's call sites. The cost is one
/// perfectly-predicted branch per submission.
enum RingBackend {
    Uring(IoUring),
    #[cfg(feature = "simulation")]
    Fake(FakeRing),
}

/// What a provided-buffer ring needs in order to install itself.
///
/// Registration is the only step involving the kernel — the mapping and its
/// buffers are ordinary memory either way. The simulated arm carries the ring's
/// handle instead, because something then has to play the consumer, and it
/// reaches the buffers by group id rather than by type.
#[derive(Clone, Copy)]
pub enum RingHandle<'a> {
    Uring(&'a IoUring),
    #[cfg(feature = "simulation")]
    Simulated(&'a FakeRingHandle),
}

impl Ring {
    pub(crate) fn add(&mut self, entry: &squeue::Entry) {
        match &mut self.inner {
            RingBackend::Uring(ring) => unsafe {
                ring.submission().push(entry).unwrap();
            },
            #[cfg(feature = "simulation")]
            RingBackend::Fake(ring) => ring.add(entry),
        }
        self.pending += 1;
    }

    fn handle(&self) -> RingHandle<'_> {
        match &self.inner {
            RingBackend::Uring(ring) => RingHandle::Uring(ring),
            #[cfg(feature = "simulation")]
            RingBackend::Fake(fake) => RingHandle::Simulated(fake.handle()),
        }
    }
}
