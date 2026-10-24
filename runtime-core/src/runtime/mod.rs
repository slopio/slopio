use std::cell::UnsafeCell;
use std::collections::VecDeque;
use std::os::fd::RawFd;
use std::pin::pin;
use std::sync::Arc;
use std::task::{Context, RawWaker, RawWakerVTable, Waker};
use std::time::Duration;

use crate::io_driver::{
    BufRingSet, Endpoint, ErasedMessage, Header, IODriver, Message, PeerMessage, RingOptions,
};
use crate::runtime::time::TimeDriver;
use crate::task::{Handle, TypeAgnosticTask, new_task};
use crate::{runtime, scoped_thread_local};

pub mod eventfd;
pub mod join;
pub mod peers;
pub mod processes;
pub mod repeat;
pub mod retry;
pub mod select;
pub mod time;

/// Yields control back to the scheduler for one iteration, then resumes.
///
/// Useful inside CPU-bound async tasks to avoid starving other tasks on the
/// same thread. Equivalent to `tokio::task::yield_now()`.
///
/// ```rust,ignore
/// async fn heavy(data: &[u8]) {
///     for chunk in data.chunks(10_000) {
///         process(chunk);
///         yield_now().await;
///     }
/// }
/// ```
pub async fn yield_now() {
    struct YieldNow(bool);

    impl Future for YieldNow {
        type Output = ();

        fn poll(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<()> {
            if self.0 {
                return std::task::Poll::Ready(());
            }
            self.0 = true;
            cx.waker().wake_by_ref();
            std::task::Poll::Pending
        }
    }

    YieldNow(false).await
}

scoped_thread_local!(pub(crate) static TASK_LIST: TaskList);
scoped_thread_local!(pub(crate) static TIME_DRIVER: TimeDriver);
thread_local!(static THREAD_ID: std::cell::Cell<usize> = const { std::cell::Cell::new(0) });
thread_local!(static ROOT_WOKEN: std::cell::Cell<bool> = const { std::cell::Cell::new(false) });

/// The root future's waker: no data, no allocation, it only flags the thread.
static ROOT_WAKER_VTABLE: RawWakerVTable =
    RawWakerVTable::new(root_waker_clone, root_wake, root_wake, root_waker_drop);

fn root_waker_clone(_: *const ()) -> RawWaker {
    RawWaker::new(std::ptr::null(), &ROOT_WAKER_VTABLE)
}

fn root_wake(_: *const ()) {
    ROOT_WOKEN.set(true);
}

fn root_waker_drop(_: *const ()) {}

#[derive(Default)]
pub struct RuntimeInner {
    tasks: TaskList,
    io_driver: IODriver,
    time_driver: TimeDriver,
    thread_id: usize,
}

impl RuntimeInner {
    pub fn with_thread_id(thread_id: usize) -> Self {
        Self {
            thread_id,
            ..Default::default()
        }
    }

    /// A runtime whose IO is simulated.
    ///
    /// If the ring was built with `virtual_time`, its clock is handed to the
    /// time driver too: the two must agree about what time it is, or the ring
    /// would jump ahead while the timewheel stayed put and no timer would ever
    /// expire.
    #[cfg(feature = "simulation")]
    pub fn simulated(fake: crate::deterministic_testing::FakeRing) -> Self {
        // Read before `fake` moves into the driver.
        let time_driver = match fake.clock() {
            Some(clock) => time::TimeDriver::simulated(clock),
            None => time::TimeDriver::default(),
        };
        // Field by field: `..Default::default()` would build a whole second
        // runtime, real io_uring included, just to discard it.
        Self {
            tasks: TaskList::default(),
            io_driver: IODriver::simulated(fake),
            time_driver,
            thread_id: 0,
        }
    }

    /// Like [`with_thread_id`](Self::with_thread_id), but attaches the shared
    /// peer table to this worker's io_driver so it can MSG_RING its peers, and
    /// builds its ring with `ring`, its SQ thread on `sq_cpu` under `sqpoll`.
    pub fn with_peers(
        thread_id: usize,
        peers: std::sync::Arc<peers::PeerTable>,
        ring: RingOptions,
        sq_cpu: Option<u32>,
    ) -> Self {
        // Field by field: `..Default::default()` would build a default ring only to drop it.
        let mut rt = Self {
            tasks: TaskList::default(),
            io_driver: IODriver::new(ring, sq_cpu),
            time_driver: TimeDriver::default(),
            thread_id,
        };
        rt.io_driver.attach_peers(peers);
        rt
    }

    pub fn block_on<R: BufRingSet, F: Future>(
        self,
        f: F,
        event_fd: Option<RawFd>,
        wrap: impl FnOnce(Box<dyn FnOnce() -> F::Output + '_>) -> F::Output,
    ) -> F::Output {
        // SAFETY: the vtable ignores the data pointer, so any clone stays valid.
        let waker = unsafe { Waker::from_raw(root_waker_clone(std::ptr::null())) };
        let mut context = Context::from_waker(&waker);
        set_thread_id(self.thread_id);
        let output = self.io_driver.with(|| {
            self.io_driver.init_buf_rings::<R>();
            TASK_LIST.set(&self.tasks, || {
                TIME_DRIVER.set(&self.time_driver, || {
                    let run: Box<dyn FnOnce() -> F::Output + '_> = Box::new(|| {
                        // Publish our ring fd into the shared peer table and
                        // rendezvous, so every worker's fd is visible before
                        // any task runs. No-op under `main_thread` (no peers).
                        self.io_driver.publish_self(self.thread_id);
                        if let Some(fd) = event_fd {
                            runtime::eventfd::register_eventfd_read(fd);
                        }
                        let mut pinned_f = pin!(f);
                        loop {
                            loop {
                                self.time_driver.tick();
                                // Drain one generation: only tasks queued
                                // before this pass. Tasks that re-queue
                                // themselves while polling (yield_now,
                                // self-wakes) run on the next pass, after
                                // timers tick and IO flushes — otherwise a
                                // single always-ready task starves the
                                // timewheel, the IO driver and the root
                                // future for as long as it stays ready.
                                let mut budget = self.tasks.len();
                                while budget > 0 {
                                    let Some(item) = self.tasks.pop() else {
                                        break;
                                    };
                                    item.poll();
                                    budget -= 1;
                                }

                                // Only enter if tasks submitted IO or the kernel holds
                                // completions back. Avoids a syscall on pure
                                // task/channel workloads.
                                if self.io_driver.has_pending_sqes()
                                    || self.io_driver.has_deferred_completions()
                                {
                                    self.io_driver.submit();
                                    self.io_driver.process_completion_queue();
                                }

                                ROOT_WOKEN.set(false);
                                let pinned = pinned_f.as_mut();
                                match pinned.poll(&mut context) {
                                    std::task::Poll::Ready(output) => {
                                        // Flush any ops submitted during this
                                        // final poll (e.g. a MSG_RING to a peer)
                                        // before returning — otherwise they sit
                                        // unsubmitted in the SQ and are lost.
                                        if self.io_driver.has_pending_sqes() {
                                            self.io_driver.submit();
                                        }
                                        self.io_driver.wait_sq_drained();
                                        return output;
                                    }
                                    std::task::Poll::Pending => {}
                                }
                                // A root that woke itself mid-poll must run again, not sleep.
                                if self.tasks.is_empty() && !ROOT_WOKEN.get() {
                                    break;
                                }
                            }
                            // Queue is empty and no completions arrived — block
                            // until the kernel has at least one event for us.
                            match self.time_driver.next_deadline() {
                                Some(delay) if !delay.is_zero() => {
                                    self.io_driver.push_wakeup_timeout(delay);
                                    self.io_driver.submit_and_wait();
                                }
                                Some(_) => {
                                    self.io_driver
                                        .push_wakeup_timeout(Duration::from_millis(10));
                                    self.io_driver.submit_and_wait();
                                }
                                None => {
                                    self.io_driver.submit_and_wait();
                                }
                            }
                            self.io_driver.process_completion_queue();
                        }
                    });
                    wrap(run)
                })
            })
        });
        // Rings outlive the runtime; reset only once tasks and driver dropped their buffers.
        #[cfg(feature = "simulation")]
        {
            let simulated = self.io_driver.is_simulated();
            drop(self);
            if simulated {
                R::reset_all();
            }
        }
        output
    }
}

pub(crate) fn set_thread_id(id: usize) {
    THREAD_ID.with(|cell| cell.set(id));
}

pub fn thread_id() -> usize {
    THREAD_ID.with(|cell| cell.get())
}

pub fn main_thread<F: Future>(f: F) -> F::Output {
    RuntimeInner::default().block_on::<(), F>(f, None, |run| run())
}

pub fn now() -> std::time::SystemTime {
    TIME_DRIVER.with(|d| d.now())
}

/// Send a message to peer `thread_id` from the current worker via MSG_RING.
/// The peer receives a [`PeerMessage`] carrying `payload` (62 bits) and `data`.
/// Only valid inside a multi-threaded runtime.
pub fn send_to_peer(thread_id: usize, data: i32, payload: u64) {
    crate::io_driver::IO_DRIVER.with(|d| d.send_to_peer(thread_id, data, payload));
}

/// Await the next message sent to this worker by a peer.
pub fn recv_from_peer() -> RecvFromPeer {
    RecvFromPeer { parked: false }
}

/// Send a heap-allocated, typed message to peer `thread_id`. The payload is
/// allocated, its type erased for the crossing, and recovered by the receiver
/// via [`recv_msg_from_peer`]. Only valid inside a multi-threaded runtime.
///
/// This hands the whole allocation over: the sender keeps nothing, so the
/// receiver's `Arc` is unique and `Arc::into_inner` gives it the payload
/// outright. Use [`send_shared_to_peer`] to keep a handle on it instead.
///
/// Safe despite delegating to an `unsafe` sender: the `Arc` is allocated here
/// and never escapes, so no clone can survive the crossing and the payload is
/// owned by exactly one thread throughout.
pub fn send_msg_to_peer<T: Send + 'static>(thread_id: usize, payload: T) {
    // SAFETY: `Message::new` is the only holder of this `Arc` and it is moved
    // straight into the send, so the receiver's reference is unique. No `&T`
    // can exist on two threads at once, which is the whole contract below.
    unsafe { send_shared_to_peer(thread_id, Message::new(payload)) };
}

/// Send an already-allocated message to peer `thread_id`, consuming one strong
/// reference.
///
/// This is the sharing form of [`send_msg_to_peer`]: clone `msg` beforehand to
/// keep reading the payload after it has crossed, or to fan the *same*
/// allocation out to several peers without re-allocating per peer:
///
/// ```ignore
/// let msg = Message::new(Config { .. });  // `Config: Sync`, so sharing is sound
/// for peer in 1..n {
///     unsafe { send_shared_to_peer(peer, Arc::clone(&msg)) };
/// }
/// // `msg` is still readable here.
/// ```
///
/// Every surviving clone means the receiver sees a non-unique `Arc` and can only
/// read the payload, never take it.
///
/// # Safety
/// If any clone of `msg` outlives this call, the payload is genuinely shared:
/// this thread and `thread_id` can hold a `&T` at the same time. The caller must
/// ensure that is sound for `T` — which for a `T: Sync` payload it always is,
/// and for a `!Sync` one means arranging the discipline by hand (no interior
/// mutability touched while shared, a handoff protocol, or the caller's own
/// locking).
///
/// The bound is `Send` alone precisely so that choice stays with the caller.
/// Dropping every clone before calling — the plain move — discharges the
/// contract outright: the receiver's `Arc` is unique and no `&T` ever exists on
/// two threads at once. That case is what safe [`send_msg_to_peer`] wraps.
pub unsafe fn send_shared_to_peer<T: Send + 'static>(thread_id: usize, msg: Arc<Message<T>>) {
    // SAFETY: forwarded from this function's own contract, which is theirs.
    let erased = unsafe { msg.erase() };
    crate::io_driver::IO_DRIVER.with(|d| unsafe { d.send_erased_to_peer(thread_id, erased) });
}

/// Await the next typed message sent to this worker by a peer, recovering it as
/// `Arc<Message<T>>`.
///
/// The `Arc` is unique — so `Arc::into_inner` yields the payload — only if the
/// sender kept no clone (i.e. it used [`send_msg_to_peer`] rather than
/// [`send_shared_to_peer`] with a retained handle).
///
/// # Safety
/// The next message in this worker's inbox must actually be a `Message<T>`.
/// The receiver is responsible for knowing which type it is sent — there is no
/// runtime tag to check against. Reinterpreting a different payload as `T` is
/// undefined behaviour.
pub unsafe fn recv_msg_from_peer<T: 'static>() -> RecvMsgFromPeer<T> {
    RecvMsgFromPeer {
        parked: false,
        _marker: std::marker::PhantomData,
    }
}

/// Future returned by [`recv_msg_from_peer`]. Resolves to the next inbound
/// message cast to `Arc<Message<T>>`, parking on the worker's message inbox until
/// one arrives.
pub struct RecvMsgFromPeer<T> {
    /// This future is the inbox's parked consumer.
    parked: bool,
    _marker: std::marker::PhantomData<fn() -> T>,
}

impl<T> Drop for RecvMsgFromPeer<T> {
    fn drop(&mut self) {
        if self.parked {
            crate::io_driver::IO_DRIVER.try_with(|d| {
                if let Some(d) = d {
                    d.leave_msg_inbox(&mut self.parked);
                }
            });
        }
    }
}

impl<T: 'static> std::future::Future for RecvMsgFromPeer<T> {
    type Output = Arc<Message<T>>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> std::task::Poll<Arc<Message<T>>> {
        let this = self.get_mut();
        crate::io_driver::IO_DRIVER
            .with(|d| d.poll_msg_inbox(cx.waker(), &mut this.parked))
            // SAFETY: forwarded from `recv_msg_from_peer`'s contract — the
            // caller guarantees the next message is a `Message<T>`.
            .map(|erased| unsafe { erased.into_typed::<T>() })
    }
}

/// Send a typed request to `peer` and await its reply.
///
/// Reserves a reply slot on this worker, stamps it into the request's
/// `sender.slab`, and returns a future that resolves when the peer replies (via
/// [`reply_msg_to_peer`], which routes the reply straight back into that slot,
/// bypassing the generic inbox). The request goes out immediately, not on first
/// poll. Only valid inside a multi-threaded runtime.
///
/// There is no timeout and no error channel: if the peer never replies, the
/// future stays `Pending` forever. Wrap it in [`select`](select::select) against
/// a [`Sleep`](time::Sleep) if you need a deadline. Dropping the future early is
/// safe — the slot stays reserved until any in-flight reply lands and is freed.
///
/// # Safety
/// The peer must answer this request with a `Message<Rep>`. There is no runtime
/// type tag: the returned future casts whatever lands in the reply slot straight
/// to `Rep`, so a reply of any other type is undefined behaviour. Calling this
/// asserts both halves of the protocol — that `peer` runs a handler for `Req`,
/// and that the handler replies with exactly `Rep`.
///
/// Note the asymmetry this leaves: that assertion can be broken from *safe*
/// code. [`send_msg_to_peer`] and [`reply_msg_to_peer`] are safe, and [`Header`]
/// is publicly constructible, so nothing stops another worker from aiming a
/// differently-typed message at this slot. ([`send_shared_to_peer`] is `unsafe`,
/// but for an unrelated reason — sharing a payload across threads, not routing.)
/// Keeping that from happening is a
/// whole-program invariant that this signature cannot enforce; the `unsafe`
/// here marks where the cast is asserted, not where it can be violated.
pub unsafe fn request_msg_from_peer<Req: Send + 'static, Rep>(
    peer: usize,
    payload: Req,
) -> RecvMsgReply<Rep> {
    let slot = crate::io_driver::IO_DRIVER.with(|d| {
        let slot = d.reserve_reply_slot();
        let header = Header::new(
            Endpoint::with_slab(thread_id() as u16, slot),
            Endpoint::new(peer as u16),
        );
        // SAFETY: the `Arc` is allocated here and moved straight in: unique.
        unsafe { d.send_erased_to_peer(peer, Message::with_header(header, payload).erase()) };
        slot
    });
    RecvMsgReply {
        slot,
        resolved: false,
        _marker: std::marker::PhantomData,
    }
}

/// Reply to a request received via [`recv_msg_from_peer`], routing `payload`
/// back to the requester's reply slot.
///
/// `request_header` is the header of the received request (`msg.header`); its
/// two ends are swapped so the reply lands in the slot the requester reserved.
/// The destination worker is taken from the swapped header, which is why there
/// is no `peer` argument.
///
/// This is the same wire operation as [`send_msg_to_peer`] — only the header
/// differs. That matters in both directions:
///
/// - Answering with [`send_msg_to_peer`] instead zeroes the header, so the reply
///   lands in the requester's *generic* inbox and its future never resolves. No
///   error is reported; the `await` simply hangs.
/// - Pass the header you received, never one you built. A forged
///   `receiver.slab` aims the reply at an arbitrary slot on the target worker,
///   whose awaiter will then cast it to *its* `Rep`. Likewise, `Rep` must be the
///   type that requester declared — see the safety contract on
///   [`request_msg_from_peer`], which this call is trusted to uphold.
///
/// Replying to a fire-and-forget message (one whose `sender.slab` is `None`) is
/// harmless: the reply just falls through to the sender's generic inbox.
pub fn reply_msg_to_peer<Rep: Send + 'static>(request_header: &Header, payload: Rep) {
    let header = request_header.swapped();
    let peer = header.receiver.thread as usize;
    // SAFETY: the `Arc` is allocated here and moved straight in: unique.
    crate::io_driver::IO_DRIVER.with(|d| unsafe {
        d.send_erased_to_peer(peer, Message::with_header(header, payload).erase())
    });
}

/// Future returned by [`request_msg_from_peer`]. Resolves to the peer's reply
/// as `Arc<Message<Rep>>` once it lands in the reserved slot.
///
/// The cast to `Rep` rides on the contract asserted at [`request_msg_from_peer`];
/// this type carries no check of its own.
///
/// Dropping it before the reply arrives does not release the slot outright — a
/// reply may still be racing toward it. The slot is kept as `Cancelled` with
/// type-recovering drop glue, so a late reply is freed when it lands rather than
/// leaking or mis-routing into a reused slot. A reply already delivered but
/// never taken is freed immediately on drop.
pub struct RecvMsgReply<Rep: 'static> {
    slot: u32,
    resolved: bool,
    _marker: std::marker::PhantomData<fn() -> Rep>,
}

impl<Rep: 'static> std::future::Future for RecvMsgReply<Rep> {
    type Output = Arc<Message<Rep>>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> std::task::Poll<Arc<Message<Rep>>> {
        let this = self.get_mut();
        let polled = crate::io_driver::IO_DRIVER.with(|d| d.poll_reply_slot(this.slot, cx.waker()));
        if polled.is_ready() {
            // `poll_reply_slot` removed the slot on the way out; remember it,
            // so `drop` does not reach for an index the slab may have reused.
            this.resolved = true;
        }
        // SAFETY: forwarded from `request_msg_from_peer`'s contract — the
        // peer replies with a `Message<Rep>`.
        polled.map(|erased| unsafe { erased.into_typed::<Rep>() })
    }
}

impl<Rep: 'static> Drop for RecvMsgReply<Rep> {
    fn drop(&mut self) {
        if self.resolved {
            return;
        }
        // Don't free the slot outright — a reply may still be racing toward it.
        // Hand `abandon_reply_slot` the type-recovering glue for `Rep`: if a
        // reply was already delivered but never polled, it is freed now; if none
        // has arrived, the slot is kept as `Cancelled` (glue stashed) so the
        // late reply is freed when it lands, rather than leaking or mis-routing
        // into a reused slot.
        crate::io_driver::IO_DRIVER
            .with(|d| d.abandon_reply_slot(self.slot, drop_erased_as::<Rep>));
    }
}

/// Type-recovering drop glue for a reply that outlived its awaiter. Stashed in
/// the `Cancelled` slot at drop time — the one place that still statically knows
/// `Rep` — so the driver can release a reply it only holds as an
/// [`ErasedMessage`]. Dropping the recovered `Arc` releases the strong reference
/// the wire was carrying; the allocation itself only goes away if the sender
/// kept no clone.
fn drop_erased_as<Rep: 'static>(erased: ErasedMessage) {
    drop(unsafe { erased.into_typed::<Rep>() });
}

/// Answer a request with a pointer-free wakeup instead of a message: nothing is
/// allocated, and nothing crosses the ring but the slot index and `status`.
///
/// This serves the protocol where the requester still *holds* what it asked
/// about — it shared the request rather than handing it over, so the answer has
/// no payload to carry and only has to say "done". Note what that removes: with
/// no pointer there is no transferred reference to reclaim and no cast to
/// assert, so unlike [`reply_msg_to_peer`] this call is safe, and its
/// counterpart [`ReplySignal`] carries no type contract at all.
///
/// `request_header` is the header of the received request; its two ends are
/// swapped so the wakeup lands in the slot the requester reserved. The requester
/// must be awaiting a [`ReplySignal`] and not a [`RecvMsgReply`] — a slot is
/// answered one way for the whole of its life — and a request that reserved no
/// slot (fire-and-forget, `sender.slab == None`) has nothing to wake, so the
/// call does nothing.
pub fn signal_reply_to_peer(request_header: &Header, status: i32) {
    let header = request_header.swapped();
    let Some(slot) = header.receiver.slab else {
        return;
    };
    let peer = header.receiver.thread as usize;
    crate::io_driver::IO_DRIVER.with(|d| d.send_signal_to_peer(peer, slot, status));
}

/// A reserved reply slot awaiting a pointer-free wakeup, resolving to the
/// `status` the peer passed to [`signal_reply_to_peer`].
///
/// Unlike [`RecvMsgReply`], which [`request_msg_from_peer`] hands back already
/// armed, this one is reserved *before* the request goes out: the caller needs
/// the slot index to stamp into the header of a request it allocates and sends
/// itself — typically with [`send_shared_to_peer`], keeping its own reference to
/// the payload, which is what makes a payload-free answer possible at all.
/// Reserve, read [`slot`](Self::slot), send, then await.
///
/// Dropping it before the wakeup arrives keeps the slot reserved as cancelled so
/// its index cannot be reused while a wakeup is still racing toward it; the late
/// wakeup releases it on arrival.
pub struct ReplySignal {
    slot: u32,
    resolved: bool,
}

impl ReplySignal {
    /// Reserve a slot on this worker for a peer to signal.
    pub fn reserve() -> Self {
        let slot = crate::io_driver::IO_DRIVER.with(|d| d.reserve_reply_slot());
        ReplySignal {
            slot,
            resolved: false,
        }
    }

    /// The reserved slot's index, to stamp into the outgoing request's
    /// `sender.slab` so the peer knows what to wake.
    #[inline]
    pub fn slot(&self) -> u32 {
        self.slot
    }
}

impl std::future::Future for ReplySignal {
    type Output = i32;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> std::task::Poll<i32> {
        let this = self.get_mut();
        let polled =
            crate::io_driver::IO_DRIVER.with(|d| d.poll_reply_signal(this.slot, cx.waker()));
        if polled.is_ready() {
            // `poll_reply_signal` removed the slot on the way out; remember it,
            // so `drop` does not reach for an index the slab may have reused.
            this.resolved = true;
        }
        polled
    }
}

impl Drop for ReplySignal {
    fn drop(&mut self) {
        if !self.resolved {
            crate::io_driver::IO_DRIVER.with(|d| d.abandon_signal_slot(self.slot));
        }
    }
}

/// Future returned by [`recv_from_peer`]. Resolves to the next inbound
/// [`PeerMessage`], parking on the worker's inbox until one arrives.
pub struct RecvFromPeer {
    /// This future is the inbox's parked consumer.
    parked: bool,
}

impl std::future::Future for RecvFromPeer {
    type Output = PeerMessage;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> std::task::Poll<PeerMessage> {
        let this = self.get_mut();
        crate::io_driver::IO_DRIVER.with(|d| d.poll_peer_inbox(cx.waker(), &mut this.parked))
    }
}

impl Drop for RecvFromPeer {
    fn drop(&mut self) {
        if self.parked {
            crate::io_driver::IO_DRIVER.try_with(|d| {
                if let Some(d) = d {
                    d.leave_peer_inbox(&mut self.parked);
                }
            });
        }
    }
}

pub fn spawn<F: Future + 'static>(f: F) -> Handle<F::Output> {
    let (task, handle) = new_task(f);
    TASK_LIST.with(|ctx| {
        ctx.push(task);
    });
    handle
}

#[derive(Default)]
pub struct TaskList {
    tasks: UnsafeCell<VecDeque<TypeAgnosticTask>>,
}

impl TaskList {
    pub(crate) fn push(&self, task: TypeAgnosticTask) {
        let tasks_list = unsafe { &mut *self.tasks.get() };
        tasks_list.push_back(task);
    }

    pub(crate) fn is_empty(&self) -> bool {
        let tasks_list = unsafe { &*self.tasks.get() };
        tasks_list.is_empty()
    }

    pub(crate) fn len(&self) -> usize {
        let tasks_list = unsafe { &*self.tasks.get() };
        tasks_list.len()
    }

    pub(crate) fn pop(&self) -> Option<TypeAgnosticTask> {
        let task_list = unsafe { &mut *self.tasks.get() };
        task_list.pop_front()
    }
}
