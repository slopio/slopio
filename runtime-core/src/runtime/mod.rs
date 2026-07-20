use std::cell::UnsafeCell;
use std::collections::VecDeque;
use std::os::fd::RawFd;
use std::pin::pin;
use std::task::{Context, Waker};
use std::time::Duration;

use crate::io_driver::{BufRingSet, Endpoint, Header, IODriver, Message, PeerMessage};
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
scoped_thread_local!(pub static TIME_DRIVER: TimeDriver);
thread_local!(static THREAD_ID: std::cell::Cell<usize> = const { std::cell::Cell::new(0) });

#[derive(Default)]
pub struct RuntimeInner {
    pub tasks: TaskList,
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

    /// Like [`with_thread_id`](Self::with_thread_id), but attaches the shared
    /// peer table to this worker's io_driver so it can MSG_RING its peers.
    pub fn with_peers(thread_id: usize, peers: std::sync::Arc<peers::PeerTable>) -> Self {
        let mut rt = Self {
            thread_id,
            ..Default::default()
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
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        set_thread_id(self.thread_id);
        self.io_driver.with(|| {
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

                                // Only flush if tasks actually submitted IO operations.
                                // Avoids a syscall on pure task/channel workloads.
                                if self.io_driver.has_pending_sqes() {
                                    self.io_driver.submit();
                                    self.io_driver.process_completion_queue();
                                }

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
                                        return output;
                                    }
                                    std::task::Poll::Pending => {}
                                }
                                if self.tasks.is_empty() {
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
        })
    }
}

pub fn set_thread_id(id: usize) {
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
    RecvFromPeer { _priv: () }
}

/// Send a heap-allocated, typed message to peer `thread_id`. The payload is
/// boxed, its type erased for the crossing, and recovered by the receiver via
/// [`ErasedMessage::downcast`]. Only valid inside a multi-threaded runtime.
pub fn send_boxed_to_peer<T: Send + 'static>(thread_id: usize, payload: T) {
    let erased = Message::new(payload).erase();
    crate::io_driver::IO_DRIVER.with(|d| d.send_boxed_to_peer(thread_id, erased));
}

/// Await the next boxed message sent to this worker by a peer, recovering it as
/// `Box<Message<T>>`.
///
/// # Safety
/// The next message in this worker's inbox must actually be a `Message<T>`.
/// The receiver is responsible for knowing which type it is sent — there is no
/// runtime tag to check against. Reinterpreting a different payload as `T` is
/// undefined behaviour.
pub unsafe fn recv_boxed_from_peer<T: 'static>() -> RecvBoxedFromPeer<T> {
    RecvBoxedFromPeer {
        _marker: std::marker::PhantomData,
    }
}

/// Future returned by [`recv_boxed_from_peer`]. Resolves to the next inbound
/// message cast to `Box<Message<T>>`, parking on the worker's boxed inbox until
/// one arrives.
pub struct RecvBoxedFromPeer<T> {
    _marker: std::marker::PhantomData<fn() -> T>,
}

impl<T: 'static> std::future::Future for RecvBoxedFromPeer<T> {
    type Output = Box<Message<T>>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> std::task::Poll<Box<Message<T>>> {
        crate::io_driver::IO_DRIVER
            .with(|d| d.poll_boxed_inbox(cx.waker()))
            // SAFETY: forwarded from `recv_boxed_from_peer`'s contract — the
            // caller guarantees the next message is a `Message<T>`.
            .map(|erased| unsafe { erased.into_typed::<T>() })
    }
}

/// Send a typed request to `peer` and await its reply.
///
/// Reserves a reply slot on this worker, stamps it into the request's
/// `sender.slab`, and returns a future that resolves when the peer replies (via
/// [`reply_boxed_to_peer`], which routes the reply straight back into that
/// slot). Only valid inside a multi-threaded runtime.
pub fn request_boxed_from_peer<Req: Send + 'static, Rep>(
    peer: usize,
    payload: Req,
) -> RecvBoxedReply<Rep> {
    let slot = crate::io_driver::IO_DRIVER.with(|d| {
        let slot = d.reserve_reply_slot();
        let header = Header::new(
            Endpoint::with_slab(thread_id() as u16, slot),
            Endpoint::new(peer as u16),
        );
        d.send_boxed_to_peer(peer, Message::with_header(header, payload).erase());
        slot
    });
    RecvBoxedReply {
        slot,
        _marker: std::marker::PhantomData,
    }
}

/// Reply to a request received via [`recv_boxed_from_peer`], routing `payload`
/// back to the requester's reply slot.
///
/// `request_header` is the header of the received request (`msg.header`); its
/// two ends are swapped so the reply lands in the slot the requester reserved.
pub fn reply_boxed_to_peer<Rep: Send + 'static>(request_header: &Header, payload: Rep) {
    let header = request_header.swapped();
    let peer = header.receiver.thread as usize;
    crate::io_driver::IO_DRIVER
        .with(|d| d.send_boxed_to_peer(peer, Message::with_header(header, payload).erase()));
}

/// Future returned by [`request_boxed_from_peer`]. Resolves to the peer's reply
/// as `Box<Message<Rep>>` once it lands in the reserved slot.
///
/// # Safety
/// The peer must reply with a `Message<Rep>`. As with [`recv_boxed_from_peer`],
/// there is no runtime type tag; reinterpreting a different payload as `Rep` is
/// undefined behaviour. Dropping the future before the reply arrives releases
/// the slot; a reply already delivered but not yet taken is freed on drop.
pub struct RecvBoxedReply<Rep: 'static> {
    slot: u32,
    _marker: std::marker::PhantomData<fn() -> Rep>,
}

impl<Rep: 'static> std::future::Future for RecvBoxedReply<Rep> {
    type Output = Box<Message<Rep>>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> std::task::Poll<Box<Message<Rep>>> {
        crate::io_driver::IO_DRIVER
            .with(|d| d.poll_reply_slot(self.slot, cx.waker()))
            // SAFETY: forwarded from `request_boxed_from_peer`'s contract — the
            // peer replies with a `Message<Rep>`.
            .map(|erased| unsafe { erased.into_typed::<Rep>() })
    }
}

impl<Rep: 'static> Drop for RecvBoxedReply<Rep> {
    fn drop(&mut self) {
        // Release the slot. If a reply was delivered but never polled out, we
        // now know its concrete type, so reclaim the allocation instead of
        // leaking it.
        if let Some(erased) = crate::io_driver::IO_DRIVER.with(|d| d.abandon_reply_slot(self.slot))
        {
            drop(unsafe { erased.into_typed::<Rep>() });
        }
    }
}

/// Future returned by [`recv_from_peer`]. Resolves to the next inbound
/// [`PeerMessage`], parking on the worker's inbox until one arrives.
pub struct RecvFromPeer {
    _priv: (),
}

impl std::future::Future for RecvFromPeer {
    type Output = PeerMessage;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> std::task::Poll<PeerMessage> {
        crate::io_driver::IO_DRIVER.with(|d| d.poll_peer_inbox(cx.waker()))
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
