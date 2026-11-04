//! The channels, with nothing underneath them.
//!
//! No kernel, no simulator, no executor: all three channels are plain
//! userspace — a raw `Box` shared by two halves that free it between them
//! through a pair of drop flags. They are polled by hand here, against a waker
//! that does nothing but count, so what is under test is the pointer
//! discipline and the wake contract rather than some executor's reading of
//! them.
//!
//! Which also makes this the cheapest file in the repo to run under Miri:
//!
//! ```text
//! MIRIFLAGS="-Zmiri-tree-borrows" cargo +nightly miri test -p slopio-core --test channels
//! ```

use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use slopio_core::channels::{
    BoundedReceiver, BoundedSender, Receiver, Sender, bounded_channel, unbounded_channel,
};
use slopio_core::channels::{VecReceiver, VecSender, vec_channel};

/// A waker that records how often it was woken and nothing else.
///
/// Counting is the whole point: a channel that wakes too little parks a task
/// forever, and one that wakes too much is a busy loop nobody notices until
/// it is on a core.
#[derive(Default)]
struct Counter(AtomicUsize);

impl Wake for Counter {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

impl Counter {
    fn new() -> (Arc<Self>, Waker) {
        let counter = Arc::new(Self::default());
        let waker = Waker::from(counter.clone());
        (counter, waker)
    }

    fn count(&self) -> usize {
        self.0.load(Ordering::Relaxed)
    }
}

/// Poll a future once. Every channel future is `Unpin`, so no pinning dance.
fn poll_once<F: Future + Unpin>(future: &mut F, waker: &Waker) -> Poll<F::Output> {
    Pin::new(future).poll(&mut Context::from_waker(waker))
}

/// A payload that counts its own drops, so "freed exactly once" is a thing a
/// test can assert rather than something Miri alone has to notice.
struct Tracked(Rc<Cell<usize>>);

impl Drop for Tracked {
    fn drop(&mut self) {
        self.0.set(self.0.get() + 1);
    }
}

fn tracked() -> (Rc<Cell<usize>>, impl Fn() -> Tracked) {
    let drops = Rc::new(Cell::new(0));
    let handle = drops.clone();
    (drops, move || Tracked(handle.clone()))
}

// ---------------------------------------------------------------- unbounded

#[test]
fn an_item_already_queued_needs_no_waker() {
    let (_counter, waker) = Counter::new();
    let (tx, rx) = unbounded_channel();

    tx.send(7u32);
    let mut recv = rx.recv();

    assert_eq!(poll_once(&mut recv, &waker), Poll::Ready(Some(7)));
}

#[test]
fn a_receiver_with_nothing_to_take_parks_until_a_send() {
    let (counter, waker) = Counter::new();
    let (tx, rx) = unbounded_channel();

    let mut recv = rx.recv();
    assert_eq!(poll_once(&mut recv, &waker), Poll::Pending);
    assert_eq!(counter.count(), 0, "parking is not a wakeup");

    tx.send(1u32);
    assert_eq!(
        counter.count(),
        1,
        "the send should have woken the receiver"
    );
    assert_eq!(poll_once(&mut recv, &waker), Poll::Ready(Some(1)));
}

#[test]
fn only_the_send_that_ends_the_drought_wakes() {
    // `send` wakes on empty-to-non-empty, not on every push. That is sound
    // rather than merely cheap: a woken receiver keeps finding the queue
    // non-empty and returns `Ready` without ever parking again, so there is no
    // wakeup left to owe it. A second wake here would be a wasted trip through
    // the scheduler for a task that is already runnable.
    let (counter, waker) = Counter::new();
    let (tx, rx) = unbounded_channel();

    let mut recv = rx.recv();
    assert_eq!(poll_once(&mut recv, &waker), Poll::Pending);

    tx.send(1u32);
    tx.send(2);
    tx.send(3);
    assert_eq!(counter.count(), 1);

    // And nothing was lost by not waking: all three come out, one poll each,
    // with no park in between.
    assert_eq!(poll_once(&mut recv, &waker), Poll::Ready(Some(1)));
    assert_eq!(poll_once(&mut rx.recv(), &waker), Poll::Ready(Some(2)));
    assert_eq!(poll_once(&mut rx.recv(), &waker), Poll::Ready(Some(3)));
    assert_eq!(poll_once(&mut rx.recv(), &waker), Poll::Pending);
}

#[test]
fn a_dropped_sender_ends_the_stream_and_wakes_whoever_waits() {
    let (counter, waker) = Counter::new();
    let (tx, rx) = unbounded_channel::<u32>();

    let mut recv = rx.recv();
    assert_eq!(poll_once(&mut recv, &waker), Poll::Pending);

    drop(tx);
    assert_eq!(
        counter.count(),
        1,
        "a parked receiver must hear about the closure",
    );
    assert_eq!(poll_once(&mut recv, &waker), Poll::Ready(None));
}

#[test]
fn what_was_queued_outlives_the_sender() {
    let (_counter, waker) = Counter::new();
    let (tx, rx) = unbounded_channel();

    tx.send(1u32);
    tx.send(2);
    drop(tx);

    // Closure is not truncation: the backlog comes out first, and only then
    // does the stream end.
    assert_eq!(poll_once(&mut rx.recv(), &waker), Poll::Ready(Some(1)));
    assert_eq!(poll_once(&mut rx.recv(), &waker), Poll::Ready(Some(2)));
    assert_eq!(poll_once(&mut rx.recv(), &waker), Poll::Ready(None));
}

#[test]
fn sending_into_a_gone_receiver_touches_nothing() {
    let (drops, make) = tracked();
    let (tx, rx) = unbounded_channel();

    tx.send(make());
    drop(rx);
    assert_eq!(drops.get(), 1, "the receiver clears what it leaves behind");

    // The shared state is still alive — the sender holds the other half — so
    // this is a live read of a flag, not a use-after-free. The element is
    // dropped on the spot rather than queued for nobody.
    tx.send(make());
    assert_eq!(drops.get(), 2);
}

#[test]
fn the_shared_state_is_freed_whichever_half_goes_last() {
    // Two flags standing in for a refcount, so both orders have to be walked.
    // Miri is the real assertion here: a double free or a leak shows up in the
    // run, not in these counters.
    for sender_first in [true, false] {
        let (drops, make) = tracked();
        let (tx, rx) = unbounded_channel();

        tx.send(make());
        tx.send(make());

        if sender_first {
            drop(tx);
            drop(rx);
        } else {
            drop(rx);
            drop(tx);
        }

        assert_eq!(
            drops.get(),
            2,
            "queued items dropped exactly once, sender_first={sender_first}",
        );
    }
}

#[test]
fn recv_all_takes_the_whole_backlog_in_one_poll() {
    let (_counter, waker) = Counter::new();
    let (tx, rx) = unbounded_channel();

    tx.send(1u32);
    tx.send(2);
    tx.send(3);

    assert_eq!(
        poll_once(&mut rx.recv_all(), &waker),
        Poll::Ready(Some(vec![1, 2, 3])),
    );
    assert_eq!(rx.try_recv_all(), None, "the queue was drained");
}

#[test]
fn purge_drops_the_backlog_without_closing_the_channel() {
    let (drops, make) = tracked();
    let (_counter, waker) = Counter::new();
    let (tx, rx) = unbounded_channel();

    tx.send(make());
    tx.send(make());
    tx.purge();
    assert_eq!(drops.get(), 2);

    // Still open: purging is not closing.
    tx.send(make());
    assert!(matches!(
        poll_once(&mut rx.recv(), &waker),
        Poll::Ready(Some(_)),
    ));
}

/// Named so the type is spelled out somewhere: `recv` borrows the receiver, so
/// a future cannot outlive the half that made it.
fn _receiver_is_the_borrow_root<T>(rx: &Receiver<T>) -> impl Future<Output = Option<T>> + '_ {
    rx.recv()
}

// ------------------------------------------------------------------ bounded

#[test]
fn a_bounded_send_below_capacity_completes_on_the_first_poll() {
    let (_counter, waker) = Counter::new();
    let (tx, rx) = bounded_channel(2);

    let mut send = tx.send(1u32);
    assert_eq!(poll_once(&mut send, &waker), Poll::Ready(Ok(())));
    assert_eq!(poll_once(&mut rx.recv(), &waker), Poll::Ready(Some(1)));
}

#[test]
fn a_full_channel_parks_the_sender_until_a_slot_opens() {
    let (counter, waker) = Counter::new();
    let (tx, rx) = bounded_channel(1);

    assert_eq!(poll_once(&mut tx.send(1u32), &waker), Poll::Ready(Ok(())));

    let mut blocked = tx.send(2);
    assert_eq!(poll_once(&mut blocked, &waker), Poll::Pending);
    assert_eq!(counter.count(), 0);

    // Taking the queued item is what frees the slot, so that is what wakes.
    assert_eq!(poll_once(&mut rx.recv(), &waker), Poll::Ready(Some(1)));
    assert_eq!(counter.count(), 1);
    assert_eq!(poll_once(&mut blocked, &waker), Poll::Ready(Ok(())));
    assert_eq!(poll_once(&mut rx.recv(), &waker), Poll::Ready(Some(2)));
}

#[test]
fn a_send_with_no_receiver_left_hands_the_element_back() {
    let (_counter, waker) = Counter::new();
    let (tx, rx) = bounded_channel(1);
    drop(rx);

    // The point of `Result<(), T>`: an undeliverable element is returned
    // rather than dropped, so a caller can decide what to do with it.
    assert_eq!(poll_once(&mut tx.send(9u32), &waker), Poll::Ready(Err(9)));
}

#[test]
fn a_gone_receiver_wakes_the_sender_it_was_blocking() {
    let (counter, waker) = Counter::new();
    let (tx, rx) = bounded_channel(1);

    assert_eq!(poll_once(&mut tx.send(1u32), &waker), Poll::Ready(Ok(())));
    let mut blocked = tx.send(2);
    assert_eq!(poll_once(&mut blocked, &waker), Poll::Pending);

    // Without this wake the sender waits on a slot that will never open,
    // because the only thing that could open one is gone.
    drop(rx);
    assert_eq!(counter.count(), 1);
    assert_eq!(poll_once(&mut blocked, &waker), Poll::Ready(Err(2)));
}

#[test]
fn a_bounded_channel_frees_its_state_whichever_half_goes_last() {
    for sender_first in [true, false] {
        let (drops, make) = tracked();
        let (_counter, waker) = Counter::new();
        let (tx, rx) = bounded_channel(2);

        // `Tracked` is neither `Debug` nor `PartialEq` — it only counts — so
        // these two go through `matches!` rather than `assert_eq!`.
        assert!(matches!(
            poll_once(&mut tx.send(make()), &waker),
            Poll::Ready(Ok(())),
        ));
        assert!(matches!(
            poll_once(&mut tx.send(make()), &waker),
            Poll::Ready(Ok(())),
        ));

        if sender_first {
            drop(tx);
            drop(rx);
        } else {
            drop(rx);
            drop(tx);
        }

        assert_eq!(drops.get(), 2, "sender_first={sender_first}");
    }
}

/// As above: the send future cannot outlive the sender it came from.
fn _bounded_receiver_is_the_borrow_root<T>(
    rx: &BoundedReceiver<T>,
) -> impl Future<Output = Option<T>> + '_ {
    rx.recv()
}

// -------------------------------------------------------------- vec_channel

#[test]
fn a_vec_channel_hands_over_the_whole_buffer() {
    let (_counter, waker) = Counter::new();
    let (tx, rx) = vec_channel();

    tx.push(1u32);
    tx.extend(&[2, 3]);

    // One `mem::take`, not an item-at-a-time drain: that is the whole reason
    // this channel exists next to the other two.
    assert_eq!(
        poll_once(&mut rx.recv(), &waker),
        Poll::Ready(Some(vec![1, 2, 3])),
    );
    assert_eq!(poll_once(&mut rx.recv(), &waker), Poll::Pending);
}

#[test]
fn an_empty_extend_is_not_a_send() {
    let (counter, waker) = Counter::new();
    let (tx, rx) = vec_channel::<u32>();

    let mut recv = rx.recv();
    assert_eq!(poll_once(&mut recv, &waker), Poll::Pending);

    tx.extend(&[]);
    assert_eq!(
        counter.count(),
        0,
        "extending by nothing would wake a receiver to find nothing",
    );
}

#[test]
fn take_empties_the_buffer_without_ending_the_stream() {
    let (_counter, waker) = Counter::new();
    let (tx, rx) = vec_channel();

    tx.push(1u32);
    assert_eq!(rx.take(), vec![1]);
    assert!(rx.take().is_empty());

    // The sender is still there, so this is a park rather than a `None`.
    assert_eq!(poll_once(&mut rx.recv(), &waker), Poll::Pending);
    tx.push(2);
    assert_eq!(
        poll_once(&mut rx.recv(), &waker),
        Poll::Ready(Some(vec![2]))
    );
}

#[test]
fn a_vec_channel_frees_its_state_whichever_half_goes_last() {
    for sender_first in [true, false] {
        let (drops, make) = tracked();
        let (tx, rx) = vec_channel();

        tx.push(make());
        tx.push(make());

        if sender_first {
            drop(tx);
            drop(rx);
        } else {
            drop(rx);
            drop(tx);
        }

        assert_eq!(drops.get(), 2, "sender_first={sender_first}");
    }
}

/// As above.
fn _vec_receiver_is_the_borrow_root<T>(
    rx: &VecReceiver<T>,
) -> impl Future<Output = Option<Vec<T>>> + '_ {
    rx.recv()
}

// ------------------------------------------------------- re-entrant drops

// Items that own the last handle on their own channel: dropping the queue
// runs that handle's drop, which must not find the queue still borrowed or
// the state already freed.
struct OwnsSender(#[allow(dead_code)] Rc<Sender<OwnsSender>>);
struct OwnsBoundedSender(#[allow(dead_code)] Rc<BoundedSender<OwnsBoundedSender>>);
struct OwnsVecSender(#[allow(dead_code)] Rc<VecSender<OwnsVecSender>>);
struct OwnsReceiver(#[allow(dead_code)] Receiver<OwnsReceiver>);

#[test]
fn dropping_the_receiver_may_drop_the_last_sender_from_the_queue() {
    let (tx, rx) = unbounded_channel();
    let tx = Rc::new(tx);
    tx.send(OwnsSender(tx.clone()));
    drop(tx);
    drop(rx);
}

#[test]
fn dropping_a_bounded_receiver_may_drop_the_last_sender_from_the_queue() {
    let (_counter, waker) = Counter::new();
    let (tx, rx) = bounded_channel(1);
    let tx = Rc::new(tx);
    let mut send = tx.send(OwnsBoundedSender(tx.clone()));
    assert!(matches!(poll_once(&mut send, &waker), Poll::Ready(Ok(()))));
    drop(send);
    drop(tx);
    drop(rx);
}

#[test]
fn dropping_a_vec_receiver_may_drop_the_last_sender_from_the_queue() {
    let (tx, rx) = vec_channel();
    let tx = Rc::new(tx);
    tx.push(OwnsVecSender(tx.clone()));
    drop(tx);
    drop(rx);
}

#[test]
fn purge_may_drop_the_receiver_from_the_queue() {
    let (tx, rx) = unbounded_channel();
    tx.send(OwnsReceiver(rx));
    tx.purge();
}

#[test]
#[should_panic(expected = "capacity must be at least 1")]
fn a_bounded_channel_cannot_have_zero_capacity() {
    let _ = bounded_channel::<u32>(0);
}

// ------------------------------------------------------- several waiters

#[test]
fn every_parked_sender_is_woken_as_slots_open() {
    let (tx, rx) = bounded_channel(1);
    let (_, waker) = Counter::new();
    assert!(poll_once(&mut tx.send(0u32), &waker).is_ready());

    let (a, wa) = Counter::new();
    let (b, wb) = Counter::new();
    let mut send_a = tx.send(1);
    let mut send_b = tx.send(2);
    assert!(poll_once(&mut send_a, &wa).is_pending());
    assert!(poll_once(&mut send_b, &wb).is_pending());

    assert_eq!(poll_once(&mut rx.recv(), &waker), Poll::Ready(Some(0)));
    assert_eq!(
        (a.count(), b.count()),
        (1, 0),
        "one slot, one sender, in order"
    );
    assert!(matches!(poll_once(&mut send_a, &wa), Poll::Ready(Ok(()))));

    assert_eq!(poll_once(&mut rx.recv(), &waker), Poll::Ready(Some(1)));
    assert_eq!(b.count(), 1, "the second sender must not be forgotten");
    assert!(matches!(poll_once(&mut send_b, &wb), Poll::Ready(Ok(()))));
    assert_eq!(poll_once(&mut rx.recv(), &waker), Poll::Ready(Some(2)));
}

#[test]
fn recv_all_wakes_one_sender_per_freed_slot() {
    let (tx, rx) = bounded_channel(2);
    let (_, waker) = Counter::new();
    assert!(poll_once(&mut tx.send(0u32), &waker).is_ready());
    assert!(poll_once(&mut tx.send(1), &waker).is_ready());

    let counters: Vec<_> = (0..3).map(|_| Counter::new()).collect();
    let mut sends: Vec<_> = (2..5).map(|i| tx.send(i)).collect();
    for (send, (_, w)) in sends.iter_mut().zip(&counters) {
        assert!(poll_once(send, w).is_pending());
    }

    assert_eq!(
        poll_once(&mut rx.recv_all(), &waker),
        Poll::Ready(Some(vec![0, 1]))
    );
    let woken: Vec<_> = counters.iter().map(|(c, _)| c.count()).collect();
    assert_eq!(woken, [1, 1, 0]);
}

#[test]
fn a_woken_sender_dropped_hands_its_slot_to_the_next() {
    let (tx, rx) = bounded_channel(1);
    let (_, waker) = Counter::new();
    assert!(poll_once(&mut tx.send(0u32), &waker).is_ready());

    let (a, wa) = Counter::new();
    let (b, wb) = Counter::new();
    let mut send_a = tx.send(1);
    let mut send_b = tx.send(2);
    assert!(poll_once(&mut send_a, &wa).is_pending());
    assert!(poll_once(&mut send_b, &wb).is_pending());

    assert_eq!(poll_once(&mut rx.recv(), &waker), Poll::Ready(Some(0)));
    assert_eq!(a.count(), 1);
    drop(send_a);
    assert_eq!(b.count(), 1, "the wake A never used must reach B");
    assert!(matches!(poll_once(&mut send_b, &wb), Poll::Ready(Ok(()))));
}

#[test]
fn a_gone_receiver_wakes_every_parked_sender() {
    let (tx, rx) = bounded_channel(1);
    let (_, waker) = Counter::new();
    assert!(poll_once(&mut tx.send(0u32), &waker).is_ready());

    let (a, wa) = Counter::new();
    let (b, wb) = Counter::new();
    let mut send_a = tx.send(1);
    let mut send_b = tx.send(2);
    assert!(poll_once(&mut send_a, &wa).is_pending());
    assert!(poll_once(&mut send_b, &wb).is_pending());

    drop(rx);
    assert_eq!((a.count(), b.count()), (1, 1));
    assert!(matches!(poll_once(&mut send_a, &wa), Poll::Ready(Err(1))));
    assert!(matches!(poll_once(&mut send_b, &wb), Poll::Ready(Err(2))));
}

#[test]
fn every_parked_receiver_is_woken_by_a_send() {
    let (tx, rx) = unbounded_channel();
    let (a, wa) = Counter::new();
    let (b, wb) = Counter::new();
    let mut recv_a = rx.recv();
    let mut recv_b = rx.recv();
    assert!(poll_once(&mut recv_a, &wa).is_pending());
    assert!(poll_once(&mut recv_b, &wb).is_pending());

    tx.send(1u32);
    tx.send(2);
    assert_eq!(
        (a.count(), b.count()),
        (1, 1),
        "the second send must not be lost"
    );
    assert_eq!(poll_once(&mut recv_a, &wa), Poll::Ready(Some(1)));
    assert_eq!(poll_once(&mut recv_b, &wb), Poll::Ready(Some(2)));
}

#[test]
fn a_woken_receiver_dropped_hands_its_item_to_the_next() {
    let (tx, rx) = unbounded_channel();
    let (a, wa) = Counter::new();
    let (b, wb) = Counter::new();
    let mut recv_a = rx.recv();
    let mut recv_b = rx.recv();
    assert!(poll_once(&mut recv_a, &wa).is_pending());
    assert!(poll_once(&mut recv_b, &wb).is_pending());

    tx.send(7u32);
    assert_eq!((a.count(), b.count()), (1, 0));
    drop(recv_a);
    assert_eq!(b.count(), 1, "the item A never took must wake B");
    assert_eq!(poll_once(&mut recv_b, &wb), Poll::Ready(Some(7)));
}

#[test]
fn a_gone_sender_wakes_every_parked_receiver() {
    let (tx, rx) = unbounded_channel::<u32>();
    let (a, wa) = Counter::new();
    let (b, wb) = Counter::new();
    let mut recv_a = rx.recv();
    let mut recv_b = rx.recv();
    assert!(poll_once(&mut recv_a, &wa).is_pending());
    assert!(poll_once(&mut recv_b, &wb).is_pending());

    drop(tx);
    assert_eq!((a.count(), b.count()), (1, 1));
    assert_eq!(poll_once(&mut recv_a, &wa), Poll::Ready(None));
    assert_eq!(poll_once(&mut recv_b, &wb), Poll::Ready(None));
}
