//! Several senders on one channel: every `Sender` is `Clone`, and the channel
//! closes only once the last one is dropped.
//!
//! ```text
//! MIRIFLAGS="-Zmiri-disable-isolation -Zmiri-permissive-provenance -Zmiri-tree-borrows" \
//!   cargo +nightly miri test -p slopio-core --features simulation --test channels_mpsc
//! ```

mod common;

use common::{on_runtime, scaled};
use slopio_core::channels::{Sender, bounded_channel, unbounded_channel, vec_channel};
use slopio_core::runtime::{spawn, yield_now};

const PRODUCERS: usize = 4;

// --- unbounded ---

#[test]
fn unbounded_many_producers() {
    on_runtime(async {
        let (tx, rx) = unbounded_channel::<usize>();
        let count = scaled(1000);

        let producers: Vec<_> = (0..PRODUCERS)
            .map(|p| {
                let tx = tx.clone();
                spawn(async move {
                    for i in 0..count {
                        tx.send(p * count + i);
                        yield_now().await;
                    }
                })
            })
            .collect();
        drop(tx);

        let consumer = spawn(async move {
            let mut sum = 0usize;
            let mut n = 0usize;
            while let Some(v) = rx.recv().await {
                sum += v;
                n += 1;
            }
            (n, sum)
        });

        for p in producers {
            p.await;
        }
        let total = PRODUCERS * count;
        assert_eq!(consumer.await, (total, (0..total).sum()));
    });
}

#[test]
fn unbounded_per_producer_order() {
    on_runtime(async {
        let (tx, rx) = unbounded_channel::<(usize, usize)>();
        let count = scaled(500);

        for p in 0..PRODUCERS {
            let tx = tx.clone();
            spawn(async move {
                for i in 0..count {
                    tx.send((p, i));
                    yield_now().await;
                }
            });
        }
        drop(tx);

        let mut next = [0usize; PRODUCERS];
        while let Some((p, i)) = rx.recv().await {
            assert_eq!(i, next[p]);
            next[p] += 1;
        }
        assert_eq!(next, [count; PRODUCERS]);
    });
}

#[test]
fn unbounded_stays_open_while_a_clone_lives() {
    on_runtime(async {
        let (tx, rx) = unbounded_channel::<i32>();
        let tx2 = tx.clone();
        drop(tx);

        let late = spawn(async move {
            yield_now().await;
            tx2.send(7);
        });

        assert_eq!(rx.recv().await, Some(7));
        late.await;
        assert_eq!(rx.recv().await, None);
    });
}

#[test]
fn unbounded_last_drop_wakes_parked_receiver() {
    on_runtime(async {
        let (tx, rx) = unbounded_channel::<i32>();
        let clones: Vec<_> = (0..PRODUCERS).map(|_| tx.clone()).collect();
        drop(tx);

        let consumer = spawn(async move { rx.recv().await });
        yield_now().await;
        for c in clones {
            drop(c);
            yield_now().await;
        }
        assert_eq!(consumer.await, None);
    });
}

#[test]
fn unbounded_receiver_dropped_first() {
    on_runtime(async {
        let (tx, rx) = unbounded_channel::<String>();
        let tx2 = tx.clone();
        tx.send("a".into());
        drop(rx);
        tx2.send("b".into());
        drop(tx);
        tx2.send("c".into());
    });
}

// Dropping the receiver drops the queued item, which owns the last sender.
#[test]
fn unbounded_item_owns_last_sender() {
    struct Carry(#[allow(dead_code)] Sender<Carry>);

    on_runtime(async {
        let (tx, rx) = unbounded_channel::<Carry>();
        let tx2 = tx.clone();
        tx2.send(Carry(tx));
        drop(tx2);
        drop(rx);
    });
}

// --- bounded ---

#[test]
fn bounded_many_producers_backpressure() {
    on_runtime(async {
        let (tx, rx) = bounded_channel::<usize>(2);
        let count = scaled(500);

        let producers: Vec<_> = (0..PRODUCERS)
            .map(|p| {
                let tx = tx.clone();
                spawn(async move {
                    for i in 0..count {
                        tx.send(p * count + i).await.unwrap();
                    }
                })
            })
            .collect();
        drop(tx);

        let consumer = spawn(async move {
            let mut sum = 0usize;
            let mut n = 0usize;
            while let Some(v) = rx.recv().await {
                sum += v;
                n += 1;
            }
            (n, sum)
        });

        for p in producers {
            p.await;
        }
        let total = PRODUCERS * count;
        assert_eq!(consumer.await, (total, (0..total).sum()));
    });
}

#[test]
fn bounded_receiver_drop_fails_every_parked_sender() {
    on_runtime(async {
        let (tx, rx) = bounded_channel::<usize>(1);
        tx.send(0).await.unwrap();

        let blocked: Vec<_> = (1..=PRODUCERS)
            .map(|p| {
                let tx = tx.clone();
                spawn(async move { tx.send(p).await })
            })
            .collect();
        drop(tx);
        yield_now().await;
        drop(rx);

        for (p, b) in (1..=PRODUCERS).zip(blocked) {
            assert_eq!(b.await, Err(p));
        }
    });
}

#[test]
fn bounded_stays_open_while_a_clone_lives() {
    on_runtime(async {
        let (tx, rx) = bounded_channel::<i32>(4);
        let tx2 = tx.clone();
        drop(tx);
        tx2.send(1).await.unwrap();
        assert_eq!(rx.recv().await, Some(1));
        drop(tx2);
        assert_eq!(rx.recv().await, None);
    });
}

// --- vec ---

#[test]
fn vec_many_producers() {
    on_runtime(async {
        let (tx, rx) = vec_channel::<usize>();
        let count = scaled(500);

        for p in 0..PRODUCERS {
            let tx = tx.clone();
            spawn(async move {
                for i in 0..count {
                    tx.push(p * count + i);
                    yield_now().await;
                }
            });
        }
        drop(tx);

        let mut all = Vec::new();
        while let Some(batch) = rx.recv().await {
            all.extend(batch);
        }
        all.sort_unstable();
        assert_eq!(all, (0..PRODUCERS * count).collect::<Vec<_>>());
    });
}

#[test]
fn vec_stays_open_while_a_clone_lives() {
    on_runtime(async {
        let (tx, rx) = vec_channel::<i32>();
        let tx2 = tx.clone();
        drop(tx);
        tx2.extend(&[1, 2]);
        assert_eq!(rx.recv().await, Some(vec![1, 2]));
        drop(tx2);
        assert_eq!(rx.recv().await, None);
    });
}
