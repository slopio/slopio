//! The bounded channel, driven by a real runtime over a simulated ring.
//!
//! Migrated from `slopio/tests/bounded_channels_test.rs`. See
//! `channels_unbounded.rs` for why, and `channels.rs` for the same channel
//! polled by hand with no runtime at all.

mod common;

use common::{on_runtime, scaled};
use slopio_core::channels::bounded_channel;
use slopio_core::runtime::spawn;

// --- recv_all ---

// Basic: fill buffer then drain everything at once.
#[test]
fn recv_all_basic() {
    on_runtime(async {
        let (tx, rx) = bounded_channel::<i32>(8);
        tx.send(1).await.unwrap();
        tx.send(2).await.unwrap();
        tx.send(3).await.unwrap();

        let batch = rx.recv_all().await.unwrap();
        assert_eq!(batch, vec![1, 2, 3]);
    });
}

// recv_all on an empty channel waits until at least one item arrives.
#[test]
fn recv_all_waits_when_empty() {
    on_runtime(async {
        let (tx, rx) = bounded_channel::<i32>(4);

        let consumer = spawn(async move { rx.recv_all().await });
        spawn(async {}).await; // let consumer register its waker

        tx.send(10).await.unwrap();
        tx.send(20).await.unwrap();

        let batch = consumer.await.unwrap();
        assert!(!batch.is_empty());
        assert!(batch.iter().all(|v| [10, 20].contains(v)));
    });
}

// recv_all returns None when sender is dropped and buffer is empty.
#[test]
fn recv_all_returns_none_on_closed_empty() {
    on_runtime(async {
        let (tx, rx) = bounded_channel::<i32>(4);
        drop(tx);
        assert_eq!(rx.recv_all().await, None);
    });
}

// recv_all drains remaining items before returning None after sender drop.
#[test]
fn recv_all_drains_before_none() {
    on_runtime(async {
        let (tx, rx) = bounded_channel::<i32>(8);
        tx.send(1).await.unwrap();
        tx.send(2).await.unwrap();
        drop(tx);

        let batch = rx.recv_all().await.unwrap();
        assert_eq!(batch, vec![1, 2]);
        assert_eq!(rx.recv_all().await, None);
    });
}

// recv_all frees the whole buffer at once, unblocking a sender that was waiting.
#[test]
fn recv_all_unblocks_full_sender() {
    on_runtime(async {
        let (tx, rx) = bounded_channel::<i32>(3);
        tx.send(1).await.unwrap();
        tx.send(2).await.unwrap();
        tx.send(3).await.unwrap(); // full

        let sender = spawn(async move {
            tx.send(4).await.unwrap(); // blocked until recv_all frees space
            tx.send(5).await.unwrap();
        });

        spawn(async {}).await; // let sender register its waker

        let first = rx.recv_all().await.unwrap(); // drains all 3, wakes sender
        assert_eq!(first, vec![1, 2, 3]);

        sender.await;

        let second = rx.recv_all().await.unwrap();
        assert_eq!(second, vec![4, 5]);
    });
}

// Multiple recv_all calls, each collecting whatever is buffered at that moment.
#[test]
fn recv_all_multiple_batches() {
    on_runtime(async {
        let (tx, rx) = bounded_channel::<i32>(8);

        tx.send(1).await.unwrap();
        tx.send(2).await.unwrap();
        let batch1 = rx.recv_all().await.unwrap();
        assert_eq!(batch1, vec![1, 2]);

        tx.send(3).await.unwrap();
        tx.send(4).await.unwrap();
        tx.send(5).await.unwrap();
        let batch2 = rx.recv_all().await.unwrap();
        assert_eq!(batch2, vec![3, 4, 5]);
    });
}

// recv_all preserves FIFO order within the batch.
#[test]
fn recv_all_fifo_order() {
    on_runtime(async {
        let (tx, rx) = bounded_channel::<i32>(64);
        for i in 0..50 {
            tx.send(i).await.unwrap();
        }
        let batch = rx.recv_all().await.unwrap();
        assert_eq!(batch, (0..50).collect::<Vec<_>>());
    });
}

// --- Basic cases ---

#[test]
fn basic_send_recv() {
    on_runtime(async {
        let (tx, rx) = bounded_channel::<i32>(4);
        tx.send(42).await.unwrap();
        assert_eq!(rx.recv().await, Some(42));
    });
}

#[test]
fn multiple_sends_before_recv() {
    on_runtime(async {
        let (tx, rx) = bounded_channel::<i32>(8);
        for i in 0..5 {
            tx.send(i).await.unwrap();
        }
        for i in 0..5 {
            assert_eq!(rx.recv().await, Some(i));
        }
    });
}

#[test]
fn interleaved_send_recv() {
    on_runtime(async {
        let (tx, rx) = bounded_channel::<i32>(4);

        tx.send(1).await.unwrap();
        assert_eq!(rx.recv().await, Some(1));

        tx.send(2).await.unwrap();
        tx.send(3).await.unwrap();
        assert_eq!(rx.recv().await, Some(2));

        tx.send(4).await.unwrap();
        assert_eq!(rx.recv().await, Some(3));
        assert_eq!(rx.recv().await, Some(4));
    });
}

#[test]
fn fifo_ordering() {
    on_runtime(async {
        let (tx, rx) = bounded_channel::<i32>(128);
        for i in 0..100 {
            tx.send(i).await.unwrap();
        }
        for i in 0..100 {
            assert_eq!(rx.recv().await, Some(i));
        }
    });
}

#[test]
fn zero_sized_type() {
    on_runtime(async {
        let (tx, rx) = bounded_channel::<()>(4);
        tx.send(()).await.unwrap();
        tx.send(()).await.unwrap();
        assert_eq!(rx.recv().await, Some(()));
        assert_eq!(rx.recv().await, Some(()));
    });
}

#[test]
fn string_messages() {
    on_runtime(async {
        let (tx, rx) = bounded_channel::<String>(4);
        tx.send("hello".to_string()).await.unwrap();
        tx.send("world".to_string()).await.unwrap();
        assert_eq!(rx.recv().await.unwrap(), "hello");
        assert_eq!(rx.recv().await.unwrap(), "world");
    });
}

// --- Backpressure ---

// Sender blocks when the buffer is full and is unblocked by the receiver.
#[test]
fn send_blocks_when_full() {
    on_runtime(async {
        let (tx, rx) = bounded_channel::<i32>(2);
        tx.send(1).await.unwrap();
        tx.send(2).await.unwrap(); // now full

        // This send will block until the receiver frees a slot.
        let sender = spawn(async move {
            tx.send(3).await.unwrap();
        });

        // Yield so the sender task runs and registers its waker.
        spawn(async {}).await;

        assert_eq!(rx.recv().await, Some(1)); // frees a slot -> wakes sender
        sender.await;

        assert_eq!(rx.recv().await, Some(2));
        assert_eq!(rx.recv().await, Some(3));
    });
}

// Fill to capacity, drain completely, then fill again.
#[test]
fn fill_drain_refill() {
    on_runtime(async {
        let (tx, rx) = bounded_channel::<i32>(3);

        for i in 0..3 {
            tx.send(i).await.unwrap();
        }
        for i in 0..3 {
            assert_eq!(rx.recv().await, Some(i));
        }

        for i in 10..13 {
            tx.send(i).await.unwrap();
        }
        for i in 10..13 {
            assert_eq!(rx.recv().await, Some(i));
        }
    });
}

// Capacity of 1: every send must wait for the previous item to be consumed.
#[test]
fn capacity_one() {
    on_runtime(async {
        let (tx, rx) = bounded_channel::<i32>(1);

        let receiver = spawn(async move {
            let mut results = Vec::new();
            for _ in 0..3 {
                results.push(rx.recv().await.unwrap());
            }
            results
        });

        for i in 0..3 {
            tx.send(i).await.unwrap();
        }

        let results = receiver.await;
        assert_eq!(results, vec![0, 1, 2]);
    });
}

// Receiver blocks waiting for a sender.
#[test]
fn receiver_waits_for_sender() {
    on_runtime(async {
        let (tx, rx) = bounded_channel::<i32>(4);

        let consumer = spawn(async move { rx.recv().await });
        // Yield so consumer registers its waker.
        spawn(async {}).await;

        tx.send(99).await.unwrap();
        assert_eq!(consumer.await, Some(99));
    });
}

// Concurrent producer / consumer.
#[test]
fn concurrent_producer_consumer() {
    on_runtime(async {
        let (tx, rx) = bounded_channel::<i32>(8);
        let count = scaled(200) as i32;

        let producer = spawn(async move {
            for i in 0..count {
                tx.send(i).await.unwrap();
            }
        });

        let consumer = spawn(async move {
            let mut sum = 0i64;
            for _ in 0..count {
                sum += rx.recv().await.unwrap() as i64;
            }
            sum
        });

        producer.await;
        let sum = consumer.await;
        assert_eq!(sum, (0..count as i64).sum());
    });
}

// Ping-pong between two tasks.
#[test]
fn ping_pong() {
    on_runtime(async {
        let (tx1, rx1) = bounded_channel::<i32>(4);
        let (tx2, rx2) = bounded_channel::<i32>(4);

        let task1 = spawn(async move {
            for i in 0..10 {
                tx1.send(i).await.unwrap();
                assert_eq!(rx2.recv().await.unwrap(), i * 2);
            }
        });

        let task2 = spawn(async move {
            for _ in 0..10 {
                let v = rx1.recv().await.unwrap();
                tx2.send(v * 2).await.unwrap();
            }
        });

        task1.await;
        task2.await;
    });
}

// --- Drop edge cases ---

// Sender dropped with buffered items: receiver drains them then gets None.
#[test]
fn drop_sender_with_pending_messages() {
    on_runtime(async {
        let (tx, rx) = bounded_channel::<i32>(8);
        tx.send(1).await.unwrap();
        tx.send(2).await.unwrap();
        tx.send(3).await.unwrap();
        drop(tx);

        assert_eq!(rx.recv().await, Some(1));
        assert_eq!(rx.recv().await, Some(2));
        assert_eq!(rx.recv().await, Some(3));
        assert_eq!(rx.recv().await, None);
    });
}

// Sender dropped on an empty channel: receiver gets None immediately.
#[test]
fn drop_sender_empty_channel() {
    on_runtime(async {
        let (tx, rx) = bounded_channel::<i32>(4);
        drop(tx);
        assert_eq!(rx.recv().await, None);
    });
}

// Receiver dropped while sender has a pending send: send resolves with Err.
#[test]
fn drop_receiver_unblocks_pending_send() {
    on_runtime(async {
        let (tx, rx) = bounded_channel::<i32>(1);
        tx.send(1).await.unwrap(); // fill the channel

        // This send will block because the channel is full.
        let sender = spawn(async move { tx.send(2).await });

        // Yield so the sender task registers its waker.
        spawn(async {}).await;

        // Dropping the receiver should wake the sender with Err.
        drop(rx);

        let result = sender.await;
        assert_eq!(result, Err(2));
    });
}

// Receiver dropped: subsequent sends resolve with Err immediately.
#[test]
fn send_after_receiver_dropped() {
    on_runtime(async {
        let (tx, rx) = bounded_channel::<i32>(4);
        drop(rx);
        assert_eq!(tx.send(1).await, Err(1));
        assert_eq!(tx.send(2).await, Err(2));
    });
}

// Both ends dropped on an empty channel: no panic or leak.
#[test]
fn drop_both_empty() {
    on_runtime(async {
        let (tx, rx) = bounded_channel::<i32>(4);
        drop(tx);
        drop(rx);
    });
}

// Receiver dropped first, then sender.
#[test]
fn drop_receiver_then_sender() {
    on_runtime(async {
        let (tx, rx) = bounded_channel::<i32>(4);
        tx.send(1).await.unwrap();
        drop(rx);
        drop(tx);
    });
}

// Rapid channel create/destroy cycles.
#[test]
fn rapid_channel_lifecycle() {
    on_runtime(async {
        for _ in 0..scaled(1000) {
            let (tx, rx) = bounded_channel::<i32>(4);
            tx.send(1).await.unwrap();
            let _ = rx.recv().await;
        }
    });
}
