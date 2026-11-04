//! The vec channel, driven by a real runtime over a simulated ring.
//!
//! Migrated from `slopio/tests/vec_channel_test.rs`. See
//! `channels_unbounded.rs` for why, and `channels.rs` for the same channel
//! polled by hand with no runtime at all.
//!
//! Two of the original cases are missing. `peek` was removed outright — it
//! handed out a slice into the shared buffer that the sender could realloc
//! from under it, a use-after-free reachable from safe code. `get_mut` has the
//! same shape and is commented out in the channel for now, so its test is
//! commented out here to match; both come back or go for good together.

mod common;

use common::{on_runtime, scaled};
use slopio_core::channels::vec_channel;
use slopio_core::runtime::spawn;

// Basic: push items then recv batches them into one Vec.
#[test]
fn push_then_recv_batches() {
    on_runtime(async {
        let (tx, rx) = vec_channel::<i32>();
        tx.push(1);
        tx.push(2);
        tx.push(3);

        let batch = rx.recv().await.unwrap();
        assert_eq!(batch, vec![1, 2, 3]);
    });
}

// recv on empty channel waits until a push arrives, then returns the batch.
#[test]
fn recv_waits_when_empty() {
    on_runtime(async {
        let (tx, rx) = vec_channel::<i32>();

        let consumer = spawn(async move { rx.recv().await });
        spawn(async {}).await; // let consumer register its waker

        tx.push(42);
        let batch = consumer.await.unwrap();
        assert_eq!(batch, vec![42]);
    });
}

// recv returns None once sender is dropped and buffer is empty.
#[test]
fn recv_returns_none_on_closed_empty() {
    on_runtime(async {
        let (tx, rx) = vec_channel::<i32>();
        drop(tx);
        assert_eq!(rx.recv().await, None);
    });
}

// recv drains remaining items before returning None.
#[test]
fn recv_drains_before_none() {
    on_runtime(async {
        let (tx, rx) = vec_channel::<i32>();
        tx.push(1);
        tx.push(2);
        drop(tx);

        let batch = rx.recv().await.unwrap();
        assert_eq!(batch, vec![1, 2]);
        assert_eq!(rx.recv().await, None);
    });
}

// extend with a slice appends in order.
#[test]
fn extend_appends_in_order() {
    on_runtime(async {
        let (tx, rx) = vec_channel::<i32>();
        tx.push(1);
        tx.extend(&[2, 3, 4]);
        tx.extend(&[5, 6, 7]);

        let batch = rx.recv().await.unwrap();
        assert_eq!(batch, vec![1, 2, 3, 4, 5, 6, 7]);
    });
}

// extend on empty wakes a waiting receiver.
#[test]
fn extend_wakes_receiver() {
    on_runtime(async {
        let (tx, rx) = vec_channel::<i32>();

        let consumer = spawn(async move { rx.recv().await });
        spawn(async {}).await;

        tx.extend(&[10, 20, 30]);
        assert_eq!(consumer.await.unwrap(), vec![10, 20, 30]);
    });
}

// extend with an empty slice must not wake the receiver.
#[test]
fn extend_empty_does_not_wake() {
    on_runtime(async {
        let (tx, rx) = vec_channel::<i32>();
        tx.extend(&[]);
        assert!(rx.take().is_empty());

        // recv would still block here; verify by dropping sender after.
        drop(tx);
        assert_eq!(rx.recv().await, None);
    });
}

// take returns the whole batch and leaves the buffer empty.
#[test]
fn take_returns_and_empties() {
    on_runtime(async {
        let (tx, rx) = vec_channel::<i32>();
        tx.push(7);
        tx.push(8);
        tx.push(9);

        let batch = rx.take();
        assert_eq!(batch, vec![7, 8, 9]);
        assert!(rx.take().is_empty());

        tx.push(10);
        assert_eq!(rx.take(), vec![10]);
    });
}

// take on empty returns an empty Vec.
#[test]
fn take_empty_returns_empty_vec() {
    on_runtime(async {
        let (_tx, rx) = vec_channel::<i32>();
        let batch = rx.take();
        assert!(batch.is_empty());
    });
}

// get_mut allows in-place mutation of the buffer.
//
// Commented out alongside `Receiver::get_mut` itself; see the note there.
//
// #[test]
// fn get_mut_modifies_in_place() {
//     on_runtime(async {
//         let (tx, mut rx) = vec_channel::<i32>();
//         tx.push(1);
//         tx.push(2);
//         tx.push(3);
//
//         for v in rx.get_mut().iter_mut() {
//             *v *= 10;
//         }
//         rx.get_mut().push(40);
//
//         assert_eq!(rx.take(), vec![10, 20, 30, 40]);
//     });
// }

// Drop receiver: sender pushes become silent no-ops.
#[test]
fn drop_receiver_sender_continues() {
    on_runtime(async {
        let (tx, rx) = vec_channel::<i32>();
        tx.push(1);
        drop(rx);
        tx.push(2);
        tx.extend(&[3, 4, 5]);
    });
}

// Drop both ends without sending anything.
#[test]
fn drop_empty_channel() {
    on_runtime(async {
        let (tx, rx) = vec_channel::<i32>();
        drop(tx);
        drop(rx);
    });
}

// recv coalesces everything sent before consumer wakes into one batch.
#[test]
fn recv_coalesces_multiple_pushes() {
    on_runtime(async {
        let (tx, rx) = vec_channel::<i32>();

        let consumer = spawn(async move { rx.recv().await });
        spawn(async {}).await;

        // First push wakes the consumer; the rest land before it polls again.
        tx.push(1);
        tx.push(2);
        tx.push(3);

        let batch = consumer.await.unwrap();
        assert_eq!(batch, vec![1, 2, 3]);
    });
}

// Concurrent producer/consumer over many batches.
#[test]
fn concurrent_producer_consumer() {
    on_runtime(async {
        let (tx, rx) = vec_channel::<i32>();
        let count = scaled(1000) as i32;

        let producer = spawn(async move {
            for i in 0..count {
                tx.push(i);
            }
        });

        let consumer = spawn(async move {
            let mut total = 0i64;
            let mut seen = 0;
            while seen < count {
                let batch = rx.recv().await.unwrap();
                seen += batch.len() as i32;
                total += batch.iter().map(|&v| v as i64).sum::<i64>();
            }
            total
        });

        producer.await;
        let total = consumer.await;
        let expected: i64 = (0..count as i64).sum();
        assert_eq!(total, expected);
    });
}

// FIFO ordering preserved across pushes and extends.
#[test]
fn fifo_ordering() {
    on_runtime(async {
        let (tx, rx) = vec_channel::<i32>();
        for i in 0..100 {
            tx.push(i);
        }
        let tail: Vec<i32> = (100..200).collect();
        tx.extend(&tail);

        let batch = rx.recv().await.unwrap();
        assert_eq!(batch.len(), 200);
        for (i, v) in batch.iter().enumerate() {
            assert_eq!(*v, i as i32, "FIFO order violated at {}", i);
        }
    });
}

// Zero-sized types round-trip correctly.
#[test]
fn zero_sized_type() {
    on_runtime(async {
        let (tx, rx) = vec_channel::<()>();
        tx.push(());
        tx.push(());
        tx.push(());

        let batch = rx.recv().await.unwrap();
        assert_eq!(batch.len(), 3);
    });
}

// Owned heap types (no Clone needed for push/take).
#[test]
fn owned_string_messages() {
    on_runtime(async {
        let (tx, rx) = vec_channel::<String>();
        tx.push("hello".to_string());
        tx.push("world".to_string());

        let batch = rx.recv().await.unwrap();
        assert_eq!(batch, vec!["hello".to_string(), "world".to_string()]);
    });
}

// After draining via take(), recv() blocks again until the next push.
#[test]
fn take_then_recv_waits() {
    on_runtime(async {
        let (tx, rx) = vec_channel::<i32>();
        tx.push(1);
        assert_eq!(rx.take(), vec![1]);

        let consumer = spawn(async move { rx.recv().await });
        spawn(async {}).await;

        tx.push(2);
        assert_eq!(consumer.await.unwrap(), vec![2]);
    });
}
