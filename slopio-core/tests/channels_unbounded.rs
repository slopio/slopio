//! The unbounded channel, driven by a real runtime over a simulated ring.
//!
//! Migrated from `slopio/tests/channels_test.rs`, which ran the same cases
//! under `main_thread`. Nothing here does IO, so the only thing the real ring
//! contributed was making the file impossible to run under Miri.
//!
//! ```text
//! MIRIFLAGS="-Zmiri-disable-isolation -Zmiri-permissive-provenance -Zmiri-tree-borrows" \
//!   cargo +nightly miri test -p slopio-core --features simulation --test channels_unbounded
//! ```
//!
//! See `channels.rs` for the same channels polled by hand, with no runtime at
//! all.

mod common;

use common::{on_runtime, scaled};
use slopio_core::channels::unbounded_channel;
use slopio_core::runtime::spawn;

#[test]
fn unbounding() {
    on_runtime(async {
        let (send, recv) = unbounded_channel::<usize>();
        let k = spawn(async move {
            send.send(3);
        });
        let k2 = spawn(async move {
            let v = recv.recv().await;
            v.unwrap();
        });
        let _ = k.await;
        let _ = k2.await;
    });
}

#[test]
fn unbounded_drop_parts() {
    on_runtime(async {
        let (send, recv) = unbounded_channel::<usize>();
        let k = spawn(async move { drop(send) });
        let k2 = spawn(async move { recv.recv().await });
        let _ = k.await;
        let k2 = k2.await;
        assert_eq!(k2, None);
    });
}

// Send multiple messages before any recv
#[test]
fn multiple_sends_before_recv() {
    on_runtime(async {
        let (send, recv) = unbounded_channel::<i32>();
        send.send(1);
        send.send(2);
        send.send(3);
        send.send(4);
        send.send(5);

        assert_eq!(recv.recv().await, Some(1));
        assert_eq!(recv.recv().await, Some(2));
        assert_eq!(recv.recv().await, Some(3));
        assert_eq!(recv.recv().await, Some(4));
        assert_eq!(recv.recv().await, Some(5));
    });
}

// Interleaved send/recv
#[test]
fn interleaved_send_recv() {
    on_runtime(async {
        let (send, recv) = unbounded_channel::<i32>();

        send.send(1);
        assert_eq!(recv.recv().await, Some(1));

        send.send(2);
        send.send(3);
        assert_eq!(recv.recv().await, Some(2));

        send.send(4);
        assert_eq!(recv.recv().await, Some(3));
        assert_eq!(recv.recv().await, Some(4));
    });
}

// Drop sender with pending messages - receiver should drain them
#[test]
fn drop_sender_with_pending_messages() {
    on_runtime(async {
        let (send, recv) = unbounded_channel::<i32>();
        send.send(1);
        send.send(2);
        send.send(3);
        drop(send);

        assert_eq!(recv.recv().await, Some(1));
        assert_eq!(recv.recv().await, Some(2));
        assert_eq!(recv.recv().await, Some(3));
        assert_eq!(recv.recv().await, None); // Now closed
    });
}

// Drop receiver - sender should not panic
#[test]
fn drop_receiver_sender_continues() {
    on_runtime(async {
        let (send, recv) = unbounded_channel::<i32>();
        send.send(1);
        drop(recv);
        // Should not panic, just silently drop
        send.send(2);
        send.send(3);
    });
}

// Zero-sized types
#[test]
fn zero_sized_type() {
    on_runtime(async {
        let (send, recv) = unbounded_channel::<()>();
        send.send(());
        send.send(());
        send.send(());

        assert_eq!(recv.recv().await, Some(()));
        assert_eq!(recv.recv().await, Some(()));
        assert_eq!(recv.recv().await, Some(()));
    });
}

// Large messages
#[test]
fn large_messages() {
    on_runtime(async {
        let (send, recv) = unbounded_channel::<Vec<u8>>();
        let big_data: Vec<u8> = (0..10000).map(|i| (i % 256) as u8).collect();

        send.send(big_data.clone());
        let received = recv.recv().await.unwrap();
        assert_eq!(received, big_data);
    });
}

// Many small messages (stress test queue growth)
#[test]
fn many_messages() {
    on_runtime(async {
        let (send, recv) = unbounded_channel::<i32>();
        let count = scaled(10000) as i32;

        for i in 0..count {
            send.send(i);
        }

        for i in 0..count {
            assert_eq!(recv.recv().await, Some(i));
        }
    });
}

// Concurrent producer/consumer
#[test]
fn concurrent_producer_consumer() {
    on_runtime(async {
        let (send, recv) = unbounded_channel::<i32>();
        let count = scaled(1000) as i32;

        let producer = spawn(async move {
            for i in 0..count {
                send.send(i);
            }
        });

        let consumer = spawn(async move {
            let mut sum = 0i64;
            for _ in 0..count {
                sum += recv.recv().await.unwrap() as i64;
            }
            sum
        });

        producer.await;
        let sum = consumer.await;
        let expected: i64 = (0..count as i64).sum();
        assert_eq!(sum, expected);
    });
}

// Receiver waits for sender
#[test]
fn receiver_waits_for_sender() {
    on_runtime(async {
        let (send, recv) = unbounded_channel::<i32>();

        let consumer = spawn(async move {
            // This should block until sender sends
            recv.recv().await
        });

        let producer = spawn(async move {
            send.send(42);
        });

        producer.await;
        let result = consumer.await;
        assert_eq!(result, Some(42));
    });
}

// Multiple recv() futures created but only one consumed
#[test]
fn multiple_recv_futures() {
    on_runtime(async {
        let (send, recv) = unbounded_channel::<i32>();
        send.send(1);
        send.send(2);

        let _fut1 = recv.recv(); // Created but not awaited
        let fut2 = recv.recv();

        // fut1 is dropped without being polled
        assert_eq!(fut2.await, Some(1)); // Should get first message
        assert_eq!(recv.recv().await, Some(2));
    });
}

// Send after receiver starts waiting
#[test]
fn send_wakes_waiting_receiver() {
    on_runtime(async {
        let (send, recv) = unbounded_channel::<i32>();

        let handle = spawn(async move {
            // Receiver will be waiting
            let val = recv.recv().await;
            val.unwrap()
        });

        // Give receiver time to start waiting
        spawn(async {}).await;

        send.send(99);
        let result = handle.await;
        assert_eq!(result, 99);
    });
}

// Drop both ends without any messages
#[test]
fn drop_empty_channel() {
    on_runtime(async {
        let (send, recv) = unbounded_channel::<i32>();
        drop(send);
        drop(recv);
    });
}

// Drop receiver first, then sender
#[test]
fn drop_receiver_then_sender() {
    on_runtime(async {
        let (send, recv) = unbounded_channel::<i32>();
        send.send(1);
        drop(recv);
        drop(send);
    });
}

// String messages
#[test]
fn string_messages() {
    on_runtime(async {
        let (send, recv) = unbounded_channel::<String>();

        send.send("hello".to_string());
        send.send("world".to_string());
        send.send("!".to_string());

        assert_eq!(recv.recv().await, Some("hello".to_string()));
        assert_eq!(recv.recv().await, Some("world".to_string()));
        assert_eq!(recv.recv().await, Some("!".to_string()));
    });
}

// Boxed values (heap allocation)
#[test]
fn boxed_values() {
    on_runtime(async {
        let (send, recv) = unbounded_channel::<Box<[u8; 1024]>>();

        send.send(Box::new([0u8; 1024]));
        send.send(Box::new([1u8; 1024]));

        let v1 = recv.recv().await.unwrap();
        let v2 = recv.recv().await.unwrap();

        assert_eq!(v1[0], 0);
        assert_eq!(v2[0], 1);
    });
}

// Channel with Option<T> - distinguish None message from closed
#[test]
fn option_messages() {
    on_runtime(async {
        let (send, recv) = unbounded_channel::<Option<i32>>();

        send.send(Some(1));
        send.send(None); // This is a valid message!
        send.send(Some(2));
        drop(send);

        assert_eq!(recv.recv().await, Some(Some(1)));
        assert_eq!(recv.recv().await, Some(None)); // None message
        assert_eq!(recv.recv().await, Some(Some(2)));
        assert_eq!(recv.recv().await, None); // Channel closed
    });
}

// Ping-pong between two tasks
#[test]
fn ping_pong() {
    on_runtime(async {
        let (send1, recv1) = unbounded_channel::<i32>();
        let (send2, recv2) = unbounded_channel::<i32>();

        let task1 = spawn(async move {
            for i in 0..10 {
                send1.send(i);
                let v = recv2.recv().await.unwrap();
                assert_eq!(v, i * 2);
            }
        });

        let task2 = spawn(async move {
            for _ in 0..10 {
                let v = recv1.recv().await.unwrap();
                send2.send(v * 2);
            }
        });

        task1.await;
        task2.await;
    });
}

// Rapid create/destroy channels
#[test]
fn rapid_channel_lifecycle() {
    on_runtime(async {
        for _ in 0..scaled(1000) {
            let (send, recv) = unbounded_channel::<i32>();
            send.send(1);
            let _ = recv.recv().await;
        }
    });
}

// Empty recv then send pattern
#[test]
fn recv_before_send() {
    on_runtime(async {
        let (send, recv) = unbounded_channel::<i32>();

        let h = spawn(async move { recv.recv().await });

        // Yield to let receiver start waiting
        spawn(async {}).await;
        spawn(async {}).await;

        send.send(42);
        assert_eq!(h.await, Some(42));
    });
}

// Test FIFO ordering under contention
#[test]
fn fifo_ordering() {
    on_runtime(async {
        let (send, recv) = unbounded_channel::<i32>();

        for i in 0..100 {
            send.send(i);
        }

        for i in 0..100 {
            let v = recv.recv().await.unwrap();
            assert_eq!(v, i, "FIFO order violated at index {}", i);
        }
    });
}
