//! The runtime, running for real against a simulated ring.
//!
//! Everything the other suite proves is about the simulator in isolation. These
//! run an actual future through `block_on`, so what is under test is the driver
//! — slab, wakers, the completion routing, the event loop — with the kernel
//! swapped out from under it.

use std::time::{Duration, Instant};

use runtime_core::deterministic_testing::{
    FakeRing, FakeRingHandle, ReadEvent, WriteEvent, is_open, simulated_socket,
};
use runtime_core::io_driver::operations::sockets::multi_recv::{CompletionError, MultiRecv};
use runtime_core::io_driver::operations::sockets::recv::{socket_peek, socket_recv};
use runtime_core::io_driver::operations::sockets::send::socket_send;
use runtime_core::io_driver::operations::sockets::send_vectored::socket_send_vectored;
use runtime_core::io_driver::operations::sockets::send_vectored_zc::socket_send_vectored_zc;
use runtime_core::runtime::select::{SelectResult, select};
use runtime_core::runtime::{RuntimeInner, spawn, time::Sleep, yield_now};
use runtime_streams::StreamExt;

// A real provided-buffer ring. `block_on::<R, _>` installs it, which on a
// simulated backend also registers the simulator as its consumer.
runtime_core::define_buf_ring!(ManagedRing, bgid = 3, buffer_size = 64, ring_size = 2);
// Room for a whole batch at once: every buffer of a batch is taken before any
// of them is recycled, so a batch cannot outgrow its ring.
runtime_core::define_buf_ring!(BigRing, bgid = 2, buffer_size = 64, ring_size = 8);

fn simulated() -> (FakeRing, FakeRingHandle) {
    FakeRing::builder().virtual_time().build()
}

#[test]
fn a_send_completes_through_the_whole_driver() {
    let (fake, _handle) = simulated();
    let socket = simulated_socket();

    let (result, _buf) = RuntimeInner::simulated(fake).block_on::<(), _>(
        async move { socket_send(&socket, b"hello").await },
        None,
        |run| run(),
    );

    // Nothing here was scripted: a send completes on its own, and getting the
    // byte count back means the token round-tripped through the slab and the
    // waker actually woke the task.
    assert_eq!(result.expect("the send succeeded"), 5);
}

#[test]
fn a_dropped_socket_closes_through_the_ring_after_its_cancelled_read() {
    let (fake, handle) = simulated();
    let socket = simulated_socket();
    let fd = socket.fd();
    handle.on_read(fd, ReadEvent::data(b"late").after(Duration::from_secs(10)));

    RuntimeInner::simulated(fake).block_on::<(), _>(
        async move {
            // The cancelled read parks its ref, so dropping the wrapper does not close yet.
            select(socket_recv(&socket, vec![0u8; 8]), Sleep::during(Duration::from_millis(1))).await;
            drop(socket);
            assert!(is_open(fd), "closed while the cancelled read was still in flight");

            for _ in 0..3 {
                Sleep::during(Duration::from_millis(1)).await;
            }
            assert!(!is_open(fd), "the cancel's CQE should have released the last ref");
        },
        None,
        |run| run(),
    );
}

#[test]
fn a_scripted_write_reports_and_records_what_it_accepted() {
    let (fake, handle) = simulated();
    let socket = simulated_socket();
    let fd = socket.fd();

    handle.on_write(fd, WriteEvent::Short(3));
    handle.on_write(fd, WriteEvent::Error(libc::EPIPE));

    let (short, broken) = RuntimeInner::simulated(fake).block_on::<(), _>(
        async move {
            let (short, _) = socket_send(&socket, b"hello").await;
            let (broken, _) = socket_send(&socket, b"world").await;
            (short, broken)
        },
        None,
        |run| run(),
    );

    assert_eq!(short.expect("a short write is a success"), 3);
    assert_eq!(
        broken.expect_err("the pipe broke").raw_os_error(),
        Some(libc::EPIPE)
    );
    // Only what the peer would have received, with the failed send absent.
    assert_eq!(handle.writes(fd), vec![b"hel".to_vec()]);
    assert_eq!(handle.scenario_remaining(), 0);
}

#[test]
fn a_vectored_send_is_recorded_as_one_write() {
    let (fake, handle) = simulated();
    let socket = simulated_socket();
    let fd = socket.fd();
    let head = b"HTTP/1.1 200 OK\r\n\r\n".to_vec();
    let body = b"hello".to_vec();
    let whole = [&head[..], &body[..]].concat();

    let (result, bufs) = RuntimeInner::simulated(fake).block_on::<(), _>(
        async move { socket_send_vectored(&socket, vec![head, body]).await },
        None,
        |run| run(),
    );

    // The count is bytes across every chunk, not the number of chunks, and
    // the peer sees one contiguous write — the chunking was the sender's.
    assert_eq!(result.expect("the send succeeded"), whole.len());
    assert_eq!(bufs.len(), 2, "every chunk came back");
    assert_eq!(handle.writes(fd), vec![whole]);
}

#[test]
fn a_zero_copy_vectored_send_is_recorded_as_one_write() {
    let (fake, handle) = simulated();
    let socket = simulated_socket();
    let fd = socket.fd();
    let head = b"HTTP/1.1 200 OK\r\n\r\n".to_vec();
    let body = b"hello".to_vec();
    let whole = [&head[..], &body[..]].concat();

    let (result, bufs) = RuntimeInner::simulated(fake).block_on::<(), _>(
        async move { socket_send_vectored_zc(&socket, vec![head, body]).await },
        None,
        |run| run(),
    );

    // A single CQE from the simulator is the kernel's copy-fallback path.
    assert_eq!(result.expect("the send succeeded"), whole.len());
    assert_eq!(bufs.len(), 2, "every chunk came back");
    assert_eq!(handle.writes(fd), vec![whole]);
}

#[test]
fn a_scripted_read_reaches_the_awaiting_task() {
    let (fake, handle) = simulated();
    let socket = simulated_socket();

    handle.on_read(socket.fd(), ReadEvent::data(b"hello"));

    let (result, buf) = RuntimeInner::simulated(fake).block_on::<(), _>(
        async move { socket_recv(&socket, vec![0u8; 32]).await },
        None,
        |run| run(),
    );

    let n = result.expect("the read succeeded");
    assert_eq!(&buf[..n], b"hello");
    assert_eq!(handle.scenario_remaining(), 0);
}

#[test]
fn a_peek_leaves_the_scripted_read_for_the_next_recv() {
    let (fake, handle) = simulated();
    let socket = simulated_socket();

    // One delivery scripted; two peeks and a recv all see it, then EOF.
    handle.on_read(
        socket.fd(),
        ReadEvent::data(b"hello").after(Duration::from_millis(50)),
    );
    handle.on_read(socket.fd(), ReadEvent::Eof);

    let got = RuntimeInner::simulated(fake).block_on::<(), _>(
        async move {
            let (result, buf) = socket_peek(&socket, vec![0u8; 32]).await;
            let first = buf[..result.expect("the first peek succeeded")].to_vec();

            // The bytes have landed, so this one must not wait out the delay
            // again: it beats a sleep shorter than the scripted 50ms.
            let raced = select(
                socket_peek(&socket, vec![0u8; 32]),
                Sleep::during(Duration::from_millis(25)),
            )
            .await;
            let SelectResult::First((result, buf)) = raced else {
                panic!("a peek re-armed the scripted delay");
            };
            let second = buf[..result.expect("the second peek succeeded")].to_vec();

            let (result, buf) = socket_recv(&socket, vec![0u8; 32]).await;
            let consumed = buf[..result.expect("the recv succeeded")].to_vec();

            // Peeking EOF leaves it too; the recv after it still sees the close.
            let (peeked_eof, _) = socket_peek(&socket, vec![0u8; 32]).await;
            let (eof, _) = socket_recv(&socket, vec![0u8; 32]).await;
            (first, second, consumed, peeked_eof.unwrap(), eof.unwrap())
        },
        None,
        |run| run(),
    );

    assert_eq!(got.0, b"hello");
    assert_eq!(got.1, b"hello");
    assert_eq!(got.2, b"hello");
    assert_eq!(got.3, 0);
    assert_eq!(got.4, 0);
    assert_eq!(handle.scenario_remaining(), 0);
}

#[test]
fn concurrent_sends_all_complete() {
    let (fake, _handle) = simulated();

    let total = RuntimeInner::simulated(fake).block_on::<(), _>(
        async move {
            let handles: Vec<_> = (0..8)
                .map(|_| {
                    let socket = simulated_socket();
                    spawn(async move {
                        let (result, _) = socket_send(&socket, b"xyz").await;
                        result.expect("the send succeeded")
                    })
                })
                .collect();

            let mut total = 0;
            for handle in handles {
                total += handle.await;
            }
            total
        },
        None,
        |run| run(),
    );

    // Eight tasks, eight completions routed back to eight distinct wakers.
    assert_eq!(total, 24);
}

#[test]
fn a_timer_fires_without_any_time_actually_passing() {
    // The ring shares its clock with the timewheel, so a sleep is a jump. The
    // runtime is not told, and does not need to be.
    let (fake, handle) = simulated();
    let clock = handle.clock().expect("built with virtual_time");

    let started = Instant::now();
    RuntimeInner::simulated(fake).block_on::<(), _>(
        async move { Sleep::during(Duration::from_secs(5)).await },
        None,
        |run| run(),
    );
    let wall = started.elapsed();

    assert!(
        wall < Duration::from_secs(1),
        "a 5s sleep took {wall:?} of real time",
    );
    // And it was not skipped: simulated time really moved five seconds.
    assert!(
        clock.elapsed() >= Duration::from_secs(5),
        "the sleep resolved after only {:?} of simulated time",
        clock.elapsed(),
    );
}

/// A sleep that fails the test if it is ever reached.
async fn never_reached(duration: Duration) {
    Sleep::during(duration).await;
    panic!("the {duration:?} arm resolved first");
}

#[test]
fn select_resolves_on_whichever_sleep_is_shorter() {
    // `long` only has to outlive `short` and never win.
    let long = Duration::from_secs(5);
    let short = Duration::from_millis(100);

    // Both orders, so "always the second arm" cannot pass either.
    for long_first in [true, false] {
        let (fake, handle) = simulated();
        let clock = handle.clock().expect("built with virtual_time");

        RuntimeInner::simulated(fake).block_on::<(), _>(
            async move {
                if long_first {
                    select(never_reached(long), Sleep::during(short)).await;
                } else {
                    select(Sleep::during(short), never_reached(long)).await;
                }
            },
            None,
            |run| run(),
        );

        // The short arm was waited on rather than resolved on the spot — the
        // one thing a panic on the long arm cannot show.
        assert!(
            clock.elapsed() >= short,
            "resolved after only {:?} of simulated time",
            clock.elapsed(),
        );
    }
}

#[test]
fn work_continues_normally_after_a_select() {
    let (fake, _handle) = simulated();

    RuntimeInner::simulated(fake).block_on::<(), _>(
        async move {
            select(
                never_reached(Duration::from_secs(5)),
                Sleep::during(Duration::from_millis(100)),
            )
            .await;
            // Armed after the select, so this takes another full pass of the
            // loop — and must not queue behind the arm that lost.
            Sleep::during(Duration::from_millis(50)).await;
        },
        None,
        |run| run(),
    );
}

#[test]
fn a_managed_recv_hands_a_borrowed_buffer_to_the_task() {
    let (fake, handle) = simulated();
    let socket = simulated_socket();
    handle.on_multishot_read(socket.fd(), [ReadEvent::data(b"hello")]);

    let got = RuntimeInner::simulated(fake).block_on::<ManagedRing, _>(
        async move {
            let mut recv = MultiRecv::<ManagedRing>::new(socket);
            let buffer = recv.next().await.expect("a chunk").expect("no error");
            buffer.as_ref().to_vec()
        },
        None,
        |run| run(),
    );

    // The bytes went into a buffer the ring provided, the CQE named it in its
    // flags, and the driver turned that into a typed BorrowedBuffer.
    assert_eq!(got, b"hello");
}

#[test]
fn a_ring_of_two_serves_more_reads_than_it_has_buffers() {
    let (fake, handle) = simulated();
    let socket = simulated_socket();

    // One op's whole life, declared as one thing.
    handle.on_multishot_read(
        socket.fd(),
        [b"one".as_slice(), b"two", b"three", b"four", b"five"].map(ReadEvent::data),
    );

    let got = RuntimeInner::simulated(fake).block_on::<ManagedRing, _>(
        async move {
            let mut recv = MultiRecv::<ManagedRing>::new(socket);
            let mut got: Vec<Vec<u8>> = Vec::new();
            for _ in 0..5 {
                let buffer = recv.next().await.expect("a chunk").expect("no error");
                got.push(buffer.as_ref().to_vec());
                // Dropped here, which recycles it.
            }
            got
        },
        None,
        |run| run(),
    );

    // Five reads through a ring of two only works if each buffer went back
    // after the task was done with it. The negative control is below.
    assert_eq!(
        got,
        vec![
            b"one".to_vec(),
            b"two".to_vec(),
            b"three".to_vec(),
            b"four".to_vec(),
            b"five".to_vec()
        ]
    );
    assert_eq!(handle.scenario_remaining(), 0);
}

#[test]
fn holding_every_buffer_drains_the_ring() {
    // The mirror of the test above: same ring, same reads, but the task keeps
    // its buffers instead of dropping them. If recycling were not what makes
    // that one pass, this one would pass too.
    let (fake, handle) = simulated();
    let socket = simulated_socket();

    handle.on_multishot_read(
        socket.fd(),
        [b"one".as_slice(), b"two", b"three"].map(ReadEvent::data),
    );

    let (delivered, failure) = RuntimeInner::simulated(fake).block_on::<ManagedRing, _>(
        async move {
            let mut recv = MultiRecv::<ManagedRing>::new(socket);
            let mut held = Vec::new();
            let mut failure = None;

            for _ in 0..3 {
                match recv.next().await {
                    // Pushed, not dropped: this buffer never goes back.
                    Some(Ok(buffer)) => held.push(buffer),
                    Some(Err(error)) => {
                        failure = Some(error);
                        break;
                    }
                    None => break,
                }
            }
            (held.len(), failure)
        },
        None,
        |run| run(),
    );

    assert_eq!(delivered, 2, "the ring holds exactly two buffers");
    assert!(
        matches!(failure, Some(CompletionError::Other(code)) if code == -libc::ENOBUFS),
        "the third read should have run the ring dry, got {failure:?}",
    );
}

#[test]
fn a_timed_out_read_leaves_its_data_for_the_next_one() {
    let (fake, handle) = simulated();
    let socket = simulated_socket();

    // Arrives long after the timeout below, so the first read never sees it.
    handle.on_read(
        socket.fd(),
        ReadEvent::data(b"hello").after(Duration::from_secs(10)),
    );

    let got = RuntimeInner::simulated(fake).block_on::<(), _>(
        async move {
            // The timeout wins, and dropping the losing read cancels it.
            let raced = select(
                socket_recv(&socket, vec![0u8; 32]),
                Sleep::during(Duration::from_millis(100)),
            )
            .await;
            assert!(
                matches!(raced, SelectResult::Second(_)),
                "the read should have timed out",
            );

            // A kernel would still have those bytes buffered, so the next read
            // collects them. The cancel must not have eaten the delivery.
            let (result, buf) = socket_recv(&socket, vec![0u8; 32]).await;
            let n = result.expect("the second read succeeded");
            buf[..n].to_vec()
        },
        None,
        |run| run(),
    );

    assert_eq!(got, b"hello");
    assert_eq!(handle.scenario_remaining(), 0);
}

/// Whether the second completion was already queued when the task asked for
/// it, rather than needing another round of the event loop.
///
/// Raced against `yield_now`, which resolves only once the task has gone back
/// to the loop. It is polled first, so it wins any tie: `Second` therefore means
/// the completion was there on the very first poll, without a round trip — the
/// two landed in one drain.
fn second_completion_was_already_there(batched: bool) -> bool {
    let (fake, handle) = simulated();
    let socket = simulated_socket();

    if batched {
        handle.on_multishot_read(
            socket.fd(),
            [vec![ReadEvent::data(b"a"), ReadEvent::data(b"b")]],
        );
    } else {
        handle.on_multishot_read(socket.fd(), [ReadEvent::data(b"a"), ReadEvent::data(b"b")]);
    }

    RuntimeInner::simulated(fake).block_on::<ManagedRing, _>(
        async move {
            let mut recv = MultiRecv::<ManagedRing>::new(socket);
            recv.next().await.expect("a chunk").expect("no error");
            matches!(
                select(yield_now(), recv.next()).await,
                SelectResult::Second(_),
            )
        },
        None,
        |run| run(),
    )
}

#[test]
fn a_batch_reaches_the_task_in_one_go() {
    assert!(
        second_completion_was_already_there(true),
        "the task had to go back to the loop for a completion declared alongside",
    );
}

#[test]
fn separate_batches_let_the_program_run_in_between() {
    // The same two completions, declared apart. This is the difference batching
    // makes, and the reason it is worth stating rather than inheriting.
    assert!(
        !second_completion_was_already_there(false),
        "a completion declared in its own batch arrived with the previous one",
    );
}

#[test]
fn a_whole_batch_is_drained_in_one_pass() {
    let (fake, handle) = simulated();
    let socket = simulated_socket();

    handle.on_multishot_read(
        socket.fd(),
        [vec![
            ReadEvent::data(b"a"),
            ReadEvent::data(b"b"),
            ReadEvent::data(b"c"),
            ReadEvent::data(b"d"),
            ReadEvent::data(b"e"),
        ]],
    );

    let drained = RuntimeInner::simulated(fake).block_on::<BigRing, _>(
        async move {
            let mut recv = MultiRecv::<BigRing>::new(socket);
            recv.next().await.expect("a chunk").expect("no error");

            // The rest of the batch, raced against a single round of the loop.
            let rest = async {
                let mut taken = 0;
                while taken < 4 {
                    recv.next().await.expect("a chunk").expect("no error");
                    taken += 1;
                }
                taken
            };
            match select(yield_now(), rest).await {
                SelectResult::First(_) => 0,
                SelectResult::Second(taken) => taken,
            }
        },
        None,
        |run| run(),
    );

    assert_eq!(
        drained, 4,
        "the task went back to the loop before the batch was exhausted",
    );
}

#[test]
fn a_ring_buffer_is_sent_back_as_is() {
    let (fake, handle) = simulated();
    let socket = simulated_socket();
    let fd = socket.fd();
    handle.on_multishot_read(fd, [ReadEvent::data(b"hello")]);

    let sent = RuntimeInner::simulated(fake).block_on::<ManagedRing, _>(
        async move {
            let mut recv = MultiRecv::<ManagedRing>::new(socket);
            let buffer = recv.next().await.expect("a chunk").expect("no error");
            // The kernel's own receive buffer goes straight back out.
            let (result, _buffer) = socket_send(recv.socket(), buffer).await;
            result.expect("the send succeeded")
        },
        None,
        |run| run(),
    );

    assert_eq!(sent, 5);
    // What the peer got is the bytes of the ring buffer, no copy in between.
    assert_eq!(handle.writes(fd), vec![b"hello".to_vec()]);
}

#[test]
fn a_sent_ring_buffer_returns_to_the_ring_once_the_send_completes() {
    // Five reads through a ring of two, each buffer sent before the next read:
    // only possible if a buffer handed to a send comes back to the ring after
    // the send completes, rather than being lost to it.
    let (fake, handle) = simulated();
    let socket = simulated_socket();
    let fd = socket.fd();
    handle.on_multishot_read(
        fd,
        [b"one".as_slice(), b"two", b"three", b"four", b"five"].map(ReadEvent::data),
    );

    let echoed = RuntimeInner::simulated(fake).block_on::<ManagedRing, _>(
        async move {
            let mut recv = MultiRecv::<ManagedRing>::new(socket);
            let mut echoed = 0;
            for _ in 0..5 {
                let buffer = recv.next().await.expect("a chunk").expect("no error");
                let (result, _buffer) = socket_send(recv.socket(), buffer).await;
                result.expect("the send succeeded");
                echoed += 1;
            }
            echoed
        },
        None,
        |run| run(),
    );

    assert_eq!(echoed, 5);
    assert_eq!(
        handle.writes(fd),
        [b"one".as_slice(), b"two", b"three", b"four", b"five"].map(<[u8]>::to_vec)
    );
    assert_eq!(handle.scenario_remaining(), 0);
}
