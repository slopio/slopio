//! A multishot recv the kernel ended on its own: EOF, or an error.
//!
//! An error CQE without `IORING_CQE_F_MORE` ends the op in the kernel. What
//! the stream owes its consumer next is the error's `Continuation`: re-arm on
//! the next poll rather than wait for a CQE that will never come, stay armable
//! but say so, or end — and EOF stays EOF without re-arming anything.

use slopio::deterministic_testing::{AcceptEvent, FakeRing, ReadEvent, simulated_socket};
use slopio::io_driver::operations::sockets::multi_accept::{AcceptError, MultiAccept};
use slopio::io_driver::operations::sockets::multi_recv::{CompletionError, MultiRecv};
use std::time::Duration;

use slopio::runtime::RuntimeInner;
use slopio::runtime::time::Sleep;
use slopio::streams::StreamExt;

slopio::define_buf_ring!(ReArmRing, bgid = 0, buffer_size = 64, ring_size = 4);
// One buffer, so holding it runs the ring dry on the very next read.
slopio::define_buf_ring!(OneBuf, bgid = 1, buffer_size = 64, ring_size = 1);

#[test]
fn a_transient_error_re_arms_the_stream() {
    let (fake, handle) = FakeRing::builder().virtual_time().build();
    let socket = simulated_socket();

    // The first op dies on EINTR; the re-armed one delivers.
    handle.on_multishot_read(socket.fd(), [ReadEvent::Error(libc::EINTR)]);
    handle.on_multishot_read(socket.fd(), [ReadEvent::data(b"after"), ReadEvent::Eof]);

    let (got, end) = RuntimeInner::simulated(fake).block_on::<ReArmRing, _>(
        async move {
            let mut recv = MultiRecv::<ReArmRing>::new(socket);
            let got = recv.next().await.expect("a chunk").expect("no error");
            let end = recv.next().await.is_none();
            (got.as_ref().to_vec(), end)
        },
        None,
        |run| run(),
    );

    assert_eq!(got, b"after");
    assert!(end);
}

#[test]
fn a_dry_ring_reports_once_then_re_arms_when_a_buffer_returns() {
    let (fake, handle) = FakeRing::builder().virtual_time().build();
    let socket = simulated_socket();

    // "b" lands while "a" is still held: ENOBUFS, which ends the op.
    handle.on_multishot_read(socket.fd(), [ReadEvent::data(b"a"), ReadEvent::data(b"b")]);
    handle.on_multishot_read(socket.fd(), [ReadEvent::data(b"c"), ReadEvent::Eof]);

    let (dry, got, end) = RuntimeInner::simulated(fake).block_on::<OneBuf, _>(
        async move {
            let mut recv = MultiRecv::<OneBuf>::new(socket);
            let a = recv.next().await.expect("a chunk").expect("no error");
            let dry = match recv.next().await {
                Some(Err(CompletionError::Other(code))) => code == -libc::ENOBUFS,
                Some(Err(other)) => panic!("expected ENOBUFS, got {other:?}"),
                Some(Ok(buffer)) => panic!("expected ENOBUFS, got {} bytes", buffer.len()),
                None => panic!("expected ENOBUFS, got EOF"),
            };
            // The buffer goes back, and the next poll arms a fresh recv.
            drop(a);
            let got = recv.next().await.expect("a chunk").expect("no error");
            let end = recv.next().await.is_none();
            (dry, got.as_ref().to_vec(), end)
        },
        None,
        |run| run(),
    );

    assert!(dry);
    assert_eq!(got, b"c");
    assert!(end);
}

#[test]
fn eof_stays_eof_without_re_arming() {
    let (fake, handle) = FakeRing::builder().virtual_time().build();
    let socket = simulated_socket();

    handle.on_multishot_read(socket.fd(), [ReadEvent::data(b"a"), ReadEvent::Eof]);

    let ends = RuntimeInner::simulated(fake).block_on::<ReArmRing, _>(
        async move {
            let mut recv = MultiRecv::<ReArmRing>::new(socket);
            let _a = recv.next().await.expect("a chunk").expect("no error");
            let mut ends = 0;
            for _ in 0..3 {
                if recv.next().await.is_none() {
                    ends += 1;
                }
            }
            ends
        },
        None,
        |run| run(),
    );

    assert_eq!(ends, 3);
    // Nothing was re-armed: an unscripted read would be parked here.
    assert_eq!(handle.parked(), 0);
    assert!(handle.inflight().is_empty());
}

#[test]
fn a_fatal_error_is_reported_once_and_ends_the_stream() {
    let (fake, handle) = FakeRing::builder().virtual_time().build();
    let socket = simulated_socket();

    handle.on_multishot_read(socket.fd(), [ReadEvent::Error(libc::ECONNRESET)]);
    // Scripted but unreachable: a re-arm would pick this up, and must not.
    handle.on_multishot_read(socket.fd(), [ReadEvent::data(b"never"), ReadEvent::Eof]);

    let (reset, ends) = RuntimeInner::simulated(fake).block_on::<ReArmRing, _>(
        async move {
            let mut recv = MultiRecv::<ReArmRing>::new(socket);
            let reset = matches!(
                recv.next().await,
                Some(Err(CompletionError::ConnectionReset))
            );
            let mut ends = 0;
            for _ in 0..3 {
                if recv.next().await.is_none() {
                    ends += 1;
                }
            }
            (reset, ends)
        },
        None,
        |run| run(),
    );

    assert!(reset);
    assert_eq!(ends, 3, "the error is not repeated, and nothing re-arms");
}

// Enough buffers for a whole burst, so only the queue bound can stop it.
slopio::define_buf_ring!(BurstRing, bgid = 2, buffer_size = 64, ring_size = 16);

/// A burst larger than the queue pauses the multishot mid-CQ-processing: the
/// pause cancel is pushed once the CQ loop lets go of the ring, and the stream
/// resumes where it stopped. Under Miri this is the aliasing check.
#[test]
fn a_burst_past_the_queue_bound_pauses_and_resumes_in_order() {
    let (fake, handle) = FakeRing::builder().virtual_time().build();
    let socket = simulated_socket();

    let chunks: Vec<Vec<u8>> = (0..12u8).map(|i| vec![i]).collect();
    // One batch: every chunk lands in the same CQ pass, past the bound of 4.
    let burst: Vec<ReadEvent> = chunks.iter().map(|c| ReadEvent::data(c.clone())).collect();
    handle.on_multishot_read(socket.fd(), [burst, vec![ReadEvent::Eof]]);

    let got = RuntimeInner::simulated(fake).block_on::<BurstRing, _>(
        async move {
            let mut recv = MultiRecv::<BurstRing, _, 4>::with_queue_size(socket);
            let mut got = Vec::new();
            while let Some(chunk) = recv.next().await {
                got.push(chunk.expect("no error").as_ref().to_vec());
            }
            got
        },
        None,
        |run| run(),
    );

    assert_eq!(got, chunks);
}

/// Chunks landing one at a time while nobody polls: `limit` past the first,
/// then the pause, which leaves the rest with the peer.
fn held_while_unpolled(limit: Option<usize>) -> (usize, Vec<Vec<u8>>, Vec<Vec<u8>>) {
    let (fake, handle) = FakeRing::builder().virtual_time().build();
    let socket = simulated_socket();
    let fd = socket.fd();

    let chunks: Vec<Vec<u8>> = (0..12u8).map(|i| vec![i]).collect();
    // A batch each, a millisecond apart: each lands in its own CQ pass.
    let spaced = chunks
        .iter()
        .map(|c| vec![ReadEvent::data(c.clone()).after(Duration::from_millis(1))]);
    let eof = std::iter::once(vec![ReadEvent::Eof.after(Duration::from_millis(1))]);
    handle.on_multishot_read(fd, spaced.chain(eof));

    let script = handle.clone();
    let (held, got) = RuntimeInner::simulated(fake).block_on::<BurstRing, _>(
        async move {
            slopio::runtime::spawn(async move {
                let mut recv = match limit {
                    Some(limit) => MultiRecv::<BurstRing>::with_queue_limit(socket, limit),
                    None => MultiRecv::<BurstRing>::new(socket),
                };
                let mut got = vec![
                    recv.next()
                        .await
                        .expect("a chunk")
                        .expect("no error")
                        .as_ref()
                        .to_vec(),
                ];
                // Every chunk is due by now, were the stream still reading.
                Sleep::during(Duration::from_millis(50)).await;
                let held = script.reads(fd).len() - 1;
                while let Some(chunk) = recv.next().await {
                    got.push(chunk.expect("no error").as_ref().to_vec());
                }
                (held, got)
            })
            .await
        },
        None,
        |run| run(),
    );
    (held, got, chunks)
}

/// A consumer that stops polling holds no more than its queue limit of the
/// shared ring; the default bound lets the whole burst in.
#[test]
fn a_stream_nobody_polls_holds_no_more_than_its_queue_limit() {
    let (held, got, chunks) = held_while_unpolled(Some(2));
    assert!(held <= 3, "{held} chunks held past a limit of 2");
    assert_eq!(
        got, chunks,
        "and the rest arrives, in order, once it polls again"
    );

    let (held, got, chunks) = held_while_unpolled(None);
    assert_eq!(held, 11, "the default bound of 32 holds all of them");
    assert_eq!(got, chunks);
}

/// Out of descriptors, a listener reports it and stays open: the next poll
/// re-arms. A fatal error is reported once, then the stream ends.
#[test]
fn an_accept_reports_emfile_and_re_arms_then_ends_on_a_fatal_error() {
    let (fake, handle) = FakeRing::builder().virtual_time().build();

    let seen = RuntimeInner::simulated(fake).block_on::<(), _>(
        async move {
            let mut accept = MultiAccept::new(8080).unwrap();
            let fd = accept.socket().fd();
            handle.on_multishot_accept(
                fd,
                [AcceptEvent::Connection, AcceptEvent::Error(libc::EMFILE)],
            );
            handle.on_multishot_accept(
                fd,
                [AcceptEvent::Connection, AcceptEvent::Error(libc::EINVAL)],
            );

            let mut seen = Vec::new();
            while let Some(item) = accept.next().await {
                seen.push(match item {
                    Ok(_) => "conn".to_string(),
                    Err(AcceptError::Other(errno)) => format!("errno {errno}"),
                    Err(AcceptError::Cancelled) => "cancelled".to_string(),
                });
            }
            // Ended for good: polling again does not re-arm.
            assert!(accept.next().await.is_none());
            seen
        },
        None,
        |run| run(),
    );

    assert_eq!(
        seen,
        [
            "conn".to_string(),
            format!("errno {}", libc::EMFILE),
            "conn".to_string(),
            format!("errno {}", libc::EINVAL),
        ]
    );
}

/// Reads `socket` to its end through a fresh stream: what a stopped stream
/// left behind for whoever takes the socket next.
async fn rest_of<S: slopio::BufRingSpec>(socket: slopio::net::SocketWrapper) -> Vec<Vec<u8>> {
    let mut recv = MultiRecv::<S>::new(socket);
    let mut rest = Vec::new();
    while let Some(chunk) = recv.next().await {
        rest.push(chunk.expect("no error").as_ref().to_vec());
    }
    rest
}

/// `stop` keeps what the kernel had already read: the chunk that landed with
/// the first one is yielded, then `None`, and the socket handed back by
/// `into_inner` reads on from where the stream stopped.
#[test]
fn a_stopped_stream_yields_what_it_read_and_hands_the_socket_back_whole() {
    let (fake, handle) = FakeRing::builder().virtual_time().build();
    let socket = simulated_socket();

    // "a" and "b" land in one CQ pass; "c" is still on its way at the stop.
    handle.on_multishot_read(
        socket.fd(),
        [
            vec![ReadEvent::data(b"a").into(), ReadEvent::data(b"b").into()],
            vec![ReadEvent::data(b"c").after(Duration::from_millis(10))],
            vec![ReadEvent::Eof.after(Duration::from_millis(1))],
        ],
    );

    let (first, drained, rest) = RuntimeInner::simulated(fake).block_on::<BurstRing, _>(
        async move {
            let mut recv = MultiRecv::<BurstRing>::new(socket);
            let first = recv.next().await.expect("a chunk").expect("no error");
            let first = first.as_ref().to_vec();

            recv.stop();
            let mut drained = Vec::new();
            while let Some(chunk) = recv.next().await {
                drained.push(chunk.expect("no error").as_ref().to_vec());
            }
            // Ended for good: polling again does not re-arm.
            assert!(recv.next().await.is_none());

            let rest = rest_of::<BurstRing>(recv.into_inner()).await;
            (first, drained, rest)
        },
        None,
        |run| run(),
    );

    assert_eq!(first, b"a");
    assert_eq!(drained, [b"b".to_vec()], "read before the stop, so yielded");
    assert_eq!(rest, [b"c".to_vec()], "not read yet, so left in the socket");
}

/// A stream already pausing under backpressure has a cancel in flight: `stop`
/// rides on it, and the whole burst the kernel delivered comes out in order.
#[test]
fn stopping_a_stream_that_is_pausing_loses_none_of_the_burst() {
    let (fake, handle) = FakeRing::builder().virtual_time().build();
    let socket = simulated_socket();

    let chunks: Vec<Vec<u8>> = (0..12u8).map(|i| vec![i]).collect();
    // One batch, past the bound of 4: the queue overflows and the op pauses.
    let burst: Vec<ReadEvent> = chunks.iter().map(|c| ReadEvent::data(c.clone())).collect();
    handle.on_multishot_read(socket.fd(), [burst, vec![ReadEvent::data(b"later")]]);

    let (got, rest) = RuntimeInner::simulated(fake).block_on::<BurstRing, _>(
        async move {
            let mut recv = MultiRecv::<BurstRing, _, 4>::with_queue_size(socket);
            let mut got = vec![
                recv.next()
                    .await
                    .expect("a chunk")
                    .expect("no error")
                    .as_ref()
                    .to_vec(),
            ];
            recv.stop();
            while let Some(chunk) = recv.next().await {
                got.push(chunk.expect("no error").as_ref().to_vec());
            }
            let socket = recv.into_inner();
            // Whatever the pause left with the peer is still there to read.
            let mut again = MultiRecv::<BurstRing>::new(socket);
            let mut rest = Vec::new();
            while got.len() + rest.len() < 13 {
                let chunk = again.next().await.expect("a chunk").expect("no error");
                rest.push(chunk.as_ref().to_vec());
            }
            (got, rest)
        },
        None,
        |run| run(),
    );

    let all: Vec<Vec<u8>> = got.into_iter().chain(rest).collect();
    let expected: Vec<Vec<u8>> = chunks.into_iter().chain([b"later".to_vec()]).collect();
    assert_eq!(all, expected);
}

/// Stopped before its first poll, a stream arms nothing and takes nothing
/// from the socket.
#[test]
fn a_stream_stopped_before_its_first_poll_reads_nothing() {
    let (fake, handle) = FakeRing::builder().virtual_time().build();
    let socket = simulated_socket();
    handle.on_multishot_read(socket.fd(), [ReadEvent::data(b"all"), ReadEvent::Eof]);

    let rest = RuntimeInner::simulated(fake).block_on::<BurstRing, _>(
        async move {
            let mut recv = MultiRecv::<BurstRing>::new(socket);
            recv.stop();
            assert!(recv.next().await.is_none());
            rest_of::<BurstRing>(recv.into_inner()).await
        },
        None,
        |run| run(),
    );

    assert_eq!(rest, [b"all".to_vec()]);
}

/// A reset that landed before the stop is still reported, once: the caller
/// must not take the socket for a live one.
#[test]
fn a_stopped_stream_still_reports_a_fatal_error() {
    let (fake, handle) = FakeRing::builder().virtual_time().build();
    let socket = simulated_socket();
    handle.on_multishot_read(
        socket.fd(),
        [ReadEvent::data(b"a"), ReadEvent::Error(libc::ECONNRESET)],
    );

    let (reset, end) = RuntimeInner::simulated(fake).block_on::<BurstRing, _>(
        async move {
            let mut recv = MultiRecv::<BurstRing>::new(socket);
            drop(recv.next().await.expect("a chunk").expect("no error"));
            recv.stop();
            let reset = matches!(
                recv.next().await,
                Some(Err(CompletionError::ConnectionReset))
            );
            (reset, recv.next().await.is_none())
        },
        None,
        |run| run(),
    );

    assert!(reset);
    assert!(end);
}

/// A dry ring had already ended the op when the stream is stopped: there is
/// nothing to re-arm for, so the stream ends instead of reporting `ENOBUFS`.
#[test]
fn a_stream_stopped_on_a_dry_ring_ends_without_an_error() {
    let (fake, handle) = FakeRing::builder().virtual_time().build();
    let socket = simulated_socket();
    handle.on_multishot_read(socket.fd(), [ReadEvent::data(b"a"), ReadEvent::data(b"b")]);

    let end = RuntimeInner::simulated(fake).block_on::<OneBuf, _>(
        async move {
            let mut recv = MultiRecv::<OneBuf>::new(socket);
            // Held, so "b" finds no buffer.
            let a = recv.next().await.expect("a chunk").expect("no error");
            recv.stop();
            let end = recv.next().await.is_none();
            drop(a);
            drop(recv.into_inner());
            end
        },
        None,
        |run| run(),
    );

    assert!(end);
}
