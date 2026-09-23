//! A multishot recv the kernel ended on its own: EOF, or an error.
//!
//! An error CQE without `IORING_CQE_F_MORE` ends the op in the kernel. What
//! the stream owes its consumer next is the error's `Continuation`: re-arm on
//! the next poll rather than wait for a CQE that will never come, stay armable
//! but say so, or end — and EOF stays EOF without re-arming anything.

use runtime_core::deterministic_testing::{FakeRing, ReadEvent, simulated_socket};
use runtime_core::io_driver::operations::sockets::multi_recv::{CompletionError, MultiRecv};
use runtime_core::runtime::RuntimeInner;
use runtime_streams::StreamExt;

runtime_core::define_buf_ring!(ReArmRing, bgid = 0, buffer_size = 64, ring_size = 4);
// One buffer, so holding it runs the ring dry on the very next read.
runtime_core::define_buf_ring!(OneBuf, bgid = 1, buffer_size = 64, ring_size = 1);

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
runtime_core::define_buf_ring!(BurstRing, bgid = 2, buffer_size = 64, ring_size = 16);

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
