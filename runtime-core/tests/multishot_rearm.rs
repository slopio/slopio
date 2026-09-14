//! A multishot recv the kernel ended on its own: EOF, or an error.
//!
//! An error CQE without `IORING_CQE_F_MORE` ends the op in the kernel. The
//! stream must then re-arm on the next poll rather than wait for a CQE that
//! will never come, and EOF must stay EOF without re-arming anything.

use runtime_core::deterministic_testing::{FakeRing, ReadEvent, simulated_socket};
use runtime_core::io_driver::operations::sockets::multi_recv::{CompletionError, MultiRecv};
use runtime_core::runtime::RuntimeInner;
use runtime_streams::StreamExt;

runtime_core::define_buf_ring!(ReArmRing, bgid = 9, buffer_size = 64, ring_size = 4);
// One buffer, so holding it runs the ring dry on the very next read.
runtime_core::define_buf_ring!(OneBuf, bgid = 8, buffer_size = 64, ring_size = 1);

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
