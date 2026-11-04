//! `Bytes` views on buffer-ring slots: the slot goes back to its ring only
//! when the last view on it is dropped.

use slopio::bytes::Bytes;
use slopio::deterministic_testing::{FakeRing, ReadEvent, simulated_socket};
use slopio::io_driver::operations::sockets::multi_recv::MultiRecv;
use slopio::io_driver::{BufRingSpec, BufRingUsage};
use slopio::runtime::RuntimeInner;
use slopio::streams::StreamExt;

slopio::define_buf_ring!(Small, bgid = 1, buffer_size = 64, ring_size = 4);

fn in_flight() -> usize {
    let BufRingUsage { in_flight, .. } = Small::usage();
    in_flight
}

/// Run `body` against a socket that receives `chunks`, one per read.
fn with_reads<const N: usize>(
    chunks: [&'static [u8]; N],
    body: impl AsyncFnOnce(MultiRecv<Small>),
) {
    let (fake, handle) = FakeRing::builder().virtual_time().build();
    let socket = simulated_socket();
    handle.on_multishot_read(socket.fd(), chunks.map(ReadEvent::data));
    RuntimeInner::simulated(fake).block_on::<Small, _>(
        async move { body(MultiRecv::<Small>::new(socket)).await },
        None,
        |run| run(),
    );
}

async fn next(recv: &mut MultiRecv<Small>) -> Bytes {
    Bytes::from(recv.next().await.expect("a chunk").expect("no error"))
}

#[test]
fn the_slot_stays_out_while_any_view_holds_it() {
    with_reads([b"GET / HTTP/1.1"], async |mut recv| {
        let mut data = next(&mut recv).await;
        assert_eq!(in_flight(), 1);

        let method = data.split_to(3);
        let version = data.split_off(3);
        let copy = version.clone();
        drop(data);
        drop(method);
        drop(version);
        assert_eq!(in_flight(), 1, "one view is still alive");
        assert_eq!(copy, "HTTP/1.1");

        drop(copy);
        assert_eq!(in_flight(), 0);
    });
}

#[test]
fn copying_out_lets_the_slot_go() {
    with_reads([b"x-request-id: 42"], async |mut recv| {
        let data = next(&mut recv).await;
        let kept = Bytes::copy_from_slice(&data[14..]);
        drop(data);
        assert_eq!(in_flight(), 0);
        assert_eq!(kept, "42");
    });
}

#[test]
fn a_sole_view_is_mutable_in_place() {
    with_reads([b"\x01\x02\x03\x04"], async |mut recv| {
        let data = next(&mut recv).await;
        let at = data.as_ptr();
        let other = data.clone();
        let data = data.try_into_mut().expect_err("shared");
        drop(other);

        let mut data = data.try_into_mut().expect("now the only view");
        assert_eq!(data.as_ptr(), at);
        assert_eq!(data.capacity(), 4, "a slot does not grow in place");
        data.iter_mut().for_each(|b| *b ^= 0xff);
        assert_eq!(data, [0xfe, 0xfd, 0xfc, 0xfb]);

        // Growing moves it to the heap and gives the slot back.
        data.extend_from_slice(b"!");
        assert_eq!(in_flight(), 0);
        assert_eq!(data, [0xfe, 0xfd, 0xfc, 0xfb, b'!']);
    });
}

#[test]
fn frozen_back_it_still_holds_the_slot() {
    with_reads([b"abcd"], async |mut recv| {
        let data = next(&mut recv).await;
        let mut data = data.try_into_mut().expect("the only view");
        data[0] = b'A';
        let data = data.freeze();
        assert_eq!(in_flight(), 1);
        assert_eq!(data, "Abcd");
        drop(data);
        assert_eq!(in_flight(), 0);
    });
}

#[test]
fn every_slot_of_the_pool_can_be_viewed_at_once() {
    with_reads([b"a", b"b", b"c", b"d"], async |mut recv| {
        let mut views = Vec::new();
        for _ in 0..4 {
            views.push(next(&mut recv).await);
        }
        assert_eq!(in_flight(), 4);
        let halves: Vec<Bytes> = views.iter().map(|v| v.slice(..)).collect();
        views.clear();
        assert_eq!(in_flight(), 4);
        assert_eq!(halves, [&b"a"[..], b"b", b"c", b"d"]);
        drop(halves);
        assert_eq!(in_flight(), 0);
    });
}
