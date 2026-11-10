use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket as StdUdpSocket};
use std::time::Duration;

use slopio::io_driver::operations::sockets::multi_recv::CompletionError;
use slopio::net::{UdpMultiRecv, UdpRecvHalf, UdpReuniteError, UdpSocket};
use slopio::runtime::select::{SelectResult, select};
use slopio::runtime::time::Sleep;
use slopio::streams::StreamExt;
use slopio::{main_thread, main_thread_with, spawn};

slopio::define_buf_ring!(pub Small, bgid = 0, buffer_size = 4096, ring_size = 64);
slopio::define_buf_ring!(pub Tiny, bgid = 1, buffer_size = 4096, ring_size = 2);

const LOCALHOST_V6: Ipv6Addr = Ipv6Addr::LOCALHOST;

#[test]
fn a_v6_peer_is_seen_and_answered_with_its_own_address() {
    main_thread(async {
        let rt = UdpSocket::bind_addr(SocketAddr::from((LOCALHOST_V6, 19401))).unwrap();
        let peer = StdUdpSocket::bind((LOCALHOST_V6, 0)).unwrap();
        peer.send_to(b"ping", (LOCALHOST_V6, 19401)).unwrap();

        let (packet, buf) = rt.recv_from(Vec::with_capacity(64)).await;
        let packet = packet.unwrap();
        assert_eq!(&buf[..], b"ping");
        assert_eq!(packet.addr(), peer.local_addr().unwrap());

        let (sent, _) = rt.send_to(b"pong", packet.addr()).await;
        assert_eq!(sent.unwrap(), 4);
        let mut reply = [0u8; 8];
        let (n, from) = peer.recv_from(&mut reply).unwrap();
        assert_eq!(&reply[..n], b"pong");
        assert_eq!(from.port(), 19401);
    });
}

#[test]
fn a_v4_peer_on_a_dual_stack_socket_is_seen_as_v4_mapped() {
    main_thread(async {
        let rt = UdpSocket::bind_v6(19402).unwrap();
        let peer = StdUdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        peer.send_to(b"hi", (Ipv4Addr::LOCALHOST, 19402)).unwrap();

        let (packet, _) = rt.recv_from(Vec::with_capacity(64)).await;
        let SocketAddr::V6(addr) = packet.unwrap().addr() else {
            panic!("a v6 socket reports v6 peers");
        };
        assert_eq!(addr.ip().to_ipv4_mapped(), Some(Ipv4Addr::LOCALHOST));
        assert_eq!(addr.port(), peer.local_addr().unwrap().port());
    });
}

#[test]
fn a_datagram_larger_than_the_buffer_is_flagged_truncated() {
    main_thread(async {
        let rt = UdpSocket::bind(19403).unwrap();
        let peer = StdUdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        peer.send_to(b"0123456789", (Ipv4Addr::LOCALHOST, 19403))
            .unwrap();

        let (packet, buf) = rt.recv_from(Vec::with_capacity(4)).await;
        let packet = packet.unwrap();
        assert!(packet.is_truncated());
        assert_eq!(packet.len(), 4);
        assert_eq!(&buf[..], b"0123");
        assert_eq!(packet.addr(), peer.local_addr().unwrap());
    });
}

#[test]
fn a_multishot_stream_reports_v6_peers() {
    main_thread_with::<Small, _>(async {
        let rt = UdpSocket::bind_addr(SocketAddr::from((LOCALHOST_V6, 19404))).unwrap();
        let peer = StdUdpSocket::bind((LOCALHOST_V6, 0)).unwrap();
        peer.send_to(b"multi", (LOCALHOST_V6, 19404)).unwrap();

        let mut stream = UdpMultiRecv::<Small>::new(rt);
        let datagram = stream.next().await.unwrap().unwrap();
        assert_eq!(datagram.payload(), b"multi");
        assert_eq!(datagram.addr(), peer.local_addr().unwrap());
        assert!(!datagram.is_truncated());
    });
}

#[test]
fn an_empty_ring_is_reported_and_the_stream_resumes_once_buffers_return() {
    main_thread_with::<(Small, Tiny), _>(async {
        let rt = UdpSocket::bind(19405).unwrap();
        let peer = StdUdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        for i in 0..4u8 {
            peer.send_to(&[i], (Ipv4Addr::LOCALHOST, 19405)).unwrap();
        }

        let mut stream = UdpMultiRecv::<Tiny>::new(rt);
        let first = stream.next().await.unwrap().unwrap();
        let second = stream.next().await.unwrap().unwrap();

        // Both buffers are held: the next poll must report ENOBUFS, not spin.
        let err = match select(stream.next(), Sleep::during(Duration::from_secs(5))).await {
            SelectResult::First(item) => item.unwrap().err().expect("ENOBUFS expected"),
            SelectResult::Second(_) => panic!("stream spun instead of reporting ENOBUFS"),
        };
        assert!(matches!(err, CompletionError::Other(e) if e == -libc::ENOBUFS));

        drop((first, second));
        let mut rest = Vec::new();
        for _ in 0..2 {
            match select(stream.next(), Sleep::during(Duration::from_secs(5))).await {
                SelectResult::First(item) => rest.push(item.unwrap().unwrap().payload()[0]),
                SelectResult::Second(_) => panic!("stream never resumed"),
            }
        }
        assert_eq!(rest, [2, 3]);
    });
}

/// The send half moves into a spawned task while the recv half feeds a
/// multishot stream. Dropping the send half in the other task must not close
/// the fd under the armed multishot.
#[test]
fn a_split_socket_streams_on_its_recv_half_while_another_task_sends() {
    main_thread_with::<Small, _>(async {
        let rt = UdpSocket::bind_addr(SocketAddr::from((Ipv4Addr::LOCALHOST, 19406))).unwrap();
        let peer = StdUdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let peer_addr = peer.local_addr().unwrap();
        let (recv, send) = rt.into_split();

        // Armed by the first datagram, before the send half goes away.
        let mut stream = UdpMultiRecv::<Small, UdpRecvHalf>::new(recv);
        peer.send_to(b"ping", (Ipv4Addr::LOCALHOST, 19406)).unwrap();
        assert_eq!(stream.next().await.unwrap().unwrap().payload(), b"ping");

        spawn(async move {
            let (sent, _) = send.socket().send_to(b"hello", peer_addr).await;
            sent.unwrap();
            // `send` drops here; `recv` keeps the fd open.
        })
        .await;

        let mut reply = [0u8; 8];
        let (n, from) = peer.recv_from(&mut reply).unwrap();
        assert_eq!(&reply[..n], b"hello");
        assert_eq!(from.port(), 19406);

        peer.send_to(b"pong", (Ipv4Addr::LOCALHOST, 19406)).unwrap();
        let datagram = stream.next().await.unwrap().unwrap();
        assert_eq!(datagram.payload(), b"pong");
        assert_eq!(datagram.addr(), peer_addr);
    });
}

#[test]
fn halves_reunite_only_with_their_own_socket() {
    main_thread(async {
        let any = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
        let a = UdpSocket::bind_addr(any).unwrap();
        let b = UdpSocket::bind_addr(any).unwrap();
        let (a_fd, b_fd) = (a.fd(), b.fd());
        let (a_recv, a_send) = a.into_split();
        let (b_recv, b_send) = b.into_split();

        let UdpReuniteError(a_recv, b_send) = a_recv.reunite(b_send).unwrap_err();
        assert_eq!(a_recv.reunite(a_send).unwrap().fd(), a_fd);
        assert_eq!(b_send.reunite(b_recv).unwrap().fd(), b_fd);
    });
}

/// `stop`, drain, `into_inner`: the datagrams the stream had received are
/// yielded, and the socket taken back receives the one that came after.
/// `into_inner` alone drops datagrams 1 and 2.
#[test]
fn a_stopped_stream_yields_the_datagrams_it_had_received() {
    main_thread_with::<Small, _>(async {
        let rt = UdpSocket::bind(19407).unwrap();
        let peer = StdUdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        for i in 0..3u8 {
            peer.send_to(&[i], (Ipv4Addr::LOCALHOST, 19407)).unwrap();
        }

        let mut stream = UdpMultiRecv::<Small>::new(rt);
        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(first.payload(), [0]);
        drop(first);

        stream.stop();
        let mut drained = Vec::new();
        while let Some(datagram) = stream.next().await {
            drained.push(datagram.unwrap().payload()[0]);
        }
        assert_eq!(drained, [1, 2], "received before the stop, so yielded");
        // Ended for good: polling again does not re-arm.
        assert!(stream.next().await.is_none());

        let rt = stream.into_inner();
        peer.send_to(&[3], (Ipv4Addr::LOCALHOST, 19407)).unwrap();
        let (packet, buf) = rt.recv_from(Vec::with_capacity(8)).await;
        assert_eq!(packet.unwrap().len(), 1);
        assert_eq!(&buf[..], [3]);
    });
}

/// Stopped before its first poll, a stream arms nothing and takes nothing
/// from the socket.
#[test]
fn a_stream_stopped_before_its_first_poll_receives_nothing() {
    main_thread_with::<Small, _>(async {
        let rt = UdpSocket::bind(19408).unwrap();
        let peer = StdUdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        peer.send_to(b"kept", (Ipv4Addr::LOCALHOST, 19408)).unwrap();

        let mut stream = UdpMultiRecv::<Small>::new(rt);
        stream.stop();
        assert!(stream.next().await.is_none());

        let rt = stream.into_inner();
        let (packet, buf) = rt.recv_from(Vec::with_capacity(8)).await;
        packet.unwrap();
        assert_eq!(&buf[..], b"kept");
    });
}

/// The ring ran dry before the stop: the stream ends instead of reporting
/// `ENOBUFS`, and the datagrams it could not take are still in the socket.
#[test]
fn a_stream_stopped_on_a_dry_ring_leaves_the_rest_in_the_socket() {
    main_thread_with::<(Small, Tiny), _>(async {
        let rt = UdpSocket::bind(19409).unwrap();
        let peer = StdUdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        for i in 0..4u8 {
            peer.send_to(&[i], (Ipv4Addr::LOCALHOST, 19409)).unwrap();
        }

        let mut stream = UdpMultiRecv::<Tiny>::new(rt);
        // Both buffers of the ring, held.
        let first = stream.next().await.unwrap().unwrap();
        let second = stream.next().await.unwrap().unwrap();
        assert_eq!((first.payload(), second.payload()), (&[0][..], &[1][..]));

        stream.stop();
        match select(stream.next(), Sleep::during(Duration::from_secs(5))).await {
            SelectResult::First(None) => {}
            SelectResult::First(Some(item)) => panic!("expected the end, got {:?}", item.err()),
            SelectResult::Second(_) => panic!("the stopped stream never ended"),
        }
        drop((first, second));

        let rt = stream.into_inner();
        for i in 2..4u8 {
            let (packet, buf) = rt.recv_from(Vec::with_capacity(8)).await;
            packet.unwrap();
            assert_eq!(&buf[..], [i]);
        }
    });
}
