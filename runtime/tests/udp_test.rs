use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket as StdUdpSocket};
use std::time::Duration;

use runtime::io_driver::operations::sockets::multi_recv::CompletionError;
use runtime::net::{UdpMultiRecv, UdpSocket};
use runtime::runtime::select::{SelectResult, select};
use runtime::runtime::time::Sleep;
use runtime::{main_thread, main_thread_with};
use runtime_streams::StreamExt;

runtime::define_buf_ring!(pub Small, bgid = 0, buffer_size = 4096, ring_size = 64);
runtime::define_buf_ring!(pub Tiny, bgid = 1, buffer_size = 4096, ring_size = 2);

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
