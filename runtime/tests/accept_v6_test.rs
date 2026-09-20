use std::{
    io::{Read, Write},
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, TcpListener, TcpStream},
    sync::mpsc,
};

use runtime::{
    main_thread,
    net::{MultiAccept, socket_recv, socket_send},
};
use runtime_streams::StreamExt;

/// Skip the v6 cases on a host without IPv6 rather than fail them.
fn has_ipv6() -> bool {
    TcpListener::bind((Ipv6Addr::LOCALHOST, 0)).is_ok()
}

/// A v6 listener accepts a v6 client, and both directions carry bytes.
#[test]
fn test_accept_v6_roundtrip() {
    if !has_ipv6() {
        return;
    }
    let (tx, rx) = mpsc::channel();

    let server = std::thread::spawn(move || {
        main_thread(async move {
            let mut ma = MultiAccept::bind(SocketAddr::from((Ipv6Addr::LOCALHOST, 0))).unwrap();
            tx.send(ma.local_addr().unwrap()).unwrap();

            let socket = ma.next().await.unwrap();
            let peer = socket.peer_addr().unwrap();

            let buf = Vec::<u8>::with_capacity(64);
            let (res, buf) = socket_recv(&socket, buf).await;
            let n = res.unwrap();

            let (res, _) = socket_send(&socket, b"ok".to_vec()).await;
            res.unwrap();

            (peer, String::from_utf8_lossy(&buf[..n]).to_string())
        })
    });

    let addr = rx.recv().unwrap();
    assert!(addr.is_ipv6());

    let mut client = TcpStream::connect(addr).unwrap();
    client.write_all(b"salut v6").unwrap();
    let mut reply = [0u8; 2];
    client.read_exact(&mut reply).unwrap();
    assert_eq!(&reply, b"ok");

    let (peer, got) = server.join().unwrap();
    assert!(peer.is_ipv6(), "peer addr: {peer}");
    assert_eq!(got, "salut v6");
}

/// `bind` on a v6 address sets IPV6_V6ONLY regardless of the host's
/// `bindv6only` sysctl, so a v4 client on the same port is refused.
#[test]
fn test_accept_v6_is_v6_only() {
    if !has_ipv6() {
        return;
    }
    let (tx, rx) = mpsc::channel();

    let server = std::thread::spawn(move || {
        main_thread(async move {
            let mut ma = MultiAccept::bind(SocketAddr::from((Ipv6Addr::UNSPECIFIED, 0))).unwrap();
            tx.send(ma.local_addr().unwrap()).unwrap();
            ma.next().await.is_some()
        })
    });

    let port = rx.recv().unwrap().port();

    let v4 = TcpStream::connect(SocketAddr::from((Ipv4Addr::LOCALHOST, port)));
    assert!(v4.is_err(), "v4 client reached a v6-only listener");

    // Unblock the accept with the family it does serve.
    let _client = TcpStream::connect(SocketAddr::from((Ipv6Addr::LOCALHOST, port))).unwrap();
    assert!(server.join().unwrap());
}

/// `bind_dual_stack` serves both families on one listener; the v4 peer shows
/// up as a v4-mapped v6 address.
#[test]
fn test_accept_dual_stack_maps_v4_peer() {
    if !has_ipv6() {
        return;
    }
    let (tx, rx) = mpsc::channel();

    let server = std::thread::spawn(move || {
        main_thread(async move {
            let mut ma = MultiAccept::bind_dual_stack(0).unwrap();
            tx.send(ma.local_addr().unwrap()).unwrap();
            let socket = ma.next().await.unwrap();
            socket.peer_addr().unwrap()
        })
    });

    let port = rx.recv().unwrap().port();
    let _client = TcpStream::connect(SocketAddr::from((Ipv4Addr::LOCALHOST, port))).unwrap();

    let peer = server.join().unwrap();
    assert_eq!(peer.ip(), IpAddr::V6(Ipv4Addr::LOCALHOST.to_ipv6_mapped()));
}

/// The v4 path is unchanged by the shared `bind_inner`.
#[test]
fn test_accept_v4_roundtrip() {
    let (tx, rx) = mpsc::channel();

    let server = std::thread::spawn(move || {
        main_thread(async move {
            let mut ma = MultiAccept::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).unwrap();
            tx.send(ma.local_addr().unwrap()).unwrap();

            let socket = ma.next().await.unwrap();
            let peer = socket.peer_addr().unwrap();

            let buf = Vec::<u8>::with_capacity(64);
            let (res, buf) = socket_recv(&socket, buf).await;
            let n = res.unwrap();
            (peer, String::from_utf8_lossy(&buf[..n]).to_string())
        })
    });

    let addr = rx.recv().unwrap();
    assert!(addr.is_ipv4());

    let mut client = TcpStream::connect(addr).unwrap();
    client.write_all(b"salut v4").unwrap();
    drop(client);

    let (peer, got) = server.join().unwrap();
    assert!(peer.is_ipv4(), "peer addr: {peer}");
    assert_eq!(got, "salut v4");
}
