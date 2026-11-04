use std::io::Read;
use std::net::TcpListener;

use slopio::net::{connect_tcp, send_vectored_all, send_vectored_all_zc, socket_send_vectored_zc};
use slopio::runtime::main_thread;

fn serve_read_to_end() -> (std::net::SocketAddr, std::thread::JoinHandle<Vec<u8>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).unwrap();
        buf
    });
    (addr, server)
}

// Chunks large enough for real zero-copy rather than the copy fallback.
#[test]
fn zero_copy_gather_send_delivers_every_chunk_in_order() {
    let (addr, server) = serve_read_to_end();

    let chunks: Vec<Vec<u8>> = (0..4u8).map(|i| vec![b'a' + i; 64 * 1024]).collect();
    let expected: Vec<u8> = chunks.concat();

    main_thread(async move {
        let socket = connect_tcp(addr).await.unwrap();
        let (result, returned) = send_vectored_all_zc(&socket, chunks).await;
        result.unwrap();
        assert_eq!(returned.len(), 4, "every chunk came back");
        assert!(returned.iter().all(|c| c.len() == 64 * 1024));
        drop(socket);
    });

    assert_eq!(server.join().unwrap(), expected);
}

// A single submission reports the byte count across all chunks.
#[test]
fn zero_copy_gather_send_counts_bytes_across_chunks() {
    let (addr, server) = serve_read_to_end();

    let head = b"HTTP/1.1 200 OK\r\n\r\n".to_vec();
    let body = b"hello".to_vec();
    let expected = [&head[..], &body[..]].concat();
    let total = expected.len();

    main_thread(async move {
        let socket = connect_tcp(addr).await.unwrap();
        let (result, returned) = socket_send_vectored_zc(&socket, vec![head, body]).await;
        assert_eq!(result.unwrap(), total);
        assert_eq!(returned.len(), 2);
        drop(socket);
    });

    assert_eq!(server.join().unwrap(), expected);
}

// `&'static` chunks take the no-keepalive branch.
#[test]
fn zero_copy_gather_send_accepts_static_chunks() {
    let (addr, server) = serve_read_to_end();

    let chunks: Vec<&'static [u8]> = vec![b"one ", b"two ", b"three"];

    main_thread(async move {
        let socket = connect_tcp(addr).await.unwrap();
        let (result, _) = send_vectored_all_zc(&socket, chunks).await;
        result.unwrap();
        drop(socket);
    });

    assert_eq!(server.join().unwrap(), b"one two three");
}

// More chunks than IOV_MAX: the loop splits them across submissions.
#[test]
fn zero_copy_gather_send_splits_past_iov_max() {
    let (addr, server) = serve_read_to_end();

    let count = libc::UIO_MAXIOV as usize + 100;
    let chunks: Vec<Vec<u8>> = (0..count).map(|i| vec![(i % 251) as u8; 16]).collect();
    let expected: Vec<u8> = chunks.concat();

    main_thread(async move {
        let socket = connect_tcp(addr).await.unwrap();
        let (result, returned) = send_vectored_all_zc(&socket, chunks).await;
        result.unwrap();
        assert_eq!(returned.len(), count);
        drop(socket);
    });

    assert_eq!(server.join().unwrap(), expected);
}

// Same split for the copying loop; every chunk comes back even across short writes.
#[test]
fn gather_send_splits_past_iov_max() {
    let (addr, server) = serve_read_to_end();

    let count = libc::UIO_MAXIOV as usize + 100;
    let chunks: Vec<Vec<u8>> = (0..count).map(|i| vec![(i % 251) as u8; 4096]).collect();
    let expected: Vec<u8> = chunks.concat();

    main_thread(async move {
        let socket = connect_tcp(addr).await.unwrap();
        let (result, returned) = send_vectored_all(&socket, chunks).await;
        result.unwrap();
        assert_eq!(returned.len(), count);
        assert_eq!(returned.concat(), expected);
        drop(socket);
    });

    server.join().unwrap();
}

#[test]
fn zero_copy_gather_send_of_nothing_is_ok() {
    let (addr, server) = serve_read_to_end();

    main_thread(async move {
        let socket = connect_tcp(addr).await.unwrap();
        let (result, returned) = send_vectored_all_zc::<Vec<u8>>(&socket, Vec::new()).await;
        result.unwrap();
        assert!(returned.is_empty());
        drop(socket);
    });

    assert!(server.join().unwrap().is_empty());
}
