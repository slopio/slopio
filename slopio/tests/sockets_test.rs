use slopio::io_driver::operations::sockets::connect::connect_tcp;
use slopio::io_driver::operations::sockets::managed_recv::socket_managed_recv;
use slopio::io_driver::operations::sockets::multi_accept::MultiAccept;
use slopio::io_driver::operations::sockets::recv::{socket_peek, socket_recv};
use slopio::io_driver::operations::sockets::send::socket_send;
use slopio::io_driver::operations::sockets::send_vectored::send_vectored_all;
use slopio::main_thread;
use slopio::runtime::spawn;
use slopio::runtime::time::timeout;
use slopio_streams::StreamExt;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

#[test]
fn test_multi_accept_single_connection() {
    let handle = std::thread::spawn(|| {
        main_thread(async {
            let mut ma = MultiAccept::new(8081).unwrap();

            if let Some(Ok(_socket)) = ma.next().await {
                // Successfully accepted a connection
                true
            } else {
                false
            }
        })
    });

    // Give the server time to start
    std::thread::sleep(Duration::from_millis(100));

    // Connect a client
    let _client = TcpStream::connect("127.0.0.1:8081").unwrap();

    let result = handle.join().unwrap();
    assert!(result);
}

#[test]
fn test_socket_recv_data() {
    let handle = std::thread::spawn(|| {
        main_thread(async {
            let mut ma = MultiAccept::new(8082).unwrap();

            if let Some(Ok(socket)) = ma.next().await {
                let buffer = Vec::<u8>::with_capacity(128);
                let (result, buffer) = socket_recv(&socket, buffer).await;
                let n_bytes = result.unwrap();

                // Return the received data
                String::from_utf8_lossy(&buffer[..n_bytes]).to_string()
            } else {
                String::new()
            }
        })
    });

    // Give the server time to start
    std::thread::sleep(Duration::from_millis(100));

    // Connect and send data
    let mut client = TcpStream::connect("127.0.0.1:8082").unwrap();
    client.write_all(b"Hello, Server!").unwrap();

    let received = handle.join().unwrap();
    assert_eq!(received, "Hello, Server!");
}

#[test]
fn test_socket_send_data() {
    let handle = std::thread::spawn(|| {
        main_thread(async {
            let mut ma = MultiAccept::new(8083).unwrap();

            if let Some(Ok(socket)) = ma.next().await {
                let data = b"Response from server";
                socket_send(&socket, data).await.0.unwrap()
            } else {
                0
            }
        })
    });

    // Give the server time to start
    std::thread::sleep(Duration::from_millis(100));

    // Connect and receive data
    let mut client = TcpStream::connect("127.0.0.1:8083").unwrap();
    let mut buffer = [0u8; 128];
    let n = client.read(&mut buffer).unwrap();

    let bytes_written = handle.join().unwrap();
    assert_eq!(bytes_written, 20);
    assert_eq!(&buffer[..n], b"Response from server");
}

#[test]
fn test_echo_server() {
    let handle = std::thread::spawn(|| {
        main_thread(async {
            let mut ma = MultiAccept::new(8084).unwrap();

            if let Some(Ok(socket)) = ma.next().await {
                let buffer = Vec::<u8>::with_capacity(128);
                let (result, buffer) = socket_recv(&socket, buffer).await;
                let n_bytes = result.unwrap();

                // Echo back the data
                socket_send(&socket, buffer[..n_bytes].to_vec())
                    .await
                    .0
                    .unwrap()
            } else {
                0
            }
        })
    });

    // Give the server time to start
    std::thread::sleep(Duration::from_millis(100));

    // Connect, send data, and receive echo
    let mut client = TcpStream::connect("127.0.0.1:8084").unwrap();
    client.write_all(b"Echo this!").unwrap();

    let mut buffer = [0u8; 128];
    let n = client.read(&mut buffer).unwrap();

    let bytes_sent = handle.join().unwrap();
    assert_eq!(bytes_sent, 10);
    assert_eq!(&buffer[..n], b"Echo this!");
}

#[test]
fn test_multiple_recv_send() {
    let handle = std::thread::spawn(|| {
        main_thread(async {
            let mut ma = MultiAccept::new(8085).unwrap();

            if let Some(Ok(socket)) = ma.next().await {
                let mut total_bytes = 0;

                // Receive first message
                let buffer1 = Vec::<u8>::with_capacity(64);
                let (result, _buffer1) = socket_recv(&socket, buffer1).await;
                let n1 = result.unwrap();
                total_bytes += n1;

                // Send response
                socket_send(&socket, b"ACK1").await.0.unwrap();

                // Receive second message
                let buffer2 = Vec::<u8>::with_capacity(64);
                let (result, _buffer2) = socket_recv(&socket, buffer2).await;
                let n2 = result.unwrap();
                total_bytes += n2;

                // Send final response
                socket_send(&socket, b"ACK2").await.0.unwrap();

                total_bytes
            } else {
                0
            }
        })
    });

    // Give the server time to start
    std::thread::sleep(Duration::from_millis(100));

    let mut client = TcpStream::connect("127.0.0.1:8085").unwrap();

    // Send first message
    client.write_all(b"Message 1").unwrap();

    // Read ACK1
    let mut ack = [0u8; 4];
    client.read_exact(&mut ack).unwrap();
    assert_eq!(&ack, b"ACK1");

    // Send second message
    client.write_all(b"Message 2").unwrap();

    // Read ACK2
    client.read_exact(&mut ack).unwrap();
    assert_eq!(&ack, b"ACK2");

    let total = handle.join().unwrap();
    assert_eq!(total, 18);
}

#[test]
fn test_spawned_socket_handler() {
    let handle = std::thread::spawn(|| {
        main_thread(async {
            let mut ma = MultiAccept::new(8086).unwrap();

            if let Some(Ok(socket)) = ma.next().await {
                // Spawn a task to handle the socket
                let task_handle = spawn(async move {
                    let buffer = Vec::<u8>::with_capacity(128);
                    let (result, buffer) = socket_recv(&socket, buffer).await;
                    let n_bytes = result.unwrap();
                    socket_send(&socket, buffer[..n_bytes].to_vec())
                        .await
                        .0
                        .unwrap();
                    n_bytes
                });

                task_handle.await
            } else {
                0
            }
        })
    });

    // Give the server time to start
    std::thread::sleep(Duration::from_millis(100));

    let mut client = TcpStream::connect("127.0.0.1:8086").unwrap();
    client.write_all(b"Spawned task test").unwrap();

    let mut buffer = [0u8; 128];
    let n = client.read(&mut buffer).unwrap();

    let bytes_echoed = handle.join().unwrap();
    assert_eq!(bytes_echoed, 17);
    assert_eq!(&buffer[..n], b"Spawned task test");
}

#[test]
fn test_zero_length_recv() {
    let handle = std::thread::spawn(|| {
        main_thread(async {
            let mut ma = MultiAccept::new(8087).unwrap();

            if let Some(Ok(socket)) = ma.next().await {
                let buffer = Vec::<u8>::with_capacity(64);

                // Receive some data first
                let (result, mut buffer) = socket_recv(&socket, buffer).await;
                let n = result.unwrap();

                // Client closes connection, should get 0 bytes
                buffer.clear();
                let (result, _buffer) = socket_recv(&socket, buffer).await;
                let n_zero = result.unwrap();

                (n, n_zero)
            } else {
                (0, 0)
            }
        })
    });

    // Give the server time to start
    std::thread::sleep(Duration::from_millis(100));

    let mut client = TcpStream::connect("127.0.0.1:8087").unwrap();
    client.write_all(b"Data").unwrap();
    drop(client); // Close the connection

    let (bytes_recv, zero_recv) = handle.join().unwrap();
    assert_eq!(bytes_recv, 4);
    assert_eq!(zero_recv, 0);
}

#[test]
fn test_socket_peek_leaves_data_in_queue() {
    let handle = std::thread::spawn(|| {
        main_thread(async {
            let mut ma = MultiAccept::new(8092).unwrap();

            if let Some(Ok(socket)) = ma.next().await {
                // Peek twice: both see the bytes, neither consumes them.
                let (result, buffer) = socket_peek(&socket, Vec::<u8>::with_capacity(64)).await;
                let peeked1 = buffer[..result.unwrap()].to_vec();

                let (result, buffer) = socket_peek(&socket, Vec::<u8>::with_capacity(64)).await;
                let peeked2 = buffer[..result.unwrap()].to_vec();

                // A real recv still finds them, and then the peer's close.
                let (result, buffer) = socket_recv(&socket, Vec::<u8>::with_capacity(64)).await;
                let received = buffer[..result.unwrap()].to_vec();

                let (result, _buffer) = socket_recv(&socket, Vec::<u8>::with_capacity(64)).await;
                let eof = result.unwrap();

                (peeked1, peeked2, received, eof)
            } else {
                (Vec::new(), Vec::new(), Vec::new(), usize::MAX)
            }
        })
    });

    std::thread::sleep(Duration::from_millis(100));

    let mut client = TcpStream::connect("127.0.0.1:8092").unwrap();
    client.write_all(b"peekaboo").unwrap();
    // Let the bytes land before the server's first peek is answered.
    std::thread::sleep(Duration::from_millis(100));
    drop(client);

    let (peeked1, peeked2, received, eof) = handle.join().unwrap();
    assert_eq!(peeked1, b"peekaboo");
    assert_eq!(peeked2, b"peekaboo");
    assert_eq!(received, b"peekaboo");
    assert_eq!(eof, 0);
}

#[test]
fn test_recv_timeout() {
    let handle = std::thread::spawn(|| {
        main_thread(async {
            let mut ma = MultiAccept::new(8088).unwrap();

            if let Some(Ok(socket)) = ma.next().await {
                let buf: Vec<u8> = Vec::with_capacity(128);

                // First recv: client sends data quickly, should succeed
                let first: Result<(std::io::Result<usize>, Vec<u8>), ()> =
                    timeout(Duration::from_secs(1), socket_recv(&socket, buf)).await;
                assert!(first.is_ok(), "first recv should not time out");
                let (result, buf) = first.unwrap();
                let n = result.unwrap();
                assert_eq!(&buf[..n], b"hello");

                // Signal client that we're about to start the timed recv
                socket_send(&socket, b"ok").await.0.unwrap();

                // Second recv: client doesn't send anything, should time out
                let buf2: Vec<u8> = Vec::with_capacity(128);
                let second: Result<(std::io::Result<usize>, Vec<u8>), ()> =
                    timeout(Duration::from_millis(150), socket_recv(&socket, buf2)).await;
                assert!(second.is_err(), "second recv should time out");

                true
            } else {
                false
            }
        })
    });

    std::thread::sleep(Duration::from_millis(100));

    let mut client = TcpStream::connect("127.0.0.1:8088").unwrap();
    client.write_all(b"hello").unwrap();

    // Wait for server ACK — server is now entering the timed recv
    let mut ack = [0u8; 2];
    client.read_exact(&mut ack).unwrap();
    assert_eq!(&ack, b"ok");

    // Measure from outside: how long until the server's timeout fires
    let before = std::time::Instant::now();
    let result = handle.join().unwrap();
    let elapsed = before.elapsed();

    assert!(result);
    assert!(
        elapsed >= Duration::from_millis(150),
        "timed out too early: {:?}",
        elapsed
    );
    assert!(
        elapsed < Duration::from_millis(400),
        "timeout took too long: {:?}",
        elapsed
    );
}

#[test]
fn test_vectored_write() {
    let handle = std::thread::spawn(|| {
        main_thread(async {
            let mut ma = MultiAccept::new(8089).unwrap();

            if let Some(Ok(socket)) = ma.next().await {
                let bufs: Vec<&'static [u8]> = vec![
                    b"Hello".as_slice(),
                    b", ".as_slice(),
                    b"vectored".as_slice(),
                    b" world!".as_slice(),
                ];
                socket.vectored_write(bufs).await.0.unwrap()
            } else {
                0
            }
        })
    });

    std::thread::sleep(Duration::from_millis(100));

    let mut client = TcpStream::connect("127.0.0.1:8089").unwrap();
    let mut buffer = [0u8; 128];
    let n = client.read(&mut buffer).unwrap();

    let bytes_written = handle.join().unwrap();
    assert_eq!(bytes_written, 22);
    assert_eq!(&buffer[..n], b"Hello, vectored world!");
}

#[test]
fn test_vectored_write_echo() {
    let handle = std::thread::spawn(|| {
        main_thread(async {
            let mut ma = MultiAccept::new(8090).unwrap();

            if let Some(Ok(socket)) = ma.next().await {
                // Receive data first
                let buf1 = Vec::<u8>::with_capacity(64);
                let (result, buf1) = socket_recv(&socket, buf1).await;
                let n1 = result.unwrap();

                let buf2 = Vec::<u8>::with_capacity(64);
                let (result, buf2) = socket_recv(&socket, buf2).await;
                let n2 = result.unwrap();

                // Echo both chunks back in a single vectored write
                let bufs: Vec<Vec<u8>> = vec![buf1[..n1].to_vec(), buf2[..n2].to_vec()];
                socket.vectored_write(bufs).await.0.unwrap()
            } else {
                0
            }
        })
    });

    std::thread::sleep(Duration::from_millis(100));

    let mut client = TcpStream::connect("127.0.0.1:8090").unwrap();

    // Send two separate messages
    client.write_all(b"part one").unwrap();
    std::thread::sleep(Duration::from_millis(50));
    client.write_all(b"part two").unwrap();

    // Read the combined echo
    let mut buffer = [0u8; 128];
    let n = client.read(&mut buffer).unwrap();

    let bytes_written = handle.join().unwrap();
    assert_eq!(bytes_written, 16);
    assert_eq!(&buffer[..n], b"part onepart two");
}

// Force `send_vectored_all` through its partial-write loop by shrinking both
// the server send buffer and the client receive buffer, and having the client
// read slowly so the kernel-side TCP buffers stay near-full. The byte pattern
// check catches any off-by-one in chunk advancement.
#[test]
fn test_vectored_write_partial() {
    use socket2::SockRef;

    const CHUNK_COUNT: usize = 32;
    const CHUNK_SIZE: usize = 4 * 1024;
    const TOTAL: usize = CHUNK_COUNT * CHUNK_SIZE;

    let server = std::thread::spawn(|| {
        main_thread(async {
            let mut ma = MultiAccept::new(8094).unwrap();
            let socket = ma.next().await.unwrap().unwrap();
            // Shrink the send buffer to force partial writes. Linux doubles
            // the requested value internally, so the effective size will be
            // around 8 KB — far smaller than the 1 MB payload.
            socket.set_send_buffer_size(4096).ok();

            // Chunk i is all `(i % 256) as u8`. If chunk-advance is wrong,
            // bytes will shift and the per-chunk pattern assertion will fail.
            let bufs: Vec<Vec<u8>> = (0..CHUNK_COUNT)
                .map(|i| vec![(i % 256) as u8; CHUNK_SIZE])
                .collect();

            let (result, _bufs) = send_vectored_all(&socket, bufs).await;
            result.unwrap();
            // Socket dropped here → close → client gets EOF.
        })
    });

    std::thread::sleep(Duration::from_millis(100));

    let mut client = TcpStream::connect("127.0.0.1:8094").unwrap();
    // Shrink the client's recv buffer too so the kernel can't absorb the
    // whole payload at once.
    SockRef::from(&client).set_recv_buffer_size(4096).ok();

    let mut received = Vec::with_capacity(TOTAL);
    let mut buf = [0u8; 4096];
    loop {
        match client.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                received.extend_from_slice(&buf[..n]);
                // Read slowly so the server-side send buffer fills and the
                // kernel forces partial writes on each writev op.
                std::thread::sleep(Duration::from_micros(500));
            }
            Err(_) => break,
        }
    }

    server.join().unwrap();

    assert_eq!(received.len(), TOTAL, "total byte count mismatch");

    // Verify byte pattern per chunk — catches off-by-one in chunk advance.
    for i in 0..CHUNK_COUNT {
        let chunk_start = i * CHUNK_SIZE;
        let expected = (i % 256) as u8;
        for j in 0..CHUNK_SIZE {
            assert_eq!(
                received[chunk_start + j],
                expected,
                "byte mismatch at chunk {}, offset {}",
                i,
                j
            );
        }
    }
}

#[test]
fn test_local_and_peer_addr() {
    let server = std::thread::spawn(|| {
        main_thread(async {
            let mut ma = MultiAccept::new(8091).unwrap();
            let accepted = ma.next().await.unwrap().unwrap();
            // On the accepted socket, peer_addr is the client; local_addr is
            // the listener's bound endpoint.
            let local = accepted.local_addr().unwrap();
            let peer = accepted.peer_addr().unwrap();
            (local, peer)
        })
    });

    std::thread::sleep(Duration::from_millis(100));

    let client = std::thread::spawn(|| {
        main_thread(async {
            let addr: SocketAddr = "127.0.0.1:8091".parse().unwrap();
            let socket = connect_tcp(addr).await.unwrap();
            let local = socket.local_addr().unwrap();
            let peer = socket.peer_addr().unwrap();
            (local, peer)
        })
    });

    let (server_local, server_peer) = server.join().unwrap();
    let (client_local, client_peer) = client.join().unwrap();

    assert_eq!(server_local.port(), 8091);
    assert_eq!(client_peer.port(), 8091);
    // The accepted socket's peer is the client; the client's local is itself.
    assert_eq!(server_peer, client_local);
    assert_eq!(client_peer, server_local);
}

slopio::define_buf_ring!(pub EofRing, bgid = 0, buffer_size = 4096, ring_size = 8);

#[test]
fn a_managed_recv_reports_eof_as_none() {
    let handle = std::thread::spawn(|| {
        slopio::main_thread_with::<EofRing, _>(async {
            let mut ma = MultiAccept::new(9199).unwrap();
            let socket = ma.next().await.unwrap().unwrap();

            let data = socket_managed_recv::<EofRing>(&socket).await.unwrap();
            let data = data.expect("data before EOF").as_ref().to_vec();
            let eof = socket_managed_recv::<EofRing>(&socket).await.unwrap();
            (data, eof.is_none())
        })
    });

    std::thread::sleep(Duration::from_millis(100));
    let mut client = TcpStream::connect("127.0.0.1:9199").unwrap();
    client.write_all(b"Data").unwrap();
    drop(client);

    let (data, eof) = handle.join().unwrap();
    assert_eq!(data, b"Data");
    assert!(eof, "EOF should be Ok(None), not an error");
}
