use runtime::io_driver::operations::sockets::multi_accept::MultiAccept;
use runtime::io_driver::operations::sockets::recv::socket_recv_init as socket_recv;
use runtime::io_driver::operations::sockets::send::socket_send;
use runtime::main_thread;
use runtime::runtime::spawn;
use runtime::runtime::time::timeout;
use runtime_streams::StreamExt;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

#[test]
fn test_multi_accept_single_connection() {
    let handle = std::thread::spawn(|| {
        main_thread(async {
            let mut ma = MultiAccept::new(8081).unwrap();

            if let Some(_socket) = ma.next().await {
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

            if let Some(socket) = ma.next().await {
                let mut buffer = Box::new([0u8; 128]);
                let n_bytes = socket_recv(&socket, buffer.as_mut(), 128).await.unwrap();

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

            if let Some(socket) = ma.next().await {
                let data = b"Response from server";
                socket_send(&socket, data).await.unwrap()
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

            if let Some(socket) = ma.next().await {
                let mut buffer = Box::new([0u8; 128]);
                let n_bytes = socket_recv(&socket, buffer.as_mut(), 128).await.unwrap();

                // Echo back the data
                socket_send(&socket, &buffer[..n_bytes]).await.unwrap()
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

            if let Some(socket) = ma.next().await {
                let mut total_bytes = 0;

                // Receive first message
                let mut buffer1 = Box::new([0u8; 64]);
                let n1 = socket_recv(&socket, buffer1.as_mut(), 64).await.unwrap();
                total_bytes += n1;

                // Send response
                socket_send(&socket, b"ACK1").await.unwrap();

                // Receive second message
                let mut buffer2 = Box::new([0u8; 64]);
                let n2 = socket_recv(&socket, buffer2.as_mut(), 64).await.unwrap();
                total_bytes += n2;

                // Send final response
                socket_send(&socket, b"ACK2").await.unwrap();

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

            if let Some(socket) = ma.next().await {
                // Spawn a task to handle the socket
                let task_handle = spawn(async move {
                    let mut buffer = Box::new([0u8; 128]);
                    let n_bytes = socket_recv(&socket, buffer.as_mut(), 128).await.unwrap();
                    socket_send(&socket, &buffer[..n_bytes]).await.unwrap();
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

            if let Some(socket) = ma.next().await {
                let mut buffer = Box::new([0u8; 64]);

                // Receive some data first
                let n = socket_recv(&socket, buffer.as_mut(), 64).await.unwrap();

                // Client closes connection, should get 0 bytes
                let n_zero = socket_recv(&socket, buffer.as_mut(), 64).await.unwrap();

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
fn test_recv_timeout() {
    let handle = std::thread::spawn(|| {
        main_thread(async {
            let mut ma = MultiAccept::new(8088).unwrap();

            if let Some(socket) = ma.next().await {
                let mut buf = [0u8; 128];

                // First recv: client sends data quickly, should succeed
                let first: Result<std::io::Result<usize>, ()> =
                    timeout(Duration::from_secs(1), socket_recv(&socket, &mut buf, 128)).await;
                assert!(first.is_ok(), "first recv should not time out");
                let n = first.unwrap().unwrap();
                assert_eq!(&buf[..n], b"hello");

                // Signal client that we're about to start the timed recv
                socket_send(&socket, b"ok").await.unwrap();

                // Second recv: client doesn't send anything, should time out
                let second: Result<std::io::Result<usize>, ()> = timeout(
                    Duration::from_millis(150),
                    socket_recv(&socket, &mut buf, 128),
                )
                .await;
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
