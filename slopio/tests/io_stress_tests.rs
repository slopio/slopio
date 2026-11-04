use slopio::io_driver::operations::sockets::multi_accept::MultiAccept;
use slopio::io_driver::operations::sockets::recv::socket_recv;
use slopio::io_driver::operations::sockets::send::socket_send;
use slopio::main_thread;
use slopio::runtime::spawn;
use slopio_streams::StreamExt;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[test]
fn test_rapid_connections() {
    let handle = std::thread::spawn(|| {
        main_thread(async {
            let mut ma = MultiAccept::new(9000).unwrap();
            let mut count = 0;

            // Accept 50 connections
            for _ in 0..50 {
                if ma.next().await.is_some() {
                    println!("xd");
                    count += 1;
                }
            }
            println!("FINI");
            count
        })
    });

    std::thread::sleep(Duration::from_millis(100));

    // Create 50 connections rapidly
    let mut clients = Vec::new();
    for _ in 0..50 {
        if let Ok(client) = TcpStream::connect("127.0.0.1:9000") {
            println!("client");
            clients.push(client);
        }
    }

    let accepted = handle.join().unwrap();
    assert_eq!(accepted, 50);
}

// Test receiving data in very small chunks
#[test]
fn test_tiny_recv_chunks() {
    let handle = std::thread::spawn(|| {
        main_thread(async {
            let mut ma = MultiAccept::new(9001).unwrap();

            if let Some(Ok(socket)) = ma.next().await {
                let mut total_received = Vec::new();

                // Read 1 byte at a time, 20 times
                for _ in 0..20 {
                    let buffer = Vec::<u8>::with_capacity(1);
                    let (result, buffer) = socket_recv(&socket, buffer).await;
                    let n = result.unwrap();
                    if n > 0 {
                        total_received.push(buffer[0]);
                    }
                }

                String::from_utf8_lossy(&total_received).to_string()
            } else {
                String::new()
            }
        })
    });

    std::thread::sleep(Duration::from_millis(100));

    let mut client = TcpStream::connect("127.0.0.1:9001").unwrap();
    client.write_all(b"12345678901234567890").unwrap();

    let received = handle.join().unwrap();
    assert_eq!(received, "12345678901234567890");
}

// Test sending data in tiny chunks
#[test]
fn test_tiny_send_chunks() {
    let handle = std::thread::spawn(|| {
        main_thread(async {
            let mut ma = MultiAccept::new(9002).unwrap();

            if let Some(Ok(socket)) = ma.next().await {
                let data = b"ABCDEFGHIJ";
                let mut total_sent = 0;

                // Send 1 byte at a time
                for &byte in data.iter() {
                    let n = socket_send(&socket, vec![byte]).await.0.unwrap();
                    total_sent += n;
                }

                total_sent
            } else {
                0
            }
        })
    });

    std::thread::sleep(Duration::from_millis(100));

    let mut client = TcpStream::connect("127.0.0.1:9002").unwrap();
    let mut buffer = Vec::new();

    // Read all available data
    let mut chunk = [0u8; 20];
    loop {
        match client.read(&mut chunk) {
            Ok(0) => break, // Connection closed
            Ok(n) => buffer.extend_from_slice(&chunk[..n]),
            Err(_) => break,
        }
        if buffer.len() >= 10 {
            break;
        }
    }

    let total_sent = handle.join().unwrap();
    assert_eq!(total_sent, 10);
    assert_eq!(&buffer[..], b"ABCDEFGHIJ");
}

// Test multiple concurrent connections with spawned handlers
#[test]
fn test_concurrent_connections() {
    let handle = std::thread::spawn(|| {
        main_thread(async {
            let mut ma = MultiAccept::new(9003).unwrap();
            let mut handlers = Vec::new();

            // Accept 10 connections and spawn a handler for each
            for _ in 0..10 {
                if let Some(Ok(socket)) = ma.next().await {
                    let handler = spawn(async move {
                        let buffer = Vec::<u8>::with_capacity(64);
                        let (result, buffer) = socket_recv(&socket, buffer).await;
                        let n = result.unwrap();
                        socket_send(&socket, buffer[..n].to_vec()).await.0.unwrap();
                        n
                    });
                    handlers.push(handler);
                }
            }

            let mut total = 0;
            for handler in handlers {
                total += handler.await;
            }
            total
        })
    });

    std::thread::sleep(Duration::from_millis(200));

    // Create 10 clients that each send different data
    let mut client_handles = Vec::new();
    for i in 0..10 {
        let h = std::thread::spawn(move || {
            let mut client = TcpStream::connect("127.0.0.1:9003").unwrap();
            let msg = format!("Client{}", i);
            client.write_all(msg.as_bytes()).unwrap();

            let mut buffer = [0u8; 64];
            let n = client.read(&mut buffer).unwrap();
            String::from_utf8_lossy(&buffer[..n]).to_string()
        });
        client_handles.push(h);
    }

    // Wait for all clients
    for (i, h) in client_handles.into_iter().enumerate() {
        let response = h.join().unwrap();
        assert_eq!(response, format!("Client{}", i));
    }

    let total_bytes = handle.join().unwrap();
    // "Client0" = 7, "Client1" = 7, ..., "Client9" = 7
    assert_eq!(total_bytes, 70);
}

// Test recv with buffer larger than data
#[test]
fn test_large_buffer_small_data() {
    let handle = std::thread::spawn(|| {
        main_thread(async {
            let mut ma = MultiAccept::new(9004).unwrap();

            if let Some(Ok(socket)) = ma.next().await {
                let buffer = Vec::<u8>::with_capacity(4096);
                socket_recv(&socket, buffer).await.0.unwrap()
            } else {
                0
            }
        })
    });

    std::thread::sleep(Duration::from_millis(100));

    let mut client = TcpStream::connect("127.0.0.1:9004").unwrap();
    client.write_all(b"small").unwrap();

    let bytes_received = handle.join().unwrap();
    assert_eq!(bytes_received, 5);
}

// Test multiple recv/send cycles on same socket
#[test]
fn test_multiple_cycles() {
    let handle = std::thread::spawn(|| {
        main_thread(async {
            let mut ma = MultiAccept::new(9005).unwrap();

            if let Some(Ok(socket)) = ma.next().await {
                let mut total = 0;

                for _ in 0..10 {
                    let buffer = Vec::<u8>::with_capacity(64);
                    let (result, buffer) = socket_recv(&socket, buffer).await;
                    let n = result.unwrap();
                    socket_send(&socket, buffer[..n].to_vec()).await.0.unwrap();
                    total += n;
                }

                total
            } else {
                0
            }
        })
    });

    std::thread::sleep(Duration::from_millis(100));

    let mut client = TcpStream::connect("127.0.0.1:9005").unwrap();

    for i in 0..10 {
        let msg = format!("Msg{}", i);
        client.write_all(msg.as_bytes()).unwrap();

        let mut buffer = [0u8; 64];
        let n = client.read(&mut buffer).unwrap();
        assert_eq!(&buffer[..n], msg.as_bytes());
    }

    let total_bytes = handle.join().unwrap();
    // "Msg0" = 4, "Msg1" = 4, ..., "Msg9" = 4
    assert_eq!(total_bytes, 40);
}

// Test connection close during recv
#[test]
fn test_client_disconnect_during_recv() {
    let handle = std::thread::spawn(|| {
        main_thread(async {
            let mut ma = MultiAccept::new(9006).unwrap();

            if let Some(Ok(socket)) = ma.next().await {
                let buffer = Vec::<u8>::with_capacity(64);

                // First recv should succeed
                let (result, mut buffer) = socket_recv(&socket, buffer).await;
                let n1 = result.unwrap();

                // Second recv should get 0 (connection closed)
                buffer.clear();
                let (result, _buffer) = socket_recv(&socket, buffer).await;
                let n2 = result.unwrap();

                (n1, n2)
            } else {
                (0, 0)
            }
        })
    });

    std::thread::sleep(Duration::from_millis(100));

    let mut client = TcpStream::connect("127.0.0.1:9006").unwrap();
    client.write_all(b"Before close").unwrap();
    drop(client); // Close connection

    let (first, second) = handle.join().unwrap();
    assert_eq!(first, 12);
    assert_eq!(second, 0);
}

// Test spawning socket handlers that spawn more tasks
#[test]
fn test_nested_socket_spawns() {
    let handle = std::thread::spawn(|| {
        main_thread(async {
            let mut ma = MultiAccept::new(9007).unwrap();

            if let Some(Ok(socket)) = ma.next().await {
                let outer_task = spawn(async move {
                    let inner_task = spawn(async move {
                        let buffer = Vec::<u8>::with_capacity(64);
                        let (result, _buffer) = socket_recv(&socket, buffer).await;
                        let n = result.unwrap();
                        socket_send(&socket, b"PONG").await.0.unwrap();
                        n
                    });
                    inner_task.await
                });
                outer_task.await
            } else {
                0
            }
        })
    });

    std::thread::sleep(Duration::from_millis(100));

    let mut client = TcpStream::connect("127.0.0.1:9007").unwrap();
    client.write_all(b"PING").unwrap();

    let mut buffer = [0u8; 8];
    let n = client.read(&mut buffer).unwrap();

    let bytes_received = handle.join().unwrap();
    assert_eq!(bytes_received, 4);
    assert_eq!(&buffer[..n], b"PONG");
}

// Test many sequential accept/handle cycles
#[test]
fn test_sequential_accept_handle() {
    let handle = std::thread::spawn(|| {
        main_thread(async {
            let mut ma = MultiAccept::new(9008).unwrap();
            let mut count = 0;

            for _ in 0..20 {
                if let Some(Ok(socket)) = ma.next().await {
                    let buffer = Vec::<u8>::with_capacity(32);
                    socket_recv(&socket, buffer).await.0.unwrap();
                    socket_send(&socket, b"OK").await.0.unwrap();
                    count += 1;
                }
            }

            count
        })
    });

    std::thread::sleep(Duration::from_millis(100));

    // Connect, send, receive, close - 20 times
    for i in 0..20 {
        let mut client = TcpStream::connect("127.0.0.1:9008").unwrap();
        client
            .write_all(format!("Request{}", i).as_bytes())
            .unwrap();

        let mut buffer = [0u8; 8];
        let _ = client.read(&mut buffer).unwrap();
    }

    let processed = handle.join().unwrap();
    assert_eq!(processed, 20);
}

// Test interleaved operations across multiple sockets
#[test]
fn test_interleaved_socket_operations() {
    let handle = std::thread::spawn(|| {
        main_thread(async {
            let mut ma = MultiAccept::new(9009).unwrap();

            // Accept 3 sockets and spawn handlers for each
            let mut handlers = Vec::new();

            for i in 0..3 {
                if let Some(Ok(socket)) = ma.next().await {
                    let handler = spawn(async move {
                        // Receive data
                        let buffer = Vec::<u8>::with_capacity(32);
                        let (result, buffer) = socket_recv(&socket, buffer).await;
                        let n = result.unwrap();

                        // Echo it back
                        socket_send(&socket, buffer[..n].to_vec()).await.0.unwrap();
                        (i, n)
                    });
                    handlers.push(handler);
                }
            }

            let mut total = 0;
            for handler in handlers {
                let (_, n) = handler.await;
                total += n;
            }
            total
        })
    });

    std::thread::sleep(Duration::from_millis(200));

    let h1 = std::thread::spawn(|| {
        let mut c = TcpStream::connect("127.0.0.1:9009").unwrap();
        c.write_all(b"C1").unwrap();
        let mut buf = [0u8; 8];
        let n = c.read(&mut buf).unwrap();
        (buf, n)
    });

    let h2 = std::thread::spawn(|| {
        let mut c = TcpStream::connect("127.0.0.1:9009").unwrap();
        c.write_all(b"C2").unwrap();
        let mut buf = [0u8; 8];
        let n = c.read(&mut buf).unwrap();
        (buf, n)
    });

    let h3 = std::thread::spawn(|| {
        let mut c = TcpStream::connect("127.0.0.1:9009").unwrap();
        c.write_all(b"C3").unwrap();
        let mut buf = [0u8; 8];
        let n = c.read(&mut buf).unwrap();
        (buf, n)
    });

    let (buf1, n1) = h1.join().unwrap();
    let (buf2, n2) = h2.join().unwrap();
    let (buf3, n3) = h3.join().unwrap();

    // Each client should get back what it sent
    assert_eq!(&buf1[..n1], b"C1");
    assert_eq!(&buf2[..n2], b"C2");
    assert_eq!(&buf3[..n3], b"C3");

    let total = handle.join().unwrap();
    assert_eq!(total, 6); // 2 + 2 + 2
}

// Test recv on freshly accepted socket (data may already be in kernel buffer)
#[test]
fn test_immediate_recv_after_accept() {
    let handle = std::thread::spawn(|| {
        main_thread(async {
            let mut ma = MultiAccept::new(9010).unwrap();

            if let Some(Ok(socket)) = ma.next().await {
                // Immediately try to recv - client may have already sent data
                let buffer = Vec::<u8>::with_capacity(64);
                let (result, buffer) = socket_recv(&socket, buffer).await;
                let n = result.unwrap();
                String::from_utf8_lossy(&buffer[..n]).to_string()
            } else {
                String::new()
            }
        })
    });

    std::thread::sleep(Duration::from_millis(100));

    // Send data immediately on connection
    let mut client = TcpStream::connect("127.0.0.1:9010").unwrap();
    client.write_all(b"Immediate data").unwrap();

    let received = handle.join().unwrap();
    assert_eq!(received, "Immediate data");
}

// Test empty send (edge case)
#[test]
fn test_empty_send() {
    let handle = std::thread::spawn(|| {
        main_thread(async {
            let mut ma = MultiAccept::new(9011).unwrap();

            if let Some(Ok(socket)) = ma.next().await {
                socket_send(&socket, b"").await.0.unwrap()
            } else {
                999
            }
        })
    });

    std::thread::sleep(Duration::from_millis(100));

    let _client = TcpStream::connect("127.0.0.1:9011").unwrap();

    let bytes_sent = handle.join().unwrap();
    assert_eq!(bytes_sent, 0);
}

// Test mixed spawned and direct socket handling
#[test]
fn test_mixed_socket_handling() {
    let handle = std::thread::spawn(|| {
        main_thread(async {
            let mut ma = MultiAccept::new(9012).unwrap();
            let results = Arc::new(Mutex::new(Vec::new()));

            // First socket - handle directly
            if let Some(Ok(socket)) = ma.next().await {
                let buffer = Vec::<u8>::with_capacity(32);
                socket_recv(&socket, buffer).await.0.unwrap();
                results.lock().unwrap().push(1);
            }

            // Second socket - spawn task
            if let Some(Ok(socket)) = ma.next().await {
                let r = results.clone();
                let task = spawn(async move {
                    let buffer = Vec::<u8>::with_capacity(32);
                    socket_recv(&socket, buffer).await.0.unwrap();
                    r.lock().unwrap().push(2);
                });
                task.await;
            }

            // Third socket - handle directly again
            if let Some(Ok(socket)) = ma.next().await {
                let buffer = Vec::<u8>::with_capacity(32);
                socket_recv(&socket, buffer).await.0.unwrap();
                results.lock().unwrap().push(3);
            }

            results.lock().unwrap().clone()
        })
    });

    std::thread::sleep(Duration::from_millis(100));

    for _ in 0..3 {
        let mut client = TcpStream::connect("127.0.0.1:9012").unwrap();
        client.write_all(b"Data").unwrap();
        std::thread::sleep(Duration::from_millis(50));
    }

    let results = handle.join().unwrap();
    assert_eq!(results, vec![1, 2, 3]);
}
