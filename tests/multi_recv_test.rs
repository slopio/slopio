use runtime::io_driver::operations::StreamExt;
use runtime::io_driver::operations::sockets::AsyncWrite;
use runtime::io_driver::operations::sockets::multi_accept::MultiAccept;
use runtime::io_driver::operations::sockets::multi_recv::MultiRecv;
use runtime::main_thread;
use runtime::runtime::spawn;
use std::io::Write;
use std::net::TcpStream;
use std::time::Duration;

#[test]
fn test_multi_recv_single_message() {
    let handle = std::thread::spawn(|| {
        main_thread(async {
            let mut ma = MultiAccept::new(9001).unwrap();
            if let Some(socket) = ma.next().await {
                let fd = socket.fd;
                std::mem::forget(socket); // Don't close the fd

                let mut mr = MultiRecv::new(fd);

                if let Some(read) = mr.next().await {
                    String::from_utf8_lossy(read.as_ref()).to_string()
                } else {
                    String::new()
                }
            } else {
                String::new()
            }
        })
    });

    std::thread::sleep(Duration::from_millis(100));

    let mut client = TcpStream::connect("127.0.0.1:9001").unwrap();
    client.write_all(b"Hello MultiRecv!").unwrap();

    let received = handle.join().unwrap();
    assert_eq!(received, "Hello MultiRecv!");
}

#[test]
fn test_multi_recv_multiple_messages() {
    let handle = std::thread::spawn(|| {
        main_thread(async {
            let mut ma = MultiAccept::new(9002).unwrap();

            if let Some(socket) = ma.next().await {
                let fd = socket.fd;
                std::mem::forget(socket);

                let mut mr = MultiRecv::new(fd);

                let mut messages = Vec::new();

                // Receive 3 messages
                for _ in 0..3 {
                    if let Some(read) = mr.next().await {
                        messages.push(String::from_utf8_lossy(read.as_ref()).to_string());
                    }
                }

                messages
            } else {
                Vec::new()
            }
        })
    });

    std::thread::sleep(Duration::from_millis(100));

    let mut client = TcpStream::connect("127.0.0.1:9002").unwrap();
    client.write_all(b"Message 1").unwrap();
    std::thread::sleep(Duration::from_millis(50));
    client.write_all(b"Message 2").unwrap();
    std::thread::sleep(Duration::from_millis(50));
    client.write_all(b"Message 3").unwrap();

    let messages = handle.join().unwrap();
    assert_eq!(messages.len(), 3);
    assert_eq!(messages[0], "Message 1");
    assert_eq!(messages[1], "Message 2");
    assert_eq!(messages[2], "Message 3");
}

#[test]
fn test_multi_recv_echo_loop() {
    let handle = std::thread::spawn(|| {
        main_thread(async {
            let mut ma = MultiAccept::new(9003).unwrap();

            if let Some(socket) = ma.next().await {
                let fd = socket.fd;
                let socket_for_send = runtime::io_driver::operations::sockets::SocketWrapper { fd };
                std::mem::forget(socket);

                let mut mr = MultiRecv::new(fd);

                let mut total_bytes = 0;

                // Echo 3 messages
                for _ in 0..3 {
                    if let Some(read) = mr.next().await {
                        let data = read.as_ref();
                        total_bytes += data.len();
                        socket_for_send.send(data).await.unwrap();
                    }
                }

                total_bytes
            } else {
                0
            }
        })
    });

    std::thread::sleep(Duration::from_millis(100));

    let mut client = TcpStream::connect("127.0.0.1:9003").unwrap();

    // Send and receive 3 messages
    client.write_all(b"Echo 1").unwrap();
    let mut buf = [0u8; 64];
    let n = std::io::Read::read(&mut client, &mut buf).unwrap();
    assert_eq!(&buf[..n], b"Echo 1");

    client.write_all(b"Echo 2").unwrap();
    let n = std::io::Read::read(&mut client, &mut buf).unwrap();
    assert_eq!(&buf[..n], b"Echo 2");

    client.write_all(b"Echo 3").unwrap();
    let n = std::io::Read::read(&mut client, &mut buf).unwrap();
    assert_eq!(&buf[..n], b"Echo 3");

    let total = handle.join().unwrap();
    assert_eq!(total, 18); // 6 + 6 + 6
}

#[test]
fn test_multi_recv_large_message() {
    let handle = std::thread::spawn(|| {
        main_thread(async {
            let mut ma = MultiAccept::new(9004).unwrap();

            if let Some(socket) = ma.next().await {
                let fd = socket.fd;
                std::mem::forget(socket);

                let mut mr = MultiRecv::new(fd);

                if let Some(read) = mr.next().await {
                    read.as_ref().len()
                } else {
                    0
                }
            } else {
                0
            }
        })
    });

    std::thread::sleep(Duration::from_millis(100));

    let mut client = TcpStream::connect("127.0.0.1:9004").unwrap();
    let large_data = vec![b'A'; 8192];
    client.write_all(&large_data).unwrap();

    let len = handle.join().unwrap();
    assert_eq!(len, 8192);
}

#[test]
fn test_multi_recv_spawned_task() {
    let handle = std::thread::spawn(|| {
        main_thread(async {
            let mut ma = MultiAccept::new(9005).unwrap();

            if let Some(socket) = ma.next().await {
                let fd = socket.fd;
                std::mem::forget(socket);

                // Spawn a task to handle the MultiRecv stream
                let task_handle = spawn(async move {
                    let mut mr = MultiRecv::new(fd);
                    let mut count = 0;

                    while let Some(read) = mr.next().await {
                        count += 1;
                        if read.as_ref() == b"STOP" {
                            break;
                        }
                    }

                    count
                });

                task_handle.await
            } else {
                0
            }
        })
    });

    std::thread::sleep(Duration::from_millis(100));

    let mut client = TcpStream::connect("127.0.0.1:9005").unwrap();
    client.write_all(b"Message 1").unwrap();
    std::thread::sleep(Duration::from_millis(50));
    client.write_all(b"Message 2").unwrap();
    std::thread::sleep(Duration::from_millis(50));
    client.write_all(b"STOP").unwrap();

    let count = handle.join().unwrap();
    assert_eq!(count, 3);
}

#[test]
fn test_multi_recv_binary_data() {
    let handle = std::thread::spawn(|| {
        main_thread(async {
            let mut ma = MultiAccept::new(9006).unwrap();

            if let Some(socket) = ma.next().await {
                let fd = socket.fd;
                std::mem::forget(socket);

                let mut mr = MultiRecv::new(fd);
                if let Some(read) = mr.next().await {
                    read.as_ref().to_vec()
                } else {
                    Vec::new()
                }
            } else {
                Vec::new()
            }
        })
    });

    std::thread::sleep(Duration::from_millis(100));

    let mut client = TcpStream::connect("127.0.0.1:9006").unwrap();
    let binary_data = vec![0x00, 0xFF, 0x42, 0xAA, 0x55, 0x01, 0x02, 0x03];
    client.write_all(&binary_data).unwrap();

    let received = handle.join().unwrap();
    assert_eq!(received, binary_data);
}

#[test]
fn test_multi_recv_empty_reads() {
    let handle = std::thread::spawn(|| {
        main_thread(async {
            let mut ma = MultiAccept::new(9007).unwrap();

            if let Some(socket) = ma.next().await {
                let fd = socket.fd;
                let mut mr = MultiRecv::new(fd);

                // Read one message
                if let Some(read) = mr.next().await {
                    let msg = String::from_utf8_lossy(read.as_ref()).to_string();

                    // Try to read again - connection will close
                    if let Some(read2) = mr.next().await {
                        (msg, read2.as_ref().len())
                    } else {
                        (msg, 0)
                    }
                } else {
                    println!("no read");
                    (String::new(), 0)
                }
            } else {
                (String::new(), 0)
            }
        })
    });

    std::thread::sleep(Duration::from_millis(100));

    let mut client = TcpStream::connect("127.0.0.1:9007").unwrap();
    client.write_all(b"Final message").unwrap();
    drop(client); // Close connection

    let (msg, len) = handle.join().unwrap();
    assert_eq!(msg, "Final message");
    assert_eq!(len, 0); // Connection closed
}

#[test]
fn test_multi_recv_concurrent_sockets() {
    let handle = std::thread::spawn(|| {
        main_thread(async {
            let mut ma = MultiAccept::new(9008).unwrap();
            let mut tasks = Vec::new();

            // Accept 2 connections and spawn handlers
            for _ in 0..2 {
                if let Some(socket) = ma.next().await {
                    let fd = socket.fd;
                    std::mem::forget(socket);

                    let task = spawn(async move {
                        let mut mr = MultiRecv::new(fd);
                        if let Some(read) = mr.next().await {
                            String::from_utf8_lossy(read.as_ref()).to_string()
                        } else {
                            String::new()
                        }
                    });

                    tasks.push(task);
                }
            }

            let mut results = Vec::new();
            for task in tasks {
                results.push(task.await);
            }

            results
        })
    });

    std::thread::sleep(Duration::from_millis(100));

    let mut client1 = TcpStream::connect("127.0.0.1:9008").unwrap();
    let mut client2 = TcpStream::connect("127.0.0.1:9008").unwrap();

    client1.write_all(b"Client 1").unwrap();
    client2.write_all(b"Client 2").unwrap();

    let results = handle.join().unwrap();
    assert_eq!(results.len(), 2);
    assert!(results.contains(&"Client 1".to_string()));
    assert!(results.contains(&"Client 2".to_string()));
}
