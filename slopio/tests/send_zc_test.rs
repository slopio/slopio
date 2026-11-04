use slopio::io_driver::operations::sockets::multi_accept::MultiAccept;
use slopio::io_driver::operations::sockets::recv::socket_recv;
use slopio::io_driver::operations::sockets::send_zc::send_all_zc;
use slopio::main_thread;
use slopio_streams::StreamExt;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

// Small payload: the kernel may copy-fallback (single CQE, no notification) or
// do a true zero-copy (completion + notification). Either way the future must
// resolve with the right byte count and the buffer handed back intact.
#[test]
fn test_socket_send_zc_data() {
    let handle = std::thread::spawn(|| {
        main_thread(async {
            let mut ma = MultiAccept::new(8101).unwrap();

            if let Some(Ok(socket)) = ma.next().await {
                let data = b"Response from server".to_vec();
                let (result, buf) = socket.send_zc(data).await;
                // Buffer is returned only after the notification — safe to read.
                assert_eq!(&buf, b"Response from server");
                result.unwrap()
            } else {
                0
            }
        })
    });

    std::thread::sleep(Duration::from_millis(100));

    let mut client = TcpStream::connect("127.0.0.1:8101").unwrap();
    let mut buffer = [0u8; 128];
    let n = client.read(&mut buffer).unwrap();

    let bytes_written = handle.join().unwrap();
    assert_eq!(bytes_written, 20);
    assert_eq!(&buffer[..n], b"Response from server");
}

// Large payload forces the genuine zero-copy path: the kernel pins the buffer,
// the completion CQE carries F_MORE, and the future parks in `Notif` until the
// notification lands. `send_all_zc` loops over short writes so we also drive
// many notification CQEs back-to-back.
#[test]
fn test_send_all_zc_large() {
    const LEN: usize = 4 * 1024 * 1024;

    let server = std::thread::spawn(|| {
        main_thread(async {
            let mut ma = MultiAccept::new(8102).unwrap();
            let socket = ma.next().await.unwrap().unwrap();
            // Byte i is `(i % 251) as u8` — a long non-repeating-mod pattern so
            // any reordering or truncation in the loop shows up downstream.
            let payload: Vec<u8> = (0..LEN).map(|i| (i % 251) as u8).collect();
            let (result, _buf) = send_all_zc(&socket, payload).await;
            result.unwrap();
            // socket dropped → client gets EOF
        })
    });

    std::thread::sleep(Duration::from_millis(100));

    let mut client = TcpStream::connect("127.0.0.1:8102").unwrap();
    let mut received = Vec::with_capacity(LEN);
    let mut buf = [0u8; 64 * 1024];
    loop {
        match client.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => received.extend_from_slice(&buf[..n]),
            Err(_) => break,
        }
    }

    server.join().unwrap();

    assert_eq!(received.len(), LEN, "total byte count mismatch");
    for (i, &byte) in received.iter().enumerate() {
        assert_eq!(byte, (i % 251) as u8, "byte mismatch at offset {}", i);
    }
}

// Zero-copy echo: receive, then send the data back with `send_zc`.
// Confirms the notification-gated completion interleaves correctly with a
// following recv on the same task.
#[test]
fn test_zc_echo_server() {
    let handle = std::thread::spawn(|| {
        main_thread(async {
            let mut ma = MultiAccept::new(8103).unwrap();

            if let Some(Ok(socket)) = ma.next().await {
                let buffer = Vec::<u8>::with_capacity(128);
                let (result, buffer) = socket_recv(&socket, buffer).await;
                let n_bytes = result.unwrap();

                socket.send_zc(buffer[..n_bytes].to_vec()).await.0.unwrap()
            } else {
                0
            }
        })
    });

    std::thread::sleep(Duration::from_millis(100));

    let mut client = TcpStream::connect("127.0.0.1:8103").unwrap();
    client.write_all(b"Echo this zerocopy!").unwrap();

    let mut buffer = [0u8; 128];
    let n = client.read(&mut buffer).unwrap();

    let bytes_sent = handle.join().unwrap();
    assert_eq!(bytes_sent, 19);
    assert_eq!(&buffer[..n], b"Echo this zerocopy!");
}

// Drop a zero-copy send before it can complete. The buffer must stay pinned in
// the driver's keepalive slot until the final (no-F_MORE) CQE arrives; this
// test mainly asserts the cancel path doesn't panic or corrupt the slab, and
// that a subsequent send on a fresh connection still works.
#[test]
fn test_zc_send_drop_cancel_safe() {
    let server = std::thread::spawn(|| {
        main_thread(async {
            let mut ma = MultiAccept::new(8104).unwrap();

            // First connection: start a large zero-copy send and drop it
            // immediately without awaiting to completion.
            {
                let socket = ma.next().await.unwrap().unwrap();
                let payload = vec![0xABu8; 2 * 1024 * 1024];
                let fut = socket.send_zc(payload);
                drop(fut); // cancel mid-flight; keepalive holds the buffer
                // socket dropped here too
            }

            // Second connection: a normal completed zero-copy send must still
            // work, proving the slab/keepalive machinery recovered.
            let socket = ma.next().await.unwrap().unwrap();
            socket.send_zc(b"after-cancel".to_vec()).await.0.unwrap()
        })
    });

    std::thread::sleep(Duration::from_millis(100));

    // First client just connects and closes.
    let c1 = TcpStream::connect("127.0.0.1:8104").unwrap();
    std::thread::sleep(Duration::from_millis(50));
    drop(c1);

    let mut c2 = TcpStream::connect("127.0.0.1:8104").unwrap();
    let mut buf = [0u8; 64];
    let n = c2.read(&mut buf).unwrap();

    let bytes = server.join().unwrap();
    assert_eq!(bytes, 12);
    assert_eq!(&buf[..n], b"after-cancel");
}
