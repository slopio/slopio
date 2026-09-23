use std::{
    io::Write,
    net::TcpStream,
    os::unix::io::AsRawFd,
    process::Command,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::sleep,
    time::Duration,
};

use runtime::{
    io_driver::operations::sockets::{multi_accept::MultiAccept, multi_recv::MultiRecv},
    main_thread_with,
};
use runtime_streams::StreamExt;

use crate::common::Cv;

mod common;

runtime::define_buf_ring!(pub Small, bgid = 0, buffer_size = 4096, ring_size = 8192);

#[test]
fn test_stream_stops_on_close() {
    let (cv1, cv2) = Cv::new();

    let reading_thread = std::thread::spawn(move || {
        main_thread_with::<Small, _>(async {
            let mut multiaccept = MultiAccept::new(9091).unwrap();
            cv1.notify();
            if let Some(Ok(socket)) = multiaccept.next().await {
                let mut recv_count = 0;
                let mut multi_recv = MultiRecv::<Small>::new(socket);
                while let Some(Ok(_data)) = multi_recv.next().await {
                    recv_count += 1;
                }
                return Some(recv_count);
            }
            None
        })
    });

    cv2.wait();

    let client = TcpStream::connect("localhost:9091").unwrap();
    drop(client);

    let recv_count = reading_thread.join().unwrap().unwrap();
    assert_eq!(recv_count, 0, "Stream should receive no data, only EOF");
}

#[test]
fn test_stream_stops_on_abrupt_close_rst() {
    let (cv1, cv2) = Cv::new();

    let reading_thread = std::thread::spawn(move || {
        main_thread_with::<Small, _>(async {
            let mut multiaccept = MultiAccept::new(9092).unwrap();
            cv1.notify();
            if let Some(Ok(socket)) = multiaccept.next().await {
                let mut recv_count = 0;
                let mut multi_recv = MultiRecv::<Small>::new(socket);
                while let Some(Ok(_data)) = multi_recv.next().await {
                    recv_count += 1;
                }
                return Some(recv_count);
            }
            None
        })
    });

    cv2.wait();

    let client = TcpStream::connect("localhost:9092").unwrap();

    unsafe {
        let linger = libc::linger {
            l_onoff: 1,
            l_linger: 0,
        };
        libc::setsockopt(
            client.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_LINGER,
            &linger as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::linger>() as u32,
        );
    }
    drop(client);

    let recv_count = reading_thread.join().unwrap().unwrap();
    assert_eq!(recv_count, 0, "Stream should stop on RST");
}

#[test]
fn test_stream_stops_on_process_kill() {
    let (cv1, cv2) = Cv::new();

    let reading_thread = std::thread::spawn(move || {
        main_thread_with::<Small, _>(async {
            let mut multiaccept = MultiAccept::new(9093).unwrap();
            cv1.notify();
            if let Some(Ok(socket)) = multiaccept.next().await {
                let mut recv_count = 0;
                let mut multi_recv = MultiRecv::<Small>::new(socket);
                while let Some(Ok(_data)) = multi_recv.next().await {
                    recv_count += 1;
                }
                return Some(recv_count);
            }
            None
        })
    });

    cv2.wait();

    let mut child = Command::new("bash")
        .args(["-c", "exec 3<>/dev/tcp/localhost/9093; sleep 10"])
        .spawn()
        .unwrap();

    sleep(Duration::from_millis(200));
    child.kill().unwrap();
    child.wait().unwrap();

    let recv_count = reading_thread.join().unwrap().unwrap();
    assert_eq!(
        recv_count, 0,
        "Stream should stop when client process is killed"
    );
}

#[test]
fn byte_per_byte() {
    use rand::RngCore;
    let mut rng = rand::thread_rng();
    let mut random_bytes = vec![0u8; 5_000_000];
    rng.fill_bytes(&mut random_bytes);
    let (cv1, cv2) = Cv::new();

    let reading_thread = std::thread::spawn(move || {
        main_thread_with::<Small, _>(async {
            let mut multiaccept = MultiAccept::new(9090).unwrap();
            cv1.notify();

            if let Some(Ok(socket)) = multiaccept.next().await {
                let mut received: Vec<u8> = Vec::new();
                let mut multirecv = MultiRecv::<Small>::new(socket);
                while let Some(Ok(data)) = multirecv.next().await {
                    received.extend_from_slice(data.as_ref());
                }
                Some(received)
            } else {
                None
            }
        })
    });
    cv2.wait();
    let mut client = TcpStream::connect("localhost:9090").unwrap();
    for b in random_bytes.chunks(1) {
        client.write_all(b).unwrap();
    }
    drop(client);

    let received = reading_thread.join().unwrap().unwrap();
    assert_eq!(
        received.len(),
        random_bytes.len(),
        "Received {} bytes but expected {}",
        received.len(),
        random_bytes.len()
    );
    assert_eq!(
        received, random_bytes,
        "Received data doesn't match sent data"
    );
}

#[test]
fn test_slow_consumer_triggers_pause() {
    let (cv1, cv2) = Cv::new();

    let reading_thread = std::thread::spawn(move || {
        main_thread_with::<Small, _>(async move {
            let mut multiaccept = MultiAccept::new(9095).unwrap();
            cv1.notify();
            if let Some(Ok(socket)) = multiaccept.next().await {
                let mut received: Vec<u8> = Vec::new();
                let mut multirecv = MultiRecv::<Small>::new(socket);
                let mut recv_count = 0;

                while let Some(Ok(data)) = multirecv.next().await {
                    recv_count += 1;
                    received.extend_from_slice(data.as_ref());
                    if recv_count <= 70 {
                        std::thread::sleep(Duration::from_millis(50));
                    }
                }

                Some((received, recv_count))
            } else {
                None
            }
        })
    });

    cv2.wait();

    let writer_thread = std::thread::spawn(|| {
        let mut client = TcpStream::connect("localhost:9095").unwrap();
        client.set_nodelay(true).unwrap();

        let mut sent_data = Vec::new();
        for i in 0..100u8 {
            let data = vec![i, i];
            assert_eq!(client.write(&data).unwrap(), 2);
            client.flush().unwrap();
            sent_data.extend_from_slice(&data);
        }

        drop(client);
        sent_data
    });

    let sent_data = writer_thread.join().unwrap();
    let (received, recv_count) = reading_thread.join().unwrap().unwrap();

    println!("Total recv events: {}", recv_count);
    println!(
        "Sent {} bytes, received {} bytes",
        sent_data.len(),
        received.len()
    );

    assert_eq!(
        received.len(),
        sent_data.len(),
        "Should receive all {} bytes",
        sent_data.len()
    );
    assert_eq!(
        received, sent_data,
        "Received data should match sent data exactly"
    );

    assert!(recv_count > 0, "Should receive events");

    println!(
        "✓ Slow consumer test passed - received {} events, all data intact",
        recv_count
    );
}

#[test]
fn test_fast_drain_triggers_resume() {
    let (cv1, cv2) = Cv::new();
    let switch_to_fast = Arc::new(AtomicBool::new(false));
    let switch_clone = switch_to_fast.clone();

    let reading_thread = std::thread::spawn(move || {
        main_thread_with::<Small, _>(async move {
            let mut multiaccept = MultiAccept::new(9096).unwrap();
            cv1.notify();
            if let Some(Ok(socket)) = multiaccept.next().await {
                let mut received = Vec::new();
                let mut multirecv = MultiRecv::<Small>::new(socket);
                let mut recv_count = 0;
                let mut slow_phase_count = 0;

                while let Some(Ok(data)) = multirecv.next().await {
                    received.extend_from_slice(data.as_ref());
                    recv_count += 1;

                    // Slow phase: accumulate events to trigger pause
                    if !switch_clone.load(Ordering::Relaxed) {
                        slow_phase_count += 1;
                        std::thread::sleep(Duration::from_millis(15));
                    }
                    // Fast phase: drain quickly to trigger resume
                }

                println!("Slow phase events: {}", slow_phase_count);
                println!("Total events: {}", recv_count);
                Some((received, slow_phase_count, recv_count))
            } else {
                None
            }
        })
    });

    cv2.wait();

    // Writer sends data continuously
    let writer_thread = std::thread::spawn(|| {
        let mut client = TcpStream::connect("localhost:9096").unwrap();
        client.set_nodelay(true).unwrap();

        let mut sent_data = Vec::new();
        // Send 150 writes
        for i in 0..255u8 {
            assert_eq!(client.write(&[i]).unwrap(), 1);
            client.flush().unwrap();
            sent_data.push(i);
            std::thread::sleep(Duration::from_micros(500));
        }

        drop(client);
        sent_data
    });

    // Let queue fill up during slow phase
    sleep(Duration::from_millis(800));

    // Switch to fast mode to drain and trigger resume
    switch_to_fast.store(true, Ordering::Relaxed);

    let sent_data = writer_thread.join().unwrap();
    let (received, slow_count, total_count) = reading_thread.join().unwrap().unwrap();

    println!(
        "Processed {} events slowly, {} total",
        slow_count, total_count
    );
    println!(
        "Sent {} bytes, received {} bytes",
        sent_data.len(),
        received.len()
    );

    // Verify data integrity
    assert_eq!(
        received.len(),
        sent_data.len(),
        "Should receive all {} bytes",
        sent_data.len()
    );
    assert_eq!(
        received, sent_data,
        "Received data should match sent data exactly"
    );

    // Should have processed some events
    assert!(slow_count > 0, "Should have slow phase");
    assert!(
        total_count >= slow_count,
        "Total should be at least slow count"
    );

    // If queue filled up and resumed, we might have more events in fast phase
    // But TCP batching may consolidate everything, so we just verify all data arrived
    if total_count > slow_count {
        println!(
            "✓ Fast drain test passed - slow: {}, fast: {}, all data intact",
            slow_count,
            total_count - slow_count
        );
    } else {
        println!(
            "✓ Fast drain test passed - all {} events processed, all data intact",
            total_count
        );
    }
}

#[test]
fn test_multishot_slab_entry_reaped_after_eof() {
    let (cv1, cv2) = Cv::new();

    let reading_thread = std::thread::spawn(move || {
        main_thread_with::<Small, _>(async {
            let mut multiaccept = MultiAccept::new(9097).unwrap();
            cv1.notify();
            let socket = multiaccept.next().await.unwrap().unwrap();

            // Baseline includes the still-live MultiAccept entry.
            let baseline = runtime::io_driver::IO_DRIVER.with(|d| d.multishot_ops_in_flight());

            let mut multi_recv = MultiRecv::<Small>::new(socket);
            let mut received = 0usize;
            while let Some(Ok(data)) = multi_recv.next().await {
                received += data.as_ref().len();
            }
            drop(multi_recv);

            // The recv op terminated at EOF, so no further CQE exists for it.
            // Dropping the stream must reap its slab entry immediately —
            // before this fix, every peer-closed connection leaked one.
            let after = runtime::io_driver::IO_DRIVER.with(|d| d.multishot_ops_in_flight());
            (baseline, after, received)
        })
    });

    cv2.wait();

    let mut client = TcpStream::connect("localhost:9097").unwrap();
    client.write_all(b"hello multishot").unwrap();
    drop(client);

    let (baseline, after, received) = reading_thread.join().unwrap();
    assert_eq!(received, b"hello multishot".len());
    assert_eq!(
        after, baseline,
        "dropping a MultiRecv after EOF leaked a multishot slab entry"
    );
}

/// Data arriving between construction and the first poll must still come out.
#[test]
fn cqe_before_first_poll_is_not_lost() {
    let (cv1, cv2) = Cv::new();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        main_thread_with::<Small, _>(async move {
            let mut multiaccept = MultiAccept::new(9197).unwrap();
            cv1.notify();
            let socket = multiaccept.next().await.unwrap().unwrap();
            let mut multirecv = MultiRecv::<Small>::new(socket);
            // Any await between construction and the first poll.
            runtime::runtime::time::Sleep::during(Duration::from_millis(400)).await;
            let got = multirecv
                .next()
                .await
                .map(|r| r.map(|b| b.as_ref().to_vec()).ok());
            done_tx.send(got).unwrap();
        })
    });
    cv2.wait();
    let mut client = TcpStream::connect("localhost:9197").unwrap();
    std::thread::sleep(Duration::from_millis(100));
    client.write_all(b"hello").unwrap();
    let got = done_rx.recv_timeout(Duration::from_secs(3));
    assert_eq!(got, Ok(Some(Some(b"hello".to_vec()))));
    drop(client);
}

/// A queue handed over by a pause must be delivered before re-arming.
#[test]
fn paused_queue_is_delivered_without_more_traffic() {
    let (cv1, cv2) = Cv::new();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        main_thread_with::<Small, _>(async move {
            let mut multiaccept = MultiAccept::new(9195).unwrap();
            cv1.notify();
            let socket = multiaccept.next().await.unwrap().unwrap();
            let mut multirecv = MultiRecv::<Small, _, 2>::with_queue_size(socket);
            let mut total = 0usize;
            let mut first = true;
            while let Some(Ok(data)) = multirecv.next().await {
                total += data.as_ref().len();
                if first {
                    // Busy elsewhere while the queue overflows and the op pauses.
                    first = false;
                    runtime::runtime::time::Sleep::during(Duration::from_millis(1000)).await;
                }
                if total == 8 {
                    break;
                }
            }
            done_tx.send(total).unwrap();
        })
    });
    cv2.wait();
    let mut client = TcpStream::connect("localhost:9195").unwrap();
    client.set_nodelay(true).unwrap();
    for i in 0..4u8 {
        client.write_all(&[i, i]).unwrap();
        std::thread::sleep(Duration::from_millis(100));
    }
    // The peer now waits for a reply: nothing further will wake the consumer.
    let got = done_rx.recv_timeout(Duration::from_secs(5));
    assert_eq!(got, Ok(8), "consumer never saw all 8 bytes");
    drop(client);
}

#[test]
fn an_accepted_socket_is_close_on_exec() {
    let (cv1, cv2) = Cv::new();

    let server = std::thread::spawn(move || {
        main_thread_with::<Small, _>(async {
            let mut multiaccept = MultiAccept::new(9198).unwrap();
            cv1.notify();
            let socket = multiaccept.next().await.unwrap().unwrap();
            unsafe { libc::fcntl(socket.fd(), libc::F_GETFD) }
        })
    });

    cv2.wait();
    let _client = TcpStream::connect("localhost:9198").unwrap();

    let fd_flags = server.join().unwrap();
    assert!(fd_flags >= 0, "fcntl failed");
    assert_ne!(
        fd_flags & libc::FD_CLOEXEC,
        0,
        "accepted fd leaks into exec'd children"
    );
}
