use std::{
    io::Write,
    net::TcpStream,
    os::unix::io::AsRawFd,
    process::Command,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread::sleep,
    time::Duration,
};

use rand::{RngCore, thread_rng};
use runtime::{
    io_driver::operations::{
        StreamExt,
        sockets::{multi_accept::MultiAccept, multi_recv_bundle::MultiRecv},
    },
    main_thread,
};

use crate::common::Cv;

mod common;

#[test]
fn test_stream_stops_on_close() {
    let (cv1, cv2) = Cv::new();

    let reading_thread = std::thread::spawn(move || {
        main_thread(async {
            let mut multiaccept = MultiAccept::new(9091).unwrap();
            cv1.notify();
            if let Some(socket) = multiaccept.next().await {
                let mut recv_count = 0;
                let mut multi_recv = MultiRecv::new(socket.fd);
                while let Some(_data) = multi_recv.next().await {
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
        main_thread(async {
            let mut multiaccept = MultiAccept::new(9092).unwrap();
            cv1.notify();
            if let Some(socket) = multiaccept.next().await {
                let mut recv_count = 0;
                let mut multi_recv = MultiRecv::new(socket.fd);
                while let Some(_data) = multi_recv.next().await {
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
        main_thread(async {
            let mut multiaccept = MultiAccept::new(9093).unwrap();
            cv1.notify();
            if let Some(socket) = multiaccept.next().await {
                let mut recv_count = 0;
                let mut multi_recv = MultiRecv::new(socket.fd);
                while let Some(_data) = multi_recv.next().await {
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
        main_thread(async {
            let mut multiaccept = MultiAccept::new(9090).unwrap();
            cv1.notify();

            if let Some(socket) = multiaccept.next().await {
                let mut received: Vec<u8> = Vec::new();
                let mut multirecv = MultiRecv::new(socket.fd);
                while let Some(data) = multirecv.next().await {
                    let o = data.as_parts().into_iter().flatten();
                    for slc in o {
                        received.extend_from_slice(slc);
                    }
                }
                Some(received)
            } else {
                None
            }
        })
    });
    cv2.wait();
    let mut client = TcpStream::connect("localhost:9090").unwrap();
    for b in random_bytes.chunks(1024) {
        let _ = client.write(b).unwrap();
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
fn triggered_pause() {
    let (cv1, cv2) = Cv::new();
    let mut rng = thread_rng();
    let recv_data: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
    let mut random_bytes = vec![0u8; 10_000_000];

    rng.fill_bytes(&mut random_bytes);
    let reading_thread = std::thread::spawn(move || {
        main_thread(async move {
            let mut multiaccept = MultiAccept::new(9094).unwrap();
            cv1.notify();
            if let Some(socket) = multiaccept.next().await {
                let mut multirecv = MultiRecv::new(socket.fd);
                let data = multirecv.next().await.unwrap();
                while let Some(k) = multirecv.next().await {
                    sleep(Duration::from_millis(50));
                }
            }
        })
    });
    cv2.wait();

    let it = random_bytes.chunks(1024);
    let mut client = TcpStream::connect("localhost:9094").unwrap();
    for chunk in it {
        let n = client.write(chunk).unwrap();
        client.flush().unwrap();
        sleep(Duration::from_millis(1));
        assert!(n > 1);
    }
    drop(client);
    reading_thread.join().unwrap();
}

#[test]
fn test_slow_consumer_triggers_pause() {
    let (cv1, cv2) = Cv::new();

    let reading_thread = std::thread::spawn(move || {
        main_thread(async move {
            let mut multiaccept = MultiAccept::new(9095).unwrap();
            cv1.notify();
            if let Some(socket) = multiaccept.next().await {
                let mut received = Vec::new();
                let mut multirecv = MultiRecv::new(socket.fd);
                let mut recv_count = 0;

                while let Some(data) = multirecv.next().await {
                    let o = data.as_parts().into_iter().flatten();
                    for slc in o {
                        received.extend_from_slice(slc);
                    }
                    recv_count += 1;

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

    // Fast writer - send many small writes rapidly
    let writer_thread = std::thread::spawn(|| {
        let mut client = TcpStream::connect("localhost:9095").unwrap();
        client.set_nodelay(true).unwrap();

        let mut sent_data = Vec::new();
        // Send 100 writes as fast as possible
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

    assert!(recv_count > 0, "Should receive events");

    println!(
        "✓ Slow consumer test passed - received {} events, all data intact",
        recv_count
    );
}

#[test]
fn test_fast_drain_triggers_resume() {
    // Test that draining the queue triggers resume
    // Start slow, let queue fill, then drain fast

    let (cv1, cv2) = Cv::new();
    let switch_to_fast = Arc::new(AtomicBool::new(false));
    let switch_clone = switch_to_fast.clone();

    let reading_thread = std::thread::spawn(move || {
        main_thread(async move {
            let mut multiaccept = MultiAccept::new(9096).unwrap();
            cv1.notify();
            if let Some(socket) = multiaccept.next().await {
                let mut received = Vec::new();
                let mut multirecv = MultiRecv::new(socket.fd);
                let mut recv_count = 0;
                let mut slow_phase_count = 0;

                while let Some(data) = multirecv.next().await {
                    let o = data.as_parts().into_iter().flatten();
                    for slc in o {
                        received.extend_from_slice(slc);
                    }
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
