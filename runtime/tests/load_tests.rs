use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use runtime::channels::{bounded_channel, unbounded_channel};
use runtime::io_driver::BufGroup;
use runtime::io_driver::operations::sockets::multi_accept::MultiAccept;
use runtime::io_driver::operations::sockets::multi_recv::MultiRecv;
use runtime::io_driver::operations::sockets::recv::socket_recv_init as socket_recv;
use runtime::io_driver::operations::sockets::send::socket_send;
use runtime::main_thread;
use runtime::runtime::time::Sleep;
use runtime::runtime::{spawn, yield_now};
use runtime_streams::StreamExt;

// Ports 9200–9210 — isolated from other test files.

// ═══════════════════════════════════════════════════════
// TASK SCHEDULER
// ═══════════════════════════════════════════════════════

// Spawn 100 000 tasks that each return their index, sum them.
// Exercises the VecDeque push/pop hot path and Handle allocation.
#[test]
fn load_100k_tasks() {
    let start = Instant::now();

    let sum = main_thread(async {
        let mut handles = Vec::with_capacity(100_000);
        for i in 0u64..100_000 {
            handles.push(spawn(async move { i }));
        }
        let mut s = 0u64;
        for h in handles {
            s += h.await;
        }
        s
    });

    let elapsed = start.elapsed();
    println!(
        "[load_100k_tasks] {:?}  ({:.0}K tasks/s)",
        elapsed,
        100.0 / elapsed.as_secs_f64()
    );
    assert_eq!(sum, 100_000u64 * 99_999 / 2);
}

// 3-level task cascade: 1 root spawns 20 mid-tasks, each spawns 20 leaf tasks.
// Total: 420 tasks. Tests dynamic task creation and join ordering.
#[test]
fn load_task_cascade() {
    let start = Instant::now();

    let count = main_thread(async {
        let counter = Arc::new(AtomicU64::new(0));

        let mut mid_handles = Vec::new();
        for _ in 0..20 {
            let c = counter.clone();
            mid_handles.push(spawn(async move {
                c.fetch_add(1, Ordering::Relaxed);
                let mut leaf_handles = Vec::new();
                for _ in 0..20 {
                    let cc = c.clone();
                    leaf_handles.push(spawn(async move {
                        cc.fetch_add(1, Ordering::Relaxed);
                    }));
                }
                for h in leaf_handles {
                    h.await;
                }
            }));
        }
        for h in mid_handles {
            h.await;
        }

        counter.load(Ordering::Relaxed)
    });

    let elapsed = start.elapsed();
    println!(
        "[load_task_cascade] {:?}  ({} tasks completed)",
        elapsed, count
    );
    assert_eq!(count, 420); // 20 mid + 20*20 leaf
}

// 10 000 tasks each doing real CPU work (tight loop), all in parallel.
// Exercises the scheduler under high CPU contention from a single thread.
#[test]
fn load_cpu_bound_tasks() {
    let start = Instant::now();

    let total = main_thread(async {
        let handles: Vec<_> = (0u64..10_000)
            .map(|i| {
                spawn(async move {
                    let mut acc = i;
                    for _ in 0..1000 {
                        acc = acc
                            .wrapping_mul(6364136223846793005)
                            .wrapping_add(1442695040888963407);
                    }
                    acc
                })
            })
            .collect();

        let mut sum = 0u64;
        for h in handles {
            sum = sum.wrapping_add(h.await);
        }
        sum
    });

    let elapsed = start.elapsed();
    println!("[load_cpu_bound_tasks] {:?}", elapsed);
    let _ = total; // wrapping, just checking it doesn't deadlock
}

// ═══════════════════════════════════════════════════════
// CHANNELS
// ═══════════════════════════════════════════════════════

// Producer sends 1 000 000 items through an unbounded channel.
// Tests raw channel throughput (no backpressure).
#[test]
fn load_unbounded_channel_1m_messages() {
    let start = Instant::now();

    main_thread(async {
        let (tx, rx) = unbounded_channel::<u64>();
        let n = 1_000_000u64;

        let producer = spawn(async move {
            for i in 0..n {
                tx.send(i);
            }
        });

        let consumer = spawn(async move {
            let mut sum = 0u64;
            for _ in 0..n {
                if let Some(v) = rx.recv().await {
                    sum = sum.wrapping_add(v);
                }
            }
            sum
        });

        producer.await;
        let sum = consumer.await;
        assert_eq!(sum, n * (n - 1) / 2);
    });

    let elapsed = start.elapsed();
    println!(
        "[load_unbounded_channel_1m_messages] {:?}  ({:.1}M msg/s)",
        elapsed,
        1.0 / elapsed.as_secs_f64()
    );
}

// Bounded channel (capacity 64) with 100 000 messages.
// The sender will suspend every 64 messages — exercises waker handoff under pressure.
#[test]
fn load_bounded_channel_backpressure() {
    let start = Instant::now();

    main_thread(async {
        let n = 100_000u64;
        let (tx, rx) = bounded_channel::<u64>(64);

        let producer = spawn(async move {
            for i in 0..n {
                tx.send(i).await.unwrap();
            }
        });

        let consumer = spawn(async move {
            let mut sum = 0u64;
            for _ in 0..n {
                if let Some(v) = rx.recv().await {
                    sum = sum.wrapping_add(v);
                }
            }
            sum
        });

        producer.await;
        let sum = consumer.await;
        assert_eq!(sum, n * (n - 1) / 2);
    });

    let elapsed = start.elapsed();
    println!(
        "[load_bounded_channel_backpressure] {:?}  ({:.0}K msg/s)",
        elapsed,
        100.0 / elapsed.as_secs_f64()
    );
}

// Two separate producer tasks feeding a single consumer via a shared Arc<Sender>.
// Tests concurrent send wakeup paths.
#[test]
fn load_channel_two_producers() {
    let start = Instant::now();

    main_thread(async {
        let (tx, rx) = unbounded_channel::<u64>();
        let tx = Arc::new(tx);
        let n = 50_000u64; // each producer sends n items

        let tx1 = tx.clone();
        let p1 = spawn(async move {
            for i in 0..n {
                tx1.send(i);
            }
        });

        let tx2 = tx.clone();
        let p2 = spawn(async move {
            for i in 0..n {
                tx2.send(i);
            }
        });

        let consumer = spawn(async move {
            let mut count = 0u64;
            for _ in 0..n * 2 {
                rx.recv().await;
                count += 1;
            }
            count
        });

        p1.await;
        p2.await;
        let count = consumer.await;
        assert_eq!(count, n * 2);
    });

    let elapsed = start.elapsed();
    println!("[load_channel_two_producers] {:?}", elapsed);
}

// ═══════════════════════════════════════════════════════
// TCP
// ═══════════════════════════════════════════════════════

// 200 concurrent connections, each sends "ping" and receives "ok".
// Server spawns one task per connection. Tests fd table and task-per-socket pattern.
#[test]
fn load_200_concurrent_connections() {
    const N: usize = 200;

    let server = std::thread::spawn(|| {
        main_thread(async {
            let mut ma = MultiAccept::new(9200).unwrap();
            let mut handlers = Vec::new();

            for _ in 0..N {
                if let Some(socket) = ma.next().await {
                    handlers.push(spawn(async move {
                        let mut buf = Box::new([0u8; 8]);
                        socket_recv(&socket, buf.as_mut(), 8).await.unwrap();
                        socket_send(&socket, b"ok").await.unwrap();
                    }));
                }
            }
            for h in handlers {
                h.await;
            }
        })
    });

    std::thread::sleep(Duration::from_millis(200));

    let start = Instant::now();
    let clients: Vec<_> = (0..N)
        .map(|_| {
            std::thread::spawn(|| {
                let mut c = TcpStream::connect("127.0.0.1:9200").unwrap();
                c.write_all(b"ping").unwrap();
                let mut buf = [0u8; 2];
                c.read_exact(&mut buf).unwrap();
                assert_eq!(&buf, b"ok");
            })
        })
        .collect();

    for c in clients {
        c.join().unwrap();
    }
    server.join().unwrap();

    let elapsed = start.elapsed();
    println!("[load_200_concurrent_connections] {:?}", elapsed);
}

// Client sends 1 MB, server reads and counts all bytes.
// Tests the recv loop and large data paths.
#[test]
fn load_tcp_1mb_recv() {
    let server = std::thread::spawn(|| {
        main_thread(async {
            let mut ma = MultiAccept::new(9201).unwrap();
            if let Some(socket) = ma.next().await {
                let mut total = 0usize;
                loop {
                    let mut buf = Box::new([0u8; 4096]);
                    let n = socket_recv(&socket, buf.as_mut(), 4096).await.unwrap();
                    if n == 0 {
                        break;
                    }
                    total += n;
                }
                total
            } else {
                0
            }
        })
    });

    std::thread::sleep(Duration::from_millis(100));

    let payload = vec![0xABu8; 1024 * 1024];
    let start = Instant::now();
    let mut client = TcpStream::connect("127.0.0.1:9201").unwrap();
    client.write_all(&payload).unwrap();
    drop(client);

    let total = server.join().unwrap();
    let elapsed = start.elapsed();
    println!(
        "[load_tcp_1mb_recv] {:?}  ({:.0} MB/s)",
        elapsed,
        1.0 / elapsed.as_secs_f64()
    );
    assert_eq!(total, 1024 * 1024);
}

// 10 000 ping-pong round trips on a single connection.
// Reports per-round-trip latency — good baseline for scheduler + IO overhead.
#[test]
fn load_ping_pong_10k_rounds() {
    const ROUNDS: usize = 10_000;

    let server = std::thread::spawn(|| {
        main_thread(async {
            let mut ma = MultiAccept::new(9202).unwrap();
            if let Some(socket) = ma.next().await {
                let mut buf = Box::new([0u8; 4]);
                for _ in 0..ROUNDS {
                    let n = socket_recv(&socket, buf.as_mut(), 4).await.unwrap();
                    if n == 0 {
                        break;
                    }
                    socket_send(&socket, &buf[..n]).await.unwrap();
                }
            }
        })
    });

    std::thread::sleep(Duration::from_millis(100));

    let mut client = TcpStream::connect("127.0.0.1:9202").unwrap();
    client.set_nodelay(true).unwrap();

    let start = Instant::now();
    let mut buf = [0u8; 4];
    for _ in 0..ROUNDS {
        client.write_all(b"ping").unwrap();
        client.read_exact(&mut buf).unwrap();
    }
    let elapsed = start.elapsed();

    let ns_per_rtt = elapsed.as_nanos() / ROUNDS as u128;
    println!(
        "[load_ping_pong_10k_rounds] {:?} total  ({} ns/rtt)",
        elapsed, ns_per_rtt
    );

    server.join().unwrap();
}

// 100 connections each performing 100 send/recv cycles sequentially.
// Total: 10 000 round trips spread across 100 concurrent tasks.
#[test]
fn load_100_connections_100_cycles_each() {
    const CONNS: usize = 100;
    const CYCLES: usize = 100;

    let server = std::thread::spawn(|| {
        main_thread(async {
            let mut ma = MultiAccept::new(9203).unwrap();
            let mut handlers = Vec::new();

            for _ in 0..CONNS {
                if let Some(socket) = ma.next().await {
                    handlers.push(spawn(async move {
                        let mut buf = Box::new([0u8; 8]);
                        for _ in 0..CYCLES {
                            let n = socket_recv(&socket, buf.as_mut(), 8).await.unwrap();
                            if n == 0 {
                                break;
                            }
                            socket_send(&socket, &buf[..n]).await.unwrap();
                        }
                    }));
                }
            }
            for h in handlers {
                h.await;
            }
        })
    });

    std::thread::sleep(Duration::from_millis(200));

    let start = Instant::now();
    let clients: Vec<_> = (0..CONNS)
        .map(|_| {
            std::thread::spawn(|| {
                let mut c = TcpStream::connect("127.0.0.1:9203").unwrap();
                c.set_nodelay(true).unwrap();
                let mut buf = [0u8; 8];
                for _ in 0..CYCLES {
                    c.write_all(b"abcd").unwrap();
                    c.read_exact(&mut buf[..4]).unwrap();
                }
            })
        })
        .collect();

    for c in clients {
        c.join().unwrap();
    }
    server.join().unwrap();

    let elapsed = start.elapsed();
    println!(
        "[load_100_connections_100_cycles_each] {:?}  ({:.0}K rtt/s)",
        elapsed,
        (CONNS * CYCLES) as f64 / elapsed.as_secs_f64() / 1000.0
    );
}

// 100 connections × 2000 cycles — designed to run ~2-3s for flamegraph profiling.
#[test]
fn load_tcp_profile() {
    const CONNS: usize = 100;
    const CYCLES: usize = 2000;

    let server = std::thread::spawn(|| {
        main_thread(async {
            let mut ma = MultiAccept::new(9204).unwrap();
            let mut handlers = Vec::new();

            for _ in 0..CONNS {
                if let Some(socket) = ma.next().await {
                    handlers.push(spawn(async move {
                        let mut multi_recv = MultiRecv::new(socket.fd, BufGroup::Small);
                        for _ in 0..CYCLES {
                            match multi_recv.next().await {
                                Some(data) => {
                                    socket_send(&socket, data.as_ref()).await.unwrap();
                                }
                                None => break,
                            }
                        }
                    }));
                }
            }
            for h in handlers {
                h.await;
            }
        })
    });

    std::thread::sleep(Duration::from_millis(200));

    let start = Instant::now();
    let clients: Vec<_> = (0..CONNS)
        .map(|_| {
            std::thread::spawn(|| {
                let mut c = TcpStream::connect("127.0.0.1:9204").unwrap();
                c.set_nodelay(true).unwrap();
                let mut buf = [0u8; 8];
                for _ in 0..CYCLES {
                    c.write_all(b"abcd").unwrap();
                    c.read_exact(&mut buf[..4]).unwrap();
                }
            })
        })
        .collect();

    for c in clients {
        c.join().unwrap();
    }
    server.join().unwrap();

    let elapsed = start.elapsed();
    println!(
        "[load_tcp_profile] {:?}  ({:.0}K rtt/s)",
        elapsed,
        (CONNS * CYCLES) as f64 / elapsed.as_secs_f64() / 1000.0
    );
}

// ═══════════════════════════════════════════════════════
// TIMERS
// ═══════════════════════════════════════════════════════

// 1 000 tasks all sleeping for 100 ms simultaneously.
// If the timer wheel is correct they all fire at ~100 ms, not at 100 seconds.
#[test]
fn load_1000_concurrent_timers() {
    let start = Instant::now();

    main_thread(async {
        let handles: Vec<_> = (0..1000)
            .map(|_| spawn(async { Sleep::during(Duration::from_millis(100)).await }))
            .collect();
        for h in handles {
            h.await;
        }
    });

    let elapsed = start.elapsed();
    println!("[load_1000_concurrent_timers] {:?}", elapsed);
    assert!(
        elapsed < Duration::from_millis(800),
        "Timers took {:?}, expected ~100ms",
        elapsed
    );
}

// ═══════════════════════════════════════════════════════
// SCHEDULER FAIRNESS
// ═══════════════════════════════════════════════════════

// A tight CPU loop calling yield_now must not starve channel consumers.
// If fairness breaks, consumer never makes progress and the test deadlocks.
#[test]
fn load_fairness_cpu_vs_channel() {
    main_thread(async {
        let (tx, rx) = unbounded_channel::<u64>();
        let n = 50_000u64;

        let consumer = spawn(async move {
            let mut count = 0u64;
            for _ in 0..n {
                rx.recv().await;
                count += 1;
            }
            count
        });

        // Tight producer that yields every 100 sends
        let producer = spawn(async move {
            for i in 0..n {
                tx.send(i);
                if i % 100 == 0 {
                    yield_now().await;
                }
            }
        });

        producer.await;
        let count = consumer.await;
        assert_eq!(count, n);
    });
}

// ═══════════════════════════════════════════════════════
// MIXED WORKLOAD
// ═══════════════════════════════════════════════════════

// Runs tasks, channel work, and timers all at once.
// Checks that the runtime correctly interleaves IO and CPU work.
#[test]
fn load_mixed_workload() {
    let start = Instant::now();

    main_thread(async {
        // Arm: 200 tasks doing pure computation
        let compute_handles: Vec<_> = (0u64..200)
            .map(|i| {
                spawn(async move {
                    let mut x = i;
                    for _ in 0..10_000 {
                        x = x.wrapping_add(1);
                    }
                    x
                })
            })
            .collect();

        // Arm: channel pipeline with 10 000 messages
        let (tx, rx) = unbounded_channel::<u64>();
        let channel_producer = spawn(async move {
            for i in 0..10_000u64 {
                tx.send(i);
            }
        });
        let channel_consumer = spawn(async move {
            let mut sum = 0u64;
            for _ in 0..10_000u64 {
                if let Some(v) = rx.recv().await {
                    sum += v;
                }
            }
            sum
        });

        // Arm: 20 timers sleeping 50 ms
        let timer_handles: Vec<_> = (0..20)
            .map(|_| spawn(async { Sleep::during(Duration::from_millis(50)).await }))
            .collect();

        // Join everything
        for h in compute_handles {
            h.await;
        }
        channel_producer.await;
        let channel_sum = channel_consumer.await;
        for h in timer_handles {
            h.await;
        }

        assert_eq!(channel_sum, 10_000 * 9_999 / 2);
    });

    let elapsed = start.elapsed();
    println!("[load_mixed_workload] {:?}", elapsed);
    assert!(elapsed < Duration::from_secs(5));
}
