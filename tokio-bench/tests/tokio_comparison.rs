use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

fn current_thread() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn multi_thread() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
}

// ═══════════════════════════════════════════════════════
// TASKS
// ═══════════════════════════════════════════════════════

#[test]
fn load_100k_tasks() {
    let start = Instant::now();

    let sum = current_thread().block_on(async {
        let mut handles = Vec::with_capacity(100_000);
        for i in 0u64..100_000 {
            handles.push(tokio::spawn(async move { i }));
        }
        let mut s = 0u64;
        for h in handles {
            s += h.await.unwrap();
        }
        s
    });

    let elapsed = start.elapsed();
    println!(
        "[tokio load_100k_tasks] {:?}  ({:.0}K tasks/s)",
        elapsed,
        100.0 / elapsed.as_secs_f64()
    );
    assert_eq!(sum, 100_000u64 * 99_999 / 2);
}

#[test]
fn load_task_cascade() {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    let start = Instant::now();

    let count = current_thread().block_on(async {
        let counter = Arc::new(AtomicU64::new(0));
        let mut mid_handles = Vec::new();

        for _ in 0..20 {
            let c = counter.clone();
            mid_handles.push(tokio::spawn(async move {
                c.fetch_add(1, Ordering::Relaxed);
                let mut leaf_handles = Vec::new();
                for _ in 0..20 {
                    let cc = c.clone();
                    leaf_handles.push(tokio::spawn(async move {
                        cc.fetch_add(1, Ordering::Relaxed);
                    }));
                }
                for h in leaf_handles {
                    h.await.unwrap();
                }
            }));
        }
        for h in mid_handles {
            h.await.unwrap();
        }
        counter.load(Ordering::Relaxed)
    });

    let elapsed = start.elapsed();
    println!("[tokio load_task_cascade] {:?}  ({} tasks)", elapsed, count);
    assert_eq!(count, 420);
}

// ═══════════════════════════════════════════════════════
// CHANNELS
// ═══════════════════════════════════════════════════════

#[test]
fn load_channel_1m_messages() {
    let start = Instant::now();

    current_thread().block_on(async {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<u64>();
        let n = 1_000_000u64;

        let producer = tokio::spawn(async move {
            for i in 0..n {
                tx.send(i).unwrap();
            }
        });

        let consumer = tokio::spawn(async move {
            let mut sum = 0u64;
            for _ in 0..n {
                if let Some(v) = rx.recv().await {
                    sum = sum.wrapping_add(v);
                }
            }
            sum
        });

        producer.await.unwrap();
        let sum = consumer.await.unwrap();
        assert_eq!(sum, n * (n - 1) / 2);
    });

    let elapsed = start.elapsed();
    println!(
        "[tokio load_channel_1m_messages] {:?}  ({:.1}M msg/s)",
        elapsed,
        1.0 / elapsed.as_secs_f64()
    );
}

#[test]
fn load_bounded_channel_backpressure() {
    let start = Instant::now();

    current_thread().block_on(async {
        let n = 100_000u64;
        let (tx, mut rx) = tokio::sync::mpsc::channel::<u64>(64);

        let producer = tokio::spawn(async move {
            for i in 0..n {
                tx.send(i).await.unwrap();
            }
        });

        let consumer = tokio::spawn(async move {
            let mut sum = 0u64;
            for _ in 0..n {
                if let Some(v) = rx.recv().await {
                    sum = sum.wrapping_add(v);
                }
            }
            sum
        });

        producer.await.unwrap();
        let sum = consumer.await.unwrap();
        assert_eq!(sum, n * (n - 1) / 2);
    });

    let elapsed = start.elapsed();
    println!(
        "[tokio load_bounded_channel_backpressure] {:?}  ({:.0}K msg/s)",
        elapsed,
        100.0 / elapsed.as_secs_f64()
    );
}

// ═══════════════════════════════════════════════════════
// TCP
// ═══════════════════════════════════════════════════════

#[test]
fn load_ping_pong_10k_rounds() {
    const ROUNDS: usize = 10_000;

    current_thread().block_on(async {
        let listener = TcpListener::bind("127.0.0.1:19202").await.unwrap();

        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 4];
            for _ in 0..ROUNDS {
                let n = socket.read(&mut buf).await.unwrap();
                if n == 0 {
                    break;
                }
                socket.write_all(&buf[..n]).await.unwrap();
            }
        });

        let mut client = TcpStream::connect("127.0.0.1:19202").await.unwrap();
        client.set_nodelay(true).unwrap();

        let start = Instant::now();
        let mut buf = [0u8; 4];
        for _ in 0..ROUNDS {
            client.write_all(b"ping").await.unwrap();
            client.read_exact(&mut buf).await.unwrap();
        }
        let elapsed = start.elapsed();

        let ns_per_rtt = elapsed.as_nanos() / ROUNDS as u128;
        println!(
            "[tokio load_ping_pong_10k_rounds] {:?} total  ({} ns/rtt)",
            elapsed, ns_per_rtt
        );

        server.await.unwrap();
    });
}

#[test]
fn load_200_concurrent_connections() {
    const N: usize = 200;

    let start = Instant::now();

    current_thread().block_on(async {
        let listener = TcpListener::bind("127.0.0.1:19200").await.unwrap();

        let server = tokio::spawn(async move {
            let mut handlers = Vec::new();
            for _ in 0..N {
                let (mut socket, _) = listener.accept().await.unwrap();
                handlers.push(tokio::spawn(async move {
                    let mut buf = [0u8; 8];
                    let _ = socket.read(&mut buf).await.unwrap();
                    socket.write_all(b"ok").await.unwrap();
                }));
            }
            for h in handlers {
                h.await.unwrap();
            }
        });

        let mut clients = Vec::new();
        for _ in 0..N {
            let mut c = TcpStream::connect("127.0.0.1:19200").await.unwrap();
            clients.push(tokio::spawn(async move {
                c.write_all(b"ping").await.unwrap();
                let mut buf = [0u8; 2];
                c.read_exact(&mut buf).await.unwrap();
                assert_eq!(&buf, b"ok");
            }));
        }

        for c in clients {
            c.await.unwrap();
        }
        server.await.unwrap();
    });

    let elapsed = start.elapsed();
    println!("[tokio load_200_concurrent_connections] {:?}", elapsed);
}

#[test]
fn load_tcp_1mb_recv() {
    current_thread().block_on(async {
        let listener = TcpListener::bind("127.0.0.1:19201").await.unwrap();

        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut total = 0usize;
            let mut buf = vec![0u8; 4096];
            loop {
                let n = socket.read(&mut buf).await.unwrap();
                if n == 0 {
                    break;
                }
                total += n;
            }
            total
        });

        let payload = vec![0xABu8; 1024 * 1024];
        let start = Instant::now();
        let mut client = TcpStream::connect("127.0.0.1:19201").await.unwrap();
        client.write_all(&payload).await.unwrap();
        drop(client);

        let total = server.await.unwrap();
        let elapsed = start.elapsed();
        println!(
            "[tokio load_tcp_1mb_recv] {:?}  ({:.0} MB/s)",
            elapsed,
            1.0 / elapsed.as_secs_f64()
        );
        assert_eq!(total, 1024 * 1024);
    });
}

#[test]
fn load_1000_concurrent_timers() {
    let start = Instant::now();

    current_thread().block_on(async {
        let handles: Vec<_> = (0..1000)
            .map(|_| tokio::spawn(async { tokio::time::sleep(Duration::from_millis(100)).await }))
            .collect();
        for h in handles {
            h.await.unwrap();
        }
    });

    let elapsed = start.elapsed();
    println!("[tokio load_1000_concurrent_timers] {:?}", elapsed);
    assert!(elapsed < Duration::from_millis(800));
}
