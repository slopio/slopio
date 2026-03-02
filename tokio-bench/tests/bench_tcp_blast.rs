mod common;

use std::pin::Pin;
use std::time::{Duration, Instant};

use common::{blast_tcp, Cv};

use runtime::io_driver::BufGroup;
use runtime::io_driver::operations::sockets::multi_accept::MultiAccept;
use runtime::io_driver::operations::sockets::multi_recv::MultiRecv;
use runtime::io_driver::operations::sockets::send::socket_send;
use runtime::runtime::spawn;
use runtime::thread_per_core;
use runtime_streams::StreamExt;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use monoio::io::{AsyncReadRent, AsyncWriteRentExt};

// ── Parameters ───────────────────────────────────────────────────────────────

const NUM_IPS: usize = 20_000;
const NUM_REQUESTS: usize = 20_000_000; // 1 000 writes per connection
const PAYLOAD_SIZE: usize = 4_096;   // 4 KB  →  80 GB total
const DELAY: Duration = Duration::from_micros(5);

const PORT_CUSTOM: u16 = 19_410;
const PORT_TOKIO: u16 = 19_411;
const PORT_MONOIO: u16 = 19_412;

fn multi_thread_client() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
}

// ── Custom runtime server ─────────────────────────────────────────────────────
//
// All cores run their own MultiAccept on the same port (SO_REUSEPORT).
// The kernel distributes incoming connections across all cores.
// The server future never returns — we only measure the client side.

fn bench_custom(
    port: u16,
    num_ips: usize,
    num_requests: usize,
    payload_size: usize,
) -> Duration {
    let (cv_ready, cv_ready_rx) = Cv::new();

    std::thread::spawn(move || {
        thread_per_core(move |ctx| {
            let cv_ready = cv_ready.clone();

            Box::pin(async move {
                let mut ma = MultiAccept::new(port).unwrap();
                // Signal once core 0 is listening; other cores start concurrently.
                if ctx.thread_id == 0 {
                    cv_ready.notify();
                }

                while let Some(socket) = ma.next().await {
                    spawn(async move {
                        let mut mr = MultiRecv::new(socket.fd, BufGroup::Small);
                        while let Some(buf) = mr.next().await {
                            socket_send(&socket, buf.as_ref()).await.ok();
                        }
                        drop(socket);
                    });
                }
            }) as Pin<Box<dyn std::future::Future<Output = ()>>>
        });
    });

    cv_ready_rx.wait();

    let start = Instant::now();
    multi_thread_client().block_on(async move {
        blast_tcp(
            format!("127.0.0.1:{port}").parse().unwrap(),
            num_ips,
            num_requests,
            payload_size,
            DELAY,
        )
        .await;
    });
    start.elapsed()
}

// ── Tokio multi-thread server ─────────────────────────────────────────────────
//
// Single TcpListener, work-stealing scheduler distributes accept + recv tasks.
// Same pattern: server runs forever, we measure client time.

fn bench_tokio(
    port: u16,
    num_ips: usize,
    num_requests: usize,
    payload_size: usize,
) -> Duration {
    let (cv_ready, cv_ready_rx) = Cv::new();

    std::thread::spawn(move || {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async move {
                let listener =
                    TcpListener::bind(format!("127.0.0.1:{port}")).await.unwrap();
                cv_ready.notify();

                loop {
                    let Ok((mut socket, _)) = listener.accept().await else {
                        break;
                    };
                    tokio::spawn(async move {
                        let mut buf = vec![0u8; 4_096];
                        loop {
                            match socket.read(&mut buf).await.unwrap() {
                                0 => break,
                                n => {
                                    socket.write_all(&buf[..n]).await.unwrap();
                                }
                            }
                        }
                    });
                }
            });
    });

    cv_ready_rx.wait();

    let start = Instant::now();
    multi_thread_client().block_on(async move {
        blast_tcp(
            format!("127.0.0.1:{port}").parse().unwrap(),
            num_ips,
            num_requests,
            payload_size,
            DELAY,
        )
        .await;
    });
    start.elapsed()
}

// ── Monoio thread-per-core server ────────────────────────────────────────────
//
// One monoio runtime (IoUringDriver) per CPU thread.
// Each thread creates its own TcpListener with SO_REUSEPORT via socket2.

fn bench_monoio(
    port: u16,
    num_ips: usize,
    num_requests: usize,
    payload_size: usize,
) -> Duration {
    let (cv_ready, cv_ready_rx) = Cv::new();
    let num_cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);

    std::thread::spawn(move || {
        let mut handles = vec![];
        for i in 0..num_cores {
            let cv_ready = cv_ready.clone();
            handles.push(std::thread::spawn(move || {
                let socket = socket2::Socket::new(
                    socket2::Domain::IPV4,
                    socket2::Type::STREAM,
                    None,
                )
                .unwrap();
                socket.set_reuse_port(true).unwrap();
                socket.set_reuse_address(true).unwrap();
                socket
                    .bind(
                        &format!("127.0.0.1:{port}")
                            .parse::<std::net::SocketAddr>()
                            .unwrap()
                            .into(),
                    )
                    .unwrap();
                socket.listen(1024).unwrap();
                socket.set_nonblocking(true).unwrap();
                let std_listener: std::net::TcpListener = socket.into();

                monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
                    .build()
                    .unwrap()
                    .block_on(async move {
                        let listener =
                            monoio::net::TcpListener::from_std(std_listener).unwrap();
                        if i == 0 {
                            cv_ready.notify();
                        }
                        loop {
                            let Ok((mut stream, _)) = listener.accept().await else {
                                break;
                            };
                            monoio::spawn(async move {
                                let mut buf = vec![0u8; 4_096];
                                loop {
                                    let (res, b) = stream.read(buf).await;
                                    buf = b;
                                    let n = match res {
                                        Ok(0) | Err(_) => break,
                                        Ok(n) => n,
                                    };
                                    buf.truncate(n);
                                    let (res2, mut b2) = stream.write_all(buf).await;
                                    b2.resize(4_096, 0);
                                    buf = b2;
                                    if res2.is_err() {
                                        break;
                                    }
                                }
                            });
                        }
                    });
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
    });

    cv_ready_rx.wait();

    let start = Instant::now();
    multi_thread_client().block_on(async move {
        blast_tcp(
            format!("127.0.0.1:{port}").parse().unwrap(),
            num_ips,
            num_requests,
            payload_size,
            DELAY,
        )
        .await;
    });
    start.elapsed()
}

// ── Test ──────────────────────────────────────────────────────────────────────

#[test]
fn compare_tcp_blast() {
    let total_bytes = NUM_REQUESTS * PAYLOAD_SIZE;

    let custom_time = bench_custom(PORT_CUSTOM, NUM_IPS, NUM_REQUESTS, PAYLOAD_SIZE);
    let monoio_time = bench_monoio(PORT_MONOIO, NUM_IPS, NUM_REQUESTS, PAYLOAD_SIZE);
    let tokio_time = bench_tokio(PORT_TOKIO, NUM_IPS, NUM_REQUESTS, PAYLOAD_SIZE);

    let rps = |t: Duration| NUM_REQUESTS as f64 / t.as_secs_f64();
    let mb_s = |t: Duration| total_bytes as f64 / t.as_secs_f64() / 1e6;

    println!("\n╔══════════════════════════════════════════════════════════════╗");
    println!("║               TCP Blast — Runtime Comparison                ║");
    println!("╠══════════════════════════════════════════════════════════════╣");
    println!(
        "║  IPs: {:>5}   requests: {:>8}   payload: {:>5} B      ║",
        NUM_IPS, NUM_REQUESTS, PAYLOAD_SIZE
    );
    println!(
        "║  Total: {:>5} MB   delay: {:?}   client: multi-thread  ║",
        total_bytes / 1_000_000,
        DELAY
    );
    println!("╠══════════════════════════════════════════════════════════════╣");
    println!("║  Custom  (thread_per_core + io_uring multishot):            ║");
    println!(
        "║    {:>8.2?}   {:>8.0} req/s   {:>6.0} MB/s             ║",
        custom_time, rps(custom_time), mb_s(custom_time)
    );
    println!("║  Monoio  (thread_per_core + io_uring):                      ║");
    println!(
        "║    {:>8.2?}   {:>8.0} req/s   {:>6.0} MB/s             ║",
        monoio_time, rps(monoio_time), mb_s(monoio_time)
    );
    println!("║  Tokio   (multi_thread  + epoll):                           ║");
    println!(
        "║    {:>8.2?}   {:>8.0} req/s   {:>6.0} MB/s             ║",
        tokio_time, rps(tokio_time), mb_s(tokio_time)
    );
    println!("╚══════════════════════════════════════════════════════════════╝");
}

// Profiling-only: custom runtime sous charge, paramètres réduits pour flamegraph (~10-30s).
#[test]
fn bench_custom_profile() {
    const IPS: usize = 2_000;
    const REQUESTS: usize = 2_000_000;
    const PAYLOAD: usize = 4_096;

    let elapsed = bench_custom(19_413, IPS, REQUESTS, PAYLOAD);

    let rps = REQUESTS as f64 / elapsed.as_secs_f64();
    let mb_s = (REQUESTS * PAYLOAD) as f64 / elapsed.as_secs_f64() / 1e6;
    println!(
        "\n[bench_custom_profile] {:?}   {:.0} req/s   {:.0} MB/s",
        elapsed, rps, mb_s
    );
}
