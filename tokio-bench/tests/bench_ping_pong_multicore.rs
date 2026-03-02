/// Multi-core TCP benchmarks for Tokio.
///
/// - blast: multi_thread runtime, 100 clients sending 1000-byte messages
/// - ping_pong_parallel: mirror of `multi_load_ping_pong_parallel` in our runtime,
///   N pairs (one per CPU), 10k sequential ping-pong rounds, all parallel.
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

use socket2::{Domain, Socket, Type};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const PORT: u16 = 9311;
const CLIENTS: usize = 100;
const ROUNDS: usize = 1_000;
const MSG: &[u8] = &[0xAB; 1000];

fn blast_client(port: u16, rounds: usize, delay_ms: u64, client_id: usize) -> Duration {
    std::thread::sleep(Duration::from_millis(delay_ms));

    let src_ip = format!("127.0.{}.{}", (client_id / 256) + 1, (client_id % 256) + 1);
    let sock = Socket::new(Domain::IPV4, Type::STREAM, None).unwrap();
    let src: SocketAddr = format!("{src_ip}:0").parse().unwrap();
    sock.bind(&src.into()).unwrap();
    let dst: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    sock.connect(&dst.into()).unwrap();
    sock.set_nodelay(true).unwrap();
    let client: TcpStream = sock.into();
    let mut reader = client.try_clone().unwrap();
    let mut writer = client;

    let start = Instant::now();

    let reader_handle = std::thread::spawn(move || {
        let mut buf = [0u8; 4096];
        let mut total_read = 0;
        let expected = rounds * MSG.len();
        while total_read < expected {
            let n = reader.read(&mut buf).unwrap();
            if n == 0 { break; }
            total_read += n;
        }
    });

    for _ in 0..rounds {
        writer.write_all(MSG).unwrap();
    }
    writer.flush().unwrap();

    reader_handle.join().unwrap();
    start.elapsed()
}

#[test]
fn bench_ping_pong_multicore() {
    let num_cores = std::thread::available_parallelism().unwrap().get();

    let server = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(num_cores)
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let listener = TcpListener::bind(format!("0.0.0.0:{PORT}"))
                .await
                .unwrap();

            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                stream.set_nodelay(true).unwrap();
                tokio::spawn(async move {
                    let mut buf = [0u8; 4096];
                    loop {
                        let n = match stream.read(&mut buf).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => n,
                        };
                        if stream.write_all(&buf[..n]).await.is_err() {
                            break;
                        }
                    }
                });
            }
        });
    });

    std::thread::sleep(Duration::from_millis(200));

    let start = Instant::now();
    let handles: Vec<_> = (0..CLIENTS)
        .enumerate()
        .map(|(i, _)| {
            let delay = i as u64;
            std::thread::spawn(move || blast_client(PORT, ROUNDS, delay, i))
        })
        .collect();

    let durations: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let wall = start.elapsed();

    let total_requests = CLIENTS * ROUNDS;
    let avg_latency: Duration = durations.iter().sum::<Duration>() / durations.len() as u32;
    let rps = total_requests as f64 / wall.as_secs_f64();

    println!("── tokio (multi_thread, epoll, {num_cores} cores) ──");
    println!("  clients:       {CLIENTS}");
    println!("  rounds/client: {ROUNDS}");
    println!("  total requests:{total_requests}");
    println!("  wall time:     {wall:?}");
    println!("  avg latency:   {avg_latency:?}");
    println!("  throughput:    {rps:.0} req/s");

    drop(server);
}

// ─── Mirror of `multi_load_ping_pong_parallel` ───────────────────────────────
// N = CPU count ping-pong pairs, 10k sequential rounds each, all in parallel.
// Server: tokio multi_thread (auto), one listener, one spawned task per conn.
// Client: N blocking std::net threads doing strict send→recv per round.
// Metric: wall-clock from all clients started until all done.

const PING_PORT: u16 = 9312;
const PING_ROUNDS: usize = 10_000;

#[test]
fn ping_pong_parallel() {
    let num_cores = std::thread::available_parallelism().unwrap().get();

    let server = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let listener = TcpListener::bind(format!("0.0.0.0:{PING_PORT}"))
                .await
                .unwrap();

            // Accept exactly num_cores connections then stop.
            for _ in 0..num_cores {
                let (mut stream, _) = listener.accept().await.unwrap();
                stream.set_nodelay(true).unwrap();
                tokio::spawn(async move {
                    let mut buf = [0u8; 4];
                    for _ in 0..PING_ROUNDS {
                        match stream.read_exact(&mut buf).await {
                            Ok(_) => { stream.write_all(&buf).await.unwrap(); }
                            Err(_) => break,
                        }
                    }
                });
            }

            // Wait long enough for all ping-pong sessions to finish.
            tokio::time::sleep(Duration::from_secs(30)).await;
        });
    });

    std::thread::sleep(Duration::from_millis(100));
    let start = Instant::now();

    let clients: Vec<_> = (0..num_cores)
        .map(|_| {
            std::thread::spawn(move || {
                let mut c = TcpStream::connect(format!("127.0.0.1:{PING_PORT}")).unwrap();
                c.set_nodelay(true).unwrap();
                let mut buf = [0u8; 4];
                for _ in 0..PING_ROUNDS {
                    c.write_all(b"ping").unwrap();
                    c.read_exact(&mut buf).unwrap();
                }
            })
        })
        .collect();

    for c in clients {
        c.join().unwrap();
    }

    let wall = start.elapsed();
    let ns = wall.as_nanos() / PING_ROUNDS as u128;

    println!(
        "[tokio ping_pong_parallel] {num_cores} parallel pairs  {wall:?} wall  ({ns} ns/round wall-clock)"
    );

    drop(server);
}
