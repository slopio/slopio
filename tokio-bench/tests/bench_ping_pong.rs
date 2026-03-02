/// Single-core TCP ping-pong benchmark for Tokio.
///
/// - Server: current_thread runtime, TcpListener, tokio::spawn per connection
/// - Client: N blocking std::net clients on a separate thread each
///
/// The benchmark ends when all clients have finished their rounds.
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const PORT: u16 = 9301;
const CLIENTS: usize = 100;
const ROUNDS: usize = 1_000;
const MSG: &[u8] = &[0xAB; 1000];

fn blast_client(port: u16, rounds: usize, delay_ms: u64) -> Duration {
    std::thread::sleep(Duration::from_millis(delay_ms));
    let mut client = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    client.set_nodelay(true).unwrap();

    let start = Instant::now();

    // send all messages as fast as possible
    for _ in 0..rounds {
        client.write_all(MSG).unwrap();
    }
    client.flush().unwrap();

    // read all responses back
    let mut buf = [0u8; 4096];
    let mut total_read = 0;
    let expected = rounds * MSG.len();
    while total_read < expected {
        let n = client.read(&mut buf).unwrap();
        if n == 0 { break; }
        total_read += n;
    }

    start.elapsed()
}

#[test]
fn bench_ping_pong() {
    let server = std::thread::spawn(|| {
        let rt = tokio::runtime::Builder::new_current_thread()
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

    std::thread::sleep(Duration::from_millis(100));

    let start = Instant::now();
    let handles: Vec<_> = (0..CLIENTS)
        .enumerate()
        .map(|(i, _)| {
            let delay = i as u64; // 1ms stagger between clients
            std::thread::spawn(move || blast_client(PORT, ROUNDS, delay))
        })
        .collect();

    let durations: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let wall = start.elapsed();

    let total_requests = CLIENTS * ROUNDS;
    let avg_latency: Duration = durations.iter().sum::<Duration>() / durations.len() as u32;
    let rps = total_requests as f64 / wall.as_secs_f64();

    println!("── tokio (current_thread, epoll) ──");
    println!("  clients:       {CLIENTS}");
    println!("  rounds/client: {ROUNDS}");
    println!("  total requests:{total_requests}");
    println!("  wall time:     {wall:?}");
    println!("  avg latency:   {avg_latency:?}");
    println!("  throughput:    {rps:.0} req/s");

    drop(server);
}
