use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::{Arc, Condvar, Mutex};

// ── Cv ───────────────────────────────────────────────────────────────────────

/// Simple one-shot condition variable for cross-thread signalling in tests.
/// Clone either handle freely; `notify` on one wakes `wait` on the other.
#[derive(Clone)]
pub struct Cv {
    inner: Arc<(Mutex<bool>, Condvar)>,
}

impl Cv {
    pub fn new() -> (Self, Self) {
        let cv = Cv {
            inner: Arc::new((Mutex::new(false), Condvar::new())),
        };
        (cv.clone(), cv)
    }

    pub fn notify(&self) {
        let mut b = self.inner.0.lock().unwrap();
        *b = true;
        self.inner.1.notify_all();
    }

    pub fn wait(&self) {
        let mut b = self.inner.0.lock().unwrap();
        while !*b {
            b = self.inner.1.wait(b).unwrap();
        }
    }
}

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpSocket;

/// Build a `current_thread` Tokio runtime and immediately block on `fut`.
///
/// Drop-in equivalent of `runtime::main_thread` for the tokio comparison
/// tests — keeps test bodies identical:
///
/// ```rust
/// mod common;
/// common::run(async { /* … */ });
/// ```
pub fn run<F: std::future::Future>(fut: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(fut)
}

/// Map a zero-based index to a distinct 127.x.x.x address.
/// Skips .0 host addresses, producing 127.0.0.1, 127.0.0.2, …, 127.0.0.255,
/// 127.0.1.1, … up to ~16 M distinct IPs within the loopback /8 block.
fn ip_for_index(i: usize) -> Ipv4Addr {
    // last octet cycles 1..=255 (255 values), then middle octets increment
    let c = (i % 255) as u8 + 1;
    let b = ((i / 255) % 256) as u8;
    let a = ((i / (255 * 256)) % 256) as u8;
    Ipv4Addr::new(127, a, b, c)
}

/// Blast `server_addr` with TCP traffic from `num_ips` distinct source IPs.
///
/// - `num_ips`      – how many distinct 127.x.x.x source addresses to use.
/// - `num_requests` – total number of write calls (spread evenly across IPs).
/// - `payload_size` – byte length of each write.
///
/// One TCP connection is opened per IP. Each connection sends its share of
/// the requests sequentially, but all connections run concurrently via
/// `tokio::spawn`. The function returns when every byte has been flushed.
///
/// **Linux note**: the entire 127.0.0.0/8 range is reachable without any
/// extra interface configuration, so binding to 127.0.0.2 … 127.255.255.255
/// works out of the box.
pub async fn blast_tcp(
    server_addr: SocketAddr,
    num_ips: usize,
    num_requests: usize,
    payload_size: usize,
    delay: Duration,
) {
    assert!(num_ips > 0, "num_ips must be > 0");
    assert!(num_requests > 0, "num_requests must be > 0");

    let payload = vec![0xABu8; payload_size];

    let base = num_requests / num_ips;
    let remainder = num_requests % num_ips;

    let mut handles = Vec::with_capacity(num_ips);

    for i in 0..num_ips {
        let payload = payload.clone();
        let count = base + if i < remainder { 1 } else { 0 };
        if count == 0 {
            continue;
        }

        let local_ip = ip_for_index(i);

        handles.push(tokio::spawn(async move {
            let local_addr = SocketAddr::V4(SocketAddrV4::new(local_ip, 0));

            let socket = TcpSocket::new_v4().expect("TcpSocket::new_v4");
            socket.bind(local_addr).expect("bind to loopback IP");

            let mut stream = socket
                .connect(server_addr)
                .await
                .expect("connect to server");

            let mut echo_buf = vec![0u8; payload_size];
            for _ in 0..count {
                stream
                    .write_all(&payload)
                    .await
                    .expect("write_all payload");
                stream
                    .read_exact(&mut echo_buf)
                    .await
                    .expect("read_exact echo");
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
            }
        }));
    }

    for h in handles {
        h.await.expect("blast task panicked");
    }
}
