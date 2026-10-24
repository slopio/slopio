//! An echo server under a world drawn from a seed.
//!
//! No scenario here is written by hand. Each seed draws how many clients
//! connect and when, what each sends, how the bytes split into completions and
//! how far apart they land, and which sends come back short. The server is
//! written the way a program would be, and every run is judged by properties
//! that hold whatever the seed: each connection gets back exactly what it sent,
//! and nothing is left open or unconsumed once the run is over.

use std::fmt::Write as _;
use std::time::Duration;

use runtime_core::deterministic_testing::{
    AcceptEvent, FakeRing, FakeRingHandle, FdKind, ReadEvent, Scripted, WriteEvent, mint, open_fds,
};
use runtime_core::io_driver::operations::sockets::SocketWrapper;
use runtime_core::io_driver::operations::sockets::multi_accept::MultiAccept;
use runtime_core::io_driver::operations::sockets::multi_recv::{CompletionError, MultiRecv};
use runtime_core::io_driver::operations::sockets::send::send_all;
use runtime_core::runtime::{RuntimeInner, spawn, time::Sleep};
use runtime_streams::StreamExt;

// Small buffers so payloads split into many completions, and a pool past the
// ring so the reserve is in play.
runtime_core::define_buf_ring!(
    EchoRing,
    bgid = 0,
    buffer_size = 16,
    ring_size = 8,
    pool_size = 32
);

const BUFFER: usize = 16;

/// How long the server waits for a buffer once the ring runs dry.
const DRY_BACKOFF: Duration = Duration::from_millis(1);

/// Seeds per run. Miri interprets, so it gets a handful.
const SEEDS: u64 = if cfg!(miri) { 4 } else { 1000 };

/// SplitMix64, for drawing the workload. The simulator draws the world below
/// it from the same seed, on its own stream.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `lo..=hi`.
    fn between(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.next() % (hi - lo + 1)
    }

    fn millis(&mut self, lo: u64, hi: u64) -> Duration {
        Duration::from_millis(self.between(lo, hi))
    }
}

/// Bytes that say which connection and which offset they belong to, so a
/// chunk sent to the wrong peer or out of place cannot pass for the right one.
fn marked(conn: usize, len: usize) -> Vec<u8> {
    (0..len)
        .map(|offset| {
            let mut mix = Rng(((conn as u64) << 32) | offset as u64);
            mix.next() as u8
        })
        .collect()
}

/// The program under test: echo every chunk back on the same connection.
async fn echo(conn: SocketWrapper) -> Stats {
    let mut stats = Stats::default();
    let mut recv = MultiRecv::<EchoRing>::new(conn);
    while let Some(chunk) = recv.next().await {
        match chunk {
            Ok(buffer) => {
                stats.chunks += 1;
                // The ring's own buffer goes back out, no copy.
                let (sent, _buffer) = send_all(recv.socket(), buffer).await;
                sent.expect("the echo send failed");
            }
            // The ring ran dry. Re-polling at once would spin until another
            // holder recycles, so back off and let the sends finish.
            Err(CompletionError::Other(code)) if code == -libc::ENOBUFS => {
                stats.dry += 1;
                Sleep::during(DRY_BACKOFF).await;
            }
            Err(other) => panic!("the echo read failed: {other:?}"),
        }
    }
    stats
}

#[derive(Default, Clone, Copy)]
struct Stats {
    chunks: usize,
    dry: usize,
}

/// One client: its descriptor and what it sends.
struct Client {
    fd: i32,
    payload: Vec<u8>,
}

/// Draw a workload and the world around it, and declare both on `handle`.
/// The accept script needs the listener, which only exists once the run has
/// started, so it is handed back to be declared then.
fn draw(seed: u64, handle: &FakeRingHandle) -> (Vec<Client>, Vec<Scripted>) {
    let mut rng = Rng(seed);
    let clients: Vec<Client> = (0..rng.between(1, 6) as usize)
        .map(|conn| Client {
            fd: mint(FdKind::Socket),
            payload: marked(conn, rng.between(1, 300) as usize),
        })
        .collect();

    for client in &clients {
        let gap = (rng.millis(0, 1), rng.millis(1, 4));
        handle.on_multishot_read(
            client.fd,
            [
                ReadEvent::flow(&client.payload[..])
                    .spaced(gap.0, gap.1)
                    .after_between(Duration::ZERO, rng.millis(0, 10)),
                ReadEvent::Eof.after_between(Duration::ZERO, rng.millis(0, 5)),
            ],
        );

        // Each chunk is at least one send, so this many are always consumed.
        let sends = client.payload.len().div_ceil(BUFFER) as u64;
        for _ in 0..rng.between(0, sends) {
            let accepted = rng.between(1, BUFFER as u64) as u32;
            handle.on_write(
                client.fd,
                WriteEvent::Short(accepted).after_between(Duration::ZERO, rng.millis(0, 3)),
            );
        }
    }

    let arrivals = clients
        .iter()
        .map(|client| AcceptEvent::On(client.fd).after_between(Duration::ZERO, rng.millis(0, 20)))
        .collect();
    (clients, arrivals)
}

/// Run the echo server once against the world `seed` draws, and check it.
fn run(seed: u64) -> Result<Vec<Stats>, String> {
    let (fake, handle) = FakeRing::builder().seed(seed).virtual_time().build();
    let (clients, arrivals) = draw(seed, &handle);
    let count = clients.len();

    let script = handle.clone();
    let stats = RuntimeInner::simulated(fake).block_on::<EchoRing, _>(
        async move {
            let mut listener = MultiAccept::new(0).expect("bind");
            // Declared before the first poll of the accept, so it is part of
            // the scenario and not a race with it.
            let arrivals = arrivals.into_iter().map(|arrival| vec![arrival]);
            script.on_multishot_accept(listener.socket().fd(), arrivals);

            let mut handles = Vec::with_capacity(count);
            for _ in 0..count {
                let conn = listener.next().await.expect("a client").expect("accepted");
                handles.push(spawn(echo(conn)));
            }
            drop(listener);

            let mut stats = Vec::with_capacity(count);
            for handle in handles {
                stats.push(handle.await);
            }
            stats
        },
        None,
        |run| run(),
    );

    check(&handle, &clients).map(|()| stats)
}

/// What must hold whatever the seed drew.
fn check(handle: &FakeRingHandle, clients: &[Client]) -> Result<(), String> {
    let mut failures = String::new();

    for client in clients {
        let received = handle.reads(client.fd).concat();
        let echoed = handle.writes(client.fd).concat();
        if received != client.payload {
            let _ = writeln!(
                failures,
                "fd {}: delivered {} of {} bytes{}",
                client.fd,
                received.len(),
                client.payload.len(),
                divergence(&received, &client.payload),
            );
        }
        if echoed != received {
            let _ = writeln!(
                failures,
                "fd {}: echoed {} of {} bytes{}",
                client.fd,
                echoed.len(),
                received.len(),
                divergence(&echoed, &received),
            );
        }
    }

    let open = open_fds();
    if !open.is_empty() {
        let _ = writeln!(failures, "still open: {open:?}");
    }
    let remaining = handle.scenario_remaining();
    if remaining != 0 {
        let _ = writeln!(failures, "{remaining} scripted completion(s) never claimed");
    }
    let inflight = handle.inflight().len();
    if inflight != 0 {
        let _ = writeln!(failures, "{inflight} op(s) still in flight");
    }

    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures)
    }
}

/// Where two byte strings first differ, if within both.
fn divergence(got: &[u8], want: &[u8]) -> String {
    match got.iter().zip(want).position(|(a, b)| a != b) {
        Some(at) => format!(", first differing at byte {at}"),
        None => String::new(),
    }
}

/// [`run`], with a panic inside the run reported as that seed's failure.
fn judged(seed: u64) -> Result<Vec<Stats>, String> {
    std::panic::catch_unwind(|| run(seed)).unwrap_or_else(|panic| {
        let message = panic
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| panic.downcast_ref::<&str>().copied())
            .unwrap_or("a panic with no message");
        Err(format!("{message}\nreplay with DST_SEED={seed}"))
    })
}

/// Every seed, or only `DST_SEED` to replay one.
fn seeds() -> std::ops::Range<u64> {
    match std::env::var("DST_SEED") {
        Ok(seed) => {
            let seed = seed.parse().expect("DST_SEED is a number");
            seed..seed + 1
        }
        Err(_) => 0..SEEDS,
    }
}

#[test]
fn every_client_gets_back_exactly_what_it_sent() {
    let (mut runs, mut chunks, mut dry, mut clients) = (0, 0, 0, 0);
    for seed in seeds() {
        runs += 1;
        let stats = judged(seed).unwrap_or_else(|failure| panic!("seed {seed}:\n{failure}"));
        clients += stats.len();
        chunks += stats.iter().map(|s| s.chunks).sum::<usize>();
        dry += stats.iter().map(|s| s.dry).sum::<usize>();
    }
    eprintln!("{runs} seed(s): {clients} clients, {chunks} chunks echoed, ring dry {dry} times");
}

#[test]
fn a_seed_replays_the_same_run() {
    for seed in 0..SEEDS.min(8) {
        let first = run(seed).map(|s| s.iter().map(|s| (s.chunks, s.dry)).collect::<Vec<_>>());
        let again = run(seed).map(|s| s.iter().map(|s| (s.chunks, s.dry)).collect::<Vec<_>>());
        assert_eq!(first, again, "seed {seed}");
    }
}
