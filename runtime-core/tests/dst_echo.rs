//! An echo server under a world drawn from a seed.
//!
//! No scenario here is written by hand. Each seed draws how many clients
//! connect and when, what each sends, how the bytes split into completions and
//! how far apart they land, and which sends come back short. It also draws
//! which kinds of fault this run suffers — connections cut partway, reads that
//! stumble, sends that fail — and which clients they hit. The server is written
//! the way a program would be: it never panics, and an error ends only the
//! connection it happened on.
//!
//! Every run is judged by properties that hold whatever the seed: a healthy
//! client gets back exactly what it sent; a faulted one gets back a prefix of
//! what reached the server, and its connection ends on the fault; nothing is
//! left open, owed or in flight. `DST_SEED=<n>` replays one seed.

use std::fmt::Write as _;
use std::time::Duration;

use runtime_core::deterministic_testing::{
    AcceptEvent, Cut, FakeRing, FakeRingHandle, FdKind, Hiccup, Peer, ReadEvent, Scripted,
    WriteEvent, mint, open_fds,
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

    /// True one time in `n`.
    fn one_in(&mut self, n: u64) -> bool {
        self.next() % n == 0
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

/// How the server's side of a connection ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum End {
    /// The client closed after sending everything.
    Eof,
    /// A read failed; the connection is over.
    ReadFailed,
    /// An echo send failed; the connection is over.
    SendFailed,
}

/// The program under test: echo every chunk back on the same connection, and
/// end the connection, not the server, on an error.
async fn echo(conn: SocketWrapper) -> Stats {
    let mut stats = Stats {
        fd: conn.fd(),
        chunks: 0,
        dry: 0,
        end: End::Eof,
    };
    let mut recv = MultiRecv::<EchoRing>::new(conn);
    while let Some(chunk) = recv.next().await {
        match chunk {
            Ok(buffer) => {
                stats.chunks += 1;
                // The ring's own buffer goes back out, no copy.
                let (sent, _buffer) = send_all(recv.socket(), buffer).await;
                if sent.is_err() {
                    stats.end = End::SendFailed;
                    break;
                }
            }
            // The ring ran dry. Re-polling at once would spin until another
            // holder recycles, so back off and let the sends finish.
            Err(CompletionError::Other(code)) if code == -libc::ENOBUFS => {
                stats.dry += 1;
                Sleep::during(DRY_BACKOFF).await;
            }
            Err(_) => {
                stats.end = End::ReadFailed;
                break;
            }
        }
    }
    stats
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Stats {
    fd: i32,
    chunks: usize,
    dry: usize,
    end: End,
}

/// Which kinds of fault a run suffers. Drawn per seed, so some runs are calm
/// and others hostile along one axis, rather than every run a little of all.
#[derive(Debug, Clone, Copy, Default)]
struct Faults {
    cuts: bool,
    hiccups: bool,
    send_errors: bool,
}

/// A fault that ends a client's connection.
#[derive(Debug, Clone, Copy)]
enum Fatal {
    /// The client's flow is cut.
    Cut(Cut),
    /// One of the server's sends to the client fails with this errno.
    Send(i32),
}

/// One client: its descriptor, what it sends, and what kills it, if anything.
struct Client {
    fd: i32,
    payload: Vec<u8>,
    fatal: Option<Fatal>,
    hiccups: usize,
}

/// Draw a workload and the world around it, and declare both on `handle`.
/// The accept script needs the listener, which only exists once the run has
/// started, so it is handed back to be declared then.
fn draw(seed: u64, handle: &FakeRingHandle) -> (Vec<Client>, Vec<Scripted>, Faults) {
    let mut rng = Rng(seed);
    let faults = Faults {
        cuts: rng.one_in(2),
        hiccups: rng.one_in(2),
        send_errors: rng.one_in(2),
    };
    let mut clients: Vec<Client> = (0..rng.between(1, 6) as usize)
        .map(|conn| Client {
            fd: mint(FdKind::Socket),
            payload: marked(conn, rng.between(1, 300) as usize),
            fatal: None,
            hiccups: 0,
        })
        .collect();

    for client in &mut clients {
        let gap = (rng.millis(0, 1), rng.millis(1, 4));
        let mut flow = ReadEvent::flow(&client.payload[..]).spaced(gap.0, gap.1);
        if faults.cuts && rng.one_in(3) {
            let cut = if rng.one_in(2) {
                Cut::Reset
            } else {
                Cut::Timeout
            };
            flow = flow.cut(cut);
            client.fatal = Some(Fatal::Cut(cut));
        }
        if faults.hiccups {
            client.hiccups = rng.between(0, 3) as usize;
            for _ in 0..client.hiccups {
                let hiccup = if rng.one_in(2) {
                    Hiccup::Interrupted
                } else {
                    Hiccup::Ended
                };
                flow = flow.hiccup(hiccup);
            }
        }
        handle.on_multishot_read(
            client.fd,
            [
                flow.after_between(Duration::ZERO, rng.millis(0, 10)),
                ReadEvent::Eof.after_between(Duration::ZERO, rng.millis(0, 5)),
            ],
        );

        // Each chunk is at least one send, so this many are always reached
        // unless the connection dies first.
        let sends = client.payload.len().div_ceil(BUFFER) as u64;
        let mut writes: Vec<WriteEvent> = (0..rng.between(0, sends))
            .map(|_| WriteEvent::Short(rng.between(1, BUFFER as u64) as u32))
            .collect();
        if faults.send_errors && client.fatal.is_none() && rng.one_in(4) {
            let at = rng.between(0, sends - 1) as usize;
            let errno = if rng.one_in(2) {
                libc::EPIPE
            } else {
                libc::ECONNRESET
            };
            while writes.len() <= at {
                writes.push(WriteEvent::All);
            }
            writes[at] = WriteEvent::Error(errno);
            client.fatal = Some(Fatal::Send(errno));
        }
        for write in writes {
            handle.on_write(
                client.fd,
                write.after_between(Duration::ZERO, rng.millis(0, 3)),
            );
        }
    }

    let arrivals = clients
        .iter()
        .map(|client| AcceptEvent::On(client.fd).after_between(Duration::ZERO, rng.millis(0, 20)))
        .collect();
    (clients, arrivals, faults)
}

/// What one run produced, for the counters and the replay check.
#[derive(Debug, PartialEq, Eq)]
struct Report {
    stats: Vec<Stats>,
    cuts: usize,
    send_errors: usize,
    hiccups: usize,
}

/// Run the echo server once against the world `seed` draws, and check it.
fn run(seed: u64) -> Result<Report, String> {
    let (fake, handle) = FakeRing::builder().seed(seed).virtual_time().build();
    let (clients, arrivals, _faults) = draw(seed, &handle);
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
            while handles.len() < count {
                match listener.next().await {
                    Some(Ok(conn)) => handles.push(spawn(echo(conn))),
                    // A failed accept costs one connection, not the server.
                    Some(Err(_)) => continue,
                    None => break,
                }
            }
            drop(listener);

            let mut stats = Vec::with_capacity(handles.len());
            for handle in handles {
                stats.push(handle.await);
            }
            stats
        },
        None,
        |run| run(),
    );

    check(&handle, &clients, &stats)?;
    let fatal = |want: fn(&Fatal) -> bool| {
        clients
            .iter()
            .filter(|c| c.fatal.as_ref().is_some_and(want))
            .count()
    };
    Ok(Report {
        cuts: fatal(|f| matches!(f, Fatal::Cut(_))),
        send_errors: fatal(|f| matches!(f, Fatal::Send(_))),
        hiccups: clients.iter().map(|c| c.hiccups).sum(),
        stats,
    })
}

/// What must hold whatever the seed drew.
fn check(handle: &FakeRingHandle, clients: &[Client], stats: &[Stats]) -> Result<(), String> {
    let mut failures = String::new();

    for client in clients {
        let fd = client.fd;
        let received = handle.reads(fd).concat();
        let echoed = handle.writes(fd).concat();
        let Some(served) = stats.iter().find(|s| s.fd == fd) else {
            let _ = writeln!(failures, "fd {fd}: never served");
            continue;
        };

        match client.fatal {
            None => {
                if received != client.payload {
                    let _ = writeln!(
                        failures,
                        "fd {fd}: delivered {} of {} bytes{}",
                        received.len(),
                        client.payload.len(),
                        divergence(&received, &client.payload),
                    );
                }
                if echoed != received {
                    let _ = writeln!(
                        failures,
                        "fd {fd}: echoed {} of {} bytes{}",
                        echoed.len(),
                        received.len(),
                        divergence(&echoed, &received),
                    );
                }
                if served.end != End::Eof || handle.peer(fd) != Some(Peer::Closed) {
                    let _ = writeln!(
                        failures,
                        "fd {fd}: healthy, yet ended on {:?} with the peer {:?}",
                        served.end,
                        handle.peer(fd),
                    );
                }
            }
            Some(fatal) => {
                // Whatever reached the server is what the client sent, and
                // whatever came back is what reached the server, in order.
                if !client.payload.starts_with(&received) {
                    let _ = writeln!(
                        failures,
                        "fd {fd}: delivered bytes the client never sent{}",
                        divergence(&received, &client.payload),
                    );
                }
                if !received.starts_with(&echoed) {
                    let _ = writeln!(
                        failures,
                        "fd {fd}: echoed bytes that never arrived{}",
                        divergence(&echoed, &received),
                    );
                }
                if served.end == End::Eof || handle.peer(fd) != Some(Peer::Reset) {
                    let _ = writeln!(
                        failures,
                        "fd {fd}: {fatal:?}, yet ended on {:?} with the peer {:?}",
                        served.end,
                        handle.peer(fd),
                    );
                }
            }
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
fn judged(seed: u64) -> Result<Report, String> {
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
fn every_client_gets_back_what_reached_the_server() {
    let (mut runs, mut clients, mut chunks, mut dry) = (0, 0, 0, 0);
    let (mut cuts, mut send_errors, mut hiccups) = (0, 0, 0);
    let (mut read_failed, mut send_failed) = (0, 0);
    for seed in seeds() {
        runs += 1;
        let report = judged(seed).unwrap_or_else(|failure| panic!("seed {seed}:\n{failure}"));
        clients += report.stats.len();
        chunks += report.stats.iter().map(|s| s.chunks).sum::<usize>();
        dry += report.stats.iter().map(|s| s.dry).sum::<usize>();
        read_failed += report
            .stats
            .iter()
            .filter(|s| s.end == End::ReadFailed)
            .count();
        send_failed += report
            .stats
            .iter()
            .filter(|s| s.end == End::SendFailed)
            .count();
        (cuts, send_errors, hiccups) = (
            cuts + report.cuts,
            send_errors + report.send_errors,
            hiccups + report.hiccups,
        );
    }
    eprintln!(
        "{runs} seed(s): {clients} clients, {chunks} chunks echoed, ring dry {dry} times; \
         {cuts} cut, {send_errors} send error(s), {hiccups} hiccup(s) drawn; \
         the server saw {read_failed} read and {send_failed} send failure(s)",
    );
}

#[test]
fn a_seed_replays_the_same_run() {
    for seed in 0..SEEDS.min(8) {
        assert_eq!(run(seed), run(seed), "seed {seed}");
    }
}
