//! An echo server under a world drawn from a seed.
//!
//! No scenario here is written by hand. Each seed draws how many clients
//! connect and when, and what each one says: a few rounds of "send a message,
//! wait for exactly that message back", with pauses between them. The clients
//! are [`Dialogue`] peers, so a round only starts once the last one came back,
//! and every echo is checked byte for byte, in order, however it was split.
//!
//! Each seed also draws how hostile the world is, one axis at a time: sends of
//! the server's that drain slowly or are taken only in part, messages cut by a
//! reset, clients that vanish without a word or make a send fail, and a kernel
//! that interrupts or ends multishot reads on its own. Some seeds draw none.
//!
//! The server is written the way a program would be: it never panics, an
//! error ends only the connection it happened on, and a peer quiet for too
//! long is dropped. Every run is judged by what holds whatever the seed: a
//! healthy client plays its whole dialogue; a faulted one is never answered
//! wrong, and its connection ends on the fault; the server never echoes a
//! byte it did not receive; nothing is left open, owed or in flight.
//!
//! `DST_SEED=<n>` replays one seed. A failing seed is replayed by itself with
//! the ring's journal on: the whole journal goes to a file, and the last of it
//! about the connections at fault into the failure. `DST_TRACE=1` keeps the
//! journal of a passing seed too.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::rc::Rc;
use std::time::Duration;

use runtime_core::deterministic_testing::{
    AcceptEvent, Dialogue, DialogueReport, FakeRing, FakeRingHandle, Fault, Faults, FdKind,
    KernelFaults, Scripted, mint, open_fds,
};
use runtime_core::io_driver::BufRingSpec;
use runtime_core::io_driver::operations::sockets::SocketWrapper;
use runtime_core::io_driver::operations::sockets::multi_accept::MultiAccept;
use runtime_core::io_driver::operations::sockets::multi_recv::{CompletionError, MultiRecv};
use runtime_core::io_driver::operations::sockets::send::send_all;
use runtime_core::runtime::select::{SelectResult, select};
use runtime_core::runtime::{RuntimeInner, spawn, time::Sleep};
use runtime_streams::StreamExt;

// Small buffers so messages split into many completions, and a pool past the
// ring so the reserve is in play.
runtime_core::define_buf_ring!(
    EchoRing,
    bgid = 0,
    buffer_size = 16,
    ring_size = 8,
    pool_size = 32
);
// The same buffers, enough of them for hundreds of connections at once.
runtime_core::define_buf_ring!(
    CrowdRing,
    bgid = 1,
    buffer_size = 16,
    ring_size = 64,
    pool_size = 1024
);

/// How long the server waits for a buffer once the ring runs dry.
const DRY_BACKOFF: Duration = Duration::from_millis(1);

/// How long the server gives a quiet client, reading or sending.
const IDLE: Duration = Duration::from_millis(50);

/// Seeds per run. Miri interprets, so it gets a handful.
const SEEDS: u64 = if cfg!(miri) { 4 } else { 1000 };

/// SplitMix64, for drawing the workload. The simulator and each client draw
/// the world below it from the same seed, on streams of their own.
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
        self.next().is_multiple_of(n)
    }

    fn millis(&mut self, lo: u64, hi: u64) -> Duration {
        Duration::from_millis(self.between(lo, hi))
    }

    /// `odds` on half the seeds, none on the others: one axis of hostility.
    fn axis(&mut self, odds: f64) -> f64 {
        if self.one_in(2) { odds } else { 0.0 }
    }
}

/// Bytes that say which client, which round and which offset they belong to,
/// so an echo sent to the wrong client or out of place cannot pass.
fn marked(client: usize, round: usize, len: usize) -> Vec<u8> {
    (0..len)
        .map(|offset| {
            let mut mix = Rng(((client as u64) << 40) | ((round as u64) << 32) | offset as u64);
            mix.next() as u8
        })
        .collect()
}

/// How the server's side of a connection ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum End {
    /// The client closed after its last round.
    Eof,
    /// A read failed; the connection is over.
    ReadFailed,
    /// An echo send failed; the connection is over.
    SendFailed,
    /// The client said nothing for `IDLE`; the server gave up on it.
    Idle,
}

/// What the server did on one connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Served {
    fd: i32,
    chunks: usize,
    dry: usize,
    end: End,
}

/// The program under test: echo every chunk back on the same connection; end
/// the connection, never the server, on an error or a client gone quiet.
async fn echo<R: BufRingSpec>(conn: SocketWrapper) -> Served {
    let mut served = Served {
        fd: conn.fd(),
        chunks: 0,
        dry: 0,
        end: End::Eof,
    };
    let mut recv = MultiRecv::<R>::new(conn);
    served.end = loop {
        match select(recv.next(), Sleep::during(IDLE)).await {
            SelectResult::First(Some(Ok(buffer))) => {
                served.chunks += 1;
                // The ring's own buffer goes back out, no copy.
                match select(send_all(recv.socket(), buffer), Sleep::during(IDLE)).await {
                    SelectResult::First((Ok(()), _)) => {}
                    SelectResult::First((Err(_), _)) => break End::SendFailed,
                    SelectResult::Second(_) => break End::Idle,
                }
            }
            // The ring ran dry. Re-polling at once would spin until another
            // holder recycles, so back off and let the sends finish.
            SelectResult::First(Some(Err(CompletionError::Other(code))))
                if code == -libc::ENOBUFS =>
            {
                served.dry += 1;
                Sleep::during(DRY_BACKOFF).await;
            }
            SelectResult::First(Some(Err(_))) => break End::ReadFailed,
            SelectResult::First(None) => break End::Eof,
            SelectResult::Second(_) => break End::Idle,
        }
    };
    served
}

/// How hostile one run is, drawn per seed one axis at a time, so some runs
/// are calm and others hostile along one axis rather than every run a little
/// of all.
#[derive(Debug, Clone, Copy)]
struct Profile {
    faults: Faults,
    write_latency: Option<Duration>,
    short_writes: f64,
    kernel: KernelFaults,
}

fn profile(rng: &mut Rng) -> Profile {
    Profile {
        // Each the chance a client meets that fault over its whole dialogue,
        // however long: see `spread`.
        faults: Faults {
            send_error: rng.axis(0.15),
            cut_in_send: rng.axis(0.15),
            vanish: rng.axis(0.1),
        },
        write_latency: rng.one_in(2).then(|| rng.millis(1, 5)),
        short_writes: rng.axis(0.3),
        kernel: KernelFaults {
            interrupted: rng.axis(0.05),
            ended: rng.axis(0.1),
        },
    }
}

/// How big a world is: how many clients, how many rounds each, how long a
/// message. Inclusive ranges.
struct Scale {
    clients: (u64, u64),
    rounds: (u64, u64),
    message: (u64, u64),
}

/// A handful of clients saying a few things: many seeds, each quick.
const SMALL: Scale = Scale {
    clients: (1, 6),
    rounds: (1, 4),
    message: (1, 100),
};

/// Hundreds of clients holding long conversations at once.
const CROWD: Scale = Scale {
    clients: (64, 256),
    rounds: (20, 200),
    message: (1, 200),
};

/// Turn odds over a whole dialogue into odds per occasion, so a long dialogue
/// is no likelier to be struck than a short one — only later, at times.
/// A cut can strike each message, a vanishing each step (a send and an
/// expect per message), a failed send each of the server's sends (about one
/// per eight bytes, chunks being up to sixteen).
fn spread(faults: Faults, messages: &[Vec<u8>]) -> Faults {
    let sends = (messages.iter().map(Vec::len).sum::<usize>() as f64 / 8.0).max(1.0);
    Faults {
        send_error: faults.send_error / sends,
        cut_in_send: faults.cut_in_send / messages.len() as f64,
        vanish: faults.vanish / (2 * messages.len()) as f64,
    }
}

/// One client: its descriptor, and the report its dialogue fills in.
struct Client {
    fd: i32,
    report: Rc<RefCell<DialogueReport>>,
}

/// Draw the clients and their dialogues, and attach them. The accept script
/// needs the listener, which only exists once the run has started, so it is
/// handed back to be declared then.
fn draw(
    rng: &mut Rng,
    scale: &Scale,
    profile: Profile,
    handle: &FakeRingHandle,
) -> (Vec<Client>, Vec<Scripted>) {
    let mut clients = Vec::new();
    let mut arrivals = Vec::new();
    for client in 0..rng.between(scale.clients.0, scale.clients.1) as usize {
        let rounds = rng.between(scale.rounds.0, scale.rounds.1) as usize;
        let messages: Vec<Vec<u8>> = (0..rounds)
            .map(|round| {
                let len = rng.between(scale.message.0, scale.message.1) as usize;
                marked(client, round, len)
            })
            .collect();
        let mut dialogue = Dialogue::new()
            .faults(spread(profile.faults, &messages))
            .short_writes(profile.short_writes);
        if let Some(max) = profile.write_latency {
            dialogue = dialogue.write_latency(Duration::ZERO, max);
        }
        for message in messages {
            dialogue = dialogue.send(message.clone()).expect_bytes(message);
            if rng.one_in(2) {
                dialogue = dialogue.wait(rng.millis(0, 5));
            }
        }
        let (peer, report) = dialogue.close().build();

        let fd = mint(FdKind::Socket);
        handle.attach(fd, peer);
        arrivals.push(AcceptEvent::On(fd).after_between(Duration::ZERO, rng.millis(0, 20)));
        clients.push(Client { fd, report });
    }
    (clients, arrivals)
}

/// What one run produced, for the counters and the replay check.
#[derive(Debug, PartialEq, Eq)]
struct Report {
    served: Vec<Served>,
    faults: Vec<Option<Fault>>,
    /// Steps each client played, of how many.
    progress: Vec<(usize, usize)>,
    bytes: Vec<(usize, usize)>,
    elapsed: Duration,
}

/// Run the echo server once against the world `seed` draws, and check it.
fn run(seed: u64) -> Result<Report, String> {
    run_at::<EchoRing>(seed, &SMALL, None)
}

/// [`run`] at any scale. Given a slot, the ring keeps a journal and its
/// handle is left there, to be read even if the run panics.
fn run_at<R: BufRingSpec>(
    seed: u64,
    scale: &Scale,
    journal: Option<&RefCell<Option<FakeRingHandle>>>,
) -> Result<Report, String> {
    let mut rng = Rng(seed);
    let profile = profile(&mut rng);
    let builder = FakeRing::builder()
        .seed(seed)
        .virtual_time()
        .kernel_faults(profile.kernel);
    let builder = if journal.is_some() {
        builder.journal()
    } else {
        builder
    };
    let (fake, handle) = builder.build();
    if let Some(slot) = journal {
        *slot.borrow_mut() = Some(handle.clone());
    }
    let (clients, arrivals) = draw(&mut rng, scale, profile, &handle);
    let count = clients.len();

    let script = handle.clone();
    let served = RuntimeInner::simulated(fake).block_on::<R, _>(
        async move {
            let mut listener = MultiAccept::new(0).expect("bind");
            // Declared before the first poll of the accept, so it is part of
            // the scenario and not a race with it.
            let arrivals = arrivals.into_iter().map(|arrival| vec![arrival]);
            script.on_multishot_accept(listener.socket().fd(), arrivals);

            let mut servers = Vec::with_capacity(count);
            while servers.len() < count {
                match listener.next().await {
                    Some(Ok(conn)) => servers.push(spawn(echo::<R>(conn))),
                    // A failed accept costs one connection, not the server.
                    Some(Err(_)) => continue,
                    None => break,
                }
            }
            drop(listener);

            let mut served = Vec::with_capacity(servers.len());
            for server in servers {
                served.push(server.await);
            }
            served
        },
        None,
        |run| run(),
    );

    check(&handle, &clients, &served)?;
    Ok(Report {
        faults: clients.iter().map(|c| c.report.borrow().fault).collect(),
        progress: clients
            .iter()
            .map(|c| (c.report.borrow().done, c.report.borrow().total))
            .collect(),
        bytes: clients
            .iter()
            .map(|c| {
                (
                    handle.reads(c.fd).concat().len(),
                    handle.writes(c.fd).concat().len(),
                )
            })
            .collect(),
        elapsed: handle.clock().expect("virtual time").elapsed(),
        served,
    })
}

/// What must hold whatever the seed drew.
fn check(handle: &FakeRingHandle, clients: &[Client], served: &[Served]) -> Result<(), String> {
    let mut failures = String::new();

    for client in clients {
        let fd = client.fd;
        let report = client.report.borrow();
        let Some(served) = served.iter().find(|s| s.fd == fd) else {
            let _ = writeln!(failures, "fd {fd}: never served");
            continue;
        };

        // A wrong echo is a wrong echo, fault or not.
        if !report.ok() {
            let _ = writeln!(failures, "fd {fd}: {report:?}");
        }
        let fits = match report.fault {
            None => report.finished() && served.end == End::Eof,
            // Silent, so only the idle timeout can end it.
            Some(Fault::Vanished) => served.end == End::Idle,
            Some(_) => matches!(served.end, End::ReadFailed | End::SendFailed),
        };
        if !fits {
            let _ = writeln!(
                failures,
                "fd {fd}: fault {:?}, but the server ended on {:?} after {} of {} steps",
                report.fault, served.end, report.done, report.total,
            );
        }

        // The server echoes nothing it did not receive, and in order.
        let received = handle.reads(fd).concat();
        let echoed = handle.writes(fd).concat();
        if !received.starts_with(&echoed) {
            let _ = writeln!(
                failures,
                "fd {fd}: echoed bytes that never arrived{}",
                divergence(&echoed, &received),
            );
        }
        if report.fault.is_none() && echoed != received {
            let _ = writeln!(
                failures,
                "fd {fd}: healthy, yet echoed {} of {} bytes",
                echoed.len(),
                received.len(),
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
fn judged(seed: u64) -> Result<Report, String> {
    judge(|| run(seed))
}

fn judge(
    run: impl FnOnce() -> Result<Report, String> + std::panic::UnwindSafe,
) -> Result<Report, String> {
    std::panic::catch_unwind(run).unwrap_or_else(|panic| {
        let message = panic
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| panic.downcast_ref::<&str>().copied())
            .unwrap_or("a panic with no message");
        Err(message.to_string())
    })
}

/// Replay `seed` with the journal on, on a thread of its own, so nothing a
/// failed run left behind on this one is in its way. The whole journal as
/// text, and its last events about `fds` (about anything, if `fds` is empty).
fn trace<R: BufRingSpec>(seed: u64, scale: &'static Scale, fds: Vec<i32>) -> (String, String) {
    std::thread::spawn(move || {
        let slot = RefCell::new(None);
        // The replay fails as the run did; what matters is what it saw.
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            run_at::<R>(seed, scale, Some(&slot))
        }));
        let handle = slot.into_inner().expect("the replay built its ring");
        let all = handle.journal_text(&[], None).unwrap_or_default();
        let tail = handle.journal_text(&fds, Some(60)).unwrap_or_default();
        (all, tail)
    })
    .join()
    .expect("the tracing thread")
}

/// Write `journal` where `cargo` keeps test scratch files; where it went.
fn keep(name: &str, seed: u64, journal: &str) -> String {
    let path = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("{name}-seed-{seed}.journal"));
    match std::fs::write(&path, journal) {
        Ok(()) => path.display().to_string(),
        Err(error) => format!("not written ({error})"),
    }
}

/// A failure, with the journal of its seed replayed: kept whole in a file,
/// and its end about the connections the failure names shown here.
fn explain<R: BufRingSpec>(name: &str, seed: u64, scale: &'static Scale, failure: &str) -> String {
    let fds = fds_in(failure);
    let (all, tail) = trace::<R>(seed, scale, fds.clone());
    let kept = keep(name, seed, &all);
    let about = if fds.is_empty() {
        "the run".to_string()
    } else {
        format!("fd {fds:?}")
    };
    format!(
        "seed {seed}:\n{failure}\nreplay with DST_SEED={seed}; whole journal: {kept}\n\
         last events of {about}:\n{tail}"
    )
}

/// Keep the journal of a passing seed too, when `DST_TRACE` asks for it.
fn trace_if_asked<R: BufRingSpec>(name: &str, seed: u64, scale: &'static Scale) {
    if std::env::var_os("DST_TRACE").is_some() {
        let (all, _) = trace::<R>(seed, scale, Vec::new());
        eprintln!("seed {seed}: journal in {}", keep(name, seed, &all));
    }
}

/// The descriptors a failure names, as `fd <n>`.
fn fds_in(text: &str) -> Vec<i32> {
    let mut fds: Vec<i32> = text
        .match_indices("fd ")
        .filter_map(|(at, _)| {
            let digits: String = text[at + 3..]
                .chars()
                .take_while(char::is_ascii_digit)
                .collect();
            digits.parse().ok()
        })
        .collect();
    fds.sort_unstable();
    fds.dedup();
    fds
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
fn every_client_is_answered_right_or_let_go_cleanly() {
    let (mut runs, mut clients, mut chunks, mut dry) = (0, 0, 0, 0);
    let mut faults: BTreeMap<String, usize> = BTreeMap::new();
    let mut ends: BTreeMap<End, usize> = BTreeMap::new();
    for seed in seeds() {
        runs += 1;
        let report = judged(seed).unwrap_or_else(|failure| {
            panic!(
                "{}",
                explain::<EchoRing>("dst_echo", seed, &SMALL, &failure)
            )
        });
        trace_if_asked::<EchoRing>("dst_echo", seed, &SMALL);
        clients += report.served.len();
        chunks += report.served.iter().map(|s| s.chunks).sum::<usize>();
        dry += report.served.iter().map(|s| s.dry).sum::<usize>();
        for fault in &report.faults {
            let kind = match fault {
                None => "healthy".to_string(),
                Some(Fault::CutInSend { .. }) => "cut in send".to_string(),
                Some(other) => format!("{other:?}").split('(').next().unwrap().to_string(),
            };
            *faults.entry(kind).or_default() += 1;
        }
        for served in &report.served {
            *ends.entry(served.end).or_default() += 1;
        }
    }
    eprintln!(
        "{runs} seed(s): {clients} clients, {chunks} chunks echoed, ring dry {dry} times\n\
         clients: {faults:?}\nserver ends: {ends:?}",
    );
    // Seeds that explore nothing would pass all the same: say so.
    if runs == SEEDS && !cfg!(miri) {
        for kind in ["healthy", "cut in send", "SendError", "Vanished"] {
            assert!(
                faults.contains_key(kind),
                "no client was {kind}: {faults:?}"
            );
        }
        for end in [End::Eof, End::ReadFailed, End::SendFailed, End::Idle] {
            assert!(
                ends.contains_key(&end),
                "no connection ended {end:?}: {ends:?}"
            );
        }
    }
}

#[test]
fn a_seed_replays_the_same_run() {
    for seed in 0..SEEDS.min(8) {
        assert_eq!(run(seed), run(seed), "seed {seed}");
    }
}

/// A run with the journal on, and the journal.
fn journaled(
    seed: u64,
) -> (
    Result<Report, String>,
    Vec<runtime_core::deterministic_testing::Entry>,
) {
    let slot = RefCell::new(None);
    let report = run_at::<EchoRing>(seed, &SMALL, Some(&slot));
    let journal = slot
        .into_inner()
        .and_then(|h| h.journal())
        .expect("journal on");
    (report, journal)
}

#[test]
fn the_journal_changes_nothing_and_replays_alike() {
    for seed in 0..SEEDS.min(8) {
        let plain = run(seed);
        let (watched, journal) = journaled(seed);
        assert_eq!(
            plain, watched,
            "seed {seed}: keeping a journal changed the run"
        );
        let (_, again) = journaled(seed);
        if let Some(at) = journal.iter().zip(&again).position(|(a, b)| a != b) {
            panic!(
                "seed {seed}: the journals part at event {at}:\n  {}\n  {}",
                journal[at], again[at]
            );
        }
        assert_eq!(journal.len(), again.len(), "seed {seed}");
        assert!(!journal.is_empty(), "seed {seed}: an empty journal");
    }
}

#[test]
#[cfg_attr(miri, ignore = "hundreds of clients, interpreted, would take hours")]
fn a_crowd_of_clients_holding_long_dialogues() {
    let seeds = match std::env::var("DST_SEED") {
        Ok(_) => seeds(),
        Err(_) => 0..8,
    };
    for seed in seeds {
        let started = std::time::Instant::now();
        let report = judge(|| run_at::<CrowdRing>(seed, &CROWD, None)).unwrap_or_else(|failure| {
            panic!(
                "{}",
                explain::<CrowdRing>("dst_echo_crowd", seed, &CROWD, &failure)
            )
        });
        trace_if_asked::<CrowdRing>("dst_echo_crowd", seed, &CROWD);
        let echoed: usize = report.bytes.iter().map(|&(_, written)| written).sum();
        let healthy = report.faults.iter().filter(|f| f.is_none()).count();
        // How far into its dialogue each fault struck, in percent.
        let mut struck: Vec<usize> = report
            .faults
            .iter()
            .zip(&report.progress)
            .filter(|(fault, _)| fault.is_some())
            .map(|(_, &(done, total))| 100 * done / total)
            .collect();
        struck.sort_unstable();
        let struck = match (struck.first(), struck.last()) {
            (Some(first), Some(last)) => format!(
                "faults struck at {first}% / {}% / {last}% of their dialogue (first, median, last)",
                struck[struck.len() / 2],
            ),
            _ => "no fault drawn".to_string(),
        };
        eprintln!(
            "seed {seed}: {} clients ({healthy} healthy), {echoed} bytes echoed in {} chunks, \
             {:?} simulated, {:?} real; {struck}",
            report.served.len(),
            report.served.iter().map(|s| s.chunks).sum::<usize>(),
            report.elapsed,
            started.elapsed(),
        );
    }
}
