//! Groups of threads with their own ring mode next to the per-core workers:
//! placed on CPUs of their own, SQ threads pinned too, all reachable from
//! every worker.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use slopio::io_driver::Message;
use slopio::runtime::select::{SelectResult, select};
use slopio::runtime::time::Sleep;
use slopio::runtime::{recv_msg_from_peer, send_msg_to_peer};
use slopio::{RingOptions, Threads, Topology, WorkerContext};

const TIMEOUT: Duration = Duration::from_secs(5);

struct Query {
    from: usize,
}

struct Answer {
    to: usize,
}

/// # Safety
/// The next inbound message must be a `Message<T>`.
async unsafe fn recv_or_timeout<T: 'static>(who: &str) -> Arc<Message<T>> {
    match select(unsafe { recv_msg_from_peer::<T>() }, Sleep::during(TIMEOUT)).await {
        SelectResult::First(msg) => msg,
        SelectResult::Second(()) => panic!("{who}: timed out waiting for a message"),
    }
}

fn affinity_of(tid: libc::pid_t) -> Vec<usize> {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        assert_eq!(
            libc::sched_getaffinity(tid, std::mem::size_of::<libc::cpu_set_t>(), &mut set),
            0
        );
        (0..libc::CPU_SETSIZE as usize)
            .filter(|&c| libc::CPU_ISSET(c, &set))
            .collect()
    }
}

/// The kernel names a ring's SQ thread `iou-sqp-<tid of its creator>`, once
/// that thread first runs: retried for a while when one is `expected`.
fn sq_thread_of(creator: libc::pid_t, expected: bool) -> Option<libc::pid_t> {
    let name = format!("iou-sqp-{creator}");
    let find = || {
        std::fs::read_dir("/proc/self/task")
            .unwrap()
            .filter_map(|task| {
                let task = task.ok()?;
                let comm = std::fs::read_to_string(task.path().join("comm")).ok()?;
                (comm.trim() == name).then(|| task.file_name().to_str()?.parse().ok())?
            })
            .next()
    };
    let tries = if expected { 100 } else { 1 };
    (0..tries).find_map(|_| {
        find().or_else(|| {
            std::thread::sleep(Duration::from_millis(5));
            None
        })
    })
}

/// One started thread: its context, its CPUs, and its SQ thread's if any.
#[derive(Debug)]
struct Seen {
    ctx: WorkerContext,
    cpus: Vec<usize>,
    sq_cpus: Option<Vec<usize>>,
}

fn record(seen: &Mutex<Vec<Seen>>, ctx: &WorkerContext, sqpoll: bool) {
    let tid = unsafe { libc::gettid() };
    seen.lock().unwrap().push(Seen {
        ctx: ctx.clone(),
        cpus: affinity_of(0),
        sq_cpus: sq_thread_of(tid, sqpoll).map(affinity_of),
    });
}

/// Every thread on one CPU of its own, SQ threads included.
fn assert_one_cpu_each(seen: &[Seen]) {
    let mut taken = HashSet::new();
    for s in seen {
        for cpus in std::iter::once(&s.cpus).chain(&s.sq_cpus) {
            assert_eq!(cpus.len(), 1, "thread {} pinned", s.ctx.thread_id);
            assert!(taken.insert(cpus[0]), "CPU {} taken twice", cpus[0]);
        }
    }
}

fn sqpoll() -> RingOptions {
    RingOptions {
        sqpoll: true,
        defer_taskrun: false,
        coop_taskrun: false,
        sqpoll_idle: Duration::from_millis(10),
        ..Default::default()
    }
}

fn cpus_available() -> usize {
    affinity_of(0)
        .len()
        .min(std::thread::available_parallelism().unwrap().get())
}

#[test]
fn an_sqpoll_group_answers_every_worker() {
    if cpus_available() < 5 {
        eprintln!("skipping: need at least 5 CPUs");
        return;
    }
    let seen = Arc::new(Mutex::new(Vec::new()));

    let mut topology = Topology::new();
    let log = seen.clone();
    let db = topology.group::<(), _, _>(Threads::Auto(2), sqpoll(), move |ctx| {
        let log = log.clone();
        async move {
            record(&log, &ctx, true);
            // Workers alternate between the two db threads by peer id.
            let mine = ctx
                .workers
                .clone()
                .filter(|w| ctx.group.start + w % ctx.group.len() == ctx.thread_id)
                .count();
            for _ in 0..mine {
                let query = unsafe { recv_or_timeout::<Query>("db") }.await;
                // A timed wait on an SQPOLL ring.
                Sleep::during(Duration::from_millis(20)).await;
                let to = query.payload.from;
                send_msg_to_peer(to, Answer { to });
            }
        }
    });
    let log = seen.clone();
    let group = db.clone();
    topology.per_core::<(), _, _>(RingOptions::default(), move |ctx| {
        let log = log.clone();
        let db = group.start + ctx.thread_id % group.len();
        async move {
            record(&log, &ctx, false);
            send_msg_to_peer(
                db,
                Query {
                    from: ctx.thread_id,
                },
            );
            let answer = unsafe { recv_or_timeout::<Answer>("worker") }.await;
            assert_eq!(answer.payload.to, ctx.thread_id);
        }
    });
    topology.run();

    let seen = std::mem::take(&mut *seen.lock().unwrap());
    assert_eq!(db, 0..2);
    assert_one_cpu_each(&seen);
    for s in &seen {
        if db.contains(&s.ctx.thread_id) {
            assert_eq!(s.ctx.group, db);
            assert!(
                s.sq_cpus.is_some(),
                "db {} has an SQ thread",
                s.ctx.thread_id
            );
        } else {
            assert_eq!(s.ctx.group, s.ctx.workers);
            assert!(
                s.sq_cpus.is_none(),
                "worker {} has no SQ thread",
                s.ctx.thread_id
            );
        }
    }
    assert_eq!(seen.len(), 2 + seen[0].ctx.workers.len());
}

#[test]
fn sqpoll_workers_take_two_cpus_each() {
    if cpus_available() < 2 {
        eprintln!("skipping: need at least 2 CPUs");
        return;
    }
    let seen = Arc::new(Mutex::new(Vec::new()));
    let log = seen.clone();
    let mut topology = Topology::new();
    topology.per_core::<(), _, _>(sqpoll(), move |ctx| {
        let log = log.clone();
        async move {
            record(&log, &ctx, true);
            // Ping the next worker, answer the previous one.
            let n = ctx.workers.len();
            let next = ctx.workers.start + (ctx.thread_id - ctx.workers.start + 1) % n;
            send_msg_to_peer(
                next,
                Query {
                    from: ctx.thread_id,
                },
            );
            let query = unsafe { recv_or_timeout::<Query>("worker") }.await;
            assert_eq!(
                query.payload.from,
                ctx.workers.start + (ctx.thread_id + n - 1) % n
            );
        }
    });
    topology.run();

    let seen = std::mem::take(&mut *seen.lock().unwrap());
    assert_eq!(seen.len(), cpus_available() / 2);
    assert_one_cpu_each(&seen);
    assert!(seen.iter().all(|s| s.sq_cpus.is_some()));
}

/// A group pinging between its two threads after a timer, on a ring built
/// with `ring`.
fn ping_within_a_group(ring: RingOptions) {
    let mut topology = Topology::new();
    let group = topology.group::<(), _, _>(Threads::Auto(2), ring, |ctx| async move {
        let other = if ctx.thread_id == ctx.group.start {
            ctx.group.end - 1
        } else {
            ctx.group.start
        };
        Sleep::during(Duration::from_millis(20)).await;
        send_msg_to_peer(
            other,
            Query {
                from: ctx.thread_id,
            },
        );
        let query = unsafe { recv_or_timeout::<Query>("group") }.await;
        assert_eq!(query.payload.from, other);
    });
    assert_eq!(group, 0..2);
    topology.run();
}

#[test]
fn a_group_without_defer_taskrun_runs_timers_and_messages() {
    if cpus_available() < 2 {
        eprintln!("skipping: need at least 2 CPUs");
        return;
    }
    ping_within_a_group(RingOptions {
        defer_taskrun: false,
        ..RingOptions::default()
    });
}

#[test]
fn a_group_without_any_task_run_flag_runs_timers_and_messages() {
    if cpus_available() < 2 {
        eprintln!("skipping: need at least 2 CPUs");
        return;
    }
    ping_within_a_group(RingOptions {
        defer_taskrun: false,
        coop_taskrun: false,
        ..RingOptions::default()
    });
}

#[test]
#[should_panic(expected = "sqpoll excludes defer_taskrun")]
fn sqpoll_with_defer_taskrun_is_refused_when_declared() {
    Topology::new().group::<(), _, _>(
        Threads::Auto(1),
        RingOptions {
            sqpoll: true,
            ..RingOptions::default()
        },
        |_| async {},
    );
}

#[test]
fn a_cpu_outside_the_mask_is_refused_before_anything_starts() {
    let outside = (0..libc::CPU_SETSIZE as usize)
        .find(|c| !affinity_of(0).contains(c))
        .unwrap();
    let panicked = std::thread::spawn(move || {
        let mut topology = Topology::new();
        topology.group::<(), _, _>(
            Threads::Fixed(vec![outside]),
            RingOptions::default(),
            |_| async {},
        );
        topology.run();
    })
    .join()
    .unwrap_err();
    let message = panicked.downcast_ref::<String>().unwrap();
    assert!(message.contains(&format!("CPU {outside}")), "{message}");
}
