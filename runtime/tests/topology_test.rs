//! A dedicated SQPOLL thread next to the per-core workers: placed on CPUs of
//! its own, its SQ thread pinned too, and reachable from every worker.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use runtime::io_driver::Message;
use runtime::runtime::select::{SelectResult, select};
use runtime::runtime::time::Sleep;
use runtime::runtime::{recv_msg_from_peer, send_msg_to_peer};
use runtime::{Cpu, Dedicated, SqPollPlacement, Topology};

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

/// The kernel names a ring's SQ thread `iou-sqp-<tid of its creator>`.
fn sq_thread_of(creator: libc::pid_t) -> libc::pid_t {
    let name = format!("iou-sqp-{creator}");
    std::fs::read_dir("/proc/self/task")
        .unwrap()
        .filter_map(|task| {
            let task = task.ok()?;
            let comm = std::fs::read_to_string(task.path().join("comm")).ok()?;
            (comm.trim() == name).then(|| task.file_name().to_str()?.parse().ok())?
        })
        .next()
        .unwrap_or_else(|| panic!("no {name} thread"))
}

#[derive(Debug, Default)]
struct Seen {
    db: Option<(usize, Vec<usize>, Vec<usize>, std::ops::Range<usize>)>,
    workers: Vec<(usize, Vec<usize>)>,
}

#[test]
fn a_dedicated_sqpoll_thread_answers_every_worker() {
    if affinity_of(0).len() < 3 {
        eprintln!("skipping: need at least 3 CPUs");
        return;
    }
    let seen = Arc::new(Mutex::new(Seen::default()));

    let mut topology = Topology::new();
    let record = seen.clone();
    let db = topology.dedicated::<(), _, _>(
        Dedicated {
            cpu: Cpu::Auto,
            sqpoll: Some(SqPollPlacement {
                idle: Duration::from_millis(10),
                cpu: Cpu::Auto,
            }),
        },
        move |ctx| async move {
            let tid = unsafe { libc::gettid() };
            let sq = affinity_of(sq_thread_of(tid));
            record.lock().unwrap().db =
                Some((ctx.thread_id, affinity_of(0), sq, ctx.workers.clone()));
            for _ in ctx.workers.clone() {
                let query = unsafe { recv_or_timeout::<Query>("db") }.await;
                // A timed wait on the SQPOLL ring.
                Sleep::during(Duration::from_millis(20)).await;
                send_msg_to_peer(
                    query.payload.from,
                    Answer {
                        to: query.payload.from,
                    },
                );
            }
        },
    );
    let record = seen.clone();
    topology.per_core::<(), _, _>(move |ctx| {
        let record = record.clone();
        async move {
            record
                .lock()
                .unwrap()
                .workers
                .push((ctx.thread_id, affinity_of(0)));
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
    let (db_id, db_cpus, sq_cpus, workers) = seen.db.expect("db thread ran");
    assert_eq!(db_id, db);
    assert_eq!(db_cpus.len(), 1, "db thread pinned");
    assert_eq!(sq_cpus.len(), 1, "SQ thread pinned");
    assert_ne!(db_cpus, sq_cpus, "db and SQ threads on CPUs of their own");
    assert_eq!(workers, 1..1 + seen.workers.len());
    for (id, cpus) in &seen.workers {
        assert!(workers.contains(id), "worker {id}");
        assert_eq!(cpus.len(), 1, "worker {id} pinned");
        assert!(
            cpus != &db_cpus && cpus != &sq_cpus,
            "worker {id} on a reserved CPU"
        );
    }
}

#[test]
fn a_cpu_outside_the_mask_is_refused_before_anything_starts() {
    let outside = (0..libc::CPU_SETSIZE as usize)
        .find(|c| !affinity_of(0).contains(c))
        .unwrap();
    let panicked = std::thread::spawn(move || {
        let mut topology = Topology::new();
        topology.dedicated::<(), _, _>(
            Dedicated {
                cpu: Cpu::Fixed(outside),
                sqpoll: None,
            },
            |_| async {},
        );
        topology.run();
    })
    .join()
    .unwrap_err();
    let message = panicked.downcast_ref::<String>().unwrap();
    assert!(message.contains(&format!("CPU {outside}")), "{message}");
}
