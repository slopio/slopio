//! Which threads the process runs, on which CPUs, with which rings.
//!
//! A [`Topology`] lists groups of threads that do the same work, each group
//! with its own ring options and CPUs, plus optionally one worker per remaining
//! CPU. Every thread it starts is a peer of every other, reachable by
//! `thread_id` over MSG_RING.
//!
//! ```rust,ignore
//! let mut topology = Topology::new();
//! let db = topology.group::<DbRings, _, _>(
//!     Threads::Auto(2),
//!     RingOptions { sqpoll: true, defer_taskrun: false, coop_taskrun: false, ..Default::default() },
//!     |ctx| db_loop(ctx),
//! );
//! topology.per_core::<HttpRings, _, _>(RingOptions::default(), move |ctx| serve(ctx, db.clone()));
//! topology.run();
//! ```

mod cpus;
mod thread;

use std::ops::Range;
use std::sync::Arc;
use std::thread::JoinHandle;

use runtime_core::io_driver::{BufRingSet, RingOptions};
use runtime_core::runtime::eventfd;
use runtime_core::runtime::peers::PeerTable;

use crate::telemetry::{DEFAULT_TELEMETRY, DefaultTelemetry, Telemetry};
use cpus::Cpu;
use thread::Start;

/// How many threads a group runs, and where.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Threads {
    /// This many, each on a CPU nothing else claimed.
    Auto(usize),
    /// One per listed CPU; each must be one the process may run on.
    Fixed(Vec<usize>),
}

/// What a thread knows about the topology it runs in.
#[derive(Clone, Debug)]
pub struct WorkerContext {
    /// This thread's peer id.
    pub thread_id: usize,
    /// Every peer, all groups included.
    pub num_threads: usize,
    /// The peer ids of this thread's own group.
    pub group: Range<usize>,
    /// The per-core workers' peer ids; every other group comes before them.
    pub workers: Range<usize>,
}

type LaunchEach<T> = Box<dyn Fn(Start<T>) -> JoinHandle<()> + Send>;

struct Group<T> {
    cpus: Vec<Cpu>,
    ring: RingOptions,
    launch: LaunchEach<T>,
}

struct PerCore<T> {
    ring: RingOptions,
    launch: LaunchEach<T>,
}

pub struct Topology<T> {
    telemetry: T,
    groups: Vec<Group<T>>,
    per_core: Option<PerCore<T>>,
    steer_connections: bool,
}

impl Topology<DefaultTelemetry> {
    pub fn new() -> Self {
        Self::with_telemetry(DEFAULT_TELEMETRY)
    }
}

impl Default for Topology<DefaultTelemetry> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "tracing")]
impl<C, M> Topology<M>
where
    C: crate::tracing::Collector + 'static,
    M: Fn() -> C + Send + Sync + 'static,
{
    /// Every thread exports its spans through its own collector, built on the
    /// thread by `collector`, instead of printing them to stderr.
    pub fn traced(collector: M) -> Self {
        Self::with_telemetry(collector)
    }
}

fn launcher<R, T, F, Fut>(f: F) -> LaunchEach<T>
where
    R: BufRingSet + 'static,
    T: Telemetry + Send + Sync + 'static,
    F: Fn(WorkerContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + 'static,
{
    let f = Arc::new(f);
    Box::new(move |start| {
        let f = f.clone();
        thread::spawn::<R, T, _, Fut>(start, move |ctx| f(ctx))
    })
}

impl<T: Telemetry + Send + Sync + 'static> Topology<T> {
    pub(crate) fn with_telemetry(telemetry: T) -> Self {
        Self {
            telemetry,
            groups: Vec::new(),
            per_core: None,
            steer_connections: true,
        }
    }

    /// Adds a group of `threads` running the future `f` builds, with the
    /// provided-buffer rings `R`, built with `ring`. Under `sqpoll` each
    /// thread's SQ thread gets a CPU of its own. Returns their peer ids.
    pub fn group<R, F, Fut>(&mut self, threads: Threads, ring: RingOptions, f: F) -> Range<usize>
    where
        R: BufRingSet + 'static,
        F: Fn(WorkerContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + 'static,
    {
        let cpus: Vec<Cpu> = match threads {
            Threads::Auto(n) => vec![Cpu::Auto; n],
            Threads::Fixed(cpus) => cpus.into_iter().map(Cpu::Fixed).collect(),
        };
        assert!(!cpus.is_empty(), "a group needs at least one thread");
        if let Err(e) = ring.check() {
            panic!("invalid ring options {ring:?}: {e}");
        }
        let start = self.groups.iter().map(|g| g.cpus.len()).sum::<usize>();
        let ids = start..start + cpus.len();
        self.groups.push(Group {
            cpus,
            ring,
            launch: launcher::<R, T, F, Fut>(f),
        });
        ids
    }

    /// Runs one worker per CPU left once the groups are placed, each with the
    /// provided-buffer rings `R`, built with `ring`, and the future `f` builds.
    /// Under `sqpoll` each worker takes two CPUs, its own and its SQ thread's.
    pub fn per_core<R, F, Fut>(&mut self, ring: RingOptions, f: F) -> &mut Self
    where
        R: BufRingSet + 'static,
        F: Fn(WorkerContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + 'static,
    {
        assert!(self.per_core.is_none(), "per_core set twice");
        if let Err(e) = ring.check() {
            panic!("invalid ring options {ring:?}: {e}");
        }
        self.per_core = Some(PerCore {
            ring,
            launch: launcher::<R, T, F, Fut>(f),
        });
        self
    }

    /// Whether each thread's listeners ask the kernel for the connections its
    /// own CPU receives, `SO_INCOMING_CPU`: see
    /// [`runtime_core::net::set_incoming_cpu`]. On by default; without the
    /// NIC's queues on the threads' CPUs it changes nothing.
    pub fn steer_connections(&mut self, on: bool) -> &mut Self {
        self.steer_connections = on;
        self
    }

    /// Starts every thread and blocks until all of them return.
    pub fn run(self) {
        crate::ignore_sigpipe();
        crate::block_signals_for_thread();

        let mut requests = Vec::new();
        for group in &self.groups {
            requests.extend(&group.cpus);
            if group.ring.sqpoll {
                requests.extend(vec![Cpu::Auto; group.cpus.len()]);
            }
        }
        let placed = cpus::place(&cpus::allowed_cpus(), &requests)
            .unwrap_or_else(|e| panic!("cannot place the topology: {e}"));

        let num_workers = match &self.per_core {
            None => 0,
            Some(per_core) => {
                // Under a cgroup CPU quota the budget is below the cpuset; reserved CPUs count against it.
                let budget = std::thread::available_parallelism()
                    .unwrap()
                    .get()
                    .saturating_sub(placed.reserved.len());
                let cpus_each = if per_core.ring.sqpoll { 2 } else { 1 };
                let n = cpus::per_core_count(budget, placed.free.len(), cpus_each);
                assert!(
                    n > 0,
                    "no CPU left for the per-core workers once {:?} are reserved",
                    placed.reserved
                );
                n
            }
        };

        let first_worker = self.groups.iter().map(|g| g.cpus.len()).sum::<usize>();
        let num_threads = first_worker + num_workers;
        let workers = first_worker..num_threads;
        let fds = (0..num_threads)
            .map(|_| eventfd::create_eventfd())
            .collect::<Vec<_>>();
        crate::setup_sig_handling(fds.clone());

        // Ring fds are published by each thread once it has created its ring.
        let peers = Arc::new(PeerTable::new(&fds));
        let telemetry = Arc::new(self.telemetry);
        let steer = self.steer_connections;
        let start =
            |thread_id: usize, group: Range<usize>, cpu: usize, ring: RingOptions, sq_cpu| Start {
                ctx: WorkerContext {
                    thread_id,
                    num_threads,
                    group,
                    workers: workers.clone(),
                },
                cpu,
                incoming_cpu: steer.then_some(cpu),
                ring,
                sq_cpu,
                telemetry: telemetry.clone(),
                event_fd: fds[thread_id],
                peers: peers.clone(),
            };

        let mut handles = Vec::with_capacity(num_threads);
        let mut reserved = placed.reserved.into_iter();
        let mut first = 0;
        for group in &self.groups {
            let n = group.cpus.len();
            let cpus: Vec<usize> = reserved.by_ref().take(n).collect();
            let sq: Vec<usize> = reserved
                .by_ref()
                .take(if group.ring.sqpoll { n } else { 0 })
                .collect();
            let ids = first..first + n;
            for (i, &cpu) in cpus.iter().enumerate() {
                let sq_cpu = sq.get(i).copied();
                handles.push((group.launch)(start(
                    first + i,
                    ids.clone(),
                    cpu,
                    group.ring,
                    sq_cpu,
                )));
            }
            first += n;
        }
        if let Some(per_core) = &self.per_core {
            let (cpus, rest) = placed.free.split_at(num_workers);
            let sq = if per_core.ring.sqpoll {
                &rest[..num_workers]
            } else {
                &[][..]
            };
            for (i, &cpu) in cpus.iter().enumerate() {
                let sq_cpu = sq.get(i).copied();
                handles.push((per_core.launch)(start(
                    first_worker + i,
                    workers.clone(),
                    cpu,
                    per_core.ring,
                    sq_cpu,
                )));
            }
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }
}
