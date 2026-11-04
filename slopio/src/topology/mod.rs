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
//! let http = RuntimeOptions { net: NetOptions { steer_connections: true }, ..Default::default() };
//! topology.per_core::<HttpRings, _, _>(http, move |ctx| serve(ctx, db.clone()));
//! topology.run();
//! ```

mod cpus;
mod nic;
mod plan;
mod thread;

use std::ops::Range;
use std::sync::Arc;
use std::thread::JoinHandle;

use slopio_core::io_driver::BufRingSet;
use slopio_core::runtime::RuntimeOptions;
use slopio_core::runtime::eventfd;
use slopio_core::runtime::peers::PeerTable;

use crate::telemetry::{DEFAULT_TELEMETRY, DefaultTelemetry, Telemetry};
use cpus::Cpu;
use plan::{Placed, Plan};
pub use slopio_machine::{Machine, Nic, PacketCpusError, RxQueue};
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
    options: RuntimeOptions,
    launch: LaunchEach<T>,
}

struct PerCore<T> {
    options: RuntimeOptions,
    launch: LaunchEach<T>,
}

pub struct Topology<T> {
    telemetry: T,
    groups: Vec<Group<T>>,
    per_core: Option<PerCore<T>>,
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

fn checked(options: RuntimeOptions) -> RuntimeOptions {
    if let Err(e) = options.ring.check() {
        panic!("invalid ring options {:?}: {e}", options.ring);
    }
    options
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
        }
    }

    /// Adds a group of `threads` running the future `f` builds, with the
    /// provided-buffer rings `R`, built with `options`. Under `sqpoll` each
    /// thread's SQ thread gets a CPU of its own. Returns their peer ids.
    pub fn group<R, F, Fut>(
        &mut self,
        threads: Threads,
        options: impl Into<RuntimeOptions>,
        f: F,
    ) -> Range<usize>
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
        let options = checked(options.into());
        let start = self.groups.iter().map(|g| g.cpus.len()).sum::<usize>();
        let ids = start..start + cpus.len();
        self.groups.push(Group {
            cpus,
            options,
            launch: launcher::<R, T, F, Fut>(f),
        });
        ids
    }

    /// Runs one worker per CPU left once the groups are placed, each with the
    /// provided-buffer rings `R`, built with `options`, and the future `f`
    /// builds. Under `sqpoll` each worker takes two CPUs, its own and its SQ
    /// thread's.
    pub fn per_core<R, F, Fut>(&mut self, options: impl Into<RuntimeOptions>, f: F) -> &mut Self
    where
        R: BufRingSet + 'static,
        F: Fn(WorkerContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + 'static,
    {
        assert!(self.per_core.is_none(), "per_core set twice");
        self.per_core = Some(PerCore {
            options: checked(options.into()),
            launch: launcher::<R, T, F, Fut>(f),
        });
        self
    }

    /// Starts every thread and blocks until all of them return.
    pub fn run(self) {
        crate::ignore_sigpipe();
        crate::block_signals_for_thread();

        let mut requests = Vec::new();
        for group in &self.groups {
            requests.extend(&group.cpus);
            if group.options.ring.sqpoll {
                requests.extend(vec![Cpu::Auto; group.cpus.len()]);
            }
        }
        let allowed = cpus::allowed_cpus();
        let placed = cpus::place(&allowed, &requests)
            .unwrap_or_else(|e| panic!("cannot place the topology: {e}"));
        // Under a cgroup CPU quota the budget is below the cpuset; reserved CPUs count against it.
        let parallelism = std::thread::available_parallelism().unwrap().get();

        let mut plan = Plan {
            allowed,
            parallelism,
            groups: Vec::new(),
        };
        let mut launches = Vec::new();
        let mut reserved = placed.reserved.iter().copied();
        let mut first = 0;
        for (n, group) in self.groups.iter().enumerate() {
            let count = group.cpus.len();
            let cpus: Vec<usize> = reserved.by_ref().take(count).collect();
            let sq_take = if group.options.ring.sqpoll { count } else { 0 };
            plan.groups.push(Placed {
                label: format!("group {n}"),
                ids: first..first + count,
                cpus,
                sq_cpus: reserved.by_ref().take(sq_take).collect(),
                options: group.options,
            });
            launches.push(&group.launch);
            first += count;
        }
        let first_worker = first;
        if let Some(per_core) = &self.per_core {
            let budget = parallelism.saturating_sub(placed.reserved.len());
            let cpus_each = if per_core.options.ring.sqpoll { 2 } else { 1 };
            let n = cpus::per_core_count(budget, placed.free.len(), cpus_each);
            assert!(
                n > 0,
                "no CPU left for the per-core workers once {:?} are reserved",
                placed.reserved
            );
            let (cpus, rest) = placed.free.split_at(n);
            let sq_cpus = if per_core.options.ring.sqpoll {
                &rest[..n]
            } else {
                &[][..]
            };
            plan.groups.push(Placed {
                label: "per_core".to_owned(),
                ids: first..first + n,
                cpus: cpus.to_vec(),
                sq_cpus: sq_cpus.to_vec(),
                options: per_core.options,
            });
            launches.push(&per_core.launch);
        }

        let machine = Machine::read();
        let packets = match machine.packet_cpus() {
            Ok(cpus) => format!("NIC packets processed on {}", plan::cpus(&cpus)),
            Err(e) => format!("NIC packets processed on unknown CPUs: {e}"),
        };
        let steering = plan.steering();
        let steered = if steering.is_empty() {
            String::new()
        } else {
            format!("\nslopio:   {}", nic::report(&machine, &steering))
        };
        // One write, so that nothing the threads print lands in the middle of it.
        println!("{plan}\nslopio:   {packets}{steered}");

        let num_threads = plan.groups.iter().map(|g| g.ids.len()).sum();
        let workers = first_worker..num_threads;
        let fds = (0..num_threads)
            .map(|_| eventfd::create_eventfd())
            .collect::<Vec<_>>();
        crate::setup_sig_handling(fds.clone());

        // Ring fds are published by each thread once it has created its ring.
        let peers = Arc::new(PeerTable::new(&fds));
        let telemetry = Arc::new(self.telemetry);
        let mut handles = Vec::with_capacity(num_threads);
        for (group, launch) in plan.groups.iter().zip(launches) {
            for (i, &cpu) in group.cpus.iter().enumerate() {
                let thread_id = group.ids.start + i;
                handles.push(launch(Start {
                    ctx: WorkerContext {
                        thread_id,
                        num_threads,
                        group: group.ids.clone(),
                        workers: workers.clone(),
                    },
                    cpu,
                    options: group.options,
                    sq_cpu: group.sq_cpus.get(i).copied(),
                    telemetry: telemetry.clone(),
                    event_fd: fds[thread_id],
                    peers: peers.clone(),
                }));
            }
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }
}
