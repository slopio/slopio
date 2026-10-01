//! Which threads the process runs, on which CPUs, with which rings.
//!
//! A [`Topology`] lists dedicated threads, each with its own ring setup and
//! CPU, plus optionally one worker per remaining CPU. Every thread it starts
//! is a peer of every other, reachable by `thread_id` over MSG_RING.
//!
//! ```rust,ignore
//! let mut topology = Topology::new();
//! let db = topology.dedicated::<DbRings, _, _>(
//!     Dedicated {
//!         cpu: Cpu::Auto,
//!         sqpoll: Some(SqPollPlacement { idle: Duration::from_millis(50), cpu: Cpu::Auto }),
//!     },
//!     |ctx| db_loop(ctx),
//! );
//! topology.per_core::<HttpRings, _, _>(move |ctx| serve(ctx, db));
//! topology.run();
//! ```

mod cpus;
mod thread;

use std::ops::Range;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use runtime_core::io_driver::{BufRingSet, RingConfig, SqPoll};
use runtime_core::runtime::eventfd;
use runtime_core::runtime::peers::PeerTable;

use crate::telemetry::{DEFAULT_TELEMETRY, DefaultTelemetry, Telemetry};
use thread::Start;

/// Where a thread of the topology runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cpu {
    /// Any CPU the process may run on that nothing else claimed.
    Auto,
    /// This CPU; it must be one the process may run on.
    Fixed(usize),
}

/// A thread of its own, outside the per-core workers.
#[derive(Clone, Copy, Debug)]
pub struct Dedicated {
    pub cpu: Cpu,
    /// Gives its ring a kernel SQ polling thread, on a CPU of its own.
    pub sqpoll: Option<SqPollPlacement>,
}

/// The kernel thread polling a dedicated ring's submission queue.
#[derive(Clone, Copy, Debug)]
pub struct SqPollPlacement {
    /// How long it spins on an empty queue before it sleeps.
    pub idle: Duration,
    pub cpu: Cpu,
}

impl Dedicated {
    fn cpus(&self) -> impl Iterator<Item = Cpu> {
        std::iter::once(self.cpu).chain(self.sqpoll.map(|s| s.cpu))
    }
}

/// What a thread knows about the topology it runs in.
#[derive(Clone, Debug)]
pub struct WorkerContext {
    /// This thread's peer id.
    pub thread_id: usize,
    /// Every peer, dedicated threads included.
    pub num_threads: usize,
    /// The per-core workers' peer ids; dedicated threads come before them.
    pub workers: Range<usize>,
}

type Launch<T> = Box<dyn FnOnce(Start<T>) -> JoinHandle<()> + Send>;
type LaunchEach<T> = Box<dyn Fn(Start<T>) -> JoinHandle<()> + Send>;

struct DedicatedThread<T> {
    placement: Dedicated,
    launch: Launch<T>,
}

pub struct Topology<T> {
    telemetry: T,
    dedicated: Vec<DedicatedThread<T>>,
    workers: Option<LaunchEach<T>>,
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

impl<T: Telemetry + Send + Sync + 'static> Topology<T> {
    pub(crate) fn with_telemetry(telemetry: T) -> Self {
        Self {
            telemetry,
            dedicated: Vec::new(),
            workers: None,
        }
    }

    /// Adds a dedicated thread running the future `f` builds, with the
    /// provided-buffer rings `R`. Returns its peer id.
    pub fn dedicated<R, F, Fut>(&mut self, placement: Dedicated, f: F) -> usize
    where
        R: BufRingSet + 'static,
        F: FnOnce(WorkerContext) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + 'static,
    {
        self.dedicated.push(DedicatedThread {
            placement,
            launch: Box::new(move |start| thread::spawn::<R, T, F, Fut>(start, f)),
        });
        self.dedicated.len() - 1
    }

    /// Runs one worker per CPU left once the dedicated threads are placed,
    /// each with the provided-buffer rings `R` and the future `f` builds.
    pub fn per_core<R, F, Fut>(&mut self, f: F) -> &mut Self
    where
        R: BufRingSet + 'static,
        F: Fn(WorkerContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + 'static,
    {
        assert!(self.workers.is_none(), "per_core set twice");
        let f = Arc::new(f);
        self.workers = Some(Box::new(move |start| {
            let f = f.clone();
            thread::spawn::<R, T, _, Fut>(start, move |ctx| f(ctx))
        }));
        self
    }

    /// Starts every thread and blocks until all of them return.
    pub fn run(self) {
        crate::ignore_sigpipe();
        crate::block_signals_for_thread();

        let requests: Vec<Cpu> = self
            .dedicated
            .iter()
            .flat_map(|d| d.placement.cpus())
            .collect();
        let placed = cpus::place(&cpus::allowed_cpus(), &requests)
            .unwrap_or_else(|e| panic!("cannot place the topology: {e}"));

        let num_workers = match self.workers {
            None => 0,
            Some(_) => {
                // Under a cgroup CPU quota the budget is below the cpuset; reserved CPUs count against it.
                let budget = std::thread::available_parallelism()
                    .unwrap()
                    .get()
                    .saturating_sub(placed.reserved.len());
                let n = budget.min(placed.free.len());
                assert!(
                    n > 0,
                    "no CPU left for the per-core workers once {:?} are reserved",
                    placed.reserved
                );
                n
            }
        };

        let first_worker = self.dedicated.len();
        let num_threads = first_worker + num_workers;
        let fds = (0..num_threads)
            .map(|_| eventfd::create_eventfd())
            .collect::<Vec<_>>();
        crate::setup_sig_handling(fds.clone());

        // Ring fds are published by each thread once it has created its ring.
        let peers = Arc::new(PeerTable::new(&fds));
        let telemetry = Arc::new(self.telemetry);
        let start = |thread_id: usize, cpu: usize, ring: RingConfig| Start {
            ctx: WorkerContext {
                thread_id,
                num_threads,
                workers: first_worker..num_threads,
            },
            cpu,
            ring,
            telemetry: telemetry.clone(),
            event_fd: fds[thread_id],
            peers: peers.clone(),
        };

        let mut handles = Vec::with_capacity(num_threads);
        let mut reserved = placed.reserved.into_iter();
        for (thread_id, dedicated) in self.dedicated.into_iter().enumerate() {
            let cpu = reserved.next().unwrap();
            let ring = RingConfig {
                sqpoll: dedicated.placement.sqpoll.map(|sqpoll| SqPoll {
                    idle: sqpoll.idle,
                    cpu: Some(reserved.next().unwrap() as u32),
                }),
            };
            handles.push((dedicated.launch)(start(thread_id, cpu, ring)));
        }
        if let Some(launch) = &self.workers {
            for (i, &cpu) in placed.free[..num_workers].iter().enumerate() {
                handles.push(launch(start(first_worker + i, cpu, RingConfig::default())));
            }
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }
}
