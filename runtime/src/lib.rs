use std::pin::Pin;
use std::thread::JoinHandle;

use libc::cpu_set_t;

use runtime_core::runtime::eventfd;

// Re-export core
pub use runtime_core::io_driver::{BorrowedBuffer, BufRingSet, BufRingSpec};
pub use runtime_core::runtime::{RuntimeInner, spawn, yield_now};
pub use runtime_core::task::Handle;
pub use runtime_core::{channels, io_driver, net, runtime, task};
// Re-export the buf-ring declaration macro so downstream crates can do
// `runtime::define_buf_ring!(...)` without naming `runtime_core`.
pub use runtime_core::define_buf_ring;

#[cfg(feature = "tracing")]
pub use runtime_core::tracing;

/// What a worker installs around its root future: a span collector under
/// `tracing`, built on the worker by a factory; nothing otherwise.
trait Telemetry {
    fn install<O>(&self, thread_id: Option<usize>, run: impl FnOnce() -> O) -> O;
}

#[cfg(feature = "tracing")]
impl<C: tracing::Collector + 'static, M: Fn() -> C> Telemetry for M {
    fn install<O>(&self, thread_id: Option<usize>, run: impl FnOnce() -> O) -> O {
        if let Some(n) = thread_id {
            tracing::set_thread_id(n);
        }
        let sink = tracing::init_trace_sink(self());
        tracing::TRACING_SINK.set(&sink, run)
    }
}

#[cfg(not(feature = "tracing"))]
impl Telemetry for () {
    fn install<O>(&self, _: Option<usize>, run: impl FnOnce() -> O) -> O {
        run()
    }
}

#[cfg(feature = "tracing")]
const DEFAULT_TELEMETRY: fn() -> tracing::ConsoleCollector = || tracing::ConsoleCollector;
#[cfg(not(feature = "tracing"))]
const DEFAULT_TELEMETRY: () = ();

pub mod http;

#[cfg(not(miri))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

pub fn main_thread<F: Future>(f: F) -> F::Output {
    main_thread_with::<(), F>(f)
}

/// Like [`main_thread`], but installs the provided-buffer rings `R` on the
/// worker thread before running `f`. `R` may be `()`, a single
/// `BufRingSpec`, or a tuple of `BufRingSpec`s.
pub fn main_thread_with<R: BufRingSet, F: Future>(f: F) -> F::Output {
    main_thread_on::<R, _, F>(DEFAULT_TELEMETRY, f)
}

/// Like [`main_thread_with`], but exports spans through the collector
/// `collector` builds instead of printing them to stderr.
#[cfg(feature = "tracing")]
pub fn main_thread_traced<R: BufRingSet, C: tracing::Collector + 'static, F: Future>(
    collector: impl Fn() -> C,
    f: F,
) -> F::Output {
    main_thread_on::<R, _, F>(collector, f)
}

fn main_thread_on<R: BufRingSet, T: Telemetry, F: Future>(telemetry: T, f: F) -> F::Output {
    ignore_sigpipe();
    RuntimeInner::default().block_on::<R, F>(f, None, |run| telemetry.install(None, run))
}

/// Set `SIGPIPE`'s disposition to `SIG_IGN` for the whole process.
///
/// io_uring send/write ops never raise `SIGPIPE` (a dead peer is reported as
/// `-EPIPE` in the CQE), so the runtime's own I/O path is already immune. But
/// libraries layered on top may do *synchronous* `write`/`send` off the ring
/// (e.g. logging to a closed stdout pipe, or a blocking client socket), and we
/// don't control whether they pass `MSG_NOSIGNAL`. Without this, such a write
/// to a closed peer would kill the process with `SIGPIPE`.
///
/// Signal *disposition* is process-wide (it is not part of the per-thread
/// signal mask), so installing it once — before any worker starts — protects
/// every thread. With `SIG_IGN` in place, those synchronous writes fail with
/// `EPIPE` instead, which the calling code can observe and handle.
///
/// Rust's std normally installs this same `SIG_IGN` during runtime init, but
/// we set it explicitly so the guarantee does not depend on that implicit
/// behavior (which can be opted out of, e.g. via `#[unix_sigpipe]` or a
/// `no_main` entry, or reset by a C dependency calling `signal(SIGPIPE, …)`).
fn ignore_sigpipe() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| unsafe {
        if libc::signal(libc::SIGPIPE, libc::SIG_IGN) == libc::SIG_ERR {
            panic!(
                "failed to set SIGPIPE to SIG_IGN: {}",
                std::io::Error::last_os_error()
            );
        }
    });
}

fn block_signals_for_thread() {
    unsafe {
        let mut mask: libc::sigset_t = std::mem::zeroed();
        libc::sigemptyset(&mut mask);
        libc::sigaddset(&mut mask, libc::SIGTERM);
        libc::sigaddset(&mut mask, libc::SIGINT);

        let result = libc::pthread_sigmask(libc::SIG_BLOCK, &mask, std::ptr::null_mut());
        if result != 0 {
            panic!("pthread_sigmask failed with error {result}");
        }
    }
}

fn setup_sig_handling(fds: Vec<i32>) {
    std::thread::spawn(move || unsafe {
        let mut mask: libc::sigset_t = std::mem::zeroed();
        libc::sigemptyset(&mut mask);
        libc::sigaddset(&mut mask, libc::SIGTERM);
        libc::sigaddset(&mut mask, libc::SIGINT);

        let mut sig: libc::c_int = 0;
        libc::sigwait(&mask, &mut sig);
        for fd in fds {
            eventfd::signal_eventfd(fd, sig as u64);
        }
        std::thread::sleep(std::time::Duration::from_secs(30));
        std::process::exit(1);
    });
}

pub struct WorkerContext {
    pub thread_id: usize,
    pub num_threads: usize,
}

pub fn thread_per_core<F>(f: F)
where
    F: Fn(WorkerContext) -> Pin<Box<dyn Future<Output = ()>>> + Send + Sync + 'static,
{
    thread_per_core_with::<(), F>(f)
}

/// Like [`thread_per_core`], but installs the provided-buffer rings `R` on
/// each worker thread before running its future.
pub fn thread_per_core_with<R, F>(f: F)
where
    R: BufRingSet + 'static,
    F: Fn(WorkerContext) -> Pin<Box<dyn Future<Output = ()>>> + Send + Sync + 'static,
{
    thread_per_core_on::<R, _, F>(DEFAULT_TELEMETRY, f)
}

/// Like [`thread_per_core_with`], but each worker exports its spans through
/// its own collector, built on the worker by `collector`, instead of printing
/// them to stderr.
#[cfg(feature = "tracing")]
pub fn thread_per_core_traced<R, C, F>(collector: impl Fn() -> C + Send + Sync + 'static, f: F)
where
    R: BufRingSet + 'static,
    C: tracing::Collector + 'static,
    F: Fn(WorkerContext) -> Pin<Box<dyn Future<Output = ()>>> + Send + Sync + 'static,
{
    thread_per_core_on::<R, _, F>(collector, f)
}

fn thread_per_core_on<R, T, F>(telemetry: T, f: F)
where
    R: BufRingSet + 'static,
    T: Telemetry + Send + Sync + 'static,
    F: Fn(WorkerContext) -> Pin<Box<dyn Future<Output = ()>>> + Send + Sync + 'static,
{
    ignore_sigpipe();
    block_signals_for_thread();
    // Never more workers than CPUs this process may run on; `available_parallelism`
    // can be lower still under a cgroup CPU quota.
    let cpus = allowed_cpus();
    let num_threads: usize = std::thread::available_parallelism().unwrap().into();
    let num_threads = num_threads.min(cpus.len());
    let fds = (0..num_threads)
        .map(|_| eventfd::create_eventfd())
        .collect::<Vec<_>>();

    setup_sig_handling(fds.clone());

    // Shared registry: eventfds filled now, ring fds published by each worker
    // once it has created its ring (barrier sized to num_threads).
    let peers = std::sync::Arc::new(runtime_core::runtime::peers::PeerTable::new(&fds));

    let f = std::sync::Arc::new(f);
    let telemetry = std::sync::Arc::new(telemetry);
    let handles = fds
        .iter()
        .zip(0..num_threads)
        .map(|(sig_handler_fd, n)| {
            let f = f.clone();
            let telemetry = telemetry.clone();
            spawn_worker_on::<R, T, F>(
                n,
                cpus[n],
                num_threads,
                f,
                telemetry,
                *sig_handler_fd,
                peers.clone(),
            )
        })
        .collect::<Vec<_>>();

    for handle in handles {
        handle.join().unwrap();
    }
}

/// The CPUs the calling thread may run on, in ascending order. Not `0..n`: a
/// cpuset such as `0-2,4` leaves holes.
fn allowed_cpus() -> Vec<usize> {
    unsafe {
        let mut cpu_set: cpu_set_t = std::mem::zeroed();
        let result = libc::sched_getaffinity(0, std::mem::size_of::<cpu_set_t>(), &mut cpu_set);
        if result != 0 {
            panic!(
                "sched_getaffinity failed: {}",
                std::io::Error::last_os_error()
            );
        }
        (0..libc::CPU_SETSIZE as usize)
            .filter(|&cpu| libc::CPU_ISSET(cpu, &cpu_set))
            .collect()
    }
}

/// Aborts the process if dropped armed, which only a panic can do: a worker
/// that dies takes the whole process down rather than leave its peers waiting
/// on it or sending into a ring that is gone.
struct WorkerGuard {
    worker: usize,
    started: std::rc::Rc<std::cell::Cell<bool>>,
}

impl Drop for WorkerGuard {
    fn drop(&mut self) {
        let when = if self.started.get() {
            ""
        } else {
            " during startup"
        };
        eprintln!("worker {} panicked{when}; aborting", self.worker);
        std::process::abort();
    }
}

fn pin_core(core_id: usize) {
    unsafe {
        let mut cpu_set: cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(core_id, &mut cpu_set);
        let result = libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &cpu_set);
        if result != 0 {
            panic!("sched_setaffinity failed for core {core_id}.");
        }
    }
}

#[allow(unused)]
mod numa {
    fn get_numa_node_count() -> usize {
        std::fs::read_dir("/sys/devices/system/node")
            .map(|entries| entries.filter_map(|e| e.ok()).count())
            .unwrap_or(1)
    }

    #[allow(unused)]
    fn numa_allocs(_core_id: usize) {
        unimplemented!("no numa available atm");
    }
}

fn spawn_worker_on<R, T, F>(
    n: usize,
    cpu: usize,
    num_threads: usize,
    f: std::sync::Arc<F>,
    telemetry: std::sync::Arc<T>,
    event_fd: i32,
    peers: std::sync::Arc<runtime_core::runtime::peers::PeerTable>,
) -> JoinHandle<()>
where
    R: BufRingSet + 'static,
    T: Telemetry + Send + Sync + 'static,
    F: Fn(WorkerContext) -> Pin<Box<dyn Future<Output = ()>>> + Send + Sync + 'static,
{
    std::thread::spawn(move || {
        let guard = WorkerGuard {
            worker: n,
            started: Default::default(),
        };
        let started = guard.started.clone();
        run_worker::<R, T, F>(
            n,
            cpu,
            num_threads,
            f,
            &*telemetry,
            event_fd,
            peers,
            started,
        );
        // Returned rather than unwound: nothing to abort for.
        std::mem::forget(guard);
    })
}

fn run_worker<R, T, F>(
    n: usize,
    cpu: usize,
    num_threads: usize,
    f: std::sync::Arc<F>,
    telemetry: &T,
    event_fd: i32,
    peers: std::sync::Arc<runtime_core::runtime::peers::PeerTable>,
    started: std::rc::Rc<std::cell::Cell<bool>>,
) where
    R: BufRingSet + 'static,
    T: Telemetry,
    F: Fn(WorkerContext) -> Pin<Box<dyn Future<Output = ()>>> + Send + Sync + 'static,
{
    pin_core(cpu);
    let ctx = WorkerContext {
        thread_id: n,
        num_threads,
    };
    let fut = f(ctx);
    // First polled once the ring is up and every peer has published: startup is over.
    let fut = async move {
        started.set(true);
        fut.await
    };
    RuntimeInner::with_peers(n, peers)
        .block_on::<R, _>(fut, Some(event_fd), |run| telemetry.install(Some(n), run))
}
