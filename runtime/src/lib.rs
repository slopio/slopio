use std::pin::Pin;

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

mod telemetry;
pub mod topology;

pub use runtime_core::io_driver::RingOptions;
use telemetry::{DEFAULT_TELEMETRY, Telemetry};
pub use topology::{Threads, Topology, WorkerContext};

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
    let mut topology = Topology::with_telemetry(telemetry);
    topology.per_core::<R, _, _>(RingOptions::default(), f);
    topology.run();
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
