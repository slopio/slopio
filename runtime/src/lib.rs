use std::pin::Pin;
use std::thread::JoinHandle;

use libc::cpu_set_t;

use runtime_core::runtime::eventfd;

// Re-export core
pub use runtime_core::runtime::{RuntimeInner, spawn, yield_now};
pub use runtime_core::task::{Handle, new_task};
pub use runtime_core::{channels, io_driver, net, runtime, task};

#[cfg(feature = "tracing")]
pub use runtime_core::tracing;

#[cfg(feature = "otlp")]
mod otlp_collector;

#[cfg(feature = "otlp")]
pub use otlp_collector::{OtlpConfig, set_otlp_config};

/// Selects which collector to use for telemetry. Picked at runtime by the
/// caller and passed to the runtime entry point.
///
/// Each variant is gated behind its collector's feature flag: enabling the
/// feature exposes the variant, disabling it compiles the variant (and the
/// collector itself) away. So `--features otlp` is the only way to get
/// `CollectorChoice::Otlp`, etc.
///
/// Each variant reads its own configuration from a dedicated setter
/// (`set_otlp_config`, `set_local_debug_config`, ...) — the enum only picks
/// *which* collector runs, not *how* it's configured.
///
/// There is intentionally no `None` variant: if you want no telemetry, build
/// without the `tracing` feature and the entire span machinery compiles away.
/// Enabling `tracing` means you've committed to picking a collector.
#[cfg(feature = "tracing")]
#[derive(Debug, Clone, Copy)]
pub enum CollectorChoice {
    /// `ConsoleCollector` — one line per span to stderr. Available whenever
    /// `tracing` is enabled.
    Console,
    /// `LocalDebugCollector` — per-worker JSONL files for local inspection.
    /// Configured via `runtime_core::tracing::set_local_debug_config`.
    #[cfg(feature = "local_debug_collector")]
    LocalDebug,
    /// `OtlpCollector` — gRPC export to an OTLP backend (e.g. Tempo).
    /// Configured via [`set_otlp_config`].
    #[cfg(feature = "otlp")]
    Otlp,
}

pub mod http;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

pub fn main_thread<F: Future>(f: F) -> F::Output {
    let rt = RuntimeInner::default();
    #[cfg(feature = "otlp")]
    return rt.block_on(f, None, |run| {
        use crate::otlp_collector::OtlpCollector;
        use runtime_core::tracing::{TRACING_SINK, init_trace_sink};
        let sink = init_trace_sink::<OtlpCollector>();
        TRACING_SINK.set(&sink, run)
    });
    #[cfg(all(feature = "tracing", not(feature = "otlp")))]
    return rt.block_on(f, None, |run| {
        use runtime_core::tracing::{ConsoleCollector, TRACING_SINK, init_trace_sink};
        let sink = init_trace_sink::<ConsoleCollector>();
        TRACING_SINK.set(&sink, run)
    });
    #[cfg(not(feature = "tracing"))]
    rt.block_on(f, None, |run| run())
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
    block_signals_for_thread();
    let num_threads: usize = std::thread::available_parallelism().unwrap().into();
    let fds = (0..num_threads)
        .map(|_| eventfd::create_eventfd())
        .collect::<Vec<_>>();

    setup_sig_handling(fds.clone());

    let f = std::sync::Arc::new(f);
    let handles = fds
        .iter()
        .zip(0..num_threads)
        .map(|(sig_handler_fd, n)| {
            let f = f.clone();
            spawn_worker_on(n, num_threads, f, *sig_handler_fd)
        })
        .collect::<Vec<_>>();

    for handle in handles {
        handle.join().unwrap();
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

fn spawn_worker_on<F>(
    n: usize,
    num_threads: usize,
    f: std::sync::Arc<F>,
    event_fd: i32,
) -> JoinHandle<()>
where
    F: Fn(WorkerContext) -> Pin<Box<dyn Future<Output = ()>>> + Send + Sync + 'static,
{
    std::thread::spawn(move || {
        pin_core(n);
        let ctx = WorkerContext {
            thread_id: n,
            num_threads,
        };
        let fut = f(ctx);
        let rt = RuntimeInner::with_thread_id(n);
        #[cfg(feature = "otlp")]
        return rt.block_on(fut, Some(event_fd), |run| {
            use crate::otlp_collector::OtlpCollector;
            use runtime_core::tracing::{TRACING_SINK, init_trace_sink, set_thread_id};
            set_thread_id(n);
            let sink = init_trace_sink::<OtlpCollector>();
            TRACING_SINK.set(&sink, run)
        });
        #[cfg(all(feature = "tracing", not(feature = "otlp")))]
        return rt.block_on(fut, Some(event_fd), |run| {
            use runtime_core::tracing::{ConsoleCollector, TRACING_SINK, init_trace_sink, set_thread_id};
            set_thread_id(n);
            let sink = init_trace_sink::<ConsoleCollector>();
            TRACING_SINK.set(&sink, run)
        });
        #[cfg(not(feature = "tracing"))]
        rt.block_on(fut, Some(event_fd), |run| run())
    })
}
