use std::os::fd::RawFd;
use std::sync::Arc;
use std::thread::JoinHandle;

use runtime_core::io_driver::{BufRingSet, RingOptions};
use runtime_core::runtime::RuntimeInner;
use runtime_core::runtime::peers::PeerTable;

use super::WorkerContext;
use super::cpus::pin_core;
use crate::telemetry::Telemetry;

/// Everything a thread of the topology needs, resolved before it starts.
pub(super) struct Start<T> {
    pub ctx: WorkerContext,
    pub cpu: usize,
    /// For the thread's listeners: `cpu`, unless the topology does not steer.
    pub incoming_cpu: Option<usize>,
    pub ring: RingOptions,
    pub sq_cpu: Option<usize>,
    pub telemetry: Arc<T>,
    pub event_fd: RawFd,
    pub peers: Arc<PeerTable>,
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

pub(super) fn spawn<R, T, F, Fut>(start: Start<T>, make: F) -> JoinHandle<()>
where
    R: BufRingSet + 'static,
    T: Telemetry + Send + Sync + 'static,
    F: FnOnce(WorkerContext) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + 'static,
{
    std::thread::spawn(move || {
        let guard = WorkerGuard {
            worker: start.ctx.thread_id,
            started: Default::default(),
        };
        let started = guard.started.clone();
        run::<R, T, F, Fut>(start, make, started);
        // Returned rather than unwound: nothing to abort for.
        std::mem::forget(guard);
    })
}

fn run<R, T, F, Fut>(start: Start<T>, make: F, started: std::rc::Rc<std::cell::Cell<bool>>)
where
    R: BufRingSet + 'static,
    T: Telemetry,
    F: FnOnce(WorkerContext) -> Fut,
    Fut: Future<Output = ()>,
{
    let Start {
        ctx,
        cpu,
        incoming_cpu,
        ring,
        sq_cpu,
        telemetry,
        event_fd,
        peers,
    } = start;
    let n = ctx.thread_id;
    pin_core(cpu);
    runtime_core::net::set_incoming_cpu(incoming_cpu);
    let fut = make(ctx);
    // First polled once the ring is up and every peer has published: startup is over.
    let fut = async move {
        started.set(true);
        fut.await
    };
    RuntimeInner::with_peers(n, peers, ring, sq_cpu.map(|cpu| cpu as u32)).block_on::<R, _>(
        fut,
        Some(event_fd),
        |run| telemetry.install(Some(n), run),
    )
}
