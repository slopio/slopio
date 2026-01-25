use core::panic;
use std::thread::JoinHandle;

use libc::cpu_set_t;

use crate::runtime::eventfd;

pub mod channels;
pub mod io_driver;
mod macros;
pub mod runtime;
mod task;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

pub fn main_thread<F: Future>(f: F) -> F::Output {
    let rt = runtime::RuntimeInner::default();
    rt.block_on(f, None)
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
    });
}

pub fn thread_per_core<F, Fut>(f: F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = ()> + Send + 'static,
{
    block_signals_for_thread();
    let cores_num = std::thread::available_parallelism().unwrap().into();
    let fds = (0..cores_num)
        .map(|_| eventfd::create_eventfd())
        .collect::<Vec<_>>();

    setup_sig_handling(fds.clone());

    let handles = fds
        .iter()
        .zip(0..cores_num)
        .map(|(sig_handler_fd, n)| spawn_worker_on(n, f(), *sig_handler_fd))
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

fn spawn_worker_on<F: Future<Output = ()> + Send + 'static>(
    n: usize,
    f: F,
    event_fd: i32,
) -> JoinHandle<()> {
    std::thread::spawn(move || {
        pin_core(n);
        // if get_numa_node_count() != 1 {
        //     numa_allocs(n);
        // }
        let rt = runtime::RuntimeInner::default();
        rt.block_on(f, Some(event_fd))
    })
}
