use std::thread::JoinHandle;

use libc::cpu_set_t;

pub mod io_driver;
mod macros;
pub mod runtime;
mod task;

pub fn main_thread<F: Future>(f: F) -> F::Output {
    let rt = runtime::RuntimeInner::default();
    rt.block_on(f)
}

pub fn thread_per_core<F, Fut>(f: F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = ()> + Send + 'static,
{
    // let cores_num = 2;
    let cores_num = std::thread::available_parallelism().unwrap().into();
    let handles = (0..cores_num)
        .map(|n| spawn_worker_on(n, f()))
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

fn spawn_worker_on<F: Future<Output = ()> + Send + 'static>(n: usize, f: F) -> JoinHandle<()> {
    std::thread::spawn(move || {
        pin_core(n);
        let rt = runtime::RuntimeInner::default();
        rt.block_on(f)
    })
}
