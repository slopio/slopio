//! `thread_per_core` workers: pinned to the CPUs the process may use, and a
//! worker that panics, starting or running, stops the whole process.

use std::os::unix::process::ExitStatusExt;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use slopio::thread_per_core;

fn affinity() -> Vec<usize> {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        assert_eq!(
            libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut set),
            0
        );
        (0..libc::CPU_SETSIZE as usize)
            .filter(|&c| libc::CPU_ISSET(c, &set))
            .collect()
    }
}

fn restrict_to(cpus: &[usize]) {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        for &c in cpus {
            libc::CPU_SET(c, &mut set);
        }
        assert_eq!(
            libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set),
            0
        );
    }
}

#[test]
fn workers_are_pinned_to_the_allowed_cpus_not_to_their_index() {
    // Every other CPU: a mask with holes, like a `0-2,4` cpuset.
    let allowed: Vec<usize> = affinity().into_iter().step_by(2).collect();
    if allowed.len() < 2 {
        eprintln!("skipping: need at least 3 CPUs");
        return;
    }

    let seen = std::thread::spawn(move || {
        restrict_to(&allowed);
        let seen = Arc::new(Mutex::new(Vec::new()));
        let record = seen.clone();
        thread_per_core(move |ctx| {
            record.lock().unwrap().push((ctx.thread_id, affinity()));
            Box::pin(async {})
        });
        let mut seen = std::mem::take(&mut *seen.lock().unwrap());
        seen.sort();
        (allowed, seen)
    })
    .join()
    .unwrap();

    let (allowed, seen) = seen;
    assert_eq!(seen.len(), allowed.len(), "one worker per allowed CPU");
    for (worker, pinned) in &seen {
        assert_eq!(pinned, &vec![allowed[*worker]], "worker {worker}");
    }
}

const CHILD: &str = "RUNTIME_WORKER_PANIC_CHILD";

/// Run by the tests below in a child process only: worker 1 panics either
/// while starting (`f(ctx)`) or once running, while its peers run forever.
#[test]
#[ignore]
fn worker_panic_child() {
    let Some(mode) = std::env::var_os(CHILD) else {
        return;
    };
    let while_starting = mode == "startup";
    thread_per_core(move |ctx| {
        if ctx.thread_id == 1 && while_starting {
            panic!("worker 1 cannot start");
        }
        Box::pin(async move {
            if ctx.thread_id == 1 {
                panic!("worker 1 died while running");
            }
            std::future::pending::<()>().await;
        })
    });
}

fn assert_child_aborts(mode: &str) {
    if std::thread::available_parallelism().unwrap().get() < 2 {
        eprintln!("skipping: need at least 2 CPUs");
        return;
    }
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "worker_panic_child",
            "--include-ignored",
            "--nocapture",
        ])
        .env(CHILD, mode)
        .spawn()
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(20);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if Instant::now() > deadline {
            child.kill().unwrap();
            panic!("the process kept running instead of aborting");
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(status.signal(), Some(libc::SIGABRT), "{status:?}");
}

#[test]
fn a_worker_that_fails_at_startup_aborts_the_process() {
    assert_child_aborts("startup");
}

#[test]
fn a_worker_that_panics_while_running_aborts_the_process() {
    assert_child_aborts("running");
}
