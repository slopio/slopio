//! `thread_per_core` startup: workers are pinned to the CPUs the process may
//! use, and a worker that fails before its runtime is up stops the process.

use std::os::unix::process::ExitStatusExt;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use runtime::thread_per_core;

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

const CHILD: &str = "RUNTIME_STARTUP_PANIC_CHILD";

/// Run by the test below in a child process only.
#[test]
#[ignore]
fn startup_panic_child() {
    if std::env::var_os(CHILD).is_none() {
        return;
    }
    thread_per_core(|ctx| {
        if ctx.thread_id == 1 {
            panic!("worker 1 cannot start");
        }
        Box::pin(async {})
    });
}

#[test]
fn a_worker_that_fails_at_startup_aborts_the_process() {
    if std::thread::available_parallelism().unwrap().get() < 2 {
        eprintln!("skipping: need at least 2 CPUs");
        return;
    }
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "startup_panic_child",
            "--include-ignored",
            "--nocapture",
        ])
        .env(CHILD, "1")
        .spawn()
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(20);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if Instant::now() > deadline {
            child.kill().unwrap();
            panic!("the process hung instead of aborting");
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(status.signal(), Some(libc::SIGABRT), "{status:?}");
}
