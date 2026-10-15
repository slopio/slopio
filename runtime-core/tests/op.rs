use std::any::Any;
use std::cell::Cell;
use std::ffi::CString;
use std::future::{Future, poll_fn};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::rc::Rc;
use std::task::Poll;
use std::time::Duration;

use io_uring::{
    opcode,
    squeue::Entry,
    types::{Fd, Timespec},
};
use runtime_core::io_driver::operations::{Op, OpState};
use runtime_core::runtime::main_thread;
use runtime_core::runtime::select::{SelectResult, select};
use runtime_core::runtime::time::Sleep;

struct TimeoutOp(Box<Timespec>);

impl TimeoutOp {
    fn new(d: Duration) -> Self {
        Self(Box::new(Timespec::from(d)))
    }
}

unsafe impl OpState for TimeoutOp {
    fn entry(&mut self) -> Entry {
        opcode::Timeout::new(&*self.0).build()
    }

    fn into_keepalive(self) -> Option<Box<dyn Any>> {
        Some(self.0)
    }
}

/// Owns a timespec and records when it is freed.
struct TrackedTimeout {
    ts: Box<Timespec>,
    dropped: Rc<Cell<bool>>,
}

impl Drop for TrackedTimeout {
    fn drop(&mut self) {
        self.dropped.set(true);
    }
}

unsafe impl OpState for TrackedTimeout {
    fn entry(&mut self) -> Entry {
        opcode::Timeout::new(&*self.ts).build()
    }
}

/// Polls `op` once, which submits it.
async fn submit<S: OpState>(op: &mut Op<S>) {
    poll_fn(|cx| {
        assert!(Pin::new(&mut *op).poll(cx).is_pending());
        Poll::Ready(())
    })
    .await
}

#[test]
fn completes_and_returns_the_state() {
    main_thread(async {
        let (res, state) = Op::new(TimeoutOp::new(Duration::from_millis(5))).await;
        assert_eq!(res.unwrap_err().raw_os_error(), Some(libc::ETIME));
        // The state comes back intact and can be re-armed.
        let (res, _) = Op::new(state).await;
        assert_eq!(res.unwrap_err().raw_os_error(), Some(libc::ETIME));
    });
}

#[test]
fn survives_a_lost_select_and_completes_later() {
    main_thread(async {
        let mut op = Op::new(TimeoutOp::new(Duration::from_millis(40)));
        match select(&mut op, Sleep::during(Duration::from_millis(10))).await {
            SelectResult::Second(()) => {}
            SelectResult::First(_) => panic!("the op won the race"),
        }
        let (res, _) = op.await;
        assert_eq!(res.unwrap_err().raw_os_error(), Some(libc::ETIME));
    });
}

#[test]
fn dropped_in_flight_frees_the_state_at_the_terminal_cqe() {
    main_thread(async {
        let dropped = Rc::new(Cell::new(false));
        let mut op = Op::new(TrackedTimeout {
            ts: Box::new(Timespec::from(Duration::from_secs(10))),
            dropped: dropped.clone(),
        });
        submit(&mut op).await;
        drop(op);
        assert!(!dropped.get(), "freed before the kernel let go of it");
        Sleep::during(Duration::from_millis(20)).await;
        assert!(dropped.get(), "not freed after the cancel completed");
    });
}

#[test]
fn op_in_a_select_cancels_when_the_timer_wins() {
    main_thread(async {
        let dropped = Rc::new(Cell::new(false));
        let op = Op::new(TrackedTimeout {
            ts: Box::new(Timespec::from(Duration::from_secs(10))),
            dropped: dropped.clone(),
        });
        let out = runtime_core::runtime::time::timeout(Duration::from_millis(20), op).await;
        assert!(out.is_err());
        Sleep::during(Duration::from_millis(20)).await;
        assert!(dropped.get());
    });
}

struct OpenOp(CString);

unsafe impl OpState for OpenOp {
    const RESULT_IS_FD: bool = true;

    fn entry(&mut self) -> Entry {
        opcode::OpenAt::new(Fd(libc::AT_FDCWD), self.0.as_ptr())
            .flags(libc::O_RDONLY | libc::O_CLOEXEC)
            .build()
    }
}

/// A file of its own per test, so fds from tests running alongside never match.
fn scratch_file(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("op-{name}-{}", std::process::id()));
    std::fs::write(&path, b"x").unwrap();
    path
}

fn fds_open_on(path: &Path) -> usize {
    std::fs::read_dir("/proc/self/fd")
        .unwrap()
        .filter_map(|e| std::fs::read_link(e.ok()?.path()).ok())
        .filter(|target| target == path)
        .count()
}

fn open_op(path: &Path) -> Op<OpenOp> {
    Op::new(OpenOp(CString::new(path.as_os_str().as_bytes()).unwrap()))
}

#[test]
fn open_completed_then_dropped_closes_the_fd() {
    let path = scratch_file("completed");
    main_thread(async {
        let mut op = open_op(&path);
        submit(&mut op).await;
        Sleep::during(Duration::from_millis(20)).await;
        assert_eq!(fds_open_on(&path), 1, "the open has not completed");
        drop(op);
        Sleep::during(Duration::from_millis(20)).await;
        assert_eq!(fds_open_on(&path), 0, "the fd leaked");
    });
    std::fs::remove_file(path).unwrap();
}

#[test]
fn open_dropped_in_flight_never_leaks() {
    let path = scratch_file("in-flight");
    main_thread(async {
        // Each drop races the open: either the cancel wins or the fd is closed.
        for _ in 0..200 {
            let mut op = open_op(&path);
            submit(&mut op).await;
            drop(op);
        }
        Sleep::during(Duration::from_millis(50)).await;
        assert_eq!(fds_open_on(&path), 0, "an fd leaked");
    });
    std::fs::remove_file(path).unwrap();
}

#[test]
fn open_completes_and_hands_the_fd_over() {
    let path = scratch_file("ready");
    main_thread(async {
        let (res, _) = open_op(&path).await;
        let fd = res.unwrap() as i32;
        assert_eq!(fds_open_on(&path), 1);
        unsafe { libc::close(fd) };
    });
    std::fs::remove_file(path).unwrap();
}
