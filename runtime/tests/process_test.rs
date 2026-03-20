use std::time::{Duration, Instant};

use runtime::main_thread;
use runtime::runtime::processes::spawn::Process;
use runtime::runtime::processes::wait_id::{ExitCode, IdType, WaitFlags, WaitProcess};
use runtime::runtime::select::{SelectResult, select};
use runtime::runtime::time::Sleep;

fn unwrap_finished(proc: Process) -> runtime::runtime::processes::wait_id::ExitStatus {
    match proc {
        Process::Finished { status, .. } => status,
        Process::Running { .. } => panic!("expected Finished"),
    }
}

#[test]
fn test_wait_for_short_sleep_process() {
    main_thread(async {
        let proc = Process::new("sleep", &["1"]).unwrap();
        let start = Instant::now();
        let finished = proc.wait().await.unwrap();
        let elapsed = start.elapsed();

        assert!(
            elapsed >= Duration::from_millis(900),
            "process finished too early: {:?}",
            elapsed
        );
        assert!(
            elapsed < Duration::from_secs(3),
            "process took too long: {:?}",
            elapsed
        );

        let code = unwrap_finished(finished).into_result().unwrap();
        assert_eq!(code, 0);
    });
}

#[test]
fn test_process_finishes_before_timeout() {
    main_thread(async {
        let proc = Process::new("sleep", &["1"]).unwrap();

        match select(proc.wait(), Sleep::during(Duration::from_secs(10))).await {
            SelectResult::First(result) => {
                let code = unwrap_finished(result.unwrap()).into_result().unwrap();
                assert_eq!(code, 0);
            }
            SelectResult::Second(_) => {
                panic!("timeout fired before process exited");
            }
        }
    });
}

#[test]
fn test_timeout_fires_before_process() {
    main_thread(async {
        let proc = Process::new("sleep", &["10"]).unwrap();

        match select(proc.wait(), Sleep::during(Duration::from_secs(1))).await {
            SelectResult::First(_) => {
                panic!("process exited before timeout");
            }
            SelectResult::Second(_) => {
                // timeout won — expected
            }
        }
    });
}

#[test]
fn test_process_nonzero_exit() {
    main_thread(async {
        let proc = Process::new("false", &[]).unwrap();
        let finished = proc.wait().await.unwrap();
        let status = unwrap_finished(finished);

        match status.code {
            ExitCode::Exited(code) => assert_ne!(code, 0),
            other => panic!("expected Exited, got {:?}", other),
        }
    });
}

#[test]
fn test_pipe_ping_pong() {
    main_thread(async {
        let proc = Process::new("cat", &[]).unwrap();

        // ping: write to stdin
        let ping = b"hello from runtime\n";
        let written = proc.write_stdin(ping).unwrap().await.unwrap();
        assert_eq!(written, ping.len());

        // pong: read back from stdout
        let mut buf = [0u8; 64];
        let read = proc.read_stdout(&mut buf).unwrap().await.unwrap();
        assert_eq!(&buf[..read], ping);

        // second round
        let ping2 = b"ping two\n";
        proc.write_stdin(ping2).unwrap().await.unwrap();

        let read2 = proc.read_stdout(&mut buf).unwrap().await.unwrap();
        assert_eq!(&buf[..read2], ping2);

        // close stdin (EOF) → cat exits
        let mut proc = proc;
        proc.close_stdin();
        let finished = proc.wait().await.unwrap();
        let code = unwrap_finished(finished).into_result().unwrap();
        assert_eq!(code, 0);
    });
}

#[test]
fn test_pipe_byte_by_byte() {
    main_thread(async {
        let proc = Process::new("cat", &[]).unwrap();

        // write one byte, read it back, 1024 times
        for i in 0u16..1024 {
            let byte = [(i % 256) as u8];
            let written = proc.write_stdin(&byte).unwrap().await.unwrap();
            assert_eq!(written, 1);

            let mut buf = [0u8; 1];
            let n = proc.read_stdout(&mut buf).unwrap().await.unwrap();
            assert_eq!(n, 1, "expected 1 byte back at round {}", i);
            assert_eq!(buf[0], (i % 256) as u8, "mismatch at round {}", i);
        }

        let mut proc = proc;
        proc.close_stdin();
        let finished = proc.wait().await.unwrap();
        let code = unwrap_finished(finished).into_result().unwrap();
        assert_eq!(code, 0);
    });
}

#[test]
fn test_stop_notification() {
    main_thread(async {
        let proc = Process::new("sleep", &["60"]).unwrap();
        let pid = proc.pid().unwrap();

        // watch for STOPPED, not EXITED
        let wait_stopped = WaitProcess::new(pid, IdType::Pid, WaitFlags::STOPPED);

        // send SIGSTOP after 1s
        let stop_handle = runtime::runtime::spawn(async move {
            Sleep::during(Duration::from_secs(1)).await;
            unsafe { libc::kill(pid as i32, libc::SIGSTOP) };
        });

        // race: stop notification vs 10s safety timeout
        match select(wait_stopped, Sleep::during(Duration::from_secs(10))).await {
            SelectResult::First(result) => {
                let status = result.unwrap();
                match status.code {
                    ExitCode::Stopped(sig) => {
                        assert_eq!(sig, libc::SIGSTOP, "expected SIGSTOP");
                    }
                    other => panic!("expected Stopped, got {:?}", other),
                }
            }
            SelectResult::Second(_) => {
                panic!("timeout fired — never got stop notification");
            }
        }

        stop_handle.await;

        // clean up: resume then kill
        unsafe {
            libc::kill(pid as i32, libc::SIGCONT);
            libc::kill(pid as i32, libc::SIGKILL);
        }
    });
}
