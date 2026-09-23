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

        match select(proc.wait(), Sleep::during(Duration::from_secs(5))).await {
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
        let (written, _) = proc.write_stdin(ping).unwrap().await;
        assert_eq!(written.unwrap(), ping.len());

        // pong: read back from stdout
        let (read, buf) = proc.read_stdout(Vec::with_capacity(64)).unwrap().await;
        read.unwrap();
        assert_eq!(&buf[..], ping);

        // second round
        let ping2 = b"ping two\n";
        proc.write_stdin(ping2).unwrap().await.0.unwrap();

        let mut buf = buf;
        buf.clear();
        let (read2, buf) = proc.read_stdout(buf).unwrap().await;
        read2.unwrap();
        assert_eq!(&buf[..], ping2);

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
            let byte = vec![(i % 256) as u8];
            let (written, _) = proc.write_stdin(byte).unwrap().await;
            assert_eq!(written.unwrap(), 1);

            let (n, buf) = proc.read_stdout(Vec::with_capacity(1)).unwrap().await;
            let n = n.unwrap();
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
fn a_pipe_read_created_before_the_data_arrives_still_completes() {
    main_thread(async {
        let mut proc = Process::new("cat", &[]).unwrap();

        // Built, then left unpolled while the data lands.
        let read = proc.read_stdout(Vec::with_capacity(16)).unwrap();
        proc.write_stdin(b"early\n").unwrap().await.0.unwrap();
        Sleep::during(Duration::from_millis(50)).await;

        match select(read, Sleep::during(Duration::from_secs(5))).await {
            SelectResult::First((n, buf)) => {
                n.unwrap();
                assert_eq!(&buf[..], b"early\n");
            }
            SelectResult::Second(_) => panic!("pipe read never completed"),
        }

        proc.close_stdin();
        proc.wait().await.unwrap();
    });
}

#[test]
fn a_pipe_read_dropped_mid_flight_leaves_the_pipe_usable() {
    main_thread(async {
        let mut proc = Process::new("cat", &[]).unwrap();

        // Nothing to read yet: the timer wins and the read is cancelled.
        match select(
            proc.read_stdout(Vec::with_capacity(16)).unwrap(),
            Sleep::during(Duration::from_millis(20)),
        )
        .await
        {
            SelectResult::First(_) => panic!("nothing was written"),
            SelectResult::Second(_) => {}
        }

        proc.write_stdin(b"after\n").unwrap().await.0.unwrap();
        let (n, buf) = proc.read_stdout(Vec::with_capacity(16)).unwrap().await;
        n.unwrap();
        assert_eq!(&buf[..], b"after\n");

        proc.close_stdin();
        proc.wait().await.unwrap();
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

        // race: stop notification vs 5s safety timeout
        match select(wait_stopped, Sleep::during(Duration::from_secs(5))).await {
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
