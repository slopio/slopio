//! Alone in its binary: it counts this process's open fds, so no other test
//! may open or close any while it runs.

use std::time::Duration;

use slopio::io_driver::operations::files::OpenedFile;
use slopio::main_thread;
use slopio::runtime::select::{SelectResult, select};
use slopio::runtime::time::Sleep;

fn open_fds() -> usize {
    std::fs::read_dir("/proc/self/fd").unwrap().count()
}

#[test]
fn a_cancelled_open_closes_the_fd_it_produced() {
    main_thread(async {
        let before = open_fds();
        for _ in 0..100 {
            // The open is submitted, then dropped before its CQE is seen. A
            // cached regular file opens inline, so the cancel always loses.
            match select(
                OpenedFile::open(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml")),
                std::future::ready(()),
            )
            .await
            {
                SelectResult::First(_) => panic!("the open cannot finish in its first poll"),
                SelectResult::Second(()) => {}
            }
        }
        Sleep::during(Duration::from_millis(100)).await;
        let after = open_fds();
        assert!(after <= before + 2, "leaked {} fds", after - before);
    });
}
