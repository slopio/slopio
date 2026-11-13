//! Files on a simulated ring: what `fstat` reports, and what moves it.
//!
//! The simulator keeps no content and no filesystem, only what `statx` says of
//! each open file: a size that follows the writes it accepted, the position
//! reads and writes share, the time of the last write. These pin that model,
//! what a test can script over it, and an `fstat` dropped while in flight.
//!
//! ```text
//! MIRIFLAGS="-Zmiri-disable-isolation -Zmiri-permissive-provenance -Zmiri-tree-borrows" \
//!   cargo +nightly miri test -p slopio --features simulation --test simulated_files
//! ```

use std::future::Future;
use std::os::fd::RawFd;
use std::time::{Duration, UNIX_EPOCH};

use io_uring::{opcode, types::Fd};
use slopio::deterministic_testing::{
    FIRST_SIMULATED_FD, FakeRing, FakeRingHandle, FileMeta, OpenEvent, ReadEvent, StatEvent,
    WriteEvent, is_open,
};
use slopio::io_driver::operations::files::open::mode;
use slopio::io_driver::operations::files::{OpenFileError, OpenOptions, OpenedFile};
use slopio::runtime::select::{SelectResult, select};
use slopio::runtime::{RuntimeInner, time::Sleep};

/// The descriptor of the first file a ring opens: a new ring restarts the
/// numbering, so a test can script it before the open that hands it out.
const FIRST_FILE: RawFd = FIRST_SIMULATED_FD;

fn simulated() -> (FakeRing, FakeRingHandle) {
    FakeRing::builder().virtual_time().build()
}

fn run<F: Future>(fake: FakeRing, future: F) -> F::Output {
    RuntimeInner::simulated(fake).block_on::<(), _>(future, None, |run| run())
}

/// Enough passes of the event loop to submit a cancel or a close and reap what
/// it brings back. Sleeps, because the ring refuses to block with nothing due.
async fn settle() {
    for _ in 0..2 {
        Sleep::during(Duration::from_millis(1)).await;
    }
}

/// Every descriptor the ring handed out was closed again.
fn assert_all_closed(handle: &FakeRingHandle) {
    for fd in handle.opened_fds() {
        assert!(!is_open(fd), "fd {fd} was never closed");
    }
}

// ------------------------------------------------------------ what it models

/// A file nobody described is created empty, and its size is the bytes its
/// writes were accepted for: a short write counts for what it took.
#[test]
fn a_stat_follows_the_writes_the_file_accepted() {
    let (fake, handle) = simulated();
    handle.on_write(FIRST_FILE, WriteEvent::All);
    handle.on_write(FIRST_FILE, WriteEvent::Short(3));
    handle.on_write(FIRST_FILE, WriteEvent::Error(libc::ENOSPC));

    run(fake, async {
        let file = OpenedFile::create("/sim/log").await.expect("open");
        let empty = file.fstat().await.expect("fstat");
        assert!(empty.is_file() && empty.is_empty());

        assert_eq!(file.write(b"hello").await.0.expect("whole"), 5);
        assert_eq!(file.fstat().await.expect("fstat").len(), 5);

        assert_eq!(file.write(b"0123456789").await.0.expect("short"), 3);
        assert_eq!(file.fstat().await.expect("fstat").len(), 8);

        let full = file.write(b"more").await.0.expect_err("the disk is full");
        assert_eq!(full.raw_os_error(), Some(libc::ENOSPC));
        assert_eq!(file.fstat().await.expect("fstat").len(), 8);

        drop(file);
        settle().await;
    });

    assert_eq!(handle.scenario_remaining(), 0);
    assert_all_closed(&handle);
}

/// A file the open creates has the permission bits the open asked for; one it
/// only finds has `0o644`.
#[test]
fn a_created_file_has_the_mode_it_was_created_with() {
    let (fake, handle) = simulated();

    run(fake, async {
        let private = OpenOptions::write()
            .create(mode::RW_USER)
            .open("/sim/secret")
            .await
            .expect("open");
        assert_eq!(private.fstat().await.expect("fstat").permissions(), 0o600);

        let found = OpenedFile::open("/sim/conf").await.expect("open");
        assert_eq!(found.fstat().await.expect("fstat").permissions(), 0o644);

        drop((private, found));
        settle().await;
    });

    assert_all_closed(&handle);
}

/// The dates are the ring's own clock: a file is created when its open
/// completes and modified when a write is last accepted, however long ago
/// that was by the time anyone asks.
#[test]
fn a_file_is_dated_by_the_virtual_clock() {
    let (fake, handle) = simulated();
    let clock = handle.clock().expect("built with virtual time");

    run(fake, async move {
        let file = OpenedFile::create("/sim/log").await.expect("open");
        let created = clock.now_wall();
        Sleep::during(Duration::from_secs(3)).await;

        let untouched = file.fstat().await.expect("fstat");
        assert_eq!(untouched.created(), Some(created));
        assert_eq!(untouched.modified(), created);

        file.write(b"hello").await.0.expect("write");
        let written = clock.now_wall();
        assert!(written > created, "the sleep moved no time");
        Sleep::during(Duration::from_secs(5)).await;

        let later = file.fstat().await.expect("fstat");
        assert_eq!(later.created(), Some(created));
        assert_eq!(later.modified(), written);

        drop(file);
        settle().await;
    });

    assert_all_closed(&handle);
}

/// A test says what was at the path before the run, and the open's own flags
/// then apply to it: `truncate` empties it, `append` writes at its end, and
/// neither overwrites it from the start.
#[test]
fn an_open_applies_its_flags_to_the_file_it_finds() {
    let (fake, handle) = simulated();
    for _ in 0..3 {
        handle.on_open(OpenEvent::existing(1024));
    }

    run(fake, async {
        let truncated = OpenedFile::create("/sim/data").await.expect("open");
        assert_eq!(truncated.fstat().await.expect("fstat").len(), 0);
        truncated.write(b"0123456789").await.0.expect("write");
        assert_eq!(truncated.fstat().await.expect("fstat").len(), 10);

        let appended = OpenOptions::write()
            .append()
            .open("/sim/data")
            .await
            .expect("open");
        assert_eq!(appended.fstat().await.expect("fstat").len(), 1024);
        appended.write(b"0123456789").await.0.expect("write");
        assert_eq!(appended.fstat().await.expect("fstat").len(), 1034);

        let overwritten = OpenOptions::write().open("/sim/data").await.expect("open");
        overwritten.write(b"0123456789").await.0.expect("write");
        assert_eq!(overwritten.fstat().await.expect("fstat").len(), 1024);

        drop((truncated, appended, overwritten));
        settle().await;
    });

    assert_eq!(handle.scenario_remaining(), 0);
    assert_all_closed(&handle);
}

/// An existing file keeps the date and the bits the test gave it until
/// something is written to it.
#[test]
fn an_existing_file_is_what_the_test_said_until_written() {
    let (fake, handle) = simulated();
    let clock = handle.clock().expect("built with virtual time");
    let long_ago = UNIX_EPOCH + Duration::new(1_000_000_000, 250);
    handle.on_open(OpenEvent::Existing(
        FileMeta::file(64).permissions(0o400).modified(long_ago),
    ));

    run(fake, async move {
        let file = OpenOptions::read_write()
            .open("/sim/old")
            .await
            .expect("open");
        let found = file.fstat().await.expect("fstat");
        assert_eq!(found.len(), 64);
        assert_eq!(found.permissions(), 0o400);
        assert_eq!(found.modified(), long_ago);

        file.write(b"new").await.0.expect("write");
        let touched = file.fstat().await.expect("fstat");
        assert_eq!(touched.len(), 64, "three bytes over the first three");
        assert_eq!(touched.modified(), clock.now_wall());

        drop(file);
        settle().await;
    });

    assert_all_closed(&handle);
}

/// Reads and writes share one position, so a write after a read to the end of
/// the file extends it, with no `append` asked for.
#[test]
fn a_write_lands_where_the_reads_left_the_position() {
    let (fake, handle) = simulated();
    handle.on_open(OpenEvent::existing(4));
    handle.on_read(FIRST_FILE, ReadEvent::data(b"abcd"));

    run(fake, async {
        let file = OpenedFile::open_rw("/sim/data").await.expect("open");
        let (read, _buf) = file.read(Vec::with_capacity(16)).await;
        assert_eq!(read.expect("read"), 4);

        file.write(b"efghij").await.0.expect("write");
        assert_eq!(file.fstat().await.expect("fstat").len(), 10);

        drop(file);
        settle().await;
    });

    assert_eq!(handle.scenario_remaining(), 0);
    assert_all_closed(&handle);
}

/// There is no filesystem behind the descriptors: a second open of a path is
/// another file, which the writes to the first do not reach.
#[test]
fn two_opens_of_one_path_are_two_files() {
    let (fake, handle) = simulated();

    run(fake, async {
        let first = OpenedFile::create("/sim/shared").await.expect("open");
        let second = OpenedFile::open("/sim/shared").await.expect("open");
        first.write(b"only here").await.0.expect("write");

        let (first_meta, second_meta) = (
            first.fstat().await.expect("fstat"),
            second.fstat().await.expect("fstat"),
        );
        assert_eq!(first_meta.len(), 9);
        assert_eq!(second_meta.len(), 0);
        assert_ne!(first_meta.ino(), second_meta.ino());

        drop((first, second));
        settle().await;
    });

    assert_all_closed(&handle);
}

// ----------------------------------------------------------- what is scripted

/// A scripted answer is given as it is, once, whatever the simulator knew of
/// the file; the stat after it is answered from the model again.
#[test]
fn a_scripted_stat_answers_in_place_of_the_model() {
    let (fake, handle) = simulated();
    let stamped = UNIX_EPOCH + Duration::from_secs(86_400);
    handle.on_stat(
        FIRST_FILE,
        StatEvent::file(1 << 40)
            .permissions(0o600)
            .modified(stamped),
    );
    handle.on_stat(FIRST_FILE, StatEvent::dir());

    run(fake, async move {
        let file = OpenedFile::open("/sim/huge").await.expect("open");

        let huge = file.fstat().await.expect("fstat");
        assert_eq!(huge.len(), 1 << 40);
        assert!(huge.is_file());
        assert_eq!(huge.permissions(), 0o600);
        assert_eq!(huge.modified(), stamped);

        let dir = file.fstat().await.expect("fstat");
        assert!(dir.is_dir() && !dir.is_file());
        assert_eq!(dir.permissions(), 0o755);
        assert_eq!(dir.nlink(), 2);

        let modelled = file.fstat().await.expect("fstat");
        assert!(modelled.is_file() && modelled.is_empty());

        drop(file);
        settle().await;
    });

    assert_eq!(handle.scenario_remaining(), 0);
    assert_all_closed(&handle);
}

/// A stat scripted to fail comes back as that error, and leaves the file as
/// it was for the next one.
#[test]
fn a_scripted_stat_error_reaches_the_caller() {
    let (fake, handle) = simulated();
    handle.on_stat(FIRST_FILE, StatEvent::Error(libc::ESTALE));

    run(fake, async {
        let file = OpenedFile::create("/sim/nfs").await.expect("open");
        file.write(b"abc").await.0.expect("write");

        let stale = file.fstat().await.expect_err("scripted to fail");
        assert_eq!(stale.raw_os_error(), Some(libc::ESTALE));
        assert_eq!(file.fstat().await.expect("fstat").len(), 3);

        drop(file);
        settle().await;
    });

    assert_eq!(handle.scenario_remaining(), 0);
    assert_all_closed(&handle);
}

/// An open scripted to fail hands out no descriptor, and the open after it
/// is the first file.
#[test]
fn a_scripted_open_error_reaches_the_caller() {
    let (fake, handle) = simulated();
    handle.on_open(OpenEvent::Error(libc::ENOENT));

    run(fake, async {
        let missing = OpenedFile::open("/sim/missing").await;
        match missing {
            Err(OpenFileError::Io(e)) => assert_eq!(e.raw_os_error(), Some(libc::ENOENT)),
            other => panic!("expected ENOENT, got {other:?}"),
        }

        let file = OpenedFile::open("/sim/there").await.expect("open");
        assert_eq!(file.fd(), FIRST_FILE);

        drop(file);
        settle().await;
    });

    assert_eq!(handle.scenario_remaining(), 0);
    assert_eq!(handle.opened_fds(), vec![FIRST_FILE]);
    assert_all_closed(&handle);
}

// ---------------------------------------------------------- dropped in flight

/// A stat held back and dropped is cancelled; its descriptor closes only once
/// the cancel has come back, and the answer it never got is the next stat's.
#[test]
fn a_stat_dropped_in_flight_hands_its_answer_back() {
    let (fake, handle) = simulated();
    handle.on_stat(
        FIRST_FILE,
        StatEvent::file(7).after(Duration::from_secs(10)),
    );

    run(fake, {
        let handle = handle.clone();
        async move {
            let file = OpenedFile::open("/sim/slow").await.expect("open");

            let raced = select(file.fstat(), Sleep::during(Duration::from_millis(1))).await;
            assert!(
                matches!(raced, SelectResult::Second(_)),
                "the stat answered before its ten seconds",
            );
            settle().await;
            assert!(
                handle.inflight().is_empty(),
                "the cancel left {:?} in flight",
                handle.inflight(),
            );
            assert_eq!(
                handle.scenario_remaining(),
                1,
                "the cancel consumed an answer nobody received",
            );

            assert_eq!(file.fstat().await.expect("fstat").len(), 7);

            drop(file);
            settle().await;
        }
    });

    assert_eq!(handle.scenario_remaining(), 0);
    assert_all_closed(&handle);
}

/// A stat dropped before the ring saw it still completes: the answer is
/// written after its future is gone, into the buffer the driver kept alive.
/// Nothing here asserts on that write — Miri is the assertion.
#[test]
fn a_stat_dropped_before_it_completes_outlives_its_own_future() {
    let (fake, handle) = simulated();

    run(fake, async {
        let file = OpenedFile::create("/sim/log").await.expect("open");
        file.write(b"hello").await.0.expect("write");

        // No delay to script: lost on the first poll, so the stat and its
        // cancel reach the ring together and the stat completes first.
        let raced = select(file.fstat(), std::future::ready(())).await;
        assert!(
            matches!(raced, SelectResult::Second(_)),
            "the stat completed before it could be dropped in flight",
        );
        drop(file);
        settle().await;
    });

    assert!(handle.inflight().is_empty());
    assert_all_closed(&handle);
}

// ------------------------------------------------------------- the ring alone

fn statx_of(fd: RawFd, into: &mut libc::statx, user_data: u64) -> io_uring::squeue::Entry {
    opcode::Statx::new(Fd(fd), c"".as_ptr(), std::ptr::from_mut(into).cast())
        .flags(libc::AT_EMPTY_PATH)
        .mask(libc::STATX_BASIC_STATS)
        .build()
        .user_data(user_data)
}

/// A write at an offset of its own grows the file to where it ends and leaves
/// the position alone, as `pwrite(2)` does.
#[test]
fn a_write_at_an_offset_does_not_move_the_position() {
    let (mut ring, _handle) = FakeRing::builder().build();

    ring.add(
        &opcode::OpenAt::new(Fd(libc::AT_FDCWD), c"/sim/sparse".as_ptr())
            .flags(libc::O_RDWR)
            .build()
            .user_data(1),
    );
    ring.submit_and_wait();
    let fd = ring.pop_completion().expect("the open completed").result;

    let bytes = *b"hello";
    let write_at = |offset: u64, user_data: u64| {
        opcode::Write::new(Fd(fd), bytes.as_ptr(), bytes.len() as u32)
            .offset(offset)
            .build()
            .user_data(user_data)
    };
    // SAFETY: integers and padding, so all zeroes is a `statx`.
    let mut stat: libc::statx = unsafe { std::mem::zeroed() };

    ring.add(&write_at(100, 2));
    ring.add(&statx_of(fd, &mut stat, 3));
    ring.submit_and_wait();
    assert_eq!(ring.pop_completion().expect("the write").result, 5);
    assert_eq!(ring.pop_completion().expect("the stat").result, 0);
    assert_eq!(stat.stx_size, 105);

    // At the current position, which the first write never moved: the start.
    ring.add(&write_at(u64::MAX, 4));
    ring.add(&statx_of(fd, &mut stat, 5));
    ring.submit_and_wait();
    assert_eq!(ring.pop_completion().expect("the write").result, 5);
    assert_eq!(ring.pop_completion().expect("the stat").result, 0);
    assert_eq!(stat.stx_size, 105);

    ring.add(&opcode::Close::new(Fd(fd)).build().user_data(6));
    ring.submit_and_wait();
    assert!(!is_open(fd));
}

/// The simulator answers for the files it opened. A stat on anything else
/// that nobody scripted completes and leaves the buffer as it found it.
#[test]
fn a_stat_on_a_descriptor_the_ring_does_not_know_writes_nothing() {
    let (mut ring, handle) = FakeRing::builder().journal().build();
    // SAFETY: integers and padding, so all zeroes is a `statx`.
    let mut stat: libc::statx = unsafe { std::mem::zeroed() };
    stat.stx_size = 42;

    ring.add(&statx_of(11, &mut stat, 1));
    ring.submit_and_wait();
    assert_eq!(ring.pop_completion().expect("the stat").result, 0);
    assert_eq!(stat.stx_size, 42);

    // Scripted, it is answered like any other.
    handle.on_stat(11, StatEvent::file(7));
    ring.add(&statx_of(11, &mut stat, 2));
    ring.submit_and_wait();
    assert_eq!(ring.pop_completion().expect("the stat").result, 0);
    assert_eq!(stat.stx_size, 7);

    let journal = handle.journal_text(&[], None).expect("journal on");
    assert!(journal.contains("submit   STATX"), "{journal}");
}
