use runtime::{
    io_driver::operations::files::{
        open::{flags, mode},
        open_file,
    },
    main_thread,
    runtime::join::{join, join3, join5},
    runtime::time::Sleep,
};
use std::time::{Duration, Instant};

#[test]
fn join_test() {
    let result = main_thread(async {
        let u = join(async { 2 }, async { 3 });
        u.await
    });
    assert_eq!(result, (2, 3));
}

#[test]
fn join3_test() {
    let result =
        main_thread(async { join3(async { 1u8 }, async { "two" }, async { 3.0f64 }).await });
    assert_eq!(result, (1u8, "two", 3.0f64));
}

#[test]
fn join5_test() {
    let result = main_thread(async {
        join5(async { 1 }, async { 2 }, async { 3 }, async { 4 }, async {
            5
        })
        .await
    });
    assert_eq!(result, (1, 2, 3, 4, 5));
}

// Three sleeping futures driven by one join3: the join only completes once all
// three have produced a value (proven by the tuple), and runs them concurrently
// rather than summing their durations.
// Timing bounds are deliberately loose; correctness rides on the returned
// tuple, not the clock.
#[test]
fn join3_sleep_runs_concurrently() {
    let start = Instant::now();
    let result = main_thread(async {
        join3(
            async {
                Sleep::during(Duration::from_millis(300)).await;
                10
            },
            async {
                Sleep::during(Duration::from_millis(600)).await;
                20
            },
            async {
                Sleep::during(Duration::from_millis(300)).await;
                30
            },
        )
        .await
    });
    let elapsed = start.elapsed();

    assert_eq!(result, (10, 20, 30));
    assert!(
        elapsed >= Duration::from_millis(550),
        "join3 children must actually suspend on their timers: {elapsed:?}"
    );
    // Sequential would be 1.2s; overlapping is bounded by the longest child.
    assert!(
        elapsed < Duration::from_millis(1_000),
        "join3 children should overlap, not run sequentially: {elapsed:?}"
    );
}

#[test]
fn join_io_test() {
    main_thread(async {
        let test_file = "/tmp/test_write_read22.txt";
        let test_file2 = "/tmp/test_write_read23.txt";

        let file1 = open_file(
            test_file,
            flags::WRITE_ONLY | flags::CREATE | flags::TRUNCATE,
            mode::DEFAULT,
        );
        let file2 = open_file(
            test_file2,
            flags::WRITE_ONLY | flags::CREATE | flags::TRUNCATE,
            mode::DEFAULT,
        );
        let (Ok(f1), Ok(f2)) = join(file1, file2).await else {
            panic!("afdsf");
        };

        let w1 = f1.write(b"salut");
        let w2 = f2.write(b"salut");
        let ((r1_res, _), (r2_res, _)) = join(w1, w2).await;
        let r1_res = r1_res.unwrap();
        let r2_res = r2_res.unwrap();
        assert_eq!(r1_res, r2_res);
    });
}
