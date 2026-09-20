use runtime::{
    main_thread,
    runtime::select::{SelectResult3, select3},
    runtime::time::Sleep,
};
use std::future::{pending, ready};
use std::time::{Duration, Instant};

#[test]
fn select3_first_wins_ties() {
    let result = main_thread(async { select3(ready(1), ready(2), ready(3)).await });
    match result {
        SelectResult3::First(v) => assert_eq!(v, 1),
        _ => panic!("expected first future to win the tie"),
    }
}

#[test]
fn select3_skips_pending() {
    let result =
        main_thread(async { select3(pending::<i32>(), ready("mid"), pending::<i32>()).await });
    match result {
        SelectResult3::Second(v) => assert_eq!(v, "mid"),
        _ => panic!("expected the only ready future (second) to complete"),
    }
}

// The winner is the 1s sleep in the SECOND slot, so passing proves select3
// resolves on whichever future is ready first — not merely the first in
// declaration order. The losers are 5s: select3 returns on the winner without
// ever waiting for them, and drops them, which also exercises cancellation in
// the fixed-duration store they land in.
#[test]
fn select3_ready_future_wins_regardless_of_slot() {
    let start = Instant::now();
    let result = main_thread(async {
        select3(
            async {
                Sleep::during(Duration::from_secs(5)).await;
                1
            },
            async {
                Sleep::during(Duration::from_secs(1)).await;
                2
            },
            async {
                Sleep::during(Duration::from_secs(5)).await;
                3
            },
        )
        .await
    });
    let elapsed = start.elapsed();

    match result {
        SelectResult3::Second(v) => assert_eq!(v, 2),
        _ => panic!("expected the ready (1s, slot 2) future to win"),
    }
    assert!(
        elapsed >= Duration::from_millis(900),
        "select3 should not resolve before its winner fires: {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_secs(3),
        "select3 should return on the winner, not wait for the 5s losers: {elapsed:?}"
    );
}
