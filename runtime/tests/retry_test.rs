use std::cell::Cell;
use std::time::{Duration, Instant};

use runtime::main_thread;
use runtime::runtime::retry::{RetryError, retry};

#[test]
fn test_retry_succeeds_first_attempt() {
    let result: Result<i32, RetryError<&str>> = main_thread(async {
        retry(
            Some(3),
            Duration::from_millis(10),
            || async { Ok(42) },
            |_| true,
        )
        .await
    });
    assert_eq!(result, Ok(42));
}

#[test]
fn test_retry_succeeds_after_failures() {
    let result: Result<i32, RetryError<&str>> = main_thread(async {
        let attempts = Cell::new(0);
        retry(
            Some(5),
            Duration::from_millis(10),
            || async {
                let n = attempts.get() + 1;
                attempts.set(n);
                if n < 3 { Err("not yet") } else { Ok(n) }
            },
            |_| true,
        )
        .await
    });
    assert_eq!(result, Ok(3));
}

#[test]
fn test_retry_exhausts_max() {
    let result: Result<i32, RetryError<&str>> = main_thread(async {
        retry(
            Some(3),
            Duration::from_millis(10),
            || async { Err("fail") },
            |_| true,
        )
        .await
    });
    assert_eq!(result, Err(RetryError::Failed("fail")));
}

#[test]
fn test_retry_should_retry_returns_false() {
    let result: Result<i32, RetryError<&str>> = main_thread(async {
        let attempts = Cell::new(0);
        retry(
            Some(10),
            Duration::from_millis(10),
            || async {
                attempts.set(attempts.get() + 1);
                Err("permanent")
            },
            |_| false,
        )
        .await
    });
    assert_eq!(result, Err(RetryError::Failed("permanent")));
}

#[test]
fn test_retry_none_max_retries_until_success() {
    let result: Result<i32, RetryError<&str>> = main_thread(async {
        let attempts = Cell::new(0);
        retry(
            None,
            Duration::from_millis(1),
            || async {
                let n = attempts.get() + 1;
                attempts.set(n);
                if n < 10 { Err("not yet") } else { Ok(n) }
            },
            |_| true,
        )
        .await
    });
    assert_eq!(result, Ok(10));
}

#[test]
fn test_retry_respects_delay() {
    main_thread(async {
        let start = Instant::now();
        let _: Result<i32, RetryError<&str>> = retry(
            Some(3),
            Duration::from_millis(50),
            || async { Err("fail") },
            |_| true,
        )
        .await;
        let elapsed = start.elapsed();
        // 3 attempts, 2 delays of 50ms each
        assert!(
            elapsed >= Duration::from_millis(90),
            "Expected at least 90ms of delay, got {:?}",
            elapsed
        );
    });
}

#[test]
fn test_retry_selective_should_retry() {
    #[derive(Debug, PartialEq)]
    enum MyErr {
        Transient,
        Permanent,
    }

    let result: Result<i32, RetryError<MyErr>> = main_thread(async {
        let attempts = Cell::new(0);
        retry(
            Some(10),
            Duration::from_millis(10),
            || async {
                let n = attempts.get() + 1;
                attempts.set(n);
                if n == 1 {
                    Err(MyErr::Transient)
                } else {
                    Err(MyErr::Permanent)
                }
            },
            |e| matches!(e, MyErr::Transient),
        )
        .await
    });
    assert_eq!(result, Err(RetryError::Failed(MyErr::Permanent)));
}
