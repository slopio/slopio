#[test]
fn debug_next_deadline() {
    use slopio::timers::TimeWheel;
    use std::sync::Arc;
    use std::task::{Wake, Waker};
    use std::time::Duration;

    struct NoopWake;
    impl Wake for NoopWake {
        fn wake(self: Arc<Self>) {}
    }

    let mut wheel = TimeWheel::new();
    std::thread::sleep(Duration::from_millis(5));
    wheel.tick();

    let waker = Waker::from(Arc::new(NoopWake));
    let _id = wheel.init_timer(Duration::from_millis(80), &waker).unwrap();

    let deadline = wheel.next_deadline();
    eprintln!("next_deadline after registering 80ms timer: {:?}", deadline);
    assert!(deadline.is_some());
    assert!(
        deadline.unwrap() >= Duration::from_millis(70),
        "Expected ~80ms, got {:?}",
        deadline
    );
}
