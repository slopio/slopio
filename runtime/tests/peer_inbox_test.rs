//! Each peer inbox has a single consumer: a second future parked on it at
//! the same time is a bug and panics, while consumers one after another are fine.

use std::time::Duration;

use runtime::main_thread;
use runtime::runtime::select::{SelectResult, select};
use runtime::runtime::time::Sleep;
use runtime::runtime::{recv_from_peer, recv_msg_from_peer};

#[test]
#[should_panic(expected = "single consumer")]
fn two_futures_parked_on_the_peer_inbox_panic() {
    main_thread(async {
        let _ = select(recv_from_peer(), recv_from_peer()).await;
    });
}

#[test]
#[should_panic(expected = "single consumer")]
fn two_futures_parked_on_the_message_inbox_panic() {
    main_thread(async {
        let (a, b) = unsafe { (recv_msg_from_peer::<u32>(), recv_msg_from_peer::<u32>()) };
        let _ = select(a, b).await;
    });
}

#[test]
fn a_dropped_consumer_frees_the_inbox_for_the_next() {
    main_thread(async {
        for _ in 0..2 {
            match select(recv_from_peer(), Sleep::during(Duration::from_millis(10))).await {
                SelectResult::First(_) => panic!("no peer sent anything"),
                SelectResult::Second(()) => {}
            }
            let msg = unsafe { recv_msg_from_peer::<u32>() };
            match select(msg, Sleep::during(Duration::from_millis(10))).await {
                SelectResult::First(_) => panic!("no peer sent anything"),
                SelectResult::Second(()) => {}
            }
        }
    });
}
