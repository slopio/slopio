use std::time::Duration;

use crate::io_driver::IO_DRIVER;
use crate::runtime::time::Sleep;

/// Repeatedly runs a future factory in a loop, sleeping `interval` between
/// each iteration. Stops when a graceful shutdown is detected.
///
/// ```rust,ignore
/// repeat(Duration::from_secs(5), || async {
///     println!("tick");
/// }).await;
/// // only returns on shutdown
/// ```
pub async fn repeat<F, Fut>(interval: Duration, f: F)
where
    F: Fn() -> Fut,
    Fut: Future,
{
    loop {
        if IO_DRIVER.with(|d| d.shutdown()) {
            return;
        }
        f().await;
        Sleep::during(interval).await;
    }
}
