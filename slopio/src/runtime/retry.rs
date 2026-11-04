use std::time::Duration;

use crate::io_driver::IO_DRIVER;
use crate::runtime::time::Sleep;

#[derive(Debug, PartialEq)]
pub enum RetryError<E> {
    Failed(E),
    Shutdown,
}

pub async fn retry<T, E, F, Fut, R>(
    max: Option<usize>,
    delay: Duration,
    f: F,
    should_retry: R,
) -> Result<T, RetryError<E>>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    R: Fn(&E) -> bool,
{
    let mut attempt = 0;
    loop {
        attempt += 1;
        match f().await {
            Ok(v) => return Ok(v),
            // `Some(0)` still makes the one attempt that yields an error to return.
            Err(e) if max.is_some_and(|m| attempt >= m) => return Err(RetryError::Failed(e)),
            Err(_) if IO_DRIVER.with(|d| d.shutdown()) => return Err(RetryError::Shutdown),
            Err(e) if should_retry(&e) => Sleep::during(delay).await,
            Err(e) => return Err(RetryError::Failed(e)),
        }
    }
}
