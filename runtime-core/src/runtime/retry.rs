use std::time::Duration;

use crate::runtime::time::Sleep;

pub async fn retry<T, E, F, Fut, R>(
    max: Option<usize>,
    delay: Duration,
    f: F,
    should_retry: R,
) -> Result<T, E>
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
            Err(e) if max == Some(attempt) => return Err(e),
            Err(e) if should_retry(&e) => Sleep::during(delay).await,
            Err(e) => return Err(e),
        }
    }
}
