use std::{future::Future, time::Duration};

use crate::{Error, Result};

pub(super) async fn poll_until<T, F, Fut>(
    timeout: Duration,
    interval: Duration,
    timeout_error: fn() -> Error,
    mut attempt: F,
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<Option<T>>>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let result = tokio::time::timeout_at(deadline, attempt())
            .await
            .map_err(|_| timeout_error())??;
        if let Some(value) = result {
            return Ok(value);
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(timeout_error());
        }
        tokio::time::sleep_until((tokio::time::Instant::now() + interval).min(deadline)).await;
    }
}
