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
    let deadline = tokio::time::Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| Error::InvalidInput("poll timeout exceeds the clock range".into()))?;
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
        let next = tokio::time::Instant::now()
            .checked_add(interval)
            .map_or(deadline, |next| next.min(deadline));
        tokio::time::sleep_until(next).await;
        if tokio::time::Instant::now() >= deadline {
            return Err(timeout_error());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operations::wait_until;

    #[tokio::test]
    async fn unrepresentable_timeout_is_rejected_before_the_predicate() {
        let mut calls = 0;
        let result = wait_until(Duration::MAX, Duration::ZERO, || {
            calls += 1;
            async { Ok(true) }
        })
        .await;
        assert!(matches!(result, Err(Error::InvalidInput(_))));
        assert_eq!(calls, 0);
    }

    #[tokio::test]
    async fn oversized_interval_waits_only_until_the_deadline() {
        let mut calls = 0;
        let result = wait_until(Duration::from_millis(15), Duration::MAX, || {
            calls += 1;
            async { Ok(false) }
        })
        .await;
        assert!(matches!(result, Err(Error::VisibilityTimeout)));
        assert_eq!(calls, 1, "no predicate may start after the wait deadline");
    }

    #[tokio::test]
    async fn zero_timeout_still_allows_one_immediate_successful_observation() {
        wait_until(Duration::ZERO, Duration::MAX, || async { Ok(true) })
            .await
            .unwrap();
    }
}
