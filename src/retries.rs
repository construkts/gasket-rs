use std::{
    ops::Mul,
    time::{Duration, Instant},
};

use crossbeam::{atomic::AtomicCell, utils::Backoff};
use tracing::{debug, warn};

use crate::{error::Error, runtime::Worker};

#[derive(Clone)]
pub struct Policy {
    pub max_retries: u32,
    pub backoff_unit: Duration,
    pub backoff_factor: u32,
    pub max_backoff: Duration,
}

impl Policy {
    pub fn no_retry() -> Self {
        Self {
            max_retries: 0,
            backoff_unit: Duration::from_secs(1),
            backoff_factor: 2,
            max_backoff: Duration::from_secs(60),
        }
    }
}

fn compute_backoff_delay(policy: &Policy, retry: u32) -> Duration {
    let units = policy.backoff_factor.pow(retry);
    let backoff = policy.backoff_unit.mul(units);
    core::cmp::min(backoff, policy.max_backoff)
}

#[inline]
fn should_cancel(flag: Option<&AtomicCell<bool>>) -> bool {
    match flag {
        Some(cancel) => cancel.load(),
        None => false,
    }
}

pub fn sleep_except_cancel(duration: Duration, cancel: Option<&AtomicCell<bool>>) {
    let start = Instant::now();
    let snoozer = Backoff::new();

    while !should_cancel(cancel) && start.elapsed() <= duration {
        snoozer.snooze();
    }
}

pub async fn retry_unit<W>(
    worker: &mut W,
    unit: &mut W::WorkUnit,
    policy: &Policy,
    cancel: Option<&AtomicCell<bool>>,
) -> Result<(), Error>
where
    W: Worker,
{
    let mut retry = 0;

    loop {
        if should_cancel(cancel) {
            break Err(Error::Cancelled);
        }

        let result = worker.execute(unit).await;

        match result {
            Err(Error::RetryableError) if retry < policy.max_retries => {
                warn!("retryable operation error");

                retry += 1;

                let backoff = compute_backoff_delay(policy, retry);

                debug!(
                    "backoff for {}s until next retry #{}",
                    backoff.as_secs(),
                    retry
                );

                sleep_except_cancel(backoff, cancel);
            }
            Err(Error::RetryableError) => {
                warn!("max retries reached");
                break Err(Error::RetryableError);
            }
            x => break x,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    struct FailingUnit {
        attempt: u16,
        delay: Option<Duration>,
    }

    impl WorkUnit for FailingUnit {
        async fn attempt(&mut self) -> Result<(), Error> {
            if let Some(delay) = self.delay {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }

            self.attempt += 1;
            Err(Error::RetryableError)
        }
    }

    #[tokio::test]
    async fn honors_max_retries() {
        let cancel = AtomicCell::new(false);

        let policy = Policy {
            max_retries: 3,
            backoff_unit: Duration::from_secs(1),
            backoff_factor: 0,
            max_backoff: Duration::from_secs(100),
        };

        let mut u1 = FailingUnit {
            attempt: 0,
            delay: Duration::from_secs(1).into(),
        };

        let result = retry_unit(&mut u1, &policy, Some(&cancel)).await;

        assert!(result.is_err());

        assert_eq!(u1.attempt, 4);
    }

    #[tokio::test]
    async fn honors_exponential_backoff() {
        let mut u1 = FailingUnit {
            attempt: 0,
            delay: None,
        };

        let policy = Policy {
            max_retries: 10,
            backoff_unit: Duration::from_millis(1),
            backoff_factor: 2,
            max_backoff: Duration::MAX,
        };

        let start = std::time::Instant::now();
        let cancel = AtomicCell::new(false);

        let result = retry_unit(&mut u1, &policy, Some(&cancel)).await;
        let elapsed = start.elapsed();

        assert!(result.is_err());

        // not an exact science, should be 2046, adding +/- 10%
        assert!(elapsed.as_millis() >= 1842);
        assert!(elapsed.as_millis() <= 2250);
    }

    #[tokio::test]
    async fn honors_cancel() {
        let mut u1 = FailingUnit {
            attempt: 0,
            delay: None,
        };

        let policy = Policy {
            max_retries: 100,
            backoff_unit: Duration::from_millis(2000),
            backoff_factor: 2,
            max_backoff: Duration::MAX,
        };

        let start = std::time::Instant::now();
        let cancel = Arc::new(AtomicCell::new(false));

        let cancel2 = cancel.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(500));
            cancel2.store(true);
        });

        let result = retry_unit(&mut u1, &policy, Some(&cancel)).await;
        let elapsed = start.elapsed();

        assert!(result.is_err());

        // not an exact science, should be 2046, adding +/- 10%
        assert!(elapsed.as_millis() >= 450);
        assert!(elapsed.as_millis() <= 550);
    }
}
