use std::{
    ops::Mul,
    time::{Duration, Instant},
};

use tracing::{debug, warn};

use crate::error::Error;

#[derive(Clone, Debug)]
pub struct Retry(Option<usize>);

impl Retry {
    pub fn new() -> Self {
        Self(None)
    }

    pub fn next(&self) -> Self {
        Self(Some(self.0.unwrap_or(0) + 1))
    }

    pub fn has_next(&self, policy: &Policy) -> bool {
        match &self.0 {
            Some(num) => *num <= policy.max_retries,
            None => true,
        }
    }

    pub async fn wait_backoff(
        &self,
        policy: &Policy,
        mut cancel: tokio::sync::watch::Receiver<bool>,
    ) {
        let num = match &self.0 {
            Some(x) => x,
            None => return,
        };

        let backoff = compute_backoff_delay(policy, *num);

        debug!(
            "backoff for {}s until next retry #{}",
            backoff.as_secs(),
            num
        );

        tokio::select! {
            _ = cancel.changed() => (),
            _ = tokio::time::sleep(backoff) => ()
        }
    }
}

#[derive(Clone)]
pub struct Policy {
    pub max_retries: usize,
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

fn compute_backoff_delay(policy: &Policy, retry: usize) -> Duration {
    let units = policy.backoff_factor.pow(retry as u32);
    let backoff = policy.backoff_unit.mul(units);
    core::cmp::min(backoff, policy.max_backoff)
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
