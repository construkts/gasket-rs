use std::{
    ops::Mul,
    time::{Duration, Instant},
};

use crossbeam::{atomic::AtomicCell, utils::Backoff};

use crate::error::Error;

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

pub fn sleep_except_cancel(duration: Duration, cancel: &AtomicCell<bool>) {
    let start = Instant::now();
    let snoozer = Backoff::new();

    while !cancel.load() && start.elapsed() <= duration {
        snoozer.snooze();
    }
}

pub fn retry_operation<T>(
    mut op: impl FnMut() -> Result<T, Error>,
    policy: &Policy,
    cancel: &AtomicCell<bool>,
) -> Result<T, Error> {
    let mut retry = 0;

    loop {
        if cancel.load() {
            break Err(Error::Cancelled);
        }

        let result = op();

        match result {
            Ok(x) => break Ok(x),
            Err(err) if retry < policy.max_retries => {
                log::warn!("retryable operation error: {:?}", err);

                retry += 1;

                let backoff = compute_backoff_delay(policy, retry);

                log::debug!(
                    "backoff for {}s until next retry #{}",
                    backoff.as_secs(),
                    retry
                );

                sleep_except_cancel(backoff, cancel);
            }
            Err(x) => {
                log::error!("max retries reached, failing whole operation");
                break Err(x);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc, sync::Arc};

    use super::*;

    #[test]
    fn honors_max_retries() {
        let counter = Rc::new(RefCell::new(0));
        let cancel = AtomicCell::new(false);

        let inner_counter = counter.clone();
        let op = move || -> Result<(), Error> {
            *inner_counter.borrow_mut() += 1;
            Err(Error::WorkError("very bad stuff happened".to_string()))
        };

        let policy = Policy {
            max_retries: 3,
            backoff_unit: Duration::from_secs(1),
            backoff_factor: 0,
            max_backoff: Duration::from_secs(100),
        };

        assert!(retry_operation(op, &policy, &cancel).is_err());

        assert_eq!(*counter.borrow(), 4);
    }

    #[test]
    fn honors_exponential_backoff() {
        let op = move || -> Result<(), Error> {
            Err(Error::WorkError("very bad stuff happened".to_string()))
        };

        let policy = Policy {
            max_retries: 10,
            backoff_unit: Duration::from_millis(1),
            backoff_factor: 2,
            max_backoff: Duration::MAX,
        };

        let start = std::time::Instant::now();
        let cancel = AtomicCell::new(false);
        let result = retry_operation(op, &policy, &cancel);
        let elapsed = start.elapsed();

        assert!(result.is_err());

        // not an exact science, should be 2046, adding +/- 10%
        assert!(elapsed.as_millis() >= 1842);
        assert!(elapsed.as_millis() <= 2250);
    }

    #[test]
    fn honors_cancel() {
        let op = move || -> Result<(), Error> {
            Err(Error::WorkError("very bad stuff happened".to_string()))
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

        let result = retry_operation(op, &policy, &cancel);
        let elapsed = start.elapsed();

        assert!(result.is_err());

        // not an exact science, should be 2046, adding +/- 10%
        assert!(elapsed.as_millis() >= 450);
        assert!(elapsed.as_millis() <= 550);
    }
}
