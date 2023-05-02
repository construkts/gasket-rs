use serde::{Deserialize, Serialize};
use std::{ops::Mul, time::Duration};
use tracing::debug;

#[derive(Clone, Debug, Default)]
pub struct Retry(Option<usize>);

impl Retry {
    pub fn fresh() -> Self {
        Default::default()
    }

    pub fn next(self) -> Self {
        Self(Some(self.0.unwrap_or(0) + 1))
    }

    pub fn maxed(&self, policy: &Policy) -> bool {
        match &self.0 {
            Some(num) => *num >= policy.max_retries,
            None => false,
        }
    }

    pub fn dismissed(&self, policy: &Policy) -> bool {
        self.maxed(policy) && policy.dismissible
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

#[derive(Clone, Deserialize, Serialize, Default, Debug)]
pub struct Policy {
    pub max_retries: usize,
    pub backoff_unit: Duration,
    pub backoff_factor: u32,
    pub max_backoff: Duration,
    pub dismissible: bool,
}

impl Policy {
    pub fn no_retry() -> Self {
        Self {
            max_retries: 0,
            backoff_unit: Default::default(),
            backoff_factor: Default::default(),
            max_backoff: Default::default(),
            dismissible: Default::default(),
        }
    }
}

fn compute_backoff_delay(policy: &Policy, retry: usize) -> Duration {
    let units = policy.backoff_factor.pow(retry as u32);
    let backoff = policy.backoff_unit.mul(units);
    core::cmp::min(backoff, policy.max_backoff)
}
