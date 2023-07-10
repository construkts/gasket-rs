use serde::{de::Visitor, Deserialize, Deserializer, Serialize};
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
    #[serde(rename(deserialize = "backoff_unit_sec"))]
    #[serde(deserialize_with = "deserialize_duration")]
    pub backoff_unit: Duration,
    pub backoff_factor: u32,
    #[serde(rename(deserialize = "max_backoff_sec"))]
    #[serde(deserialize_with = "deserialize_duration")]
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

fn deserialize_duration<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_map(DurationVisitor)
}

struct DurationVisitor;
impl<'de> Visitor<'de> for DurationVisitor {
    type Value = Duration;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("This Visitor expects to receive i64 seconds")
    }

    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Duration::from_secs(v as u64))
    }
}
