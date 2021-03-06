use crossbeam::atomic::AtomicCell;
use std::{collections::HashMap, sync::Arc};

pub type Registry = HashMap<&'static str, Metric>;

#[derive(Clone, Debug, Default)]
pub struct Counter {
    cell: Arc<AtomicCell<u64>>,
}

impl Counter {
    pub fn inc(&self, value: u64) {
        self.cell.fetch_add(value);
    }
}

#[derive(Clone, Debug, Default)]
pub struct Gauge {
    cell: Arc<AtomicCell<i64>>,
}

impl Gauge {
    pub fn set(&self, val: i64) {
        self.cell.store(val)
    }
}

pub enum Metric {
    Counter(Counter),
    Gauge(Gauge),
}

impl Metric {
    pub(crate) fn read(&self) -> Reading {
        match self {
            Metric::Counter(x) => Reading::Count(x.cell.load()),
            Metric::Gauge(x) => Reading::Gauge(x.cell.load()),
        }
    }
}

#[derive(Debug)]
pub enum Reading {
    Count(u64),
    Gauge(i64),
    Message(String),
}

pub type Readings = Vec<(&'static str, Reading)>;

pub fn collect_readings(registry: &Registry) -> Readings {
    registry
        .iter()
        .map(|(key, value)| (&**key, value.read()))
        .collect::<Readings>()
}

pub struct Builder(Registry);

impl Builder {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    fn with_metric(self, key: &'static str, metric: Metric) -> Self {
        let Builder(mut metrics) = self;
        metrics.insert(key, metric);
        Self(metrics)
    }

    pub fn with_counter(self, key: &'static str, source: &Counter) -> Self {
        self.with_metric(key, Metric::Counter(source.clone()))
    }

    pub fn with_gauge(self, key: &'static str, source: &Gauge) -> Self {
        self.with_metric(key, Metric::Gauge(source.clone()))
    }

    pub fn build(self) -> Registry {
        self.0
    }
}
