use crossbeam::atomic::AtomicCell;
use std::{collections::HashMap, sync::Arc};

#[derive(Default)]
pub struct Registry(HashMap<&'static str, Metric>);

impl Registry {
    pub fn track_counter(&mut self, key: &'static str, counter: &Counter) -> Counter {
        let metric = self
            .0
            .entry(key)
            .or_insert_with(|| Metric::Counter(counter.clone()));

        match metric {
            Metric::Counter(x) => x.clone(),
            _ => unreachable!(),
        }
    }

    pub fn track_gauge(&mut self, key: &'static str, gauge: &Gauge) -> Gauge {
        let metric = self
            .0
            .entry(key)
            .or_insert_with(|| Metric::Gauge(gauge.clone()));

        match metric {
            Metric::Gauge(x) => x.clone(),
            _ => unreachable!(),
        }
    }

    pub fn entries(&self) -> std::collections::hash_map::Iter<'_, &str, Metric> {
        self.0.iter()
    }
}

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
        .0
        .iter()
        .map(|(key, value)| (&**key, value.read()))
        .collect::<Readings>()
}

#[derive(Default)]
pub struct Builder(Registry);

impl Builder {
    fn with_metric(self, key: &'static str, metric: Metric) -> Self {
        let Builder(mut metrics) = self;
        metrics.0.insert(key, metric);
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
