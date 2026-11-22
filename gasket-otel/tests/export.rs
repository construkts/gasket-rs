use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use gasket::{
    daemon::Daemon,
    framework::{Stage, WorkSchedule, Worker, WorkerError},
    metrics::{self, Counter, Gauge},
    runtime::{spawn_stage, Policy},
};
use opentelemetry::{metrics::MeterProvider as _, KeyValue, Value};
use opentelemetry_sdk::metrics::{
    data::{AggregatedMetrics, Metric, MetricData, ResourceMetrics},
    InMemoryMetricExporter, PeriodicReader, SdkMeterProvider,
};

/// A stage that executes `units` work units, counting them and setting a
/// gauge to the last one, then idles until dismissed or until `finish` is set.
/// With an `own` key it also counts its units under that key.
struct TestStage {
    name: &'static str,
    units: u64,
    finish: Arc<AtomicBool>,
    own: Option<&'static str>,
    processed: Counter,
    level: Gauge,
    owned: Counter,
}

impl TestStage {
    fn new(name: &'static str, units: u64) -> Self {
        Self {
            name,
            units,
            finish: Default::default(),
            own: None,
            processed: Default::default(),
            level: Default::default(),
            owned: Default::default(),
        }
    }

    fn with_own_key(mut self, key: &'static str) -> Self {
        self.own = Some(key);
        self
    }

    fn finish_on(mut self, flag: &Arc<AtomicBool>) -> Self {
        self.finish = Arc::clone(flag);
        self
    }
}

impl Stage for TestStage {
    type Unit = u64;
    type Worker = TestWorker;

    fn name(&self) -> &str {
        self.name
    }

    fn metrics(&self) -> metrics::Registry {
        let builder = metrics::Builder::default()
            .with_counter("processed", &self.processed)
            .with_gauge("level", &self.level);

        match self.own {
            Some(key) => builder.with_counter(key, &self.owned).build(),
            None => builder.build(),
        }
    }
}

struct TestWorker(u64);

#[async_trait::async_trait(?Send)]
impl Worker<TestStage> for TestWorker {
    async fn bootstrap(_: &TestStage) -> Result<Self, WorkerError> {
        Ok(Self(0))
    }

    async fn schedule(&mut self, stage: &mut TestStage) -> Result<WorkSchedule<u64>, WorkerError> {
        if self.0 < stage.units {
            self.0 += 1;
            return Ok(WorkSchedule::Unit(self.0));
        }

        if stage.finish.load(Ordering::Relaxed) {
            return Ok(WorkSchedule::Done);
        }

        tokio::time::sleep(Duration::from_millis(5)).await;
        Ok(WorkSchedule::Idle)
    }

    async fn execute(&mut self, unit: &u64, stage: &mut TestStage) -> Result<(), WorkerError> {
        tokio::time::sleep(Duration::from_millis(2)).await;
        stage.processed.inc(1);
        stage.owned.inc(1);
        stage.level.set(*unit as i64 * 10);
        Ok(())
    }
}

fn daemon(stages: Vec<TestStage>) -> Arc<Daemon> {
    let tethers = stages
        .into_iter()
        .map(|stage| spawn_stage(stage, Policy::default()))
        .collect();

    Arc::new(Daemon::new(tethers))
}

/// Release the daemon from the bridge and tear it down; `daemon` is the
/// test's own handle, dropped first so the bridge's is the last one.
fn shutdown(instruments: gasket_otel::Instruments, daemon: Arc<Daemon>) {
    drop(daemon);

    let daemon = Arc::try_unwrap(instruments.release()).expect("the bridge is the last owner");
    daemon.teardown();
}

/// Wait until `tether` has dropped, as a stage that ended does.
fn wait_dropped(daemon: &Daemon, name: &str) {
    let tether = daemon.tethers().find(|t| t.name() == name).unwrap();

    for _ in 0..500 {
        if tether.read_metrics().is_err() {
            return;
        }
        std::thread::sleep(Duration::from_millis(5));
    }

    panic!("{name}'s tether should drop");
}

/// Wait until every stage that is still readable reports `processed == units`.
fn wait_processed(daemon: &Daemon, units: u64) {
    for _ in 0..500 {
        let done = daemon.tethers().all(|tether| match tether.read_metrics() {
            Ok(readings) => readings.iter().any(|(k, r)| {
                *k == "processed" && matches!(r, metrics::Reading::Count(x) if *x == units)
            }),
            Err(_) => true,
        });

        if done {
            return;
        }

        std::thread::sleep(Duration::from_millis(5));
    }

    panic!("stages didn't process their units in time");
}

struct Harness {
    provider: SdkMeterProvider,
    exporter: InMemoryMetricExporter,
}

impl Harness {
    fn new() -> Self {
        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_reader(PeriodicReader::builder(exporter.clone()).build())
            .build();

        Self { provider, exporter }
    }

    fn collect(&self) -> ResourceMetrics {
        self.try_collect().expect("a collection")
    }

    /// The collection, if the provider exported one; it exports nothing when
    /// no instrument observed a value.
    fn try_collect(&self) -> Option<ResourceMetrics> {
        self.exporter.reset();
        self.provider.force_flush().unwrap();
        self.exporter.get_finished_metrics().unwrap().pop()
    }
}

fn metric<'a>(collected: &'a ResourceMetrics, name: &str) -> Option<&'a Metric> {
    collected
        .scope_metrics()
        .flat_map(|scope| scope.metrics())
        .find(|metric| metric.name() == name)
}

fn stage_of<'a>(mut attributes: impl Iterator<Item = &'a KeyValue>) -> String {
    let attribute = attributes
        .find(|kv| kv.key.as_str() == "stage")
        .expect("a stage attribute");

    match &attribute.value {
        Value::String(x) => x.to_string(),
        other => panic!("unexpected stage value {other:?}"),
    }
}

/// The value per stage of a metric, as f64 whatever its type.
fn per_stage(collected: &ResourceMetrics, name: &str) -> Vec<(String, f64)> {
    let metric = metric(collected, name).unwrap_or_else(|| panic!("no metric {name}"));

    let mut values: Vec<_> = match metric.data() {
        AggregatedMetrics::U64(MetricData::Sum(sum)) => sum
            .data_points()
            .map(|p| (stage_of(p.attributes()), p.value() as f64))
            .collect(),
        AggregatedMetrics::F64(MetricData::Sum(sum)) => sum
            .data_points()
            .map(|p| (stage_of(p.attributes()), p.value()))
            .collect(),
        AggregatedMetrics::I64(MetricData::Gauge(gauge)) => gauge
            .data_points()
            .map(|p| (stage_of(p.attributes()), p.value() as f64))
            .collect(),
        _ => panic!("unexpected data for {name}"),
    };

    values.sort_by(|a, b| a.0.cmp(&b.0));
    values
}

fn stages(values: &[(String, f64)]) -> Vec<&str> {
    values.iter().map(|(stage, _)| stage.as_str()).collect()
}

#[test]
fn exports_stage_counter_and_gauge_per_stage() {
    let daemon = daemon(vec![TestStage::new("alpha", 3), TestStage::new("beta", 3)]);
    wait_processed(&daemon, 3);

    let harness = Harness::new();
    let meter = harness.provider.meter("test");
    let instruments = gasket_otel::register(&meter, "app", Arc::clone(&daemon));

    let collected = harness.collect();

    assert_eq!(
        per_stage(&collected, "app.processed"),
        vec![("alpha".into(), 3.0), ("beta".into(), 3.0)]
    );
    assert_eq!(
        per_stage(&collected, "app.level"),
        vec![("alpha".into(), 30.0), ("beta".into(), 30.0)]
    );

    assert!(matches!(
        metric(&collected, "app.processed").unwrap().data(),
        AggregatedMetrics::U64(MetricData::Sum(sum)) if sum.is_monotonic()
    ));
    assert!(matches!(
        metric(&collected, "app.level").unwrap().data(),
        AggregatedMetrics::I64(MetricData::Gauge(_))
    ));

    shutdown(instruments, daemon);
}

#[test]
fn exports_framework_series_without_prefix_and_with_units() {
    let daemon = daemon(vec![TestStage::new("alpha", 4), TestStage::new("beta", 4)]);
    wait_processed(&daemon, 4);

    let harness = Harness::new();
    let meter = harness.provider.meter("test");
    let instruments = gasket_otel::register(&meter, "app", Arc::clone(&daemon));

    let collected = harness.collect();

    for (name, unit) in [
        ("app.ticks", "{tick}"),
        ("app.units", "{unit}"),
        ("app.retries", "{retry}"),
        ("app.phase", "{phase}"),
        ("app.busy_time", "s"),
    ] {
        let values = per_stage(&collected, name);
        assert_eq!(stages(&values), vec!["alpha", "beta"], "stages of {name}");
        assert_eq!(
            metric(&collected, name).unwrap().unit(),
            unit,
            "unit of {name}"
        );
    }

    let gasket_prefixed = collected
        .scope_metrics()
        .flat_map(|scope| scope.metrics())
        .any(|metric| metric.name().contains("gasket_"));
    assert!(!gasket_prefixed, "framework names keep no gasket_ prefix");

    // each stage worked, and each unit slept at least 2ms inside `execute`
    for (stage, units) in per_stage(&collected, "app.units") {
        assert_eq!(units, 4.0, "units of {stage}");
    }

    for (stage, phase) in per_stage(&collected, "app.phase") {
        assert_eq!(phase, 1.0, "phase of {stage}");
    }

    assert!(matches!(
        metric(&collected, "app.busy_time").unwrap().data(),
        AggregatedMetrics::F64(MetricData::Sum(_))
    ));

    for (stage, seconds) in per_stage(&collected, "app.busy_time") {
        assert!(
            (0.008..1.0).contains(&seconds),
            "busy_time of {stage} is {seconds}, expected seconds"
        );
    }

    shutdown(instruments, daemon);
}

#[test]
fn skips_a_stage_whose_tether_dropped() {
    let finish = Arc::new(AtomicBool::new(false));
    let daemon = daemon(vec![
        TestStage::new("alpha", 2).finish_on(&finish),
        TestStage::new("beta", 2),
    ]);
    wait_processed(&daemon, 2);

    let harness = Harness::new();
    let meter = harness.provider.meter("test");
    let instruments = gasket_otel::register(&meter, "app", Arc::clone(&daemon));

    let before = harness.collect();
    assert_eq!(
        stages(&per_stage(&before, "app.level")),
        vec!["alpha", "beta"]
    );

    finish.store(true, Ordering::Relaxed);
    wait_dropped(&daemon, "alpha");

    let collected = harness.collect();

    assert_eq!(
        per_stage(&collected, "app.level"),
        vec![("beta".into(), 20.0)]
    );
    assert_eq!(stages(&per_stage(&collected, "app.phase")), vec!["beta"]);

    shutdown(instruments, daemon);
}

#[test]
fn one_instrument_for_a_key_shared_by_two_stages() {
    let daemon = daemon(vec![TestStage::new("alpha", 1), TestStage::new("beta", 2)]);
    wait_processed_each(&daemon, &[("alpha", 1), ("beta", 2)]);

    let harness = Harness::new();
    let meter = harness.provider.meter("test");
    let instruments = gasket_otel::register(&meter, "app", Arc::clone(&daemon));

    // two stage keys plus the five framework entries, shared by both stages
    assert_eq!(instruments.len(), 7);

    let collected = harness.collect();

    let named = collected
        .scope_metrics()
        .flat_map(|scope| scope.metrics())
        .filter(|metric| metric.name() == "app.processed")
        .count();
    assert_eq!(named, 1);

    assert_eq!(
        per_stage(&collected, "app.processed"),
        vec![("alpha".into(), 1.0), ("beta".into(), 2.0)]
    );

    shutdown(instruments, daemon);
}

#[test]
fn exports_a_key_only_one_stage_declares() {
    let daemon = daemon(vec![
        TestStage::new("alpha", 2).with_own_key("read"),
        TestStage::new("beta", 3).with_own_key("written"),
    ]);
    wait_processed_each(&daemon, &[("alpha", 2), ("beta", 3)]);

    let harness = Harness::new();
    let meter = harness.provider.meter("test");
    let instruments = gasket_otel::register(&meter, "app", Arc::clone(&daemon));

    let collected = harness.collect();

    assert_eq!(
        per_stage(&collected, "app.read"),
        vec![("alpha".into(), 2.0)]
    );
    assert_eq!(
        per_stage(&collected, "app.written"),
        vec![("beta".into(), 3.0)]
    );
    assert_eq!(
        stages(&per_stage(&collected, "app.processed")),
        vec!["alpha", "beta"]
    );

    shutdown(instruments, daemon);
}

#[test]
fn registers_after_a_stage_ended() {
    let finish = Arc::new(AtomicBool::new(true));
    let daemon = daemon(vec![
        TestStage::new("alpha", 1).finish_on(&finish),
        TestStage::new("beta", 2),
    ]);
    wait_dropped(&daemon, "alpha");
    wait_processed_each(&daemon, &[("beta", 2)]);

    let harness = Harness::new();
    let meter = harness.provider.meter("test");
    let instruments = gasket_otel::register(&meter, "app", Arc::clone(&daemon));

    let collected = harness.collect();

    assert_eq!(
        per_stage(&collected, "app.processed"),
        vec![("beta".into(), 2.0)]
    );
    assert_eq!(stages(&per_stage(&collected, "app.phase")), vec!["beta"]);

    shutdown(instruments, daemon);
}

#[test]
fn dropping_the_instruments_frees_the_daemon() {
    let daemon = daemon(vec![TestStage::new("alpha", 1), TestStage::new("beta", 1)]);
    wait_processed(&daemon, 1);

    // the provider and its callbacks outlive the instruments
    let harness = Harness::new();
    let meter = harness.provider.meter("test");
    let instruments = gasket_otel::register(&meter, "app", Arc::clone(&daemon));

    assert_eq!(
        stages(&per_stage(&harness.collect(), "app.level")),
        vec!["alpha", "beta"]
    );

    drop(instruments);

    assert!(observed(harness.try_collect(), "app.level").is_empty());

    let daemon = Arc::try_unwrap(daemon).expect("the bridge let go of the daemon");
    daemon.teardown();

    assert!(observed(harness.try_collect(), "app.level").is_empty());
}

#[test]
fn release_hands_back_the_daemon_for_teardown() {
    let daemon = daemon(vec![TestStage::new("alpha", 1), TestStage::new("beta", 1)]);
    wait_processed(&daemon, 1);

    let harness = Harness::new();
    let meter = harness.provider.meter("test");

    // the bridge takes the only handle
    let instruments = gasket_otel::register(&meter, "app", daemon);

    assert_eq!(
        stages(&per_stage(&harness.collect(), "app.processed")),
        vec!["alpha", "beta"]
    );

    let daemon = Arc::try_unwrap(instruments.release()).expect("the bridge holds no other handle");
    assert!(observed(harness.try_collect(), "app.processed").is_empty());

    daemon.teardown();
}

/// The stages a metric was observed for, empty when it wasn't collected.
fn observed(collected: Option<ResourceMetrics>, name: &str) -> Vec<String> {
    match collected {
        Some(collected) if metric(&collected, name).is_some() => per_stage(&collected, name)
            .into_iter()
            .map(|(stage, _)| stage)
            .collect(),
        _ => Vec::new(),
    }
}

fn wait_processed_each(daemon: &Daemon, expected: &[(&str, u64)]) {
    for (name, units) in expected {
        let tether = daemon.tethers().find(|t| t.name() == *name).unwrap();

        let reached = (0..500).any(|_| {
            let reached = tether.read_metrics().unwrap().iter().any(|(k, r)| {
                *k == "processed" && matches!(r, metrics::Reading::Count(x) if x == units)
            });

            if !reached {
                std::thread::sleep(Duration::from_millis(5));
            }

            reached
        });

        assert!(reached, "{name} didn't process {units} units in time");
    }
}
