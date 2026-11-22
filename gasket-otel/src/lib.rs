//! Export gasket stage metrics through an OpenTelemetry meter.
//!
//! Stages keep counting in gasket's own lock-free registry; [`register`]
//! installs observable instruments whose callbacks read every stage at
//! collection time. Nothing is recorded on the stages' hot path.

use std::{
    collections::BTreeMap,
    sync::{Arc, PoisonError, RwLock},
};

use gasket::{
    daemon::Daemon,
    metrics::{Reading, Readings},
};
use opentelemetry::{
    metrics::{AsyncInstrument, Meter, ObservableCounter, ObservableGauge},
    KeyValue,
};

/// The attribute that carries the stage name on every exported value.
pub const STAGE_ATTRIBUTE: &str = "stage";

/// How a gasket framework entry is exported.
struct FrameworkEntry {
    key: &'static str,
    name: &'static str,
    unit: &'static str,
    description: &'static str,
}

const TICKS: FrameworkEntry = FrameworkEntry {
    key: "gasket_ticks",
    name: "ticks",
    unit: "{tick}",
    description: "State-machine ticks of the stage",
};

const UNITS: FrameworkEntry = FrameworkEntry {
    key: "gasket_units",
    name: "units",
    unit: "{unit}",
    description: "Work units the stage executed, with success or error",
};

const RETRIES: FrameworkEntry = FrameworkEntry {
    key: "gasket_retries",
    name: "retries",
    unit: "{retry}",
    description: "Retries the stage took in any phase",
};

const PHASE: FrameworkEntry = FrameworkEntry {
    key: "gasket_phase",
    name: "phase",
    unit: "{phase}",
    description: "Current phase of the stage: 0 bootstrap, 1 working, 2 teardown, 3 ended",
};

const BUSY: FrameworkEntry = FrameworkEntry {
    key: "gasket_busy_micros",
    name: "busy_time",
    unit: "s",
    description: "Time the stage spent executing work units",
};

const FRAMEWORK: [&FrameworkEntry; 5] = [&TICKS, &UNITS, &RETRIES, &PHASE, &BUSY];

/// The daemon the callbacks read, until [`Instruments`] lets go of it.
type Source = Arc<RwLock<Option<Arc<Daemon>>>>;

/// Owns the daemon that [`register`] exports. Values are observed while it
/// lives; once it is dropped or [`Instruments::release`]d, the callbacks,
/// which stay registered with the meter provider, observe nothing and hold
/// no reference to the daemon, so it can be torn down.
#[must_use = "dropping the instruments stops the export"]
pub struct Instruments {
    source: Source,
    counters: Vec<ObservableCounter<u64>>,
    seconds: Vec<ObservableCounter<f64>>,
    gauges: Vec<ObservableGauge<i64>>,
}

impl Instruments {
    /// Stop the export and hand back the daemon. A collection in progress
    /// finishes first, so the bridge holds no reference once this returns.
    pub fn release(self) -> Arc<Daemon> {
        take(&self.source).expect("the daemon is held until released")
    }

    /// How many instruments were registered.
    pub fn len(&self) -> usize {
        self.counters.len() + self.seconds.len() + self.gauges.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Drop for Instruments {
    fn drop(&mut self) {
        take(&self.source);
    }
}

fn take(source: &Source) -> Option<Arc<Daemon>> {
    source
        .write()
        .unwrap_or_else(PoisonError::into_inner)
        .take()
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Count,
    Gauge,
}

/// Register one observable instrument per distinct metric key across the
/// daemon's stages, named `<namespace>.<key>` and observed once per stage
/// with the `stage` attribute. Framework entries drop their `gasket_` prefix
/// and carry units; `gasket_busy_micros` becomes `<namespace>.busy_time` in
/// seconds. Keys are discovered from the stages that are alive now. The
/// returned [`Instruments`] owns the daemon for as long as it exports.
pub fn register(meter: &Meter, namespace: &str, daemon: Arc<Daemon>) -> Instruments {
    let keys = discover_keys(&daemon);

    let mut instruments = Instruments {
        source: Arc::new(RwLock::new(Some(daemon))),
        counters: Vec::new(),
        seconds: Vec::new(),
        gauges: Vec::new(),
    };

    for (key, kind) in keys {
        let source = Arc::clone(&instruments.source);

        match (framework_entry(key), kind) {
            (Some(entry), _) if entry.key == BUSY.key => {
                let counter = meter
                    .f64_observable_counter(format!("{namespace}.{}", entry.name))
                    .with_unit(entry.unit)
                    .with_description(entry.description)
                    .with_callback(move |observer| {
                        observe(&source, key, observer, |reading| match reading {
                            Reading::Count(micros) => Some(*micros as f64 / 1_000_000.0),
                            _ => None,
                        })
                    })
                    .build();

                instruments.seconds.push(counter);
            }
            (entry, Kind::Count) => {
                let builder = meter.u64_observable_counter(export_name(namespace, key, entry));

                let builder = match entry {
                    Some(entry) => builder
                        .with_unit(entry.unit)
                        .with_description(entry.description),
                    None => builder,
                };

                let counter = builder
                    .with_callback(move |observer| {
                        observe(&source, key, observer, |reading| match reading {
                            Reading::Count(x) => Some(*x),
                            _ => None,
                        })
                    })
                    .build();

                instruments.counters.push(counter);
            }
            (entry, Kind::Gauge) => {
                let builder = meter.i64_observable_gauge(export_name(namespace, key, entry));

                let builder = match entry {
                    Some(entry) => builder
                        .with_unit(entry.unit)
                        .with_description(entry.description),
                    None => builder,
                };

                let gauge = builder
                    .with_callback(move |observer| {
                        observe(&source, key, observer, |reading| match reading {
                            Reading::Gauge(x) => Some(*x),
                            _ => None,
                        })
                    })
                    .build();

                instruments.gauges.push(gauge);
            }
        }
    }

    instruments
}

fn framework_entry(key: &str) -> Option<&'static FrameworkEntry> {
    FRAMEWORK.into_iter().find(|entry| entry.key == key)
}

fn export_name(namespace: &str, key: &str, entry: Option<&FrameworkEntry>) -> String {
    match entry {
        Some(entry) => format!("{namespace}.{}", entry.name),
        None => format!("{namespace}.{key}"),
    }
}

/// The distinct counter and gauge keys across the readable stages. A key's
/// kind is the one its first stage declares. A stage key equal to a framework
/// export name (eg: `ticks`) would collide with it and is left out.
fn discover_keys(daemon: &Daemon) -> BTreeMap<&'static str, Kind> {
    let mut keys = BTreeMap::new();

    for readings in daemon.tethers().filter_map(|t| t.read_metrics().ok()) {
        for (key, reading) in readings {
            let kind = match reading {
                Reading::Count(_) => Kind::Count,
                Reading::Gauge(_) => Kind::Gauge,
                Reading::Message(_) => continue,
            };

            let collides =
                framework_entry(key).is_none() && FRAMEWORK.iter().any(|entry| entry.name == key);

            if !collides {
                keys.entry(key).or_insert(kind);
            }
        }
    }

    keys
}

/// Observe `key` once per stage that holds it with the instrument's kind.
/// Stages whose tether has dropped are skipped, and nothing is observed once
/// the daemon was released.
fn observe<T>(
    source: &Source,
    key: &str,
    observer: &dyn AsyncInstrument<T>,
    value: impl Fn(&Reading) -> Option<T>,
) {
    let guard = source.read().unwrap_or_else(PoisonError::into_inner);

    let Some(daemon) = guard.as_ref() else {
        return;
    };

    for tether in daemon.tethers() {
        let Ok(readings) = tether.read_metrics() else {
            continue;
        };

        if let Some(x) = find(&readings, key).and_then(&value) {
            observer.observe(
                x,
                &[KeyValue::new(STAGE_ATTRIBUTE, tether.name().to_owned())],
            );
        }
    }
}

fn find<'a>(readings: &'a Readings, key: &str) -> Option<&'a Reading> {
    readings.iter().find(|(k, _)| *k == key).map(|(_, r)| r)
}
