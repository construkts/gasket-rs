# gasket-otel

Export the metrics of every [gasket](../README.md) stage through an
OpenTelemetry meter. Stages keep counting in gasket's lock-free registry; the
bridge registers observable instruments that read each stage when the meter
provider collects, so nothing is added to the stages' hot path.

The crate depends only on the `opentelemetry` API (0.32). Bring your own meter
provider and exporter (OTLP, Prometheus, ...).

## Usage

```rust
use std::sync::Arc;
use opentelemetry::global;

let daemon = Arc::new(gasket::daemon::Daemon::new(tethers));

let meter = global::meter("my-app");
let _instruments = gasket_otel::register(&meter, "my_app", Arc::clone(&daemon));
```

Keep the returned `Instruments` for as long as the metrics should be exported.
Instruments are created for the metric keys of the stages that are alive when
`register` is called.

## Exported names

Every value is observed once per stage, with the attribute `stage` set to the
stage name.

| Source in the stage registry | Instrument | Kind | Unit |
| --- | --- | --- | --- |
| a `#[metric]` counter `<key>` | `<namespace>.<key>` | counter (u64) | |
| a `#[metric]` gauge `<key>` | `<namespace>.<key>` | gauge (i64) | |
| `gasket_ticks` | `<namespace>.ticks` | counter (u64) | `{tick}` |
| `gasket_units` | `<namespace>.units` | counter (u64) | `{unit}` |
| `gasket_retries` | `<namespace>.retries` | counter (u64) | `{retry}` |
| `gasket_phase` | `<namespace>.phase` | gauge (i64) | `{phase}` |
| `gasket_busy_micros` | `<namespace>.busy_time` | counter (f64) | `s` |

- Two stages that declare the same key share one instrument, with one
  attribute set each. The key's kind is the one the first stage declares.
- A stage key equal to a framework name (`ticks`, `units`, `retries`, `phase`,
  `busy_time`) would collide with it and is not exported.
- A stage whose tether has dropped is skipped at collection.
- Message readings are ignored.
