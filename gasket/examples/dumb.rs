use std::time::{Duration, Instant};

use gasket::{
    framework::{AsWorkError, Stage, WorkSchedule, Worker, WorkerError},
    messaging::tokio::{connect_ports, InputPort, OutputPort},
    metrics::Counter,
    retries,
    runtime::{spawn_stage, Policy},
};

#[derive(Clone)]
struct TickerSpec {
    output: OutputPort<Instant>,
    value_1: Counter,
}

impl Stage for TickerSpec {
    fn name(&self) -> &str {
        "ticker"
    }

    fn policy(&self) -> gasket::runtime::Policy {
        Policy {
            tick_timeout: Some(Duration::from_secs(3)),
            bootstrap_retry: retries::Policy::no_retry(),
            work_retry: retries::Policy::no_retry(),
            teardown_retry: retries::Policy::no_retry(),
        }
    }

    fn register_metrics(&self, registry: &mut gasket::metrics::Registry) {
        registry.track_counter("value_1", &self.value_1);
    }
}

struct Ticker {
    next_delay: u64,
}

struct TickerUnit {
    instant: Instant,
    delay: u64,
}

#[async_trait::async_trait(?Send)]
impl Worker for Ticker {
    type Unit = TickerUnit;
    type Stage = TickerSpec;

    async fn bootstrap(_: &Self::Stage) -> Result<Self, WorkerError> {
        Ok(Self {
            next_delay: Default::default(),
        })
    }

    async fn schedule(
        &mut self,
        _: &mut Self::Stage,
    ) -> Result<WorkSchedule<Self::Unit>, WorkerError> {
        let unit = TickerUnit {
            instant: Instant::now(),
            delay: self.next_delay,
        };

        Ok(WorkSchedule::Unit(unit))
    }

    async fn execute(
        &mut self,
        unit: &Self::Unit,
        stage: &mut Self::Stage,
    ) -> Result<(), WorkerError> {
        tokio::time::sleep(Duration::from_secs(unit.delay)).await;
        stage.output.send(unit.instant.into()).await.or_panic()?;

        stage.value_1.inc(3);
        self.next_delay += 1;

        Ok(())
    }
}

struct TerminalSpec {
    input: InputPort<Instant>,
}

impl Stage for TerminalSpec {
    fn name(&self) -> &str {
        "terminal"
    }

    fn policy(&self) -> gasket::runtime::Policy {
        Policy {
            tick_timeout: None,
            bootstrap_retry: retries::Policy::no_retry(),
            work_retry: retries::Policy::no_retry(),
            teardown_retry: retries::Policy::no_retry(),
        }
    }

    fn register_metrics(&self, _: &mut gasket::metrics::Registry) {}
}

struct Terminal;

#[async_trait::async_trait(?Send)]
impl Worker for Terminal {
    type Unit = Instant;
    type Stage = TerminalSpec;

    async fn bootstrap(_: &Self::Stage) -> Result<Self, WorkerError> {
        Ok(Self)
    }

    async fn schedule(
        &mut self,
        stage: &mut Self::Stage,
    ) -> Result<WorkSchedule<Self::Unit>, WorkerError> {
        let msg = stage.input.recv().await.or_panic()?;
        Ok(WorkSchedule::Unit(msg.payload))
    }

    async fn execute(&mut self, unit: &Self::Unit, _: &mut Self::Stage) -> Result<(), WorkerError> {
        println!("{:?}", unit.elapsed());

        Ok(())
    }
}

fn main() {
    tracing::subscriber::set_global_default(
        tracing_subscriber::FmtSubscriber::builder()
            .with_max_level(tracing::Level::TRACE)
            .finish(),
    )
    .unwrap();

    let mut ticker = TickerSpec {
        output: Default::default(),
        value_1: Counter::default(),
    };

    let mut terminal = TerminalSpec {
        input: Default::default(),
    };

    connect_ports(&mut ticker.output, &mut terminal.input, 10);

    let tether1 = spawn_stage::<Ticker>(ticker);

    let tether2 = spawn_stage::<Terminal>(terminal);

    let tethers = vec![tether1, tether2];

    for i in 0..10 {
        for tether in tethers.iter() {
            match tether.check_state() {
                gasket::runtime::TetherState::Dropped => println!("tether dropped"),
                gasket::runtime::TetherState::Blocked(x) => {
                    println!("tether blocked, last state: {x:?}")
                }
                gasket::runtime::TetherState::Alive(x) => {
                    println!("tether alive, last state: {x:?}")
                }
            }

            match tether.read_metrics() {
                Ok(readings) => {
                    for (key, value) in readings {
                        println!("{key}: {value:?}");
                    }
                }
                Err(err) => {
                    println!("couldn't read metrics");
                    dbg!(err);
                }
            }
        }

        std::thread::sleep(Duration::from_secs(5));
        println!("check loop {i}");
    }

    for tether in tethers {
        tether.dismiss_stage().expect("stage stops");
        tether.join_stage();
    }
}
