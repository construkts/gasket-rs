use std::time::{Duration, Instant};

use gasket::{
    framework::{AsWorkError, Stage, WorkSchedule, Worker, WorkerError},
    messaging::{
        tokio::{connect_ports, funnel_ports},
        InputPort, OutputPort, TimerPort,
    },
    metrics::Counter,
    retries,
    runtime::{spawn_stage, Policy},
};
use tokio::select;

type Value = u64;

struct TickerSpec {
    frequency: f32,
    output: OutputPort<Instant>,
    value: OutputPort<Value>,
    metric_1: Counter,
}

impl Stage for TickerSpec {
    type Unit = TickerUnit;
    type Worker = Ticker;

    fn name(&self) -> &str {
        "ticker"
    }

    fn metrics(&self) -> gasket::metrics::Registry {
        let mut registry = gasket::metrics::Registry::default();
        registry.track_counter("metric_1", &self.metric_1);
        registry
    }
}

struct Ticker {
    counter: u64,
}

struct TickerUnit {
    instant: Instant,
}

#[async_trait::async_trait(?Send)]
impl Worker<TickerSpec> for Ticker {
    async fn bootstrap(_: &TickerSpec) -> Result<Self, WorkerError> {
        Ok(Self {
            counter: Default::default(),
        })
    }

    async fn schedule(
        &mut self,
        _: &mut TickerSpec,
    ) -> Result<WorkSchedule<TickerUnit>, WorkerError> {
        let unit = TickerUnit {
            instant: Instant::now(),
        };

        Ok(WorkSchedule::Unit(unit))
    }

    async fn execute(
        &mut self,
        unit: &TickerUnit,
        stage: &mut TickerSpec,
    ) -> Result<(), WorkerError> {
        let millis = (1000f32 / stage.frequency) as u64;
        let delay = Duration::from_millis(millis);
        tokio::time::sleep(delay).await;

        stage.output.send(unit.instant.into()).await.or_panic()?;

        self.counter += 1;

        if self.counter % 10 == 0 {
            stage.value.send(self.counter.into()).await.or_panic()?;
        }

        stage.metric_1.inc(3);

        Ok(())
    }
}

enum Work {
    Input1(Instant),
    Input2(Instant),
    Timer(Instant),
}

struct TerminalSpec {
    input_1: InputPort<Instant>,
    input_2: InputPort<Instant>,
    value_1: InputPort<Value>,
    timer_1: TimerPort,
}

impl Stage for TerminalSpec {
    type Unit = Work;
    type Worker = Terminal;

    fn name(&self) -> &str {
        "terminal"
    }

    fn metrics(&self) -> gasket::metrics::Registry {
        gasket::metrics::Registry::default()
    }
}

struct Terminal {
    start: Instant,
    value: Option<Value>,
}

#[async_trait::async_trait(?Send)]
impl Worker<TerminalSpec> for Terminal {
    async fn bootstrap(_: &TerminalSpec) -> Result<Self, WorkerError> {
        Ok(Self {
            value: None,
            start: Instant::now(),
        })
    }

    async fn schedule(
        &mut self,
        stage: &mut TerminalSpec,
    ) -> Result<WorkSchedule<Work>, WorkerError> {
        select! {
            input = stage.input_1.recv() => {
                let input = input.or_panic()?;
                Ok(WorkSchedule::Unit(Work::Input1(input.payload)))
            },
            input = stage.input_2.recv() => {
                let input = input.or_panic()?;
                Ok(WorkSchedule::Unit(Work::Input2(input.payload)))
            },
            input = stage.value_1.recv() => {
                let input = input.or_panic()?;
                self.value = Some(input.payload);
                Ok(WorkSchedule::Idle)
            },
            instant = stage.timer_1.recv() => {
                let instant = instant.or_panic()?;
                Ok(WorkSchedule::Unit(Work::Timer(instant)))
            }
        }
    }

    async fn execute(&mut self, unit: &Work, _: &mut TerminalSpec) -> Result<(), WorkerError> {
        match unit {
            Work::Input1(x) => println!("input 1: {:?}", x.elapsed()),
            Work::Input2(x) => println!("input 2: {:?}", x.elapsed()),
            Work::Timer(_) => println!("timer: {:?}", self.start.elapsed()),
        }

        println!("value: {:?}", self.value);

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

    let mut ticker1 = TickerSpec {
        frequency: 0.5,
        output: Default::default(),
        value: Default::default(),
        metric_1: Counter::default(),
    };

    let mut ticker2 = TickerSpec {
        frequency: 0.2,
        output: Default::default(),
        value: Default::default(),
        metric_1: Counter::default(),
    };

    let mut terminal = TerminalSpec {
        input_1: Default::default(),
        input_2: Default::default(),
        value_1: Default::default(),
        timer_1: TimerPort::from_secs(1),
    };

    connect_ports(&mut ticker1.output, &mut terminal.input_1, 10);
    connect_ports(&mut ticker2.output, &mut terminal.input_2, 10);
    funnel_ports(
        vec![&mut ticker1.value, &mut ticker2.value],
        &mut terminal.value_1,
        10,
    );

    let tether1 = spawn_stage(
        ticker1,
        Policy {
            tick_timeout: Some(Duration::from_secs(3)),
            bootstrap_retry: retries::Policy::no_retry(),
            work_retry: retries::Policy::no_retry(),
            teardown_retry: retries::Policy::no_retry(),
        },
    );

    let tether2 = spawn_stage(
        ticker2,
        Policy {
            tick_timeout: Some(Duration::from_secs(3)),
            bootstrap_retry: retries::Policy::no_retry(),
            work_retry: retries::Policy::no_retry(),
            teardown_retry: retries::Policy::no_retry(),
        },
    );

    let tether3 = spawn_stage(
        terminal,
        Policy {
            tick_timeout: None,
            bootstrap_retry: retries::Policy::no_retry(),
            work_retry: retries::Policy::no_retry(),
            teardown_retry: retries::Policy::no_retry(),
        },
    );

    let tethers = vec![tether1, tether2, tether3];

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
