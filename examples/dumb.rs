#![feature(async_fn_in_trait)]

use std::time::{Duration, Instant};

use gasket::{
    error::Error,
    messaging::tokio::{connect_ports, InputPort, OutputPort},
    metrics::{self, Counter, Registry},
    retries,
    runtime::{spawn_stage, Policy, ScheduleResult, WorkSchedule, Worker},
};

struct Ticker {
    output: OutputPort<Instant>,
    value_1: Counter,
    next_delay: u64,
}

struct TickerUnit {
    instant: Instant,
    delay: u64,
}

impl Worker for Ticker {
    type WorkUnit = TickerUnit;

    fn metrics(&self) -> Registry {
        metrics::Builder::new()
            .with_counter("value_1", &self.value_1)
            .build()
    }

    async fn schedule(&mut self) -> ScheduleResult<Self::WorkUnit> {
        let unit = TickerUnit {
            instant: Instant::now(),
            delay: self.next_delay,
        };

        Ok(WorkSchedule::Unit(unit))
    }

    async fn execute(&mut self, unit: &mut Self::WorkUnit) -> Result<(), Error> {
        tokio::time::sleep(Duration::from_secs(unit.delay)).await;
        self.output.send(unit.instant.into()).await?;

        self.value_1.inc(3);
        self.next_delay += 1;

        Ok(())
    }
}

struct Terminal {
    input: InputPort<Instant>,
}

impl Worker for Terminal {
    type WorkUnit = Instant;

    fn metrics(&self) -> Registry {
        metrics::Builder::new().build()
    }

    async fn schedule(&mut self) -> ScheduleResult<Self::WorkUnit> {
        let msg = self.input.recv().await?;
        Ok(WorkSchedule::Unit(msg.payload))
    }

    async fn execute(&mut self, unit: &mut Self::WorkUnit) -> Result<(), Error> {
        println!("{:?}", unit.elapsed());

        Ok(())
    }
}

fn main() {
    let mut ticker = Ticker {
        output: Default::default(),
        value_1: Counter::default(),
        next_delay: 0,
    };

    let mut terminal = Terminal {
        input: Default::default(),
    };

    connect_ports(&mut ticker.output, &mut terminal.input, 10);

    let tether1 = spawn_stage(
        ticker,
        Policy {
            tick_timeout: Some(Duration::from_secs(3)),
            bootstrap_retry: retries::Policy::no_retry(),
            work_retry: retries::Policy::no_retry(),
            teardown_retry: retries::Policy::no_retry(),
        },
        Some("ticker"),
    );

    let tether2 = spawn_stage(
        terminal,
        Policy {
            tick_timeout: None,
            bootstrap_retry: retries::Policy::no_retry(),
            work_retry: retries::Policy::no_retry(),
            teardown_retry: retries::Policy::no_retry(),
        },
        Some("terminal"),
    );

    let tethers = vec![tether1, tether2];

    for i in 0..10 {
        for tether in tethers.iter() {
            match tether.check_state() {
                gasket::runtime::TetherState::Dropped => println!("tether dropped"),
                gasket::runtime::TetherState::Blocked(x) => {
                    println!("tether blocked, last state: {:?}", x)
                }
                gasket::runtime::TetherState::Alive(x) => {
                    println!("tether alive, last state: {:?}", x)
                }
            }

            match tether.read_metrics() {
                Ok(readings) => {
                    for (key, value) in readings {
                        println!("{}: {:?}", key, value);
                    }
                }
                Err(err) => {
                    println!("couldn't read metrics");
                    dbg!(err);
                }
            }
        }

        std::thread::sleep(Duration::from_secs(5));
        println!("check loop {}", i);
    }

    for tether in tethers {
        tether.dismiss_stage().expect("stage stops");
        tether.join_stage();
    }
}
