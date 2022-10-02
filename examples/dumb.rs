use std::time::{Duration, Instant};

use gasket::{
    messaging::{connect_ports, InputPort, OutputPort, TwoPhaseInputPort},
    metrics::{self, Counter, Registry},
    retries,
    runtime::{spawn_stage, Policy, WorkOutcome, WorkResult, Worker},
};

struct Ticker {
    output: OutputPort<Instant>,
    next_delay: u64,
    value_1: Counter,
}

impl Worker for Ticker {
    fn metrics(&self) -> Registry {
        metrics::Builder::new()
            .with_counter("value_1", &self.value_1)
            .build()
    }

    fn work(&mut self) -> WorkResult {
        std::thread::sleep(Duration::from_secs(self.next_delay));
        let now = Instant::now();
        self.output.send(now.into())?;
        self.value_1.inc(3);
        self.next_delay += 1;

        Ok(WorkOutcome::Partial)
    }
}

struct Terminal {
    input: TwoPhaseInputPort<Instant>,
}

impl Worker for Terminal {
    fn metrics(&self) -> Registry {
        metrics::Builder::new().build()
    }

    fn work(&mut self) -> WorkResult {
        let unit = self.input.recv()?;
        println!("{:?}", unit.payload);

        self.input.commit();
        Ok(WorkOutcome::Partial)
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
