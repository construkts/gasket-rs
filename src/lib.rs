pub mod testing;

use std::{
    sync::{Arc, Weak},
    thread::JoinHandle,
    time::{Duration, Instant},
};

use crossbeam::{
    atomic::AtomicCell,
    channel::{Receiver, Sender},
    utils::Backoff,
};

use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("port is not connected")]
    NotConnected,

    #[error("error sending work unit through output port")]
    SendError,

    #[error("error receiving work unit through input port")]
    RecvError,

    #[error("error while performing stage work: {0}")]
    WorkError(String),

    #[error("can't perform action since tether to stage was dropped")]
    TetherDropped,
}

pub trait AsWorkError<T> {
    fn or_work_err(self) -> Result<T, Error>;
}

impl<T, E> AsWorkError<T> for Result<T, E>
where
    E: std::error::Error,
{
    fn or_work_err(self) -> Result<T, Error> {
        match self {
            Ok(x) => Ok(x),
            Err(x) => Err(Error::WorkError(format!("{}", x))),
        }
    }
}

pub type WorkResult = Result<WorkOutcome, Error>;

pub struct Message<T> {
    pub payload: T,
}

impl<T> From<T> for Message<T> {
    fn from(payload: T) -> Self {
        Message { payload }
    }
}

pub struct OutputPort<T> {
    sender: Option<Sender<Message<T>>>,
}

impl<T> Default for OutputPort<T> {
    fn default() -> Self {
        Self {
            sender: Default::default(),
        }
    }
}

impl<T> OutputPort<T> {
    pub fn send(&mut self, msg: Message<T>) -> Result<(), Error> {
        match &self.sender {
            Some(sender) => sender.send(msg).map_err(|_| Error::SendError),
            None => Err(Error::NotConnected),
        }
    }

    fn connect(&mut self, sender: Sender<Message<T>>) {
        self.sender = Some(sender);
    }
}

pub struct InputPort<T> {
    counter: u64,
    receiver: Option<Receiver<Message<T>>>,
}

impl<T> Default for InputPort<T> {
    fn default() -> Self {
        Self {
            counter: 0,
            receiver: Default::default(),
        }
    }
}

impl<T> InputPort<T> {
    pub fn recv(&mut self) -> Result<Message<T>, Error> {
        match &self.receiver {
            Some(receiver) => match receiver.recv() {
                Ok(unit) => {
                    self.counter += 1;
                    Ok(unit)
                }
                Err(_) => Err(Error::RecvError),
            },
            None => Err(Error::NotConnected),
        }
    }

    fn connect(&mut self, receiver: Receiver<Message<T>>) {
        self.receiver = Some(receiver);
    }
}

mod metrics {
    use crossbeam::atomic::AtomicCell;

    pub struct Counter {
        cell: AtomicCell<u64>,
    }

    impl Counter {
        pub fn inc(&self, value: u64) {
            self.cell.fetch_add(value);
        }
    }
}

pub struct Tether {
    anchor_ref: Weak<Anchor>,
    thread_handle: JoinHandle<()>,
}

impl Tether {
    pub fn join_stage(self) {
        self.thread_handle
            .join()
            .expect("called from outside thread");
    }

    fn try_anchor(&self) -> Result<Arc<Anchor>, Error> {
        match self.anchor_ref.upgrade() {
            Some(anchor) => Ok(anchor),
            None => Err(Error::TetherDropped),
        }
    }

    pub fn stop_stage(&self) -> Result<(), Error> {
        let anchor = self.try_anchor()?;
        anchor.stopped.store(true);

        Ok(())
    }

    pub fn check_state(&self) -> StageState {
        let anchor = self.try_anchor();

        match anchor {
            Ok(anchor) => {
                if anchor.done.load() {
                    return StageState::Done;
                }

                let elapsed = anchor.last_tick.load().elapsed();

                match (elapsed, anchor.stage.tick_timeout) {
                    (elapsed, Some(timeout)) if elapsed > timeout => StageState::Blocked,
                    _ => StageState::Alive,
                }
            }
            Err(_) => StageState::Dropped,
        }
    }

    pub fn wait_state(&self, expected: StageState) {
        let backoff = Backoff::new();

        while self.check_state() != expected {
            backoff.snooze();
        }
    }
}

pub struct Anchor {
    stage: StageDescription,
    stopped: AtomicCell<bool>,
    done: AtomicCell<bool>,
    last_tick: AtomicCell<Instant>,
    tick_count: AtomicCell<u64>,
    idle_count: AtomicCell<u64>,
    error_count: AtomicCell<u64>,
}

impl Anchor {
    fn new(stage: StageDescription) -> Arc<Self> {
        Arc::new(Self {
            stage,
            stopped: AtomicCell::new(false),
            done: AtomicCell::new(false),
            last_tick: AtomicCell::new(Instant::now()),
            tick_count: AtomicCell::new(0),
            idle_count: AtomicCell::new(0),
            error_count: AtomicCell::new(0),
        })
    }

    pub(crate) fn is_stopped(&self) -> bool {
        self.stopped.load()
    }

    pub(crate) fn should_work(&self) -> bool {
        !self.stopped.load() && !self.done.load()
    }

    pub(crate) fn stage_done(&self) {
        self.done.store(true);
    }

    pub(crate) fn stage_working(&self) {
        self.last_tick.store(Instant::now());
        self.tick_count.fetch_add(1);
    }

    pub(crate) fn stage_idle(&self) {
        self.tick_count.fetch_add(1);
        self.idle_count.fetch_add(1);
    }

    pub(crate) fn stage_error(&self, err: Error) {
        self.tick_count.fetch_add(1);
        self.error_count.fetch_add(1);
        println!("{:?}", err);
    }
}

pub fn connect_ports<T>(output: &mut OutputPort<T>, input: &mut InputPort<T>, cap: usize) {
    let (sender, receiver) = crossbeam::channel::bounded(cap);
    output.connect(sender);
    input.connect(receiver);
}

pub enum WorkOutcome {
    /// stage is not doing anything, but might in the future
    Idle,
    /// stage is working and need to keep working
    Partial,
    // stage has done all the owrk it needed
    Done,
}

pub struct StageDescription {
    pub name: &'static str,
    pub tick_timeout: Option<Duration>,
    pub metrics: Vec<Arc<AtomicCell<u64>>>,
}

pub trait Stage: Send {
    fn describe(&self) -> StageDescription;
    fn work(&mut self) -> WorkResult;

    fn will_start(&mut self) {}
    fn will_stop(&mut self) {}
}

pub fn execute_stage<S>(anchor: Arc<Anchor>, mut stage: S)
where
    S: Stage,
{
    let backoff = Backoff::new();

    stage.will_start();

    while !anchor.is_stopped() {
        while anchor.should_work() {
            match stage.work() {
                Ok(WorkOutcome::Idle) => {
                    anchor.stage_idle();
                    backoff.snooze();
                }
                Ok(WorkOutcome::Partial) => {
                    anchor.stage_working();
                }
                Ok(WorkOutcome::Done) => {
                    anchor.stage_done();
                }
                Err(err) => {
                    anchor.stage_error(err);
                    backoff.spin();
                }
            }
        }

        // we keep the thread alive until it is explicitely stopped so that the tether
        // can access the last state of the anchor.
        backoff.snooze();
    }

    stage.will_stop();
}

pub fn spawn_stage<S>(stage: S) -> Tether
where
    S: Stage + 'static,
{
    let description = stage.describe();
    let anchor = Anchor::new(description);
    let anchor_ref = Arc::downgrade(&anchor);

    let thread_handle = std::thread::spawn(move || execute_stage(anchor, stage));

    Tether {
        anchor_ref,
        thread_handle,
    }
}

#[derive(Debug, PartialEq)]
pub enum StageState {
    Alive,
    Blocked,
    Dropped,
    Done,
}

#[cfg(test)]
mod tests {
    use core::panic;
    use std::{
        sync::Arc,
        time::{Duration, Instant},
    };

    use crossbeam::atomic::AtomicCell;

    use crate::{
        connect_ports, spawn_stage, InputPort, OutputPort, Stage, StageDescription, StageState,
        WorkOutcome, WorkResult,
    };

    struct Ticker {
        output: OutputPort<Instant>,
        next_delay: u64,
        value_1: Arc<AtomicCell<u64>>,
    }

    impl Stage for Ticker {
        fn describe(&self) -> StageDescription {
            StageDescription {
                name: "ticker",
                metrics: vec![self.value_1.clone()],
                tick_timeout: None,
            }
        }

        fn work(&mut self) -> WorkResult {
            std::thread::sleep(Duration::from_secs(self.next_delay));
            let now = Instant::now();
            self.output.send(now.into())?;
            self.value_1.fetch_add(3);
            self.next_delay += 1;

            Ok(WorkOutcome::Partial)
        }
    }

    struct Terminal {
        input: InputPort<Instant>,
    }

    impl Stage for Terminal {
        fn describe(&self) -> StageDescription {
            StageDescription {
                name: "terminal",
                metrics: vec![],
                tick_timeout: Duration::from_secs(3).into(),
            }
        }

        fn work(&mut self) -> WorkResult {
            let unit = self.input.recv()?;
            println!("{:?}", unit.payload);

            Ok(WorkOutcome::Partial)
        }
    }

    #[test]
    fn pipeline_ergonomics_are_nice() {
        let mut ticker = Ticker {
            output: Default::default(),
            value_1: Arc::new(AtomicCell::new(0)),
            next_delay: 0,
        };

        let mut terminal = Terminal {
            input: Default::default(),
        };

        connect_ports(&mut ticker.output, &mut terminal.input, 10);

        let tether1 = spawn_stage(ticker);
        let tether2 = spawn_stage(terminal);

        let tethers = vec![tether1, tether2];

        for i in 0..10 {
            for tether in tethers.iter() {
                match tether.check_state() {
                    StageState::Alive => println!("stage alive"),
                    StageState::Blocked => println!("stage blocked"),
                    StageState::Dropped => panic!("stage dropped"),
                    StageState::Done => println!("stage done"),
                }
            }

            std::thread::sleep(Duration::from_secs(5));
            println!("check loop {}", i);
        }

        for tether in tethers {
            tether.stop_stage();
            tether.join_stage();
        }
    }
}
