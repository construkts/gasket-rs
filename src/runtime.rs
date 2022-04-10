use std::{
    ops::{Deref, DerefMut},
    sync::{Arc, Weak},
    thread::JoinHandle,
    time::{Duration, Instant},
};

use crossbeam::{atomic::AtomicCell, utils::Backoff};

use crate::metrics::{collect_readings, Readings};
use crate::retries;
use crate::{error::Error, metrics};

pub type WorkResult = Result<WorkOutcome, Error>;

pub enum WorkOutcome {
    /// worker is not doing anything, but might in the future
    Idle,
    /// worker is working and need to keep working
    Partial,
    /// worker has done all the owrk it needed
    Done,
}

pub trait Worker: Send {
    fn metrics(&self) -> metrics::Registry;

    fn work(&mut self) -> WorkResult;

    fn bootstrap(&mut self) -> Result<(), Error> {
        Ok(())
    }

    fn teardown(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

impl Worker for Box<dyn Worker> {
    fn metrics(&self) -> metrics::Registry {
        self.deref().metrics()
    }

    fn work(&mut self) -> WorkResult {
        self.deref_mut().work()
    }

    fn bootstrap(&mut self) -> Result<(), Error> {
        self.deref_mut().bootstrap()
    }

    fn teardown(&mut self) -> Result<(), Error> {
        self.deref_mut().teardown()
    }
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum StageState {
    Bootstrap,
    Working,
    StandBy,
    Teardown,
}

pub enum StageEvent {
    Dismissed,
    WorkPartial,
    WorkIdle,
    WorkDone,
    WorkError,
    BootstrapOk,
    BootstrapError,
    TeardownOk,
    TeardownError,
    StandBy,
}

struct StageMachine<W>
where
    W: Worker,
{
    state: StageState,
    anchor: Arc<Anchor>,
    policy: Policy,
    tick_count: metrics::Counter,
    idle_count: metrics::Counter,
    worker: W,
}

impl<W> StageMachine<W>
where
    W: Worker,
{
    fn new(anchor: Arc<Anchor>, worker: W, policy: Policy) -> Self {
        StageMachine {
            state: StageState::Bootstrap,
            tick_count: Default::default(),
            idle_count: Default::default(),
            anchor,
            policy,
            worker,
        }
    }

    fn actuate(&mut self) -> StageEvent {
        match self.state {
            StageState::Bootstrap => {
                if self.anchor.dismissed.load() {
                    return StageEvent::Dismissed;
                }

                let result = retries::retry_operation(
                    || self.worker.bootstrap(),
                    &self.policy.bootstrap_retry,
                    Some(&self.anchor.dismissed),
                );

                match result {
                    Ok(()) => StageEvent::BootstrapOk,
                    Err(err) => {
                        log::error!("error bootstrapping stage: {}", err);
                        StageEvent::BootstrapError
                    }
                }
            }
            StageState::Working => {
                if self.anchor.dismissed.load() {
                    return StageEvent::Dismissed;
                }

                let result = retries::retry_operation(
                    || self.worker.work(),
                    &self.policy.work_retry,
                    Some(&self.anchor.dismissed),
                );

                match result {
                    Ok(WorkOutcome::Partial) => StageEvent::WorkPartial,
                    Ok(WorkOutcome::Idle) => StageEvent::WorkIdle,
                    Ok(WorkOutcome::Done) => StageEvent::WorkDone,
                    Err(err) => {
                        log::error!("error on work loop: {}", err);
                        StageEvent::WorkError
                    }
                }
            }
            StageState::StandBy => {
                if self.anchor.dismissed.load() {
                    return StageEvent::Dismissed;
                }

                StageEvent::StandBy
            }
            StageState::Teardown => {
                let result = retries::retry_operation(
                    || self.worker.teardown(),
                    &self.policy.teardown_retry,
                    Some(&self.anchor.dismissed),
                );

                match result {
                    Ok(_) => StageEvent::TeardownOk,
                    Err(_) => StageEvent::TeardownError,
                }
            }
        }
    }

    fn report(&self) {
        self.anchor.last_state.store(self.state);
        self.anchor.last_tick.store(Instant::now());
    }

    fn fulfill(&mut self) {
        let backoff = Backoff::new();

        loop {
            let event = self.actuate();

            match event {
                StageEvent::Dismissed => {
                    self.state = StageState::Teardown;
                }
                StageEvent::WorkPartial => {
                    self.tick_count.inc(1);
                    self.state = StageState::Working;
                }
                StageEvent::WorkIdle => {
                    self.idle_count.inc(1);
                    backoff.snooze();
                    self.state = StageState::Working;
                }
                StageEvent::WorkDone => {
                    self.state = StageState::StandBy;
                }
                StageEvent::WorkError => {
                    self.state = StageState::StandBy;
                }
                StageEvent::BootstrapOk => {
                    self.state = StageState::Working;
                }
                StageEvent::BootstrapError => {
                    self.state = StageState::StandBy;
                }
                StageEvent::StandBy => {
                    backoff.snooze();
                    self.state = StageState::StandBy;
                }
                StageEvent::TeardownOk => {
                    break;
                }
                StageEvent::TeardownError => {
                    break;
                }
            }

            self.report();
        }
    }
}

/// Sentinel object that lives within the thread of the stage
pub struct Anchor {
    dismissed: AtomicCell<bool>,
    last_state: AtomicCell<StageState>,
    last_tick: AtomicCell<Instant>,
    metrics: metrics::Registry,
}

impl Anchor {
    fn new(metrics: metrics::Registry) -> Self {
        Self {
            dismissed: AtomicCell::new(false),
            last_tick: AtomicCell::new(Instant::now()),
            last_state: AtomicCell::new(StageState::Bootstrap),
            metrics,
        }
    }
}

pub struct Tether {
    anchor_ref: Weak<Anchor>,
    thread_handle: JoinHandle<()>,
    policy: Policy,
}

#[derive(PartialEq)]
pub enum TetherState {
    Dropped,
    Blocked(StageState),
    Alive(StageState),
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

    pub fn dismiss_stage(&self) -> Result<(), Error> {
        let anchor = self.try_anchor()?;
        anchor.dismissed.store(true);

        Ok(())
    }

    pub fn check_state(&self) -> TetherState {
        let anchor = self.try_anchor();

        if let Err(_) = anchor {
            return TetherState::Dropped;
        }

        let anchor = anchor.unwrap();

        let last_state = anchor.last_state.load();

        if let Some(timeout) = &self.policy.tick_timeout {
            let last_tick = anchor.last_tick.load();
            if last_tick.elapsed() > *timeout {
                TetherState::Blocked(last_state)
            } else {
                TetherState::Alive(last_state)
            }
        } else {
            TetherState::Alive(last_state)
        }
    }

    pub fn wait_state(&self, expected: TetherState) {
        let backoff = Backoff::new();

        while self.check_state() != expected {
            backoff.snooze();
        }
    }

    pub fn read_metrics(&self) -> Result<Readings, Error> {
        let anchor = self.try_anchor()?;
        let readings = collect_readings(&anchor.metrics);

        Ok(readings)
    }
}

#[derive(Clone)]
pub struct Policy {
    pub tick_timeout: Option<Duration>,
    pub bootstrap_retry: retries::Policy,
    pub work_retry: retries::Policy,
    pub teardown_retry: retries::Policy,
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            tick_timeout: None,
            bootstrap_retry: retries::Policy::no_retry(),
            work_retry: retries::Policy::no_retry(),
            teardown_retry: retries::Policy::no_retry(),
        }
    }
}

pub fn spawn_stage<W>(worker: W, policy: Policy) -> Tether
where
    W: Worker + 'static,
{
    let metrics = worker.metrics();
    let anchor = Arc::new(Anchor::new(metrics));
    let anchor_ref = Arc::downgrade(&anchor);

    let policy2 = policy.clone();
    let thread_handle = std::thread::spawn(move || {
        let mut machine = StageMachine::new(anchor, worker, policy2);
        machine.fulfill();
    });

    Tether {
        anchor_ref,
        thread_handle,
        policy,
    }
}
