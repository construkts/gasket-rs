use std::{
    ops::{Deref, DerefMut},
    sync::{Arc, Weak},
    thread::JoinHandle,
    time::{Duration, Instant},
};

use crossbeam::{atomic::AtomicCell, utils::Backoff};
use tracing::{debug, error, info, instrument, trace, warn, Level};

use crate::metrics::{collect_readings, Readings};
use crate::retries;
use crate::{error::Error, metrics};

pub type WorkResult = Result<WorkOutcome, Error>;

pub enum WorkOutcome {
    /// worker is not doing anything, but might in the future
    Idle,
    /// worker is working and need to keep working
    Partial,
    /// worker has done all the work it needed
    Done,
}

pub trait Worker: Send {
    fn metrics(&self) -> metrics::Registry;

    fn work(&mut self) -> WorkResult;

    fn bootstrap(&mut self) -> Result<(), Error> {
        info!("noop bootstrap complete");
        Ok(())
    }

    fn teardown(&mut self) -> Result<(), Error> {
        info!("noop teardown complete");
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
    Idle,
    StandBy,
    Teardown,
}

#[derive(Debug)]
pub enum StageEvent {
    Dismissed,
    WorkPartial,
    WorkIdle,
    WorkDone,
    WorkError(Error),
    BootstrapOk,
    BootstrapError(Error),
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
    name: String,
    tick_count: metrics::Counter,
    idle_count: metrics::Counter,
    worker: W,
}

#[inline]
fn log_stage_error(x: &Error) {
    match x {
        Error::RecvIdle => debug!("input port idle"),
        Error::ShouldRestart => warn!("stage should restart"),
        Error::RetryableError => warn!("work should be retried"),
        Error::WorkPanic => error!("work panic"),
        Error::RecvError => error!("stage error while receiving message"),
        Error::SendError => error!("stage error while sending message"),
        Error::NotConnected => error!("stage not connected",),
        x => error!("stage error {}", x),
    };
}

impl<W> StageMachine<W>
where
    W: Worker,
{
    fn new(anchor: Arc<Anchor>, worker: W, policy: Policy, name: String) -> Self {
        StageMachine {
            state: StageState::Bootstrap,
            tick_count: Default::default(),
            idle_count: Default::default(),
            name,
            anchor,
            policy,
            worker,
        }
    }

    #[instrument(level = Level::INFO, skip_all)]
    fn bootstrap(&mut self) -> StageEvent {
        let result = retries::retry_operation(
            || self.worker.bootstrap(),
            &self.policy.bootstrap_retry,
            Some(&self.anchor.dismissed),
        );

        match result {
            Ok(()) => StageEvent::BootstrapOk,
            Err(err) => StageEvent::BootstrapError(err),
        }
    }

    #[instrument(level = Level::INFO, skip_all)]
    fn work(&mut self) -> StageEvent {
        let result = retries::retry_operation(
            || self.worker.work(),
            &self.policy.work_retry,
            Some(&self.anchor.dismissed),
        );

        match result {
            Ok(WorkOutcome::Partial) => StageEvent::WorkPartial,
            Ok(WorkOutcome::Idle) => StageEvent::WorkIdle,
            Ok(WorkOutcome::Done) => StageEvent::WorkDone,
            Err(err) => StageEvent::WorkError(err),
        }
    }

    #[instrument(level = Level::INFO, skip_all)]
    fn teardown(&mut self) -> StageEvent {
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

    fn actuate(&mut self) -> StageEvent {
        // if stage is dismissed, return early
        let is_dismissed = self.anchor.dismissed.load();
        if self.state != StageState::Teardown && is_dismissed {
            return StageEvent::Dismissed;
        }

        match self.state {
            StageState::Bootstrap => self.bootstrap(),
            StageState::Idle | StageState::Working => self.work(),
            StageState::StandBy => StageEvent::StandBy,
            StageState::Teardown => self.teardown(),
        }
    }

    fn report(&self, event: &StageEvent, next_state: StageState) {
        self.anchor.last_state.store(next_state);
        self.anchor.last_tick.store(Instant::now());

        match event {
            StageEvent::WorkPartial => {
                self.tick_count.inc(1);
                trace!("partial work done");
            }
            StageEvent::WorkIdle => {
                self.idle_count.inc(1);
                trace!("work is idle");
            }
            StageEvent::BootstrapError(x) => log_stage_error(x),
            StageEvent::WorkError(x) => log_stage_error(x),
            StageEvent::Dismissed => info!("stage dismissed"),
            StageEvent::WorkDone => info!("stage work done"),
            StageEvent::BootstrapOk => info!("stage bootstrap ok"),
            StageEvent::TeardownOk => info!("stage teardown ok"),
            StageEvent::TeardownError => error!("stage teardown error"),
            StageEvent::StandBy => trace!("stage stand-by"),
        }

        if self.state != next_state {
            info!(next = ?next_state, "switching state");
        }
    }

    fn apply(&mut self, event: &StageEvent) -> Option<StageState> {
        match event {
            StageEvent::Dismissed => Some(StageState::Teardown),
            StageEvent::WorkPartial => Some(StageState::Working),
            StageEvent::WorkIdle => Some(StageState::Idle),
            StageEvent::WorkDone => Some(StageState::StandBy),
            StageEvent::WorkError(Error::ShouldRestart) => Some(StageState::Bootstrap),
            StageEvent::WorkError(Error::DismissableError) => Some(StageState::Working),
            StageEvent::WorkError(Error::RecvIdle) => Some(StageState::Idle),
            StageEvent::WorkError(_) => Some(StageState::StandBy),
            StageEvent::BootstrapOk => Some(StageState::Working),
            StageEvent::BootstrapError(_) => Some(StageState::StandBy),
            StageEvent::StandBy => Some(StageState::StandBy),
            StageEvent::TeardownOk => None,
            StageEvent::TeardownError => None,
        }
    }

    fn transition(&mut self) -> Option<StageState> {
        let event = self.actuate();
        let next = self.apply(&event);

        match next {
            Some(next_state) => {
                self.report(&event, next_state);
                self.state = next_state;
                Some(next_state)
            }
            None => None,
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
    name: String,
    anchor_ref: Weak<Anchor>,
    thread_handle: JoinHandle<()>,
    policy: Policy,
}

#[derive(Debug, PartialEq)]
pub enum TetherState {
    Dropped,
    Blocked(StageState),
    Alive(StageState),
}

impl Tether {
    pub fn name(&self) -> &str {
        &self.name
    }

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

#[instrument(name="stage", level = Level::INFO, skip_all, fields(stage = machine.name))]
fn fullfil_stage<W>(mut machine: StageMachine<W>)
where
    W: Worker,
{
    let backoff = Backoff::new();

    while let Some(state) = machine.transition() {
        match state {
            StageState::Idle | StageState::StandBy => {
                backoff.snooze();
            }
            _ => (),
        }
    }
}

pub fn spawn_stage<W>(worker: W, policy: Policy, name: Option<&str>) -> Tether
where
    W: Worker + 'static,
{
    let name = name
        .map(|x| x.to_owned())
        .unwrap_or("un-named stage".into());
    let metrics = worker.metrics();
    let anchor = Arc::new(Anchor::new(metrics));
    let anchor_ref = Arc::downgrade(&anchor);

    let name2 = name.clone();
    let policy2 = policy.clone();
    let thread_handle = std::thread::spawn(move || {
        let machine = StageMachine::new(anchor, worker, policy2, name2);
        fullfil_stage(machine);
    });

    Tether {
        name,
        anchor_ref,
        thread_handle,
        policy,
    }
}
