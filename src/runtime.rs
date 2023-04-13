use std::{
    sync::{Arc, Weak},
    thread::JoinHandle,
    time::{Duration, Instant},
};

use crossbeam::{atomic::AtomicCell, utils::Backoff};
use tracing::{error, info, instrument, trace, warn, Level};

use crate::retries;
use crate::{error::Error, metrics};
use crate::{
    metrics::{collect_readings, Readings},
    retries::Retry,
};

pub enum WorkSchedule<U> {
    /// worker is not doing anything, but might in the future
    Idle,
    /// a work unit should be executed
    Unit(U),
    /// worker has done all the work it needed
    Done,
}

pub type ScheduleResult<U> = Result<WorkSchedule<U>, Error>;

#[async_trait::async_trait(?Send)]
pub trait Worker: Send {
    type WorkUnit: Sized;

    fn metrics(&self) -> metrics::Registry;

    /// Schedule the next work unit for execution
    ///
    /// This usually means reading messages from input ports and returning a
    /// work unit that contains all data required for execution.
    async fn schedule(&mut self) -> ScheduleResult<Self::WorkUnit>;

    /// Execute the action described by the work unit
    ///
    /// This usually means doing required computation, generating side-effect
    /// and submitting message through the output ports
    async fn execute(&mut self, unit: &Self::WorkUnit) -> Result<(), Error>;

    /// Called before any work is performed and after each restart
    async fn bootstrap(&mut self) -> Result<(), Error> {
        Ok(())
    }

    async fn teardown(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum StagePhase {
    Bootstrap,
    Working,
    Teardown,
    Ended,
}

#[derive(Clone, Debug)]
pub enum StageState<W>
where
    W: Worker,
{
    Bootstrap(Retry),
    Scheduling(Retry),
    Executing(W::WorkUnit, Retry),
    Teardown(Retry),
    Ended,
}

impl<W> From<&StageState<W>> for StagePhase
where
    W: Worker,
{
    fn from(value: &StageState<W>) -> Self {
        match value {
            StageState::Bootstrap(_) => Self::Bootstrap,
            StageState::Scheduling(..) => Self::Working,
            StageState::Executing(..) => Self::Working,
            StageState::Teardown(_) => Self::Teardown,
            StageState::Ended => Self::Ended,
        }
    }
}

#[derive(Debug)]
pub enum StageEvent<W>
where
    W: Worker,
{
    Dismissed,
    WorkerIdle,
    WorkerDone,
    MessagingError,
    NextUnit(W::WorkUnit),
    ScheduleError(Error, Retry),
    ExecuteOk,
    ExecuteError(W::WorkUnit, Error, Retry),
    BootstrapOk,
    BootstrapError(Error, Retry),
    TeardownOk,
    TeardownError(Error, Retry),
}

struct StageMachine<W>
where
    W: Worker,
{
    state: Option<StageState<W>>,
    anchor: Arc<Anchor>,
    policy: Policy,
    name: String,
    tick_count: metrics::Counter,
    worker: W,
}

#[inline]
fn log_stage_error(err: &Error, retry: &Retry) {
    match err {
        Error::ShouldRestart => warn!(?retry, "stage should restart"),
        Error::RetryableError => warn!(?retry, "work should be retried"),
        Error::WorkPanic => error!(?retry, "work panic"),
        Error::RecvError => error!(?retry, "stage error while receiving message"),
        Error::SendError => error!(?retry, "stage error while sending message"),
        Error::NotConnected => error!(?retry, "stage not connected",),
        x => error!(?retry, "stage error {}", x),
    };
}

#[inline]
fn log_event<W>(event: &StageEvent<W>)
where
    W: Worker,
{
    match event {
        StageEvent::ExecuteOk => trace!("unit executed"),
        StageEvent::BootstrapError(e, r) => log_stage_error(e, r),
        StageEvent::NextUnit(_) => trace!("next unit scheduled"),
        StageEvent::ScheduleError(e, r) => log_stage_error(e, r),
        StageEvent::ExecuteError(_, e, r) => log_stage_error(e, r),
        StageEvent::MessagingError => error!("messaging error"),
        StageEvent::Dismissed => info!("stage dismissed"),
        StageEvent::BootstrapOk => info!("stage bootstrap ok"),
        StageEvent::TeardownOk => info!("stage teardown ok"),
        StageEvent::TeardownError(e, r) => log_stage_error(e, r),
        StageEvent::WorkerIdle => trace!("worker is idle"),
        StageEvent::WorkerDone => trace!("worker is done"),
    }
}

impl<W> StageMachine<W>
where
    W: Worker,
{
    fn new(anchor: Arc<Anchor>, worker: W, policy: Policy, name: String) -> Self {
        StageMachine {
            state: Some(StageState::Bootstrap(Retry::new())),
            tick_count: Default::default(),
            name,
            anchor,
            policy,
            worker,
        }
    }

    #[instrument(level = Level::INFO, skip_all)]
    async fn bootstrap(&mut self, retry: Retry) -> StageEvent<W> {
        if let Err(err) = retry.ok(&self.policy.bootstrap_retry) {
            return StageEvent::BootstrapError(err, retry);
        }

        retry
            .wait_backoff(
                &self.policy.bootstrap_retry,
                self.anchor.dismissed_rx.clone(),
            )
            .await;

        match self.worker.bootstrap().await {
            Ok(_) => StageEvent::BootstrapOk,
            Err(x) => return StageEvent::BootstrapError(x, retry),
        }
    }

    #[instrument(level = Level::INFO, skip_all)]
    async fn schedule(&mut self, retry: Retry) -> StageEvent<W> {
        if let Err(err) = retry.ok(&self.policy.work_retry) {
            return StageEvent::ScheduleError(err, retry);
        }

        retry
            .wait_backoff(
                &self.policy.teardown_retry,
                self.anchor.dismissed_rx.clone(),
            )
            .await;

        let schedule = match self.worker.schedule().await {
            Ok(x) => x,
            Err(x) => return StageEvent::ScheduleError(x, retry),
        };

        match schedule {
            WorkSchedule::Idle => StageEvent::WorkerIdle,
            WorkSchedule::Done => StageEvent::WorkerDone,
            WorkSchedule::Unit(u) => StageEvent::NextUnit(u),
        }
    }

    #[instrument(level = Level::INFO, skip_all)]
    async fn execute(&mut self, mut unit: W::WorkUnit, retry: Retry) -> StageEvent<W> {
        if let Err(err) = retry.ok(&self.policy.work_retry) {
            return StageEvent::ExecuteError(unit, err, retry);
        }

        retry
            .wait_backoff(
                &self.policy.teardown_retry,
                self.anchor.dismissed_rx.clone(),
            )
            .await;

        match self.worker.execute(&mut unit).await {
            Ok(_) => StageEvent::ExecuteOk,
            Err(err) => StageEvent::ExecuteError(unit, err, retry),
        }
    }

    #[instrument(level = Level::INFO, skip_all)]
    async fn teardown(&mut self, retry: Retry) -> StageEvent<W> {
        if let Err(err) = retry.ok(&self.policy.teardown_retry) {
            return StageEvent::TeardownError(err, retry);
        }

        retry
            .wait_backoff(
                &self.policy.teardown_retry,
                self.anchor.dismissed_rx.clone(),
            )
            .await;

        match self.worker.teardown().await {
            Ok(_) => StageEvent::TeardownOk,
            Err(x) => return StageEvent::TeardownError(x, retry.clone()),
        }
    }

    async fn actuate(&mut self, prev_state: StageState<W>) -> StageEvent<W> {
        {
            // if stage is dismissed, return early
            let is_dismissed = self.anchor.dismissed_rx.borrow();
            if !matches!(prev_state, StageState::Teardown(_)) && *is_dismissed {
                return StageEvent::Dismissed;
            }
        }

        match prev_state {
            StageState::Bootstrap(retry) => self.bootstrap(retry).await,
            StageState::Scheduling(retry) => self.schedule(retry).await,
            StageState::Executing(unit, retry) => self.execute(unit, retry).await,
            StageState::Teardown(retry) => self.teardown(retry).await,
            StageState::Ended => unreachable!("ended stage shouldn't actuate"),
        }
    }

    fn apply(&self, event: StageEvent<W>) -> StageState<W> {
        match event {
            StageEvent::BootstrapOk => StageState::Scheduling(Retry::new()),
            StageEvent::BootstrapError(err, retry) => match err {
                Error::ShouldRestart => StageState::Bootstrap(retry.next()),
                Error::RetryableError => StageState::Bootstrap(retry.next()),
                Error::DismissableError => StageState::Scheduling(Retry::new()),
                _ => StageState::Teardown(Retry::new()),
            },
            StageEvent::NextUnit(u) => StageState::Executing(u, Retry::new()),
            StageEvent::WorkerIdle => StageState::Scheduling(Retry::new()),
            StageEvent::ScheduleError(err, retry) => match err {
                Error::ShouldRestart => StageState::Bootstrap(Retry::new()),
                Error::RetryableError => StageState::Scheduling(retry.next()),
                Error::DismissableError => StageState::Scheduling(Retry::new()),
                _ => StageState::Teardown(Retry::new()),
            },
            StageEvent::ExecuteOk => StageState::Scheduling(Retry::new()),
            StageEvent::ExecuteError(unit, err, retry) => match err {
                Error::RetryableError => StageState::Executing(unit, retry.next()),
                Error::DismissableError => StageState::Scheduling(Retry::new()),
                Error::ShouldRestart => StageState::Bootstrap(Retry::new()),
                _ => StageState::Teardown(Retry::new()),
            },
            StageEvent::WorkerDone => StageState::Teardown(Retry::new()),
            StageEvent::MessagingError => StageState::Teardown(Retry::new()),
            StageEvent::Dismissed => StageState::Teardown(Retry::new()),
            StageEvent::TeardownOk => StageState::Ended,
            StageEvent::TeardownError(err, retry) => match err {
                Error::RetryableError => StageState::Teardown(retry.next()),
                _ => StageState::Ended,
            },
        }
    }

    async fn transition(&mut self) -> StagePhase {
        let prev_state = self.state.take().unwrap();
        let prev_phase = StagePhase::from(&prev_state);

        if prev_phase == StagePhase::Ended {
            self.state = Some(prev_state);
            return StagePhase::Ended;
        }

        let event = self.actuate(prev_state).await;
        log_event(&event);

        let next_state = self.apply(event);
        let next_phase = StagePhase::from(&next_state);

        if prev_phase != next_phase {
            info!(?prev_phase, ?next_phase, "switching stage phase");
        }

        self.state = Some(next_state);
        self.tick_count.inc(1);
        self.anchor.last_state.store(next_phase);
        self.anchor.last_tick.store(Instant::now());

        next_phase
    }
}

/// Sentinel object that lives within the thread of the stage
pub struct Anchor {
    dismissed_rx: tokio::sync::watch::Receiver<bool>,
    dismissed_tx: tokio::sync::watch::Sender<bool>,
    last_state: AtomicCell<StagePhase>,
    last_tick: AtomicCell<Instant>,
    metrics: metrics::Registry,
}

impl Anchor {
    fn for_worker(worker: &impl Worker) -> Self {
        Self::new(worker.metrics())
    }

    fn new(metrics: metrics::Registry) -> Self {
        let (dismissed_tx, dismissed_rx) = tokio::sync::watch::channel(false);

        Self {
            dismissed_rx,
            dismissed_tx,
            last_tick: AtomicCell::new(Instant::now()),
            last_state: AtomicCell::new(StagePhase::Bootstrap),
            metrics,
        }
    }

    fn dismiss_stage(&self) -> Result<(), Error> {
        self.dismissed_tx
            .send(true)
            .map_err(|_| Error::TetherDropped)?;

        Ok(())
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
    Blocked(StagePhase),
    Alive(StagePhase),
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
        anchor.dismiss_stage()
    }

    pub fn check_state(&self) -> TetherState {
        let anchor = self.try_anchor();

        if let Err(_) = anchor {
            return TetherState::Dropped;
        }

        let anchor = anchor.unwrap();
        let last_phase = anchor.last_state.load();

        if let Some(timeout) = &self.policy.tick_timeout {
            let last_tick = anchor.last_tick.load();

            if last_tick.elapsed() > *timeout {
                TetherState::Blocked(last_phase)
            } else {
                TetherState::Alive(last_phase)
            }
        } else {
            TetherState::Alive(last_phase)
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
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async { while machine.transition().await != StagePhase::Ended {} });
}

pub fn spawn_stage<W>(worker: W, policy: Policy, name: Option<&str>) -> Tether
where
    W: Worker + 'static,
{
    let name = name
        .map(|x| x.to_owned())
        .unwrap_or("un-named stage".into());
    let anchor = Arc::new(Anchor::for_worker(&worker));
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

#[cfg(test)]
pub mod tests {
    use super::*;

    #[derive(Default)]
    pub struct MockWorker {
        bootstrap_count: usize,
        schedule_count: usize,
        execute_count: usize,
        teardown_count: usize,
        failures: Vec<bool>,
    }

    impl MockWorker {
        fn should_fail(&self, unit: usize) -> bool {
            if self.failures.is_empty() {
                return false;
            }

            let failure_idx = unit % self.failures.len();
            *self.failures.get(failure_idx).unwrap()
        }
    }

    #[async_trait::async_trait(?Send)]
    impl Worker for MockWorker {
        type WorkUnit = usize;

        fn metrics(&self) -> metrics::Registry {
            metrics::Registry::new()
        }

        async fn bootstrap(&mut self) -> Result<(), Error> {
            self.bootstrap_count += 1;

            Ok(())
        }

        async fn schedule(&mut self) -> ScheduleResult<Self::WorkUnit> {
            self.schedule_count += 1;

            Ok(WorkSchedule::Unit(self.schedule_count))
        }

        async fn execute(&mut self, unit: &Self::WorkUnit) -> Result<(), Error> {
            self.execute_count += 1;

            match self.should_fail(*unit) {
                true => Err(Error::RetryableError),
                false => Ok(()),
            }
        }

        async fn teardown(&mut self) -> Result<(), Error> {
            self.teardown_count += 1;

            Ok(())
        }
    }

    async fn should_teardown_and_end(machine: &mut StageMachine<MockWorker>) {
        assert!(matches!(machine.state, Some(StageState::Teardown(_))));
        machine.transition().await;
        assert_eq!(machine.worker.teardown_count, 1);
        assert!(matches!(machine.state, Some(StageState::Ended)));
    }

    async fn should_bootstrap(machine: &mut StageMachine<MockWorker>) {
        assert!(matches!(machine.state, Some(StageState::Bootstrap(_))));
        machine.transition().await;
        assert_eq!(machine.worker.bootstrap_count, 1);
    }

    #[tokio::test]
    async fn stage_machine_happy_path() {
        let worker = MockWorker::default();
        let anchor = Arc::new(Anchor::for_worker(&worker));
        let policy = Policy::default();

        let mut machine = StageMachine::new(anchor, worker, policy, "dummy".into());

        should_bootstrap(&mut machine).await;

        for _ in 0..5 {
            assert!(matches!(machine.state, Some(StageState::Scheduling(_))));
            machine.transition().await;
            assert!(matches!(machine.state, Some(StageState::Executing(_, _))));
            machine.transition().await;
        }

        assert_eq!(machine.worker.execute_count, 5);

        machine.anchor.dismiss_stage().unwrap();
        machine.transition().await;

        should_teardown_and_end(&mut machine).await;
    }

    #[tokio::test]
    async fn honors_max_retries() {
        let worker = MockWorker {
            failures: vec![true],
            ..Default::default()
        };
        let anchor = Arc::new(Anchor::for_worker(&worker));

        let work_retry = super::retries::Policy {
            max_retries: 3,
            ..Default::default()
        };

        let mut machine = StageMachine::new(
            anchor,
            worker,
            Policy {
                work_retry,
                ..Default::default()
            },
            "dummy".into(),
        );

        should_bootstrap(&mut machine).await;

        assert!(matches!(machine.state, Some(StageState::Scheduling(_))));
        machine.transition().await;

        for _ in 0..5 {
            match machine.state {
                Some(StageState::Executing(unit, _)) => {
                    // should repeat the same unit every loop
                    assert_eq!(unit, 1);
                }
                _ => panic!("unexpected state"),
            }
            machine.transition().await;
        }

        assert_eq!(machine.worker.execute_count, 4);

        should_teardown_and_end(&mut machine).await;
    }

    // #[tokio::test]
    // async fn honors_exponential_backoff() {
    //     let mut u1 = FailingUnit {
    //         attempt: 0,
    //         delay: None,
    //     };

    //     let policy = Policy {
    //         max_retries: 10,
    //         backoff_unit: Duration::from_millis(1),
    //         backoff_factor: 2,
    //         max_backoff: Duration::MAX,
    //     };

    //     let start = std::time::Instant::now();
    //     let cancel = AtomicCell::new(false);

    //     let result = retry_unit(&mut u1, &policy, Some(&cancel)).await;
    //     let elapsed = start.elapsed();

    //     assert!(result.is_err());

    //     // not an exact science, should be 2046, adding +/- 10%
    //     assert!(elapsed.as_millis() >= 1842);
    //     assert!(elapsed.as_millis() <= 2250);
    // }

    // #[tokio::test]
    // async fn honors_cancel() {
    //     let mut u1 = FailingUnit {
    //         attempt: 0,
    //         delay: None,
    //     };

    //     let policy = Policy {
    //         max_retries: 100,
    //         backoff_unit: Duration::from_millis(2000),
    //         backoff_factor: 2,
    //         max_backoff: Duration::MAX,
    //     };

    //     let start = std::time::Instant::now();
    //     let cancel = Arc::new(AtomicCell::new(false));

    //     let cancel2 = cancel.clone();
    //     std::thread::spawn(move || {
    //         std::thread::sleep(Duration::from_millis(500));
    //         cancel2.store(true);
    //     });

    //     let result = retry_unit(&mut u1, &policy, Some(&cancel)).await;
    //     let elapsed = start.elapsed();

    //     assert!(result.is_err());

    //     // not an exact science, should be 2046, adding +/- 10%
    //     assert!(elapsed.as_millis() >= 450);
    //     assert!(elapsed.as_millis() <= 550);
    // }
}
