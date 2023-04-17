use std::{
    sync::{Arc, RwLock, Weak},
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
pub trait Worker: Send + Sized {
    type WorkUnit: Sized + Send;
    type Config: Send;

    /// Bootstrap a new worker
    ///
    /// It's responsible for initializing any resources needed by the worker.
    async fn bootstrap(
        config: &Self::Config,
        metrics: &mut metrics::Registry,
    ) -> Result<Self, Error>;

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
    Scheduling(W, Retry),
    Executing(W, W::WorkUnit, Retry),
    Teardown(W, Retry),
    Ended,
}

impl<W> StageState<W>
where
    W: Worker,
{
    #[cfg(test)]
    fn worker(&self) -> Option<&W> {
        match self {
            StageState::Bootstrap(..) => None,
            StageState::Scheduling(x, ..) => Some(x),
            StageState::Executing(x, ..) => Some(x),
            StageState::Teardown(x, ..) => Some(x),
            StageState::Ended => None,
        }
    }
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
            StageState::Teardown(..) => Self::Teardown,
            StageState::Ended => Self::Ended,
        }
    }
}

#[derive(Debug)]
pub enum StageEvent<W>
where
    W: Worker,
{
    Dismissed(W),
    WorkerIdle(W),
    WorkerDone(W),
    MessagingError(W),
    NextUnit(W, W::WorkUnit),
    ScheduleError(W, Error, Retry),
    ExecuteOk(W),
    ExecuteError(W, W::WorkUnit, Error, Retry),
    BootstrapOk(W),
    BootstrapError(Error, Retry),
    TeardownOk,
    TeardownError(W, Error, Retry),
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
    config: W::Config,
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
        StageEvent::ExecuteOk(..) => trace!("unit executed"),
        StageEvent::BootstrapError(e, r) => log_stage_error(e, r),
        StageEvent::NextUnit(..) => trace!("next unit scheduled"),
        StageEvent::ScheduleError(_, e, r) => log_stage_error(e, r),
        StageEvent::ExecuteError(_, _, e, r) => log_stage_error(e, r),
        StageEvent::MessagingError(_) => error!("messaging error"),
        StageEvent::Dismissed(_) => info!("stage dismissed"),
        StageEvent::BootstrapOk(_) => info!("stage bootstrap ok"),
        StageEvent::TeardownOk => info!("stage teardown ok"),
        StageEvent::TeardownError(_, e, r) => log_stage_error(e, r),
        StageEvent::WorkerIdle(_) => trace!("worker is idle"),
        StageEvent::WorkerDone(_) => trace!("worker is done"),
    }
}

impl<W> StageMachine<W>
where
    W: Worker,
{
    fn new(anchor: Arc<Anchor>, config: W::Config, policy: Policy, name: String) -> Self {
        StageMachine {
            state: Some(StageState::Bootstrap(Retry::new())),
            tick_count: Default::default(),
            name,
            anchor,
            policy,
            config,
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

        let mut metrics = self.anchor.metrics.write().unwrap();

        match W::bootstrap(&self.config, &mut metrics).await {
            Ok(w) => StageEvent::BootstrapOk(w),
            Err(x) => return StageEvent::BootstrapError(x, retry),
        }
    }

    #[instrument(level = Level::INFO, skip_all)]
    async fn schedule(&mut self, mut worker: W, retry: Retry) -> StageEvent<W> {
        if let Err(err) = retry.ok(&self.policy.work_retry) {
            return StageEvent::ScheduleError(worker, err, retry);
        }

        retry
            .wait_backoff(
                &self.policy.teardown_retry,
                self.anchor.dismissed_rx.clone(),
            )
            .await;

        let schedule = match worker.schedule().await {
            Ok(x) => x,
            Err(x) => return StageEvent::ScheduleError(worker, x, retry),
        };

        match schedule {
            WorkSchedule::Idle => StageEvent::WorkerIdle(worker),
            WorkSchedule::Done => StageEvent::WorkerDone(worker),
            WorkSchedule::Unit(u) => StageEvent::NextUnit(worker, u),
        }
    }

    #[instrument(level = Level::INFO, skip_all)]
    async fn execute(
        &mut self,
        mut worker: W,
        mut unit: W::WorkUnit,
        retry: Retry,
    ) -> StageEvent<W> {
        if let Err(err) = retry.ok(&self.policy.work_retry) {
            return StageEvent::ExecuteError(worker, unit, err, retry);
        }

        retry
            .wait_backoff(
                &self.policy.teardown_retry,
                self.anchor.dismissed_rx.clone(),
            )
            .await;

        match worker.execute(&mut unit).await {
            Ok(_) => StageEvent::ExecuteOk(worker),
            Err(err) => StageEvent::ExecuteError(worker, unit, err, retry),
        }
    }

    #[instrument(level = Level::INFO, skip_all)]
    async fn teardown(&mut self, mut worker: W, retry: Retry) -> StageEvent<W> {
        if let Err(err) = retry.ok(&self.policy.teardown_retry) {
            return StageEvent::TeardownError(worker, err, retry);
        }

        retry
            .wait_backoff(
                &self.policy.teardown_retry,
                self.anchor.dismissed_rx.clone(),
            )
            .await;

        match worker.teardown().await {
            Ok(_) => StageEvent::TeardownOk,
            Err(x) => return StageEvent::TeardownError(worker, x, retry.clone()),
        }
    }

    async fn actuate(&mut self, prev_state: StageState<W>) -> StageEvent<W> {
        {
            // if stage is dismissed, return early
            if *self.anchor.dismissed_rx.borrow() {
                match prev_state {
                    StageState::Bootstrap(..) => return StageEvent::TeardownOk,
                    StageState::Scheduling(w, ..) => return StageEvent::Dismissed(w),
                    StageState::Executing(w, _, _) => return StageEvent::Dismissed(w),
                    _ => (),
                };
            }
        }

        match prev_state {
            StageState::Bootstrap(retry) => self.bootstrap(retry).await,
            StageState::Scheduling(worker, retry) => self.schedule(worker, retry).await,
            StageState::Executing(worker, unit, retry) => self.execute(worker, unit, retry).await,
            StageState::Teardown(worker, retry) => self.teardown(worker, retry).await,
            StageState::Ended => unreachable!("ended stage shouldn't actuate"),
        }
    }

    fn apply(&self, event: StageEvent<W>) -> StageState<W> {
        match event {
            StageEvent::BootstrapOk(w) => StageState::Scheduling(w, Retry::new()),
            StageEvent::BootstrapError(err, retry) => match err {
                Error::ShouldRestart => StageState::Bootstrap(retry.next()),
                Error::RetryableError => StageState::Bootstrap(retry.next()),
                _ => StageState::Ended,
            },
            StageEvent::NextUnit(w, u) => StageState::Executing(w, u, Retry::new()),
            StageEvent::WorkerIdle(w) => StageState::Scheduling(w, Retry::new()),
            StageEvent::ScheduleError(w, err, retry) => match err {
                Error::ShouldRestart => StageState::Bootstrap(Retry::new()),
                Error::RetryableError => StageState::Scheduling(w, retry.next()),
                Error::DismissableError => StageState::Scheduling(w, Retry::new()),
                _ => StageState::Teardown(w, Retry::new()),
            },
            StageEvent::ExecuteOk(w) => StageState::Scheduling(w, Retry::new()),
            StageEvent::ExecuteError(w, unit, err, retry) => match err {
                Error::RetryableError => StageState::Executing(w, unit, retry.next()),
                Error::DismissableError => StageState::Scheduling(w, Retry::new()),
                Error::ShouldRestart => StageState::Bootstrap(Retry::new()),
                _ => StageState::Teardown(w, Retry::new()),
            },
            StageEvent::WorkerDone(w) => StageState::Teardown(w, Retry::new()),
            StageEvent::MessagingError(w) => StageState::Teardown(w, Retry::new()),
            StageEvent::Dismissed(w) => StageState::Teardown(w, Retry::new()),
            StageEvent::TeardownOk => StageState::Ended,
            StageEvent::TeardownError(w, err, retry) => match err {
                Error::RetryableError => StageState::Teardown(w, retry.next()),
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
    metrics: RwLock<metrics::Registry>,
}

impl Anchor {
    fn new() -> Self {
        let (dismissed_tx, dismissed_rx) = tokio::sync::watch::channel(false);

        Self {
            dismissed_rx,
            dismissed_tx,
            last_tick: AtomicCell::new(Instant::now()),
            last_state: AtomicCell::new(StagePhase::Bootstrap),
            metrics: RwLock::new(metrics::Registry::new()),
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
        let metrics = anchor.metrics.read().unwrap();
        let readings = collect_readings(&metrics);

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

pub fn spawn_stage<W>(config: W::Config, policy: Policy, name: Option<&str>) -> Tether
where
    W: Worker + 'static,
{
    let name = name
        .map(|x| x.to_owned())
        .unwrap_or("un-named stage".into());

    let anchor = Arc::new(Anchor::new());
    let anchor_ref = Arc::downgrade(&anchor);

    let policy2 = policy.clone();
    let name2 = name.clone();
    let thread_handle = std::thread::spawn(move || {
        let machine = StageMachine::<W>::new(anchor, config, policy2, name2);
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

    #[derive(Clone, Default)]
    pub struct MockConfig {
        failures: Vec<bool>,
    }

    pub struct MockWorker {
        config: MockConfig,
        bootstrap_count: usize,
        schedule_count: usize,
        execute_count: usize,
        teardown_count: usize,
    }

    impl MockWorker {
        fn should_fail(&self, unit: usize) -> bool {
            if self.config.failures.is_empty() {
                return false;
            }

            let failure_idx = unit % self.config.failures.len();
            *self.config.failures.get(failure_idx).unwrap()
        }
    }

    #[async_trait::async_trait(?Send)]
    impl Worker for MockWorker {
        type WorkUnit = usize;
        type Config = MockConfig;

        async fn bootstrap(
            config: &Self::Config,
            _: &mut metrics::Registry,
        ) -> Result<Self, Error> {
            Ok(Self {
                config: config.clone(),
                bootstrap_count: 1,
                schedule_count: 0,
                execute_count: 0,
                teardown_count: 0,
            })
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
        assert!(matches!(machine.state, Some(StageState::Teardown(..))));

        machine.transition().await;

        assert!(matches!(machine.state, Some(StageState::Ended)));
    }

    async fn should_bootstrap(machine: &mut StageMachine<MockWorker>) {
        assert!(matches!(machine.state, Some(StageState::Bootstrap(_))));
        machine.transition().await;

        let worker = machine.state.as_ref().unwrap().worker().unwrap();
        assert_eq!(worker.bootstrap_count, 1);

        assert!(matches!(machine.state, Some(StageState::Scheduling(..))));
    }

    #[tokio::test]
    async fn stage_machine_happy_path() {
        let config = MockConfig::default();
        let anchor = Arc::new(Anchor::new());
        let policy = Policy::default();

        let mut machine = StageMachine::new(anchor, config, policy, "dummy".into());

        should_bootstrap(&mut machine).await;

        for _ in 0..5 {
            assert!(matches!(machine.state, Some(StageState::Scheduling(..))));
            machine.transition().await;
            assert!(matches!(machine.state, Some(StageState::Executing(..))));
            machine.transition().await;
        }

        let worker = machine.state.as_ref().unwrap().worker().unwrap();
        assert_eq!(worker.execute_count, 5);

        machine.anchor.dismiss_stage().unwrap();
        machine.transition().await;

        should_teardown_and_end(&mut machine).await;
    }

    #[tokio::test]
    async fn honors_max_retries() {
        let config = MockConfig {
            failures: vec![true],
        };

        let anchor = Arc::new(Anchor::new());

        let work_retry = super::retries::Policy {
            max_retries: 3,
            ..Default::default()
        };

        let mut machine = StageMachine::new(
            anchor,
            config,
            Policy {
                work_retry,
                ..Default::default()
            },
            "dummy".into(),
        );

        should_bootstrap(&mut machine).await;

        assert!(matches!(machine.state, Some(StageState::Scheduling(..))));
        machine.transition().await;

        for _ in 0..5 {
            match machine.state {
                Some(StageState::Executing(_, unit, _)) => {
                    // should repeat the same unit every loop
                    assert_eq!(unit, 1);
                }
                _ => panic!("unexpected state"),
            }
            machine.transition().await;
        }

        let worker = machine.state.as_ref().unwrap().worker().unwrap();
        assert_eq!(worker.execute_count, 4);

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
