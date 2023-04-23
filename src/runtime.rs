use std::{
    sync::{Arc, Weak},
    thread::JoinHandle,
    time::{Duration, Instant},
};

use crossbeam::{atomic::AtomicCell, utils::Backoff};
use tracing::{error, info, instrument, trace, warn, Level};

use crate::metrics;
use crate::retries;
use crate::{
    metrics::{collect_readings, Readings},
    retries::Retry,
};

use crate::framework::*;

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
    Executing(W, W::Unit, Retry),
    Teardown(W, Retry, Ended),
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
            StageState::Bootstrap(..) => Self::Bootstrap,
            StageState::Scheduling(..) => Self::Working,
            StageState::Executing(..) => Self::Working,
            StageState::Teardown(..) => Self::Teardown,
            StageState::Ended => Self::Ended,
        }
    }
}

type Ended = bool;

#[derive(Debug)]
pub enum StageEvent<W>
where
    W: Worker,
{
    Dismissed(W),
    WorkerIdle(W),
    WorkerDone(W),
    MessagingError(W),
    NextUnit(W, W::Unit),
    ScheduleError(W, WorkerError, Retry),
    ExecuteOk(W),
    ExecuteError(W, W::Unit, WorkerError, Retry),
    BootstrapOk(W),
    BootstrapError(WorkerError, Retry),
    TeardownOk(Ended),
    TeardownError(W, WorkerError, Retry, Ended),
}

struct StageMachine<W>
where
    W: Worker,
{
    stage: W::Stage,
    state: Option<StageState<W>>,
    anchor: Arc<Anchor>,
    policy: Policy,
    name: String,
    tick_count: metrics::Counter,
}

#[inline]
fn log_worker_error(err: &WorkerError, retry: &Retry) {
    match err {
        WorkerError::Restart => warn!(?retry, "stage should restart"),
        WorkerError::Retry => warn!(?retry, "unit should be retried"),
        WorkerError::Panic => error!(?retry, "stage should stop"),
        WorkerError::Recv => error!(?retry, "error receiving message"),
        WorkerError::Send => error!(?retry, "error while sending message"),
        //x => error!(?retry, "stage error {}", x),
    };
}

#[inline]
fn log_event<W>(event: &StageEvent<W>)
where
    W: Worker,
{
    match event {
        StageEvent::ExecuteOk(..) => trace!("unit executed"),
        StageEvent::BootstrapError(e, r, ..) => log_worker_error(e, r),
        StageEvent::NextUnit(..) => trace!("next unit scheduled"),
        StageEvent::ScheduleError(_, e, r) => log_worker_error(e, r),
        StageEvent::ExecuteError(_, _, e, r) => log_worker_error(e, r),
        StageEvent::MessagingError(_) => error!("messaging error"),
        StageEvent::Dismissed(_) => info!("stage dismissed"),
        StageEvent::BootstrapOk(_) => info!("stage bootstrap ok"),
        StageEvent::TeardownOk(..) => info!("stage teardown ok"),
        StageEvent::TeardownError(_, e, r, ..) => log_worker_error(e, r),
        StageEvent::WorkerIdle(_) => trace!("worker is idle"),
        StageEvent::WorkerDone(_) => trace!("worker is done"),
    }
}

impl<W> StageMachine<W>
where
    W: Worker,
{
    fn new(anchor: Arc<Anchor>, stage: W::Stage, policy: Policy, name: String) -> Self {
        StageMachine {
            stage,
            state: Some(StageState::Bootstrap(Retry::fresh())),
            tick_count: Default::default(),
            name,
            anchor,
            policy,
        }
    }

    #[instrument(level = Level::INFO, skip_all)]
    async fn bootstrap(&self, retry: Retry) -> StageEvent<W> {
        retry
            .wait_backoff(
                &self.policy.bootstrap_retry,
                self.anchor.dismissed_rx.clone(),
            )
            .await;

        match W::bootstrap(&self.stage).await {
            Ok(w) => StageEvent::BootstrapOk(w),
            Err(x) => StageEvent::BootstrapError(x, retry),
        }
    }

    #[instrument(level = Level::INFO, skip_all)]
    async fn schedule(&mut self, mut worker: W, retry: Retry) -> StageEvent<W> {
        retry
            .wait_backoff(
                &self.policy.teardown_retry,
                self.anchor.dismissed_rx.clone(),
            )
            .await;

        let schedule = match worker.schedule(&mut self.stage).await {
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
    async fn execute(&mut self, mut worker: W, unit: W::Unit, retry: Retry) -> StageEvent<W> {
        retry
            .wait_backoff(
                &self.policy.teardown_retry,
                self.anchor.dismissed_rx.clone(),
            )
            .await;

        match worker.execute(&unit, &mut self.stage).await {
            Ok(_) => StageEvent::ExecuteOk(worker),
            Err(err) => StageEvent::ExecuteError(worker, unit, err, retry),
        }
    }

    #[instrument(level = Level::INFO, skip_all)]
    async fn teardown(&mut self, mut worker: W, retry: Retry, ended: Ended) -> StageEvent<W> {
        retry
            .wait_backoff(
                &self.policy.teardown_retry,
                self.anchor.dismissed_rx.clone(),
            )
            .await;

        match worker.teardown().await {
            Ok(_) => StageEvent::TeardownOk(ended),
            Err(x) => StageEvent::TeardownError(worker, x, retry.clone(), ended),
        }
    }

    async fn actuate(&mut self, prev_state: StageState<W>) -> StageEvent<W> {
        {
            // if stage is dismissed, return early
            if *self.anchor.dismissed_rx.borrow() {
                match prev_state {
                    StageState::Bootstrap(..) => return StageEvent::TeardownOk(true),
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
            StageState::Teardown(worker, retry, ended) => self.teardown(worker, retry, ended).await,
            StageState::Ended => unreachable!("ended stage shouldn't actuate"),
        }
    }

    fn apply(&self, event: StageEvent<W>) -> StageState<W> {
        match event {
            StageEvent::BootstrapOk(w) => StageState::Scheduling(w, Retry::fresh()),
            StageEvent::BootstrapError(err, retry) => match err {
                WorkerError::Retry if retry.maxed(&self.policy.bootstrap_retry) => {
                    StageState::Ended
                }
                WorkerError::Retry => StageState::Bootstrap(retry.next()),
                _ => StageState::Ended,
            },
            StageEvent::NextUnit(w, u) => StageState::Executing(w, u, Retry::fresh()),
            StageEvent::WorkerIdle(w) => StageState::Scheduling(w, Retry::fresh()),
            StageEvent::ScheduleError(w, err, retry) => match err {
                WorkerError::Restart => StageState::Teardown(w, Retry::fresh(), false),
                WorkerError::Retry if !retry.maxed(&self.policy.work_retry) => {
                    StageState::Scheduling(w, retry.next())
                }
                WorkerError::Retry if retry.dismissed(&self.policy.work_retry) => {
                    StageState::Scheduling(w, Retry::fresh())
                }
                _ => StageState::Teardown(w, Retry::fresh(), true),
            },
            StageEvent::ExecuteOk(w) => StageState::Scheduling(w, Retry::fresh()),
            StageEvent::ExecuteError(w, u, err, retry) => match err {
                WorkerError::Restart => StageState::Teardown(w, Retry::fresh(), false),
                WorkerError::Retry if !retry.maxed(&self.policy.work_retry) => {
                    StageState::Executing(w, u, retry.next())
                }
                WorkerError::Retry if retry.dismissed(&self.policy.work_retry) => {
                    StageState::Scheduling(w, Retry::fresh())
                }
                _ => StageState::Teardown(w, Retry::fresh(), true),
            },
            StageEvent::WorkerDone(w) => StageState::Teardown(w, Retry::fresh(), true),
            StageEvent::MessagingError(w) => StageState::Teardown(w, Retry::fresh(), true),
            StageEvent::Dismissed(w) => StageState::Teardown(w, Retry::fresh(), true),
            StageEvent::TeardownOk(false) => StageState::Bootstrap(Retry::fresh()),
            StageEvent::TeardownOk(true) => StageState::Ended,
            StageEvent::TeardownError(w, err, retry, ended) => match err {
                WorkerError::Retry if !retry.maxed(&self.policy.teardown_retry) => {
                    StageState::Teardown(w, retry.next(), ended)
                }
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
    metrics: metrics::Registry,
    dismissed_rx: tokio::sync::watch::Receiver<bool>,
    dismissed_tx: tokio::sync::watch::Sender<bool>,
    last_state: AtomicCell<StagePhase>,
    last_tick: AtomicCell<Instant>,
}

impl Anchor {
    fn new(metrics: metrics::Registry) -> Self {
        let (dismissed_tx, dismissed_rx) = tokio::sync::watch::channel(false);

        Self {
            metrics,
            dismissed_rx,
            dismissed_tx,
            last_tick: AtomicCell::new(Instant::now()),
            last_state: AtomicCell::new(StagePhase::Bootstrap),
        }
    }

    fn dismiss_stage(&self) -> Result<(), crate::error::Error> {
        self.dismissed_tx
            .send(true)
            .map_err(|_| crate::error::Error::TetherDropped)?;

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

    fn try_anchor(&self) -> Result<Arc<Anchor>, crate::error::Error> {
        match self.anchor_ref.upgrade() {
            Some(anchor) => Ok(anchor),
            None => Err(crate::error::Error::TetherDropped),
        }
    }

    pub fn dismiss_stage(&self) -> Result<(), crate::error::Error> {
        let anchor = self.try_anchor()?;
        anchor.dismiss_stage()
    }

    pub fn check_state(&self) -> TetherState {
        let anchor = self.try_anchor();

        if anchor.is_err() {
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

    pub fn read_metrics(&self) -> Result<Readings, crate::error::Error> {
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

pub fn spawn_stage<W>(stage: W::Stage, policy: Policy, name: Option<&str>) -> Tether
where
    W: Worker + 'static,
{
    let name = name
        .map(|x| x.to_owned())
        .unwrap_or("un-named stage".into());

    let mut metrics = metrics::Registry::default();
    stage.register_metrics(&mut metrics);
    let anchor = Arc::new(Anchor::new(metrics));
    let anchor_ref = Arc::downgrade(&anchor);

    let policy2 = policy.clone();
    let name2 = name.clone();
    let thread_handle = std::thread::spawn(move || {
        let machine = StageMachine::<W>::new(anchor, stage, policy2, name2);
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
    pub struct MockStage {
        failures: Vec<bool>,
    }

    impl Stage for MockStage {
        fn register_metrics(&self, _: &mut crate::metrics::Registry) {}
    }

    pub struct MockWorker {
        bootstrap_count: usize,
        schedule_count: usize,
        execute_count: usize,
        teardown_count: usize,
    }

    impl MockWorker {
        fn should_fail(&self, unit: usize, config: &MockStage) -> bool {
            if config.failures.is_empty() {
                return false;
            }

            let failure_idx = unit % config.failures.len();
            *config.failures.get(failure_idx).unwrap()
        }
    }

    #[async_trait::async_trait(?Send)]
    impl Worker for MockWorker {
        type Unit = usize;
        type Stage = MockStage;

        async fn bootstrap(_: &Self::Stage) -> Result<Self, WorkerError> {
            Ok(Self {
                bootstrap_count: 1,
                schedule_count: 0,
                execute_count: 0,
                teardown_count: 0,
            })
        }

        async fn schedule(
            &mut self,
            _: &mut Self::Stage,
        ) -> Result<WorkSchedule<Self::Unit>, WorkerError> {
            self.schedule_count += 1;

            Ok(WorkSchedule::Unit(self.schedule_count))
        }

        async fn execute(
            &mut self,
            unit: &Self::Unit,
            config: &mut Self::Stage,
        ) -> Result<(), WorkerError> {
            self.execute_count += 1;

            match self.should_fail(*unit, config) {
                true => Err(WorkerError::Retry),
                false => Ok(()),
            }
        }

        async fn teardown(&mut self) -> Result<(), WorkerError> {
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
        assert!(matches!(machine.state, Some(StageState::Bootstrap(..))));
        machine.transition().await;

        let worker = machine.state.as_ref().unwrap().worker().unwrap();
        assert_eq!(worker.bootstrap_count, 1);

        assert!(matches!(machine.state, Some(StageState::Scheduling(..))));
    }

    #[tokio::test]
    async fn stage_machine_happy_path() {
        let config = MockStage::default();
        let metrics = metrics::Registry::default();
        let anchor = Arc::new(Anchor::new(metrics));
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
        let config = MockStage {
            failures: vec![true],
        };

        let metrics = metrics::Registry::default();
        let anchor = Arc::new(Anchor::new(metrics));

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

        for _ in 0..4 {
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
