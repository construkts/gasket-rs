use std::{
    sync::{Arc, Weak},
    thread::JoinHandle,
    time::{Duration, Instant},
};

use crossbeam::{atomic::AtomicCell, utils::Backoff};
use tracing::{debug, error, info, instrument, trace, warn, Level};

use crate::metrics;
use crate::retries;
use crate::{
    metrics::{collect_readings, Readings},
    retries::Retry,
};

use crate::framework::*;

/// Why a stage reached its terminal `Ended` state.
///
/// Every terminal transition in the state machine carries one of these, so the
/// phase that ends a stage records *why* it ended rather than collapsing every
/// path into an indistinguishable `Ended`.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum EndCause {
    /// the worker reported that all of its work is done
    Done,
    /// the stage was dismissed from the outside
    Dismissed,
    /// a worker error (panic, exhausted retries or messaging failure)
    /// terminated the stage
    Errored,
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum StagePhase {
    Bootstrap,
    Working,
    Teardown,
    Ended(EndCause),
}

impl StagePhase {
    /// The cause the stage ended with, if this is the terminal phase.
    pub fn end_cause(&self) -> Option<EndCause> {
        match self {
            StagePhase::Ended(cause) => Some(*cause),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
pub enum StageState<S>
where
    S: Stage,
{
    Bootstrap(Retry),
    Scheduling(S::Worker, Retry),
    Executing(S::Worker, S::Unit, Retry),
    /// The `Option<EndCause>` is the pending outcome: `Some(cause)` means the
    /// stage will end with that cause once teardown completes, `None` means it
    /// will restart (`WorkerError::Restart`).
    Teardown(S::Worker, Retry, Option<EndCause>),
    Ended(EndCause),
}

impl<S> StageState<S>
where
    S: Stage,
{
    #[cfg(test)]
    fn worker(&self) -> Option<&S::Worker> {
        match self {
            StageState::Bootstrap(..) => None,
            StageState::Scheduling(x, ..) => Some(x),
            StageState::Executing(x, ..) => Some(x),
            StageState::Teardown(x, ..) => Some(x),
            StageState::Ended(_) => None,
        }
    }
}

impl<S> From<&StageState<S>> for StagePhase
where
    S: Stage,
{
    fn from(value: &StageState<S>) -> Self {
        match value {
            StageState::Bootstrap(..) => Self::Bootstrap,
            StageState::Scheduling(..) => Self::Working,
            StageState::Executing(..) => Self::Working,
            StageState::Teardown(..) => Self::Teardown,
            StageState::Ended(cause) => Self::Ended(*cause),
        }
    }
}

#[derive(Debug)]
pub enum StageEvent<S>
where
    S: Stage,
{
    Dismissed(S::Worker),
    WorkerIdle(S::Worker),
    WorkerDone(S::Worker),
    MessagingError(S::Worker),
    NextUnit(S::Worker, S::Unit),
    ScheduleError(S::Worker, WorkerError, Retry),
    ExecuteOk(S::Worker),
    ExecuteError(S::Worker, S::Unit, WorkerError, Retry),
    BootstrapOk(S::Worker),
    BootstrapError(WorkerError, Retry),
    TeardownOk(Option<EndCause>),
    TeardownError(S::Worker, WorkerError, Retry, Option<EndCause>),
}

struct StageMachine<S>
where
    S: Stage,
{
    stage: S,
    state: Option<StageState<S>>,
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
fn log_event<S>(event: &StageEvent<S>)
where
    S: Stage,
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

impl<S> StageMachine<S>
where
    S: Stage,
{
    fn new(anchor: Arc<Anchor>, stage: S, policy: Policy, name: String) -> Self {
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
    async fn bootstrap(&self, retry: Retry) -> StageEvent<S> {
        retry
            .wait_backoff(&self.policy.bootstrap_retry, self.anchor.dismissed.clone())
            .await;

        tokio::select! {
            _ = self.anchor.dismissed.cancelled() => {
                // there's no worker yet, so end straight away as dismissed
                StageEvent::TeardownOk(Some(EndCause::Dismissed))
            }
            bootstrap = S::Worker::bootstrap(&self.stage) => {
                match bootstrap {
                    Ok(w) => StageEvent::BootstrapOk(w),
                    Err(x) => StageEvent::BootstrapError(x, retry),
                }
            }
        }
    }

    #[instrument(level = Level::INFO, skip_all)]
    async fn schedule(&mut self, mut worker: S::Worker, retry: Retry) -> StageEvent<S> {
        retry
            .wait_backoff(&self.policy.teardown_retry, self.anchor.dismissed.clone())
            .await;

        tokio::select! {
            _ = self.anchor.dismissed.cancelled() => {
                StageEvent::Dismissed(worker)
            }
            schedule = worker.schedule(&mut self.stage) => {
                match schedule {
                    Ok(x) => match x {
                        WorkSchedule::Idle => StageEvent::WorkerIdle(worker),
                        WorkSchedule::Done => StageEvent::WorkerDone(worker),
                        WorkSchedule::Unit(u) => StageEvent::NextUnit(worker, u),
                    },
                    Err(x) => StageEvent::ScheduleError(worker, x, retry),
                }
            }
        }
    }

    #[instrument(level = Level::INFO, skip_all)]
    async fn execute(
        &mut self,
        mut worker: S::Worker,
        unit: S::Unit,
        retry: Retry,
    ) -> StageEvent<S> {
        retry
            .wait_backoff(&self.policy.teardown_retry, self.anchor.dismissed.clone())
            .await;

        tokio::select! {
            _ = self.anchor.dismissed.cancelled() => {
                return StageEvent::Dismissed(worker);
            }
            execute = worker.execute(&unit, &mut self.stage) => {
                match execute {
                    Ok(_) => StageEvent::ExecuteOk(worker),
                    Err(err) => StageEvent::ExecuteError(worker, unit, err, retry),
                }
            }
        }
    }

    #[instrument(level = Level::INFO, skip_all)]
    async fn teardown(
        &mut self,
        mut worker: S::Worker,
        retry: Retry,
        ended: Option<EndCause>,
    ) -> StageEvent<S> {
        retry
            .wait_backoff(&self.policy.teardown_retry, self.anchor.dismissed.clone())
            .await;

        match worker.teardown().await {
            Ok(_) => StageEvent::TeardownOk(ended),
            Err(x) => StageEvent::TeardownError(worker, x, retry.clone(), ended),
        }
    }

    async fn actuate(&mut self, prev_state: StageState<S>) -> StageEvent<S> {
        match prev_state {
            StageState::Bootstrap(retry) => self.bootstrap(retry).await,
            StageState::Scheduling(worker, retry) => self.schedule(worker, retry).await,
            StageState::Executing(worker, unit, retry) => self.execute(worker, unit, retry).await,
            StageState::Teardown(worker, retry, ended) => self.teardown(worker, retry, ended).await,
            StageState::Ended(_) => unreachable!("ended stage shouldn't actuate"),
        }
    }

    fn apply(&self, event: StageEvent<S>) -> StageState<S> {
        match event {
            StageEvent::BootstrapOk(w) => StageState::Scheduling(w, Retry::fresh()),
            StageEvent::BootstrapError(err, retry) => match err {
                WorkerError::Retry if retry.maxed(&self.policy.bootstrap_retry) => {
                    StageState::Ended(EndCause::Errored)
                }
                WorkerError::Retry => StageState::Bootstrap(retry.next()),
                _ => StageState::Ended(EndCause::Errored),
            },
            StageEvent::NextUnit(w, u) => StageState::Executing(w, u, Retry::fresh()),
            StageEvent::WorkerIdle(w) => StageState::Scheduling(w, Retry::fresh()),
            StageEvent::ScheduleError(w, err, retry) => match err {
                WorkerError::Restart => StageState::Teardown(w, Retry::fresh(), None),
                WorkerError::Retry if !retry.maxed(&self.policy.work_retry) => {
                    StageState::Scheduling(w, retry.next())
                }
                WorkerError::Retry if retry.dismissed(&self.policy.work_retry) => {
                    StageState::Scheduling(w, Retry::fresh())
                }
                _ => StageState::Teardown(w, Retry::fresh(), Some(EndCause::Errored)),
            },
            StageEvent::ExecuteOk(w) => StageState::Scheduling(w, Retry::fresh()),
            StageEvent::ExecuteError(w, u, err, retry) => match err {
                WorkerError::Restart => StageState::Teardown(w, Retry::fresh(), None),
                WorkerError::Retry if !retry.maxed(&self.policy.work_retry) => {
                    StageState::Executing(w, u, retry.next())
                }
                WorkerError::Retry if retry.dismissed(&self.policy.work_retry) => {
                    StageState::Scheduling(w, Retry::fresh())
                }
                _ => StageState::Teardown(w, Retry::fresh(), Some(EndCause::Errored)),
            },
            StageEvent::WorkerDone(w) => {
                StageState::Teardown(w, Retry::fresh(), Some(EndCause::Done))
            }
            StageEvent::MessagingError(w) => {
                StageState::Teardown(w, Retry::fresh(), Some(EndCause::Errored))
            }
            StageEvent::Dismissed(w) => {
                StageState::Teardown(w, Retry::fresh(), Some(EndCause::Dismissed))
            }
            StageEvent::TeardownOk(None) => StageState::Bootstrap(Retry::fresh()),
            StageEvent::TeardownOk(Some(cause)) => StageState::Ended(cause),
            StageEvent::TeardownError(w, err, retry, ended) => match err {
                WorkerError::Retry if !retry.maxed(&self.policy.teardown_retry) => {
                    StageState::Teardown(w, retry.next(), ended)
                }
                // a teardown that gives up ends the stage; the original cause
                // wins, falling back to `Errored` for a failed restart
                _ => StageState::Ended(ended.unwrap_or(EndCause::Errored)),
            },
        }
    }

    async fn transition(&mut self) -> StagePhase {
        let prev_state = self.state.take().unwrap();
        let prev_phase = StagePhase::from(&prev_state);

        if matches!(prev_phase, StagePhase::Ended(_)) {
            self.state = Some(prev_state);
            return prev_phase;
        }

        let event = self.actuate(prev_state).await;
        log_event(&event);

        let next_state = self.apply(event);
        let next_phase = StagePhase::from(&next_state);

        if prev_phase != next_phase {
            debug!(?prev_phase, ?next_phase, "switching stage phase");
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
    dismissed: tokio_util::sync::CancellationToken,
    last_state: Arc<AtomicCell<StagePhase>>,
    last_tick: AtomicCell<Instant>,
}

impl Anchor {
    fn new(metrics: metrics::Registry) -> Self {
        Self {
            metrics,
            dismissed: tokio_util::sync::CancellationToken::new(),
            last_tick: AtomicCell::new(Instant::now()),
            last_state: Arc::new(AtomicCell::new(StagePhase::Bootstrap)),
        }
    }

    fn dismiss_stage(&self) -> Result<(), crate::error::Error> {
        trace!("dismissing stage");
        self.dismissed.cancel();

        Ok(())
    }
}

#[derive(Debug)]
pub struct Tether {
    name: String,
    anchor_ref: Weak<Anchor>,
    // strong handle that outlives the stage thread, unlike `anchor_ref`.
    last_state: Arc<AtomicCell<StagePhase>>,
    thread_handle: JoinHandle<()>,
    policy: Policy,
}

#[derive(Debug, PartialEq)]
pub enum TetherState {
    Dropped,
    Blocked(StagePhase),
    Alive(StagePhase),
    /// The stage thread has ended; carries the last phase it reached, so a
    /// graceful `Ended` can be told apart from a stage that died mid-work.
    Finished(StagePhase),
}

impl TetherState {
    /// The cause the stage ended with, if it reached its terminal phase —
    /// whether the thread is still winding down (`Alive`) or already gone
    /// (`Finished`).
    pub fn end_cause(&self) -> Option<EndCause> {
        match self {
            TetherState::Alive(phase) | TetherState::Finished(phase) => phase.end_cause(),
            _ => None,
        }
    }

    /// The stage thread is gone but never reached `Ended`, i.e. it panicked
    /// mid-work.
    pub fn has_panicked(&self) -> bool {
        match self {
            TetherState::Dropped => true,
            TetherState::Finished(phase) => phase.end_cause().is_none(),
            _ => false,
        }
    }

    /// The stage stopped ticking within its timeout.
    pub fn is_stalled(&self) -> bool {
        matches!(self, TetherState::Blocked(_))
    }
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

    pub fn try_anchor(&self) -> Result<Arc<Anchor>, crate::error::Error> {
        match self.anchor_ref.upgrade() {
            Some(anchor) => Ok(anchor),
            None => Err(crate::error::Error::TetherDropped),
        }
    }

    pub fn dismiss_stage(&self) -> Result<(), crate::error::Error> {
        let anchor = self.try_anchor()?;
        anchor.dismiss_stage()
    }

    /// Why the stage ended, if it has reached its terminal state. Derived from
    /// the last phase, so it remains readable after the stage thread is gone.
    pub fn end_cause(&self) -> Option<EndCause> {
        self.last_state.load().end_cause()
    }

    pub fn check_state(&self) -> TetherState {
        let last_phase = self.last_state.load();

        let anchor = match self.try_anchor() {
            Ok(anchor) => anchor,
            // anchor gone => the stage thread has ended at `last_phase`.
            Err(_) => return TetherState::Finished(last_phase),
        };

        if let Some(timeout) = &self.policy.tick_timeout {
            let last_tick = anchor.last_tick.load();

            if last_tick.elapsed() > *timeout {
                return TetherState::Blocked(last_phase);
            }
        }

        TetherState::Alive(last_phase)
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

#[derive(Debug, Clone)]
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
fn fullfil_stage<S>(mut machine: StageMachine<S>)
where
    S: Stage,
{
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async { while !matches!(machine.transition().await, StagePhase::Ended(_)) {} });
}

pub fn spawn_stage<S: Stage>(stage: S, policy: Policy) -> Tether
where
    S: Stage + 'static,
{
    let name = stage.name().to_owned();

    let metrics = stage.metrics();

    let anchor = Arc::new(Anchor::new(metrics));
    let anchor_ref = Arc::downgrade(&anchor);
    let last_state = Arc::clone(&anchor.last_state);

    let policy2 = policy.clone();
    let name2 = name.clone();
    let thread_handle = std::thread::spawn(move || {
        let machine = StageMachine::<S>::new(anchor, stage, policy2, name2);
        fullfil_stage(machine);
    });

    Tether {
        name,
        anchor_ref,
        last_state,
        thread_handle,
        policy,
    }
}

#[cfg(test)]
pub mod tests {
    use approx::assert_abs_diff_eq;

    use super::*;

    #[derive(Clone, Default)]
    pub struct MockStage {
        pub failures: Vec<bool>,
        pub schedule_delay: Option<Duration>,
        pub execute_delay: Option<Duration>,
        pub done_after: Option<usize>,
    }

    impl Stage for MockStage {
        type Worker = MockWorker;
        type Unit = usize;

        fn name(&self) -> &str {
            "mockstage"
        }

        fn metrics(&self) -> crate::metrics::Registry {
            metrics::Registry::default()
        }
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
    impl Worker<MockStage> for MockWorker {
        async fn bootstrap(_: &MockStage) -> Result<Self, WorkerError> {
            Ok(Self {
                bootstrap_count: 1,
                schedule_count: 0,
                execute_count: 0,
                teardown_count: 0,
            })
        }

        async fn schedule(
            &mut self,
            stage: &mut MockStage,
        ) -> Result<WorkSchedule<usize>, WorkerError> {
            self.schedule_count += 1;

            if let Some(delay) = stage.schedule_delay {
                tokio::time::sleep(delay).await;
            }

            if stage
                .done_after
                .is_some_and(|max| self.schedule_count > max)
            {
                return Ok(WorkSchedule::Done);
            }

            Ok(WorkSchedule::Unit(self.schedule_count))
        }

        async fn execute(
            &mut self,
            unit: &usize,
            stage: &mut MockStage,
        ) -> Result<(), WorkerError> {
            self.execute_count += 1;

            if let Some(delay) = stage.execute_delay {
                tokio::time::sleep(delay).await;
            }

            match self.should_fail(*unit, stage) {
                true => Err(WorkerError::Retry),
                false => Ok(()),
            }
        }

        async fn teardown(&mut self) -> Result<(), WorkerError> {
            self.teardown_count += 1;

            Ok(())
        }
    }

    async fn should_teardown_and_end(machine: &mut StageMachine<MockStage>) {
        assert!(matches!(machine.state, Some(StageState::Teardown(..))));

        machine.transition().await;

        assert!(matches!(machine.state, Some(StageState::Ended(_))));
    }

    async fn should_bootstrap(machine: &mut StageMachine<MockStage>) {
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
            ..Default::default()
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

    #[tokio::test(flavor = "multi_thread")]
    async fn honors_cancel_in_time() {
        let expected_shutdown = Duration::from_millis(1_000);

        let stage = MockStage {
            schedule_delay: Some(expected_shutdown.mul_f64(10.0)),
            execute_delay: Some(expected_shutdown.mul_f64(10.0)),
            ..Default::default()
        };

        let start = std::time::Instant::now();
        let tether = super::spawn_stage(stage, Policy::default());

        let anchor = tether.try_anchor().unwrap();
        tokio::spawn(async move {
            tokio::time::sleep(expected_shutdown).await;
            anchor.dismiss_stage().unwrap();
        });

        tether.join_stage();

        let elapsed = start.elapsed();

        assert_abs_diff_eq!(
            elapsed.as_secs_f64(),
            expected_shutdown.as_secs_f64(),
            epsilon = 0.01
        );
    }

    #[test]
    fn check_state_reports_finished_with_terminal_phase() {
        let stage = MockStage {
            schedule_delay: Some(Duration::from_millis(20)),
            ..Default::default()
        };

        let tether = super::spawn_stage(stage, Policy::default());

        std::thread::sleep(Duration::from_millis(200));
        assert!(matches!(tether.check_state(), TetherState::Alive(_)));

        tether.dismiss_stage().unwrap();

        let mut state = tether.check_state();
        for _ in 0..200 {
            state = tether.check_state();
            if matches!(state, TetherState::Finished(_)) {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }

        // a graceful stop is retained as Finished(Ended), not collapsed to Dropped
        assert_eq!(
            state,
            TetherState::Finished(StagePhase::Ended(EndCause::Dismissed))
        );
    }
}
