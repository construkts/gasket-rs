use std::fmt::Display;

use thiserror::Error;
use tracing::{error, warn};

#[derive(Error, Debug)]
pub enum Error<W: Worker> {
    #[error("bootstrap should be retried")]
    BootstrapRetry(W::Config),

    #[error("bootstrap failed, stage should end")]
    BootstrapPanic(W::Config),

    #[error("teardown should be retried")]
    TeardownRetry(W),

    #[error("teardown failed, stage should end")]
    TeardownPanic(W),

    #[error("schedule should be retried")]
    ScheduleRetry,

    #[error("schedule requires a stage restart")]
    ScheduleRestart,

    #[error("schedule failed, stage should end")]
    SchedulePanic,

    #[error("error sending work unit through output port")]
    SendError,

    #[error("error receiving work unit through input port")]
    RecvError,

    #[error("stage panic, stopping all work")]
    ExecutePanic(W::WorkUnit),

    #[error("work unit requires a restart of the stage")]
    ExecuteRestart(W::WorkUnit),

    #[error("work unit should be retried")]
    ExecuteRetry(W::WorkUnit),

    #[error("work unit can be dismissed")]
    ExecuteDismiss(W::WorkUnit),
}

pub trait AsWorkError<T, W: Worker> {
    fn or_panic(self, unit: fn() -> W::WorkUnit) -> Result<T, Error<W>>;
    fn or_dismiss(self, unit: fn() -> W::WorkUnit) -> Result<T, Error<W>>;
    fn or_retry(self, unit: fn() -> W::WorkUnit) -> Result<T, Error<W>>;
    fn or_restart(self, unit: fn() -> W::WorkUnit) -> Result<T, Error<W>>;
}

impl<T, E, W> AsWorkError<T, W> for Result<T, E>
where
    W: Worker,
    E: Display,
{
    fn or_panic(self, unit: fn() -> W::WorkUnit) -> Result<T, Error<W>> {
        match self {
            Ok(x) => Ok(x),
            Err(x) => {
                error!(%x);
                Err(Error::WorkPanic(unit()))
            }
        }
    }

    fn or_dismiss(self, unit: fn() -> W::WorkUnit) -> Result<T, Error<W>> {
        match self {
            Ok(x) => Ok(x),
            Err(x) => {
                warn!(%x);
                Err(Error::WorkDismiss(unit()))
            }
        }
    }

    fn or_retry(self, unit: fn() -> W::WorkUnit) -> Result<T, Error<W>> {
        match self {
            Ok(x) => Ok(x),
            Err(x) => {
                warn!(%x);
                Err(Error::WorkRetry(unit()))
            }
        }
    }

    fn or_restart(self, unit: fn() -> W::WorkUnit) -> Result<T, Error<W>> {
        match self {
            Ok(x) => Ok(x),
            Err(x) => {
                warn!(%x);
                Err(Error::WorkRestart(unit()))
            }
        }
    }
}

pub enum WorkSchedule<U> {
    /// worker is not doing anything, but might in the future
    Idle,
    /// a work unit should be executed
    Unit(U),
    /// worker has done all the work it needed
    Done,
}

pub type BootstrapResult<W: Worker> = Result<W, Error<W>>;
pub type ScheduleResult<W: Worker> = Result<WorkSchedule<W::WorkUnit>, Error<W>>;
pub type ExecuteResult<W: Worker> = Result<(), Error<W>>;
pub type TeardownResult<W: Worker> = Result<W::Config, Error<W>>;

#[async_trait::async_trait(?Send)]
pub trait Worker: Send + Sized {
    type WorkUnit: Sized + Send;
    type Config: Send;

    /// Bootstrap a new worker
    ///
    /// It's responsible for initializing any resources needed by the worker.
    async fn bootstrap(config: Self::Config) -> BootstrapResult<Self>;

    /// Schedule the next work unit for execution
    ///
    /// This usually means reading messages from input ports and returning a
    /// work unit that contains all data required for execution.
    async fn schedule(&mut self) -> ScheduleResult<Self>;

    /// Execute the action described by the work unit
    ///
    /// This usually means doing required computation, generating side-effect
    /// and submitting message through the output ports
    async fn execute(&mut self, unit: Self::WorkUnit) -> ExecuteResult<Self>;

    async fn teardown(self) -> TeardownResult<Self>;
}
