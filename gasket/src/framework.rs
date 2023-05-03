use std::fmt::Display;

use thiserror::Error;
use tracing::{error, warn};

#[cfg(feature = "derive")]
pub use gasket_derive::*;

pub trait Stage: Sized + Send {
    type Unit;
    type Worker: Worker<Self>;

    fn name(&self) -> &str;

    fn metrics(&self) -> crate::metrics::Registry {
        Default::default()
    }
}

#[derive(Error, Debug)]
pub enum WorkerError {
    #[error("error sending work unit through output port")]
    Send,

    #[error("error receiving work unit through input port")]
    Recv,

    #[error("operation panic, stage should stop")]
    Panic,

    #[error("operation requires a restart of the stage")]
    Restart,

    #[error("operation should be retried")]
    Retry,
}

type Result<T> = core::result::Result<T, WorkerError>;

pub trait AsWorkError<T> {
    fn or_panic(self) -> Result<T>;
    fn or_retry(self) -> Result<T>;
    fn or_restart(self) -> Result<T>;
}

impl<T, E> AsWorkError<T> for core::result::Result<T, E>
where
    E: Display,
{
    fn or_panic(self) -> Result<T> {
        match self {
            Ok(x) => Ok(x),
            Err(x) => {
                error!(%x);
                Err(WorkerError::Panic)
            }
        }
    }

    fn or_retry(self) -> Result<T> {
        match self {
            Ok(x) => Ok(x),
            Err(x) => {
                warn!(%x);
                Err(WorkerError::Retry)
            }
        }
    }

    fn or_restart(self) -> Result<T> {
        match self {
            Ok(x) => Ok(x),
            Err(x) => {
                warn!(%x);
                Err(WorkerError::Restart)
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

#[async_trait::async_trait(?Send)]
pub trait Worker<S>: Sized
where
    S: Stage,
{
    /// Bootstrap a new worker
    ///
    /// It's responsible for initializing any resources needed by the worker.
    async fn bootstrap(stage: &S) -> Result<Self>;

    /// Schedule the next work unit for execution
    ///
    /// This usually means reading messages from input ports and returning a
    /// work unit that contains all data required for execution.
    async fn schedule(&mut self, stage: &mut S) -> Result<WorkSchedule<S::Unit>>;

    /// Execute the action described by the work unit
    ///
    /// This usually means doing required computation, generating side-effect
    /// and submitting message through the output ports
    async fn execute(&mut self, unit: &S::Unit, stage: &mut S) -> Result<()>;

    /// Shutdown the worker gracefully
    ///
    /// This usually means releasing any relevant resources in use by the
    /// current worker, either because we need them for a different worker or
    /// because the stage is being dismissed.
    async fn teardown(&mut self) -> Result<()> {
        Ok(())
    }
}
