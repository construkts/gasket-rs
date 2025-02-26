use std::fmt::Display;

use thiserror::Error;
use tracing::{error, warn};

#[cfg(feature = "derive")]
pub use gasket_derive::*;

pub trait Stage: Sized + Send + Sync {
    type Unit: Send + Sync;
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

pub trait AsWorkError<T>: Send + Sync {
    fn or_panic(self) -> Result<T>;
    fn or_retry(self) -> Result<T>;
    fn or_restart(self) -> Result<T>;
}

impl<T, E> AsWorkError<T> for core::result::Result<T, E>
where
    T: Send + Sync,
    E: Send + Sync + Display,
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

#[async_trait::async_trait]
pub trait Worker<S>: Send + Sync + Sized
where
    S: Stage,
{
    /// Bootstrap a new worker
    ///
    /// It's responsible for initializing any resources needed by the worker.
    ///
    /// This future will be cancelled if the stage is requested to shut-down.
    /// The implementation of this function needs to be _cancellation
    /// safe_. Don't rely on state that crosses await points to avoid potential
    /// data loss.
    async fn bootstrap(stage: &S) -> Result<Self>;

    /// Schedule the next work unit for execution
    ///
    /// This usually means reading messages from input ports and returning a
    /// work unit that contains all data required for execution.
    ///
    /// This future will be cancelled if the stage is requested to shut-down.
    /// The implementation of this function needs to be _cancellation safe_.
    /// Don't rely on state that crosses await points to avoid potential data
    /// loss.
    async fn schedule(&mut self, stage: &mut S) -> Result<WorkSchedule<S::Unit>>;

    /// Execute the action described by the work unit
    ///
    /// This usually means doing required computation, generating side-effect
    /// and submitting message through the output ports
    ///
    /// This future will be cancelled if the stage is requested to shut-down.
    /// The implementation of this function needs to be _cancellation safe_.
    /// Don't rely on state that crosses await points to avoid potential data
    /// loss.
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
