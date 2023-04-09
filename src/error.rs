use std::fmt::Display;

use thiserror::Error;
use tracing::{error, warn};

#[derive(Error, Debug)]
pub enum Error {
    #[error("operation was cancelled")]
    Cancelled,

    #[error("port is not connected")]
    NotConnected,

    #[error("error sending work unit through output port")]
    SendError,

    #[error("error receiving work unit through input port")]
    RecvError,

    #[error("stage panic, stopping all work")]
    WorkPanic,

    #[error("max retries reached")]
    MaxRetries,

    #[error("error that requires stage to restart")]
    ShouldRestart,

    #[error("retryable work error, will attempt again")]
    RetryableError,

    #[error("dismissable work error, will continue")]
    DismissableError,

    #[error("can't perform action since tether to stage was dropped")]
    TetherDropped,
}

pub trait AsWorkError<T> {
    fn or_panic(self) -> Result<T, Error>;
    fn or_dismiss(self) -> Result<T, Error>;
    fn or_retry(self) -> Result<T, Error>;
    fn or_restart(self) -> Result<T, Error>;
}

impl<T, E> AsWorkError<T> for Result<T, E>
where
    E: Display,
{
    fn or_panic(self) -> Result<T, Error> {
        match self {
            Ok(x) => Ok(x),
            Err(x) => {
                error!(%x);
                Err(Error::WorkPanic)
            }
        }
    }

    fn or_dismiss(self) -> Result<T, Error> {
        match self {
            Ok(x) => Ok(x),
            Err(x) => {
                warn!(%x);
                Err(Error::DismissableError)
            }
        }
    }

    fn or_retry(self) -> Result<T, Error> {
        match self {
            Ok(x) => Ok(x),
            Err(x) => {
                warn!(%x);
                Err(Error::RetryableError)
            }
        }
    }

    fn or_restart(self) -> Result<T, Error> {
        match self {
            Ok(x) => Ok(x),
            Err(x) => {
                warn!(%x);
                Err(Error::ShouldRestart)
            }
        }
    }
}
