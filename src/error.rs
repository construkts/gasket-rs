use std::fmt::Display;

use thiserror::Error;

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

    #[error("input port is idle")]
    RecvIdle,

    #[error("stage panic, stopping all work: {0}")]
    WorkPanic(String),

    #[error("error that requires stage to restart: {0}")]
    ShouldRestart(String),

    #[error("retryable work error, will attempt again: {0}")]
    RetryableError(String),

    #[error("dismissable work error, will continue: {0}")]
    DismissableError(String),

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
            Err(x) => Err(Error::WorkPanic(format!("{}", x))),
        }
    }

    fn or_dismiss(self) -> Result<T, Error> {
        match self {
            Ok(x) => Ok(x),
            Err(x) => Err(Error::DismissableError(format!("{}", x))),
        }
    }

    fn or_retry(self) -> Result<T, Error> {
        match self {
            Ok(x) => Ok(x),
            Err(x) => Err(Error::RetryableError(format!("{}", x))),
        }
    }

    fn or_restart(self) -> Result<T, Error> {
        match self {
            Ok(x) => Ok(x),
            Err(x) => Err(Error::ShouldRestart(format!("{}", x))),
        }
    }
}
