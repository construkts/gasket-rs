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

    #[error("error while performing stage work: {0}")]
    WorkError(String),

    #[error("can't perform action since tether to stage was dropped")]
    TetherDropped,
}

pub trait AsWorkError<T> {
    fn or_work_err(self) -> Result<T, Error>;
}

impl<T, E> AsWorkError<T> for Result<T, E>
where
    E: Display,
{
    fn or_work_err(self) -> Result<T, Error> {
        match self {
            Ok(x) => Ok(x),
            Err(x) => Err(Error::WorkError(format!("{}", x))),
        }
    }
}
