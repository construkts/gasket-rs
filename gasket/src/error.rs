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

    #[error("can't perform action since tether to stage was dropped")]
    TetherDropped,
}
