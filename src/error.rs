use crate::server::error::FForchaServerError;
use crate::watcher::error::FForchaWatcherError;
use tokio::io;

#[derive(Debug)]
pub enum FForchaError {
    Server(FForchaServerError),
    Watcher(FForchaWatcherError),
    IO(io::Error),
}

impl From<FForchaServerError> for FForchaError {
    fn from(error: FForchaServerError) -> Self {
        Self::Server(error)
    }
}

impl From<FForchaWatcherError> for FForchaError {
    fn from(error: FForchaWatcherError) -> Self {
        Self::Watcher(error)
    }
}

impl From<io::Error> for FForchaError {
    fn from(error: io::Error) -> Self {
        Self::IO(error)
    }
}
