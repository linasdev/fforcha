use crate::server::error::FForchaServerError;
use crate::watcher::error::FForchaWatcherError;

#[derive(Debug)]
pub enum FForchaError {
    Server(FForchaServerError),
    Watcher(FForchaWatcherError),
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
