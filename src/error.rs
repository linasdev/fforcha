use crate::asset::error::FForchaAssetError;
use crate::watcher::error::FForchaWatcherError;

#[derive(Debug)]
pub enum FForchaError {
    Asset(FForchaAssetError),
    Watcher(FForchaWatcherError),
}

impl From<FForchaAssetError> for FForchaError {
    fn from(error: FForchaAssetError) -> Self {
        Self::Asset(error)
    }
}

impl From<FForchaWatcherError> for FForchaError {
    fn from(error: FForchaWatcherError) -> Self {
        Self::Watcher(error)
    }
}
