use crate::asset::FForchaAsset;
use crate::watcher::action::FForchaWatcherActionStream;
use crate::watcher::error::FForchaWatcherError;
use async_trait::async_trait;

pub mod action;
pub mod error;
pub mod file;
pub mod settings;

#[async_trait]
pub trait FForchaWatcher: Send + Sync {
    async fn watch(
        &self,
    ) -> Result<FForchaWatcherActionStream<Box<dyn FForchaAsset>>, FForchaWatcherError>;
}
