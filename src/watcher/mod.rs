use crate::asset::FForchaAsset;
use crate::watcher::action::FForchaWatcherActionStream;
use crate::watcher::error::FForchaWatcherError;
use async_trait::async_trait;
use std::sync::Arc;

pub mod action;
pub mod error;
pub mod file;
pub mod runner;
pub mod settings;

#[async_trait]
pub trait FForchaWatcher: Send + Sync {
    async fn watch(
        &self,
    ) -> Result<FForchaWatcherActionStream<Arc<dyn FForchaAsset>>, FForchaWatcherError>;
}
