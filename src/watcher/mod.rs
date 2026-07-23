use crate::asset::FForchaAsset;
use crate::watcher::action::FForchaWatcherAction;
use crate::watcher::error::{FForchaWatcherError, FForchaWatcherErrorWithIndex};
use async_trait::async_trait;
use futures::stream::BoxStream;

pub mod action;
pub mod error;
pub mod file;
pub mod settings;

#[async_trait]
pub trait FForchaWatcher: Send + Sync {
    async fn watch(
        &self,
    ) -> Result<
        BoxStream<
            'static,
            Result<FForchaWatcherAction<Box<dyn FForchaAsset>>, FForchaWatcherErrorWithIndex>,
        >,
        FForchaWatcherError,
    >;
}
