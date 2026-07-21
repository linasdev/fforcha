use crate::asset::FForchaAsset;
use crate::watcher::action::FForchaWatcherAction;
use crate::watcher::error::FForchaWatcherError;
use async_trait::async_trait;
use futures::stream::BoxStream;

pub mod action;
pub mod error;
pub mod file;
pub mod settings;

#[async_trait]
pub trait FForchaWatcher {
    async fn watch(
        &self,
    ) -> Result<
        BoxStream<
            'static,
            Result<FForchaWatcherAction<Box<dyn FForchaAsset>>, FForchaWatcherError>,
        >,
        FForchaWatcherError,
    >;
}
