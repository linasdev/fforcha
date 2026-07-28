use crate::asset::FForchaAsset;
use crate::watcher::error::FForchaWatcherErrorWithIndex;
use futures::stream::BoxStream;
use std::sync::Arc;

pub type FForchaWatcherActionStream<A> =
    BoxStream<'static, Result<FForchaWatcherAction<A>, FForchaWatcherErrorWithIndex>>;

#[derive(Debug)]
pub enum FForchaWatcherAction<A: FForchaAsset> {
    Queue(A),
    DeQueue(A),
}

impl<A: FForchaAsset + 'static> FForchaWatcherAction<A> {
    pub fn debounce_key(&self) -> Option<String> {
        match self {
            FForchaWatcherAction::Queue(asset) => Some(asset.key() + ":queue"),
            FForchaWatcherAction::DeQueue(_) => None,
        }
    }

    pub fn map<F: FnOnce(A) -> A>(self, f: F) -> Self {
        match self {
            FForchaWatcherAction::Queue(asset) => FForchaWatcherAction::Queue(f(asset)),
            FForchaWatcherAction::DeQueue(asset) => FForchaWatcherAction::DeQueue(f(asset)),
        }
    }

    pub fn into_arc(self) -> FForchaWatcherAction<Arc<dyn FForchaAsset>> {
        match self {
            FForchaWatcherAction::Queue(asset) => FForchaWatcherAction::Queue(Arc::new(asset)),
            FForchaWatcherAction::DeQueue(asset) => FForchaWatcherAction::DeQueue(Arc::new(asset)),
        }
    }
}
