use crate::asset::FForchaAsset;

#[derive(Debug)]
pub enum FForchaWatcherAction<A: FForchaAsset> {
    Queue(A),
    ReQueue(A),
    DeQueue(A),
}

impl<A: FForchaAsset + 'static> FForchaWatcherAction<A> {
    pub fn map<F: FnOnce(A) -> A>(self, f: F) -> Self {
        match self {
            FForchaWatcherAction::Queue(asset) => FForchaWatcherAction::Queue(f(asset)),
            FForchaWatcherAction::ReQueue(asset) => FForchaWatcherAction::ReQueue(f(asset)),
            FForchaWatcherAction::DeQueue(asset) => FForchaWatcherAction::DeQueue(f(asset)),
        }
    }

    pub fn into_boxed(self) -> FForchaWatcherAction<Box<dyn FForchaAsset>> {
        match self {
            FForchaWatcherAction::Queue(asset) => FForchaWatcherAction::Queue(Box::new(asset)),
            FForchaWatcherAction::ReQueue(asset) => FForchaWatcherAction::ReQueue(Box::new(asset)),
            FForchaWatcherAction::DeQueue(asset) => FForchaWatcherAction::DeQueue(Box::new(asset)),
        }
    }
}
