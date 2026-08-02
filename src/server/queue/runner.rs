use crate::asset::FForchaAsset;
use crate::common::task::FForchaTask;
use crate::server::queue::{FForchaServerQueue, FForchaServerQueueTaskPermit};
use crate::watcher::action::FForchaWatcherAction;
use log::info;
use std::sync::Arc;
use tokio::sync::mpsc;

#[derive(Clone)]
pub struct FForchaServerQueueRunner {
    queue: Arc<FForchaServerQueue>,
}

impl FForchaServerQueueRunner {
    pub fn new() -> Self {
        Self {
            queue: Arc::new(FForchaServerQueue::new()),
        }
    }

    pub async fn run(
        &self,
        mut watcher_action_receiver: mpsc::UnboundedReceiver<
            FForchaWatcherAction<Arc<dyn FForchaAsset>>,
        >,
    ) {
        info!("Starting FForcha server queue runner");

        while let Some(watcher_action) = watcher_action_receiver.recv().await {
            match watcher_action {
                FForchaWatcherAction::Queue(asset) => {
                    self.queue.push(FForchaTask::new(asset)).await
                }
                FForchaWatcherAction::DeQueue(asset) => {
                    self.queue.remove_by_input_asset_key(asset.key()).await
                }
            }
        }

        info!("All watcher actions processed, exiting server queue runner");
    }

    pub async fn start_pop(&self) -> FForchaServerQueueTaskPermit {
        self.queue.start_pop().await
    }

    pub async fn complete_pop(&self, permit: FForchaServerQueueTaskPermit) {
        self.queue.complete_pop(permit).await;
    }
}
