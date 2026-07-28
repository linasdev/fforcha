use crate::asset::FForchaAsset;
use crate::common::task::FForchaTask;
use crate::server::queue::{FForchaServerQueue, FForchaServerQueueTaskPermit};
use crate::watcher::action::FForchaWatcherAction;
use log::info;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

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

    pub fn run(
        &self,
        mut watcher_action_receiver: mpsc::UnboundedReceiver<
            FForchaWatcherAction<Arc<dyn FForchaAsset>>,
        >,
    ) -> JoinHandle<()> {
        info!("Starting FForcha server queue runner");

        let queue = self.queue.clone();
        tokio::spawn(async move {
            while let Some(watcher_action) = watcher_action_receiver.recv().await {
                match watcher_action {
                    FForchaWatcherAction::Queue(asset) => queue.push(FForchaTask::new(asset)).await,
                    FForchaWatcherAction::DeQueue(asset) => {
                        queue.remove_by_input_asset_key(asset.key()).await
                    }
                }
            }

            info!("All watcher actions processed, exiting server queue runner");
        })
    }

    pub async fn start_pop(&self) -> FForchaServerQueueTaskPermit {
        self.queue.start_pop().await
    }

    pub async fn complete_pop(&self, permit: FForchaServerQueueTaskPermit) {
        self.queue.complete_pop(permit).await;
    }
}
