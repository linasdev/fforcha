use crate::error::FForchaError;
use crate::settings::FForchaSettings;
use crate::watcher::runner::FForchaWatcherRunner;
use log::info;

pub struct FForchaRunner {
    watcher_runner: FForchaWatcherRunner,
}

impl FForchaRunner {
    pub fn new(settings: FForchaSettings) -> Self {
        Self {
            watcher_runner: FForchaWatcherRunner::new(settings.watcher),
        }
    }

    pub async fn run(self) -> Result<(), FForchaError> {
        let (watcher_runner_join_handle, mut watcher_action_receiver) =
            self.watcher_runner.run().await?;

        while let Some(watcher_action) = watcher_action_receiver.recv().await {
            info!("Received watcher action: {watcher_action:?}");
        }

        watcher_runner_join_handle
            .await
            .expect("Failed to join with watcher action debouncer")?;

        Ok(())
    }
}
