use crate::error::FForchaError;
use crate::server::runner::FForchaServerRunner;
use crate::settings::FForchaSettings;
use crate::watcher::runner::FForchaWatcherRunner;
use tokio::sync::{broadcast, mpsc};

pub struct FForchaRunner {
    server_runner: Option<FForchaServerRunner>,
    watcher_runner: FForchaWatcherRunner,
}

impl FForchaRunner {
    pub fn new(settings: FForchaSettings) -> Self {
        let server_runner = if settings.server.enabled {
            Some(FForchaServerRunner::new(settings.server))
        } else {
            None
        };

        Self {
            server_runner,
            watcher_runner: FForchaWatcherRunner::new(settings.watcher),
        }
    }

    pub async fn run(self) -> Result<(), FForchaError> {
        let (shutdown_sender, shutdown_receiver) = broadcast::channel(1);
        let (watcher_action_sender, watcher_action_receiver) = mpsc::unbounded_channel();

        let server_runner_join_handle = if let Some(server_runner) = self.server_runner {
            let server_runner_join_handle = server_runner
                .run(watcher_action_receiver, shutdown_receiver.resubscribe())
                .await?;
            Some(server_runner_join_handle)
        } else {
            None
        };

        let watcher_runner_join_handle = self.watcher_runner.run(watcher_action_sender).await?;

        let watcher_result = watcher_runner_join_handle
            .await
            .expect("Failed to join with watcher runner");

        shutdown_sender
            .send(())
            .expect("Failed to send shutdown signal across channel");

        if let Some(server_runner_join_handle) = server_runner_join_handle {
            server_runner_join_handle
                .await
                .expect("Failed to join with server runner")?;
        }

        watcher_result?;

        Ok(())
    }
}
