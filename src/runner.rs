use crate::error::FForchaError;
use crate::server::runner::FForchaServerRunner;
use crate::settings::FForchaSettings;
use crate::watcher::runner::FForchaWatcherRunner;
use futures::TryFutureExt;
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinSet;

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
        // Channels & signals

        let (shutdown_sender, shutdown_receiver) = broadcast::channel(1);
        let (watcher_action_sender, watcher_action_receiver) = mpsc::unbounded_channel();
        let mut interrupt_signal = signal(SignalKind::interrupt())?;
        let mut terminate_signal = signal(SignalKind::terminate())?;

        // Tasks

        let mut join_set = JoinSet::new();

        // Server

        if let Some(server_runner) = self.server_runner {
            join_set.spawn(
                server_runner
                    .run(watcher_action_receiver, shutdown_receiver.resubscribe())
                    .map_err(FForchaError::Server),
            );
        }

        // Watcher

        join_set.spawn(
            self.watcher_runner
                .run(watcher_action_sender, shutdown_receiver.resubscribe())
                .map_err(FForchaError::Watcher),
        );

        // Run

        let first_run_result = tokio::select! {
            Some(join_result) = join_set.join_next() => {
                join_result.expect("Failed to join with runner")
            },
            _ = interrupt_signal.recv() => Ok(()),
            _ = terminate_signal.recv() => Ok(()),
        };

        // Shutdown & results

        shutdown_sender
            .send(())
            .expect("Failed to send shutdown signal across channel");

        let run_results = join_set.join_all().await;

        first_run_result?;

        for run_result in run_results.into_iter() {
            run_result?;
        }

        Ok(())
    }
}
