use crate::asset::FForchaAsset;
use crate::server::authenticator::FForchaServerAuthenticator;
use crate::server::error::FForchaServerError;
use crate::server::queue::runner::FForchaServerQueueRunner;
use crate::server::settings::FForchaServerSettings;
use crate::server::worker::FForchaServerWorker;
use crate::watcher::action::FForchaWatcherAction;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use log::{info, warn};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{broadcast, mpsc};

pub struct FForchaServerRunner {
    settings: FForchaServerSettings,
    server_queue_runner: FForchaServerQueueRunner,
    authenticator: FForchaServerAuthenticator,
}

impl FForchaServerRunner {
    pub fn new(settings: FForchaServerSettings) -> Self {
        let server_queue_runner = FForchaServerQueueRunner::new();
        let authenticator = FForchaServerAuthenticator::new(settings.shared_secret.clone());

        Self {
            settings,
            server_queue_runner,
            authenticator,
        }
    }

    pub async fn run(
        self,
        watcher_action_receiver: mpsc::UnboundedReceiver<
            FForchaWatcherAction<Arc<dyn FForchaAsset>>,
        >,
        mut shutdown_receiver: broadcast::Receiver<()>,
    ) -> Result<(), FForchaServerError> {
        info!("Starting FForcha server runner");

        let server_runner = Arc::new(self);
        let server_queue_future = server_runner
            .server_queue_runner
            .run(watcher_action_receiver);

        let tcp_listener = TcpListener::bind((
            server_runner.settings.bind_ip,
            server_runner.settings.bind_port,
        ))
        .await?;
        let mut workers = FuturesUnordered::new();
        let server_future = async {
            loop {
                tokio::select! {
                    accept_result = tcp_listener.accept() => match accept_result {
                        Ok((tcp_stream, _)) => {
                            workers.push(FForchaServerWorker::new(tcp_stream, server_runner.server_queue_runner.clone(), server_runner.authenticator.clone(), server_runner.settings.worker));
                        }
                        Err(error) => {
                            warn!("Failed to accept worker connection: {:?}", error);
                        }
                    },
                    Some(Some(worker)) = workers.next(), if !workers.is_empty() => workers.push(worker),
                    _ = shutdown_receiver.recv() => {
                        info!("Received shutdown signal, exiting server runner");
                        break;
                    }
                }
            }
        };

        tokio::select! {
            _ = server_queue_future => Ok(()),
            _ = server_future => Ok(())
        }
    }
}
