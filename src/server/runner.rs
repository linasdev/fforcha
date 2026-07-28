use crate::asset::FForchaAsset;
use crate::server::error::FForchaServerError;
use crate::server::queue::runner::FForchaServerQueueRunner;
use crate::server::settings::FForchaServerSettings;
use crate::server::worker::FForchaServerWorker;
use crate::server::worker::state::FForchaServerWorkerState;
use crate::watcher::action::FForchaWatcherAction;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use http::header::AUTHORIZATION;
use log::{info, warn};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use tokio::{io, select};
use tokio_websockets::ServerBuilder;

pub struct FForchaServerRunner {
    settings: FForchaServerSettings,
    server_queue_runner: FForchaServerQueueRunner,
}

impl FForchaServerRunner {
    pub fn new(settings: FForchaServerSettings) -> Self {
        Self {
            settings,
            server_queue_runner: FForchaServerQueueRunner::new(),
        }
    }

    pub async fn run(
        self,
        watcher_action_receiver: mpsc::UnboundedReceiver<
            FForchaWatcherAction<Arc<dyn FForchaAsset>>,
        >,
        mut shutdown_receiver: broadcast::Receiver<()>,
    ) -> Result<JoinHandle<Result<(), FForchaServerError>>, FForchaServerError> {
        info!("Starting FForcha server runner");

        let server_runner = Arc::new(self);
        let server_queue_runner_join_handle = server_runner
            .server_queue_runner
            .run(watcher_action_receiver);

        let tcp_listener = TcpListener::bind((
            server_runner.settings.bind_ip,
            server_runner.settings.bind_port,
        ))
        .await?;
        let mut workers = FuturesUnordered::new();
        let server_runner_join_handle = tokio::spawn(async move {
            let server_result = loop {
                select! {
                    connection_result = tcp_listener.accept() => workers.push(FForchaServerWorker::new(server_runner.clone().handle_accepted_connection_result(connection_result).boxed(), server_runner.server_queue_runner.clone())),
                    Some(Some(worker)) = workers.next(), if !workers.is_empty() => workers.push(worker),
                    _ = shutdown_receiver.recv() => {
                        info!("Received shutdown signal, exiting server runner");
                        break Ok(());
                    }
                }
            };

            server_queue_runner_join_handle
                .await
                .expect("Failed to join server queue runner");
            server_result
        });

        Ok(server_runner_join_handle)
    }

    async fn handle_accepted_connection_result(
        self: Arc<Self>,
        connection_result: io::Result<(TcpStream, SocketAddr)>,
    ) -> FForchaServerWorkerState {
        let tcp_stream = match connection_result {
            Ok((tcp_stream, _)) => tcp_stream,
            Err(error) => {
                warn!("Failed to open worker connection");
                return FForchaServerWorkerState::Error {
                    web_socket_stream: None,
                    error: FForchaServerError::IO(error),
                };
            }
        };

        let server_result = ServerBuilder::new().accept(tcp_stream).await;
        let web_socket_stream = match server_result {
            Ok((request, web_socket_stream)) => match request.headers().get(AUTHORIZATION) {
                Some(header_value) => match header_value.to_str() {
                    Ok(header_value)
                        if header_value
                            == format!("Bearer {}", self.settings.shared_secret).as_str() =>
                    {
                        web_socket_stream
                    }
                    Ok(header_value) => {
                        warn!("Invalid authorization header: {header_value}");
                        return FForchaServerWorkerState::Error {
                            web_socket_stream: Some(web_socket_stream),
                            error: FForchaServerError::UnauthorizedWorker(request),
                        };
                    }
                    Err(error) => {
                        warn!("Failed to convert header value to string: {:?}", error);
                        return FForchaServerWorkerState::Error {
                            web_socket_stream: Some(web_socket_stream),
                            error: FForchaServerError::UnauthorizedWorker(request),
                        };
                    }
                },
                None => {
                    warn!("No authorization header found");
                    return FForchaServerWorkerState::Error {
                        web_socket_stream: Some(web_socket_stream),
                        error: FForchaServerError::UnauthorizedWorker(request),
                    };
                }
            },
            Err(error) => {
                warn!("Failed to open worker connection");
                return FForchaServerWorkerState::Error {
                    web_socket_stream: None,
                    error: FForchaServerError::WebSockets(error),
                };
            }
        };

        FForchaServerWorkerState::Idle { web_socket_stream }
    }
}
