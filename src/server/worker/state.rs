use crate::common::message::FForchaMessage;
use crate::common::task::FForchaTask;
use crate::server::error::FForchaServerError;
use crate::server::queue::FForchaServerQueueTaskPermit;
use crate::server::queue::runner::FForchaServerQueueRunner;
use futures::SinkExt;
use futures::future::OptionFuture;
use log::{trace, warn};
use std::fmt;
use std::fmt::Debug;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::select;
use tokio::time::sleep;
use tokio_websockets::{Message, WebSocketStream};

pub enum FForchaServerWorkerState {
    Idle {
        web_socket_stream: WebSocketStream<TcpStream>,
    },
    Running {
        web_socket_stream: WebSocketStream<TcpStream>,
        server_queue_task_permit: FForchaServerQueueTaskPermit,
        last_progress: f32,
    },
    Finishing {
        web_socket_stream: WebSocketStream<TcpStream>,
        server_queue_task_permit: FForchaServerQueueTaskPermit,
    },
    Cancelling {
        web_socket_stream: WebSocketStream<TcpStream>,
        server_queue_task_permit: FForchaServerQueueTaskPermit,
    },
    Error {
        web_socket_stream: Option<WebSocketStream<TcpStream>>,
        error: FForchaServerError,
    },
    Closing {
        web_socket_stream: WebSocketStream<TcpStream>,
        reason: Option<String>,
    },
}

impl FForchaServerWorkerState {
    pub async fn process(
        self,
        worker_id: String,
        server_queue_runner: FForchaServerQueueRunner,
    ) -> Option<Self> {
        self.try_process(worker_id.as_str(), server_queue_runner)
            .await
            .unwrap_or_else(|(web_socket_stream, error)| {
                Some(FForchaServerWorkerState::Error {
                    web_socket_stream: Some(web_socket_stream),
                    error,
                })
            })
    }

    async fn try_process(
        self,
        worker_id: &str,
        server_queue_runner: FForchaServerQueueRunner,
    ) -> Result<Option<Self>, (WebSocketStream<TcpStream>, FForchaServerError)> {
        trace!("Worker [{worker_id}] New worker state available: {self:?}");

        match self {
            FForchaServerWorkerState::Idle { web_socket_stream } => {
                let next_state =
                    Self::process_idle_worker(worker_id, web_socket_stream, server_queue_runner)
                        .await?;
                Ok(Some(next_state))
            }
            FForchaServerWorkerState::Running {
                web_socket_stream,
                server_queue_task_permit,
                last_progress,
            } => {
                let next_state = Self::process_running_worker(
                    worker_id,
                    web_socket_stream,
                    server_queue_task_permit,
                    last_progress,
                )
                .await?;
                Ok(Some(next_state))
            }
            FForchaServerWorkerState::Finishing {
                web_socket_stream,
                server_queue_task_permit,
            } => {
                let next_state = Self::process_finishing_worker(
                    worker_id,
                    web_socket_stream,
                    server_queue_task_permit,
                )
                .await?;
                Ok(Some(next_state))
            }
            FForchaServerWorkerState::Cancelling {
                web_socket_stream,
                server_queue_task_permit,
            } => {
                let next_state = Self::process_cancelling_worker(
                    worker_id,
                    web_socket_stream,
                    server_queue_task_permit,
                )
                .await?;
                Ok(Some(next_state))
            }
            FForchaServerWorkerState::Error {
                web_socket_stream,
                error,
            } => {
                let next_state =
                    Self::process_error_worker(worker_id, web_socket_stream, error).await;
                Ok(next_state)
            }
            FForchaServerWorkerState::Closing {
                web_socket_stream,
                reason,
            } => {
                Self::process_closing_worker(worker_id, web_socket_stream, reason).await;

                trace!("Worker [{worker_id}] Worker dropped");

                Ok(None)
            }
        }
    }

    async fn process_idle_worker(
        worker_id: &str,
        web_socket_stream: WebSocketStream<TcpStream>,
        server_queue_runner: FForchaServerQueueRunner,
    ) -> Result<Self, (WebSocketStream<TcpStream>, FForchaServerError)> {
        let server_queue_task_permit = server_queue_runner.start_pop().await;
        Self::wrap_cancellable_with_timeout(
            web_socket_stream,
            server_queue_task_permit,
            None,
            |_, web_socket_stream, server_queue_task_permit| FForchaServerWorkerState::Running {
                web_socket_stream,
                server_queue_task_permit,
                last_progress: 0.0,
            },
            async |web_socket_stream, task| Ok(()),
        )
        .await
    }

    async fn process_running_worker(
        worker_id: &str,
        web_socket_stream: WebSocketStream<TcpStream>,
        server_queue_task_permit: FForchaServerQueueTaskPermit,
        last_progress: f32,
    ) -> Result<Self, (WebSocketStream<TcpStream>, FForchaServerError)> {
        unimplemented!();
    }

    async fn process_finishing_worker(
        worker_id: &str,
        web_socket_stream: WebSocketStream<TcpStream>,
        server_queue_task_permit: FForchaServerQueueTaskPermit,
    ) -> Result<Self, (WebSocketStream<TcpStream>, FForchaServerError)> {
        unimplemented!();
    }

    async fn process_cancelling_worker(
        worker_id: &str,
        web_socket_stream: WebSocketStream<TcpStream>,
        server_queue_task_permit: FForchaServerQueueTaskPermit,
    ) -> Result<Self, (WebSocketStream<TcpStream>, FForchaServerError)> {
        unimplemented!();
    }

    async fn process_error_worker(
        worker_id: &str,
        web_socket_stream: Option<WebSocketStream<TcpStream>>,
        error: FForchaServerError,
    ) -> Option<Self> {
        warn!("Worker [{worker_id}] Worker encountered a fatal error: {error:?}");

        if let Some(web_socket_stream) = web_socket_stream {
            match error {
                FForchaServerError::UnauthorizedWorker(request) => {
                    warn!(
                        "Worker [{worker_id}] Unauthorized worker attempted to connect with request: {request:?}"
                    );

                    Some(FForchaServerWorkerState::Closing {
                        web_socket_stream,
                        reason: Some("Unauthorized".to_string()),
                    })
                }
                FForchaServerError::WorkerTimedOut => Some(FForchaServerWorkerState::Closing {
                    web_socket_stream,
                    reason: Some("Timed out".to_string()),
                }),
                _ => Some(FForchaServerWorkerState::Closing {
                    web_socket_stream,
                    reason: None,
                }),
            }
        } else {
            None
        }
    }

    async fn process_closing_worker(
        worker_id: &str,
        mut web_socket_stream: WebSocketStream<TcpStream>,
        reason: Option<String>,
    ) {
        let message = FForchaMessage::CloseConnection { reason };

        match serde_json::to_string(&message) {
            Ok(message) => {
                if let Err(error) = web_socket_stream.send(Message::text(message)).await {
                    warn!(
                        "Worker [{worker_id}] Failed to send message to worker with error: {error:?}"
                    );
                }
            }
            Err(error) => warn!(
                "Worker [{worker_id}] Failed to serialize worker message with error: {error:?}"
            ),
        }

        if let Err(error) = web_socket_stream.close().await {
            warn!("Worker [{worker_id}] Failed to close worker connection with error: {error:?}");
        }
    }

    async fn wrap_cancellable_with_timeout<T, M, F>(
        mut web_socket_stream: WebSocketStream<TcpStream>,
        mut server_queue_task_permit: FForchaServerQueueTaskPermit,
        worker_timeout: Option<Duration>,
        mapper: M,
        f: F,
    ) -> Result<FForchaServerWorkerState, (WebSocketStream<TcpStream>, FForchaServerError)>
    where
        M: FnOnce(
            T,
            WebSocketStream<TcpStream>,
            FForchaServerQueueTaskPermit,
        ) -> FForchaServerWorkerState,
        F: AsyncFnOnce(
            &mut WebSocketStream<TcpStream>,
            &FForchaTask,
        ) -> Result<T, FForchaServerError>,
    {
        let (task, cancel_receiver) = server_queue_task_permit.task_and_cancel_receiver();
        let worker_timeout: OptionFuture<_> = worker_timeout
            .map(|worker_timeout| sleep(worker_timeout))
            .into();

        select! {
            t = f(&mut web_socket_stream, task) => {
                match t {
                    Ok(t) => Ok(mapper(t, web_socket_stream, server_queue_task_permit)),
                    Err(error) => Err((web_socket_stream, error)),
                }
            },
            _ = cancel_receiver => Ok(FForchaServerWorkerState::Cancelling {
                web_socket_stream,
                server_queue_task_permit,
            }),
            Some(_) = worker_timeout => {
                Ok(FForchaServerWorkerState::Error {
                    web_socket_stream: Some(web_socket_stream),
                    error: FForchaServerError::WorkerTimedOut,
                })
            },
        }
    }
}

impl Debug for FForchaServerWorkerState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FForchaServerWorkerState::Idle { .. } => f.debug_struct("Idle").finish(),
            FForchaServerWorkerState::Running {
                server_queue_task_permit,
                last_progress,
                ..
            } => f
                .debug_struct("Running")
                .field("server_queue_task_permit", server_queue_task_permit)
                .field("last_progress", last_progress)
                .finish(),
            FForchaServerWorkerState::Finishing {
                server_queue_task_permit,
                ..
            } => f
                .debug_struct("Finishing")
                .field("server_queue_task_permit", server_queue_task_permit)
                .finish(),
            FForchaServerWorkerState::Cancelling {
                server_queue_task_permit,
                ..
            } => f
                .debug_struct("Cancelling")
                .field("server_queue_task_permit", server_queue_task_permit)
                .finish(),
            FForchaServerWorkerState::Error { error, .. } => {
                f.debug_struct("Error").field("error", error).finish()
            }
            FForchaServerWorkerState::Closing { reason, .. } => {
                f.debug_struct("Closing").field("reason", reason).finish()
            }
        }
    }
}
