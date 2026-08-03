use crate::asset::FForchaAsset;
use crate::common::message::FForchaMessage;
use crate::common::task::FForchaTask;
use crate::server::authenticator::FForchaServerAuthenticator;
use crate::server::error::FForchaServerError;
use crate::server::queue::FForchaServerQueueTaskPermit;
use crate::server::queue::runner::FForchaServerQueueRunner;
use crate::server::settings::FForchaServerWorkerSettings;
use futures::future::{BoxFuture, OptionFuture};
use futures::{FutureExt, SinkExt, TryStream, TryStreamExt, stream};
use log::{debug, info, trace, warn};
use std::fmt;
use std::fmt::Debug;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tokio::select;
use tokio::time::sleep;
use tokio_websockets::{CloseCode, Message, ServerBuilder, WebSocketStream};

struct FForchaServerWorkerError(FForchaServerError, Option<WebSocketStream<TcpStream>>);

pub enum FForchaServerWorkerState {
    Establishing {
        tcp_stream: TcpStream,
    },
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
        reason: String,
    },
}

impl FForchaServerWorkerState {
    pub async fn process(
        self,
        worker_id: String,
        server_queue_runner: FForchaServerQueueRunner,
        authenticator: FForchaServerAuthenticator,
        settings: FForchaServerWorkerSettings,
    ) -> Option<Self> {
        self.try_process(worker_id, server_queue_runner, authenticator, settings)
            .await
            .unwrap_or_else(|FForchaServerWorkerError(error, web_socket_stream)| {
                Some(FForchaServerWorkerState::Error {
                    web_socket_stream,
                    error,
                })
            })
    }

    async fn try_process(
        self,
        worker_id: String,
        server_queue_runner: FForchaServerQueueRunner,
        authenticator: FForchaServerAuthenticator,
        settings: FForchaServerWorkerSettings,
    ) -> Result<Option<Self>, FForchaServerWorkerError> {
        trace!("Worker [{worker_id}] New worker state available: {self:?}");

        match self {
            FForchaServerWorkerState::Establishing { tcp_stream } => {
                debug!("Worker [{worker_id}] Worker is establishing");
                let next_state =
                    Self::process_establishing_worker(worker_id.clone(), tcp_stream, authenticator)
                        .await?;
                Ok(Some(next_state))
            }
            FForchaServerWorkerState::Idle { web_socket_stream } => {
                debug!("Worker [{worker_id}] Worker is idle");
                let next_state = Self::process_idle_worker(
                    worker_id.clone(),
                    web_socket_stream,
                    server_queue_runner,
                    settings,
                )
                .await?;
                Ok(Some(next_state))
            }
            FForchaServerWorkerState::Running {
                web_socket_stream,
                server_queue_task_permit,
                last_progress,
            } => {
                debug!(
                    "Worker [{worker_id}] Worker is running task '{}' with last reported progress: {:.2}",
                    server_queue_task_permit.task().id(),
                    last_progress * 100.0
                );
                let next_state = Self::process_running_worker(
                    worker_id.clone(),
                    web_socket_stream,
                    server_queue_task_permit,
                    settings.worker_timeout,
                )
                .await?;
                Ok(Some(next_state))
            }
            FForchaServerWorkerState::Finishing {
                web_socket_stream,
                server_queue_task_permit,
            } => {
                debug!(
                    "Worker [{worker_id}] Worker is finishing task: {}",
                    server_queue_task_permit.task().id()
                );
                let next_state = Self::process_finishing_worker(
                    worker_id.clone(),
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
                debug!(
                    "Worker [{worker_id}] Worker is cancelling task: {}",
                    server_queue_task_permit.task().id()
                );
                let next_state = Self::process_cancelling_worker(
                    worker_id.clone(),
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
                debug!("Worker [{worker_id}] Worker is in error state: {error:?}");
                let next_state =
                    Self::process_error_worker(worker_id.clone(), web_socket_stream, error).await;
                Ok(next_state)
            }
            FForchaServerWorkerState::Closing {
                web_socket_stream,
                reason,
            } => {
                debug!("Worker [{worker_id}] Worker connection is closing with reason: {reason:?}");
                Self::process_closing_worker(worker_id.clone(), web_socket_stream, reason).await;
                trace!("Worker [{worker_id}] Worker dropped");
                Ok(None)
            }
        }
    }

    async fn process_establishing_worker(
        worker_id: String,
        tcp_stream: TcpStream,
        authenticator: FForchaServerAuthenticator,
    ) -> Result<Self, FForchaServerWorkerError> {
        let server_result = ServerBuilder::new().accept(tcp_stream).await;
        match server_result {
            Ok((request, web_socket_stream)) => {
                trace!("Worker [{worker_id}] Authenticating worker");

                authenticator
                    .authenticate(request)
                    .map_err(|error| FForchaServerWorkerError(error, None))?;

                Ok(FForchaServerWorkerState::Idle { web_socket_stream })
            }
            Err(error) => Err(FForchaServerWorkerError(
                FForchaServerError::WebSockets(error),
                None,
            )),
        }
    }

    async fn process_idle_worker(
        worker_id: String,
        web_socket_stream: WebSocketStream<TcpStream>,
        server_queue_runner: FForchaServerQueueRunner,
        settings: FForchaServerWorkerSettings,
    ) -> Result<Self, FForchaServerWorkerError> {
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
            |web_socket_stream, task| async move {
                trace!(
                    "Worker [{worker_id}] Sending task '{}' to worker",
                    task.id()
                );

                let task_assignment_message = FForchaMessage::TaskAssignment {
                    task_id: task.id(),
                    input_asset_key: task.input_asset().key(),
                };
                Self::send_message_to_worker(
                    worker_id.clone(),
                    Message::text(serde_json::to_string(&task_assignment_message)?),
                    web_socket_stream,
                )
                .await?;
                Self::wait_for_worker_acknowledgement(worker_id.clone(), Some(task.id()), web_socket_stream).await?;

                let input_asset = task.input_asset();

                trace!(
                    "Worker [{worker_id}] Sending input asset with key '{}' for task '{}' to worker",
                    input_asset.key(),
                    task.id(),
                );

                let mut input_asset_async_read = input_asset.async_read().await?;

                let mut buffer = vec![0; settings.asset_buffer_size];
                let bytes_read = input_asset_async_read.read(&mut buffer).await?;
                let next_message = Some(Message::binary(buffer[0..bytes_read].to_vec()));
                let bytes_read = usize::MAX; // Make sure we always send at least one message

                let stream = stream::try_unfold((input_asset_async_read, buffer, bytes_read, next_message), |(mut input_asset_async_read, mut buffer, mut bytes_read, mut next_message)| async move {
                    if let Some(current_message) = next_message.take() {
                        if bytes_read > 0 {
                            bytes_read = input_asset_async_read.read(&mut buffer).await?;
                            next_message = Some(Message::binary(buffer[0..bytes_read].to_vec()));

                            Ok(Some((current_message, (input_asset_async_read, buffer, bytes_read, next_message))))
                        } else {
                            Ok(None)
                        }
                    } else {
                        Ok(None)
                    }
                });

                Self::send_messages_to_worker(worker_id.clone(), web_socket_stream, Box::pin(stream)).await?;
                Self::wait_for_worker_acknowledgement(worker_id.clone(), Some(task.id()), web_socket_stream).await?;

                Ok(())
            }.boxed(),
        )
        .await
    }

    async fn process_running_worker(
        worker_id: String,
        web_socket_stream: WebSocketStream<TcpStream>,
        server_queue_task_permit: FForchaServerQueueTaskPermit,
        worker_timeout: Duration,
    ) -> Result<Self, FForchaServerWorkerError> {
        Self::wrap_cancellable_with_timeout(
            web_socket_stream,
            server_queue_task_permit,
            Some(worker_timeout),
            |progress, web_socket_stream, server_queue_task_permit| {
                if let Some(progress) = progress {
                    FForchaServerWorkerState::Running {
                        web_socket_stream,
                        server_queue_task_permit,
                        last_progress: progress,
                    }
                } else {
                    FForchaServerWorkerState::Finishing {
                        web_socket_stream,
                        server_queue_task_permit,
                    }
                }
            },
            |web_socket_stream, task| async move {
                let progress = Self::wait_for_mapped_and_filtered_message(
                    worker_id.clone(),
                    web_socket_stream,
                    |message| {
                        match message {
                            FForchaMessage::TaskProgress { task_id, progress } => if task_id == task.id() {
                                Some(Some(progress))
                            } else {
                                info!("Worker [{worker_id}] Received unexpected progress for task: {task_id:?}");
                                None
                            },
                            FForchaMessage::TaskCompletion { task_id } => if task_id == task.id() {
                                Some(None)
                            } else {
                                info!("Worker [{worker_id}] Received unexpected completion for task: {task_id:?}");
                                None
                            }
                            _ => None,
                        }
                    },
                )
                .await?;

                Ok(progress)
            }.boxed(),
        )
        .await
    }

    async fn process_finishing_worker(
        worker_id: String,
        web_socket_stream: WebSocketStream<TcpStream>,
        server_queue_task_permit: FForchaServerQueueTaskPermit,
    ) -> Result<Self, FForchaServerWorkerError> {
        unimplemented!();
    }

    async fn process_cancelling_worker(
        worker_id: String,
        web_socket_stream: WebSocketStream<TcpStream>,
        server_queue_task_permit: FForchaServerQueueTaskPermit,
    ) -> Result<Self, FForchaServerWorkerError> {
        unimplemented!();
    }

    async fn process_error_worker(
        worker_id: String,
        web_socket_stream: Option<WebSocketStream<TcpStream>>,
        error: FForchaServerError,
    ) -> Option<Self> {
        warn!("Worker [{worker_id}] Worker encountered a fatal error: {error:?}");

        if let FForchaServerError::WorkerConnectionClosed = error {
            warn!("Worker [{worker_id}] Worker connection was closed unexpectedly");
            return None;
        }

        if let Some(web_socket_stream) = web_socket_stream {
            match error {
                FForchaServerError::UnauthorizedWorker(request) => {
                    warn!(
                        "Worker [{worker_id}] Unauthorized worker attempted to connect with request: {request:?}"
                    );

                    Some(FForchaServerWorkerState::Closing {
                        web_socket_stream,
                        reason: "Unauthorized".to_string(),
                    })
                }
                FForchaServerError::WorkerTimedOut => Some(FForchaServerWorkerState::Closing {
                    web_socket_stream,
                    reason: "Timed out".to_string(),
                }),
                _ => Some(FForchaServerWorkerState::Closing {
                    web_socket_stream,
                    reason: "Fatal error".to_string(),
                }),
            }
        } else {
            None
        }
    }

    async fn process_closing_worker(
        worker_id: String,
        mut web_socket_stream: WebSocketStream<TcpStream>,
        reason: String,
    ) {
        let message = Message::close(Some(CloseCode::NORMAL_CLOSURE), reason.as_str());

        if let Err(error) =
            Self::send_message_to_worker(worker_id.clone(), message, &mut web_socket_stream).await
        {
            warn!("Worker [{worker_id}] Failed to send message to worker with error: {error:?}");
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
    ) -> Result<FForchaServerWorkerState, FForchaServerWorkerError>
    where
        M: FnOnce(
                T,
                WebSocketStream<TcpStream>,
                FForchaServerQueueTaskPermit,
            ) -> FForchaServerWorkerState
            + Send,
        for<'f> F: FnOnce(
            &'f mut WebSocketStream<TcpStream>,
            &'f FForchaTask,
        ) -> BoxFuture<'f, Result<T, FForchaServerError>>,
    {
        let (task, cancel_receiver) = server_queue_task_permit.task_and_cancel_receiver();
        let worker_timeout: OptionFuture<_> = worker_timeout
            .map(|worker_timeout| sleep(worker_timeout))
            .into();

        select! {
            t = f(&mut web_socket_stream, task) => {
                match t {
                    Ok(t) => Ok(mapper(t, web_socket_stream, server_queue_task_permit)),
                    Err(error) => Err(FForchaServerWorkerError(error, Some(web_socket_stream))),
                }
            },
            _ = cancel_receiver => Ok(FForchaServerWorkerState::Cancelling {
                web_socket_stream,
                server_queue_task_permit,
            }),
            Some(_) = worker_timeout => {
                Err(FForchaServerWorkerError(FForchaServerError::WorkerTimedOut, Some(web_socket_stream)))
            },
        }
    }

    async fn wait_for_worker_acknowledgement(
        worker_id: String,
        expected_task_id: Option<String>,
        web_socket_stream: &mut WebSocketStream<TcpStream>,
    ) -> Result<(), FForchaServerError> {
        trace!("Worker [{worker_id}] Waiting for worker acknowledgement");

        Self::wait_for_mapped_and_filtered_message(worker_id.clone(), web_socket_stream, |message| {
            match message {
                FForchaMessage::Acknowledgement { task_id } => if task_id.as_ref() != expected_task_id.as_ref() {
                    info!("Worker [{worker_id}] Received unexpected acknowledgement for task: {task_id:?}");
                    None
                } else {
                    Some(())
                },
                _ => {
                    info!("Worker [{worker_id}] Received unexpected message from worker: {message:?}");
                    None
                },
            }
        }).await?;

        Ok(())
    }

    async fn wait_for_mapped_and_filtered_message<M, T>(
        worker_id: String,
        web_socket_stream: &mut WebSocketStream<TcpStream>,
        mut mapper: M,
    ) -> Result<T, FForchaServerError>
    where
        M: FnMut(FForchaMessage) -> Option<T>,
    {
        while let Some(message) = web_socket_stream.try_next().await? {
            if !message.is_text() {
                trace!("Worker [{worker_id}] Received non-text message from worker: {message:?}");
                continue;
            }

            let message: FForchaMessage = serde_json::from_slice(message.into_payload().as_ref())?;
            if let Some(t) = mapper(message) {
                return Ok(t);
            }
        }

        Err(FForchaServerError::WorkerConnectionClosed)
    }

    async fn send_message_to_worker(
        worker_id: String,
        message: Message,
        web_socket_stream: &mut WebSocketStream<TcpStream>,
    ) -> Result<(), FForchaServerError> {
        trace!("Worker [{worker_id}] Sending message to worker: {message:?}");
        web_socket_stream.send(message).await?;
        Ok(())
    }

    async fn send_messages_to_worker<S>(
        worker_id: String,
        web_socket_stream: &mut WebSocketStream<TcpStream>,
        mut message_supplier: S,
    ) -> Result<(), FForchaServerError>
    where
        S: TryStream<Ok = Message, Error = FForchaServerError> + Unpin,
    {
        while let Some(message) = message_supplier.try_next().await? {
            trace!("Worker [{worker_id}] Feeding message to worker: {message:?}");
            web_socket_stream.feed(message).await?;
        }

        trace!("Worker [{worker_id}] Flushing messages to worker");
        web_socket_stream.flush().await?;

        Ok(())
    }
}

impl Debug for FForchaServerWorkerState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FForchaServerWorkerState::Establishing { .. } => {
                f.debug_struct("Establishing").finish()
            }
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
