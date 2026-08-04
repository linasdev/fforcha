use crate::server::authenticator::FForchaServerAuthenticator;
use crate::server::queue::runner::FForchaServerQueueRunner;
use crate::server::settings::FForchaServerWorkerSettings;
use crate::server::worker::state::FForchaServerWorkerState;
use futures::FutureExt;
use futures::future::BoxFuture;
use nanoid::nanoid;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::net::TcpStream;
use tokio_rustls::TlsAcceptor;

pub mod state;
pub mod stream;

pub struct FForchaServerWorker {
    id: String,
    server_queue_runner: FForchaServerQueueRunner,
    settings: FForchaServerWorkerSettings,
    next_state_future: BoxFuture<'static, Option<FForchaServerWorkerState>>,
}

impl FForchaServerWorker {
    pub fn new(
        tcp_stream: TcpStream,
        tls_acceptor: Option<TlsAcceptor>,
        server_queue_runner: FForchaServerQueueRunner,
        authenticator: FForchaServerAuthenticator,
        settings: FForchaServerWorkerSettings,
    ) -> Self {
        let worker_id = nanoid!();
        let worker_state = FForchaServerWorkerState::Establishing {
            tcp_stream,
            tls_acceptor,
            authenticator,
        };

        Self {
            id: worker_id.clone(),
            server_queue_runner: server_queue_runner.clone(),
            settings,
            next_state_future: worker_state
                .process(worker_id, server_queue_runner, settings)
                .boxed(),
        }
    }
}

impl Future for FForchaServerWorker {
    type Output = Option<FForchaServerWorker>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        match Future::poll(self.next_state_future.as_mut(), ctx) {
            Poll::Ready(worker_state) => match worker_state {
                Some(worker_state) => Poll::Ready(Some(FForchaServerWorker {
                    id: self.id.clone(),
                    server_queue_runner: self.server_queue_runner.clone(),
                    settings: self.settings,
                    next_state_future: worker_state
                        .process(
                            self.id.clone(),
                            self.server_queue_runner.clone(),
                            self.settings,
                        )
                        .boxed(),
                })),
                None => Poll::Ready(None),
            },
            Poll::Pending => Poll::Pending,
        }
    }
}
