use crate::server::queue::runner::FForchaServerQueueRunner;
use crate::server::worker::state::FForchaServerWorkerState;
use futures::FutureExt;
use futures::future::BoxFuture;
use nanoid::nanoid;
use std::pin::Pin;
use std::task::{Context, Poll};

pub mod state;

pub struct FForchaServerWorker {
    id: String,
    server_queue_runner: FForchaServerQueueRunner,
    next_state_future: BoxFuture<'static, Option<FForchaServerWorkerState>>,
}

impl FForchaServerWorker {
    pub fn new(
        worker_state_future: BoxFuture<'static, FForchaServerWorkerState>,
        server_queue_runner: FForchaServerQueueRunner,
    ) -> Self {
        Self {
            id: nanoid!(),
            server_queue_runner,
            next_state_future: worker_state_future.map(Some).boxed(),
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
                    next_state_future: worker_state
                        .process(self.id.clone(), self.server_queue_runner.clone())
                        .boxed(),
                })),
                None => Poll::Ready(None),
            },
            Poll::Pending => Poll::Pending,
        }
    }
}
