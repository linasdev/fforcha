use http::Request;
use tokio::io;

#[derive(Debug)]
pub enum FForchaServerError {
    IO(io::Error),
    WebSockets(tokio_websockets::Error),
    UnauthorizedWorker(Request<()>),
    WorkerTimedOut,
}

impl From<io::Error> for FForchaServerError {
    fn from(error: io::Error) -> Self {
        Self::IO(error)
    }
}

impl From<tokio_websockets::Error> for FForchaServerError {
    fn from(error: tokio_websockets::Error) -> Self {
        Self::WebSockets(error)
    }
}
