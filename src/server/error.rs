use crate::asset::error::FForchaAssetError;
use http::Request;
use tokio::io;

#[derive(Debug)]
pub enum FForchaServerError {
    Asset(FForchaAssetError),
    IO(io::Error),
    WebSockets(tokio_websockets::Error),
    SerdeJson(serde_json::Error),
    UnauthorizedWorker(Request<()>),
    WorkerTimedOut,
    WorkerConnectionClosed,
}

impl From<FForchaAssetError> for FForchaServerError {
    fn from(error: FForchaAssetError) -> Self {
        Self::Asset(error)
    }
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

impl From<serde_json::Error> for FForchaServerError {
    fn from(error: serde_json::Error) -> Self {
        Self::SerdeJson(error)
    }
}
