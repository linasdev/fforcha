use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::pin;
use tokio_rustls::server::TlsStream;
use tokio_websockets::WebSocketStream;

pub struct FForchaServerWorkerStream(WebSocketStream<FForchaServerMaybeTlsStream<TcpStream>>);

impl From<WebSocketStream<FForchaServerMaybeTlsStream<TcpStream>>> for FForchaServerWorkerStream {
    fn from(value: WebSocketStream<FForchaServerMaybeTlsStream<TcpStream>>) -> Self {
        Self(value)
    }
}

impl Deref for FForchaServerWorkerStream {
    type Target = WebSocketStream<FForchaServerMaybeTlsStream<TcpStream>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for FForchaServerWorkerStream {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

pub enum FForchaServerMaybeTlsStream<S: AsyncRead + AsyncWrite + Unpin> {
    Plain(S),
    Encrypted(TlsStream<S>),
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncRead for FForchaServerMaybeTlsStream<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        ctx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => {
                pin!(stream);
                stream.poll_read(ctx, buf)
            }
            Self::Encrypted(stream) => {
                pin!(stream);
                stream.poll_read(ctx, buf)
            }
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncWrite for FForchaServerMaybeTlsStream<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        ctx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(stream) => {
                pin!(stream);
                stream.poll_write(ctx, buf)
            }
            Self::Encrypted(stream) => {
                pin!(stream);
                stream.poll_write(ctx, buf)
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => {
                pin!(stream);
                stream.poll_flush(ctx)
            }
            Self::Encrypted(stream) => {
                pin!(stream);
                stream.poll_flush(ctx)
            }
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => {
                pin!(stream);
                stream.poll_shutdown(ctx)
            }
            Self::Encrypted(stream) => {
                pin!(stream);
                stream.poll_shutdown(ctx)
            }
        }
    }
}
