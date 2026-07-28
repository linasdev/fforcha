use crate::asset::error::FForchaAssetError;
use async_trait::async_trait;
use std::fmt::Debug;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use tokio::io::AsyncRead;

pub mod error;
pub mod file;

#[async_trait]
pub trait FForchaAsset: Send + Sync + Debug {
    fn key(&self) -> String;
    async fn async_read(&self) -> Result<Pin<Box<dyn AsyncRead + Send + Sync>>, FForchaAssetError>;
}

#[async_trait]
impl<A: FForchaAsset> FForchaAsset for Arc<A> {
    fn key(&self) -> String {
        self.deref().key()
    }

    async fn async_read(&self) -> Result<Pin<Box<dyn AsyncRead + Send + Sync>>, FForchaAssetError> {
        self.deref().async_read().await
    }
}

#[async_trait]
impl FForchaAsset for Arc<dyn FForchaAsset> {
    fn key(&self) -> String {
        self.deref().key()
    }

    async fn async_read(&self) -> Result<Pin<Box<dyn AsyncRead + Send + Sync>>, FForchaAssetError> {
        self.deref().async_read().await
    }
}
