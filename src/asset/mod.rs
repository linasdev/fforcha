use crate::asset::error::FForchaAssetError;
use async_trait::async_trait;
use std::fmt::Debug;
use tokio::io::AsyncRead;

pub mod error;
pub mod file;

#[async_trait]
pub trait FForchaAsset: Send + Sync + Debug {
    async fn async_read(&self) -> Result<Box<dyn AsyncRead>, FForchaAssetError>;
}

#[async_trait]
impl<A: FForchaAsset> FForchaAsset for Box<A> {
    async fn async_read(&self) -> Result<Box<dyn AsyncRead>, FForchaAssetError> {
        self.async_read().await
    }
}

#[async_trait]
impl FForchaAsset for Box<dyn FForchaAsset> {
    async fn async_read(&self) -> Result<Box<dyn AsyncRead>, FForchaAssetError> {
        self.async_read().await
    }
}
