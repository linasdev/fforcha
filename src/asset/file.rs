use crate::asset::FForchaAsset;
use crate::asset::error::FForchaAssetError;
use async_trait::async_trait;
use log::{debug, warn};
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::AsyncRead;

#[derive(Eq, PartialEq, Debug)]
pub struct FForchaFileAsset {
    path: PathBuf,
}

impl FForchaFileAsset {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    pub fn extension(&self) -> Option<String> {
        match self.path.extension() {
            Some(extension) => match extension.to_str() {
                Some(extension) => Some(extension.to_string()),
                None => {
                    warn!("File has an invalid extension: {}", self.path.display());
                    None
                }
            },
            None => {
                debug!("File has no extension: {}", self.path.display());
                None
            }
        }
    }
}

#[async_trait]
impl FForchaAsset for FForchaFileAsset {
    async fn async_read(&self) -> Result<Box<dyn AsyncRead>, FForchaAssetError> {
        let file = File::open(self.path.clone()).await?;
        Ok(Box::new(file))
    }
}
