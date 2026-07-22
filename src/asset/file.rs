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

    pub fn is_hidden(&self) -> Option<bool> {
        match self.path.file_name() {
            Some(file_name) => match file_name.to_str() {
                Some(file_name) => Some(file_name.starts_with(".")),
                None => {
                    warn!("File has an invalid name: {}", self.path.display());
                    None
                }
            },
            None => {
                warn!("File has no name: {}", self.path.display());
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
