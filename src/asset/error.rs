use tokio::io;

#[derive(Debug)]
pub enum FForchaAssetError {
    IOError(io::Error),
}

impl From<io::Error> for FForchaAssetError {
    fn from(error: io::Error) -> Self {
        Self::IOError(error)
    }
}
