use tokio::io;

#[derive(Debug)]
pub enum FForchaWatcherError {
    IOError(io::Error),
    WatchSourceVanished,
}

impl From<io::Error> for FForchaWatcherError {
    fn from(error: io::Error) -> Self {
        Self::IOError(error)
    }
}
