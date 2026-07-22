use std::io;

#[derive(Debug)]
pub enum FForchaWatcherError {
    IO(io::Error),
    WalkDir(async_walkdir::Error),
    WatchSourceVanished,
}

impl From<io::Error> for FForchaWatcherError {
    fn from(error: io::Error) -> Self {
        Self::IO(error)
    }
}

impl From<async_walkdir::Error> for FForchaWatcherError {
    fn from(error: async_walkdir::Error) -> Self {
        Self::WalkDir(error)
    }
}
