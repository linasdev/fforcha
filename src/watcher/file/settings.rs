use serde::Deserialize;
use std::path::PathBuf;

#[derive(Deserialize, Debug)]
pub struct FForchaFileWatcherSettings {
    #[serde(default = "default_directory_path")]
    pub directory_path: PathBuf,

    #[serde(default = "default_file_extensions")]
    pub file_extensions: Vec<String>,

    #[serde(default)]
    pub ignore_hidden_files: bool,

    #[serde(default)]
    pub follow_links: bool,
}

impl Default for FForchaFileWatcherSettings {
    fn default() -> Self {
        Self {
            directory_path: default_directory_path(),
            file_extensions: default_file_extensions(),
            ignore_hidden_files: Default::default(),
            follow_links: Default::default(),
        }
    }
}

fn default_directory_path() -> PathBuf {
    PathBuf::from("/fforcha/watch")
}

fn default_file_extensions() -> Vec<String> {
    vec!["mkv".to_string(), "mov".to_string(), "mp4".to_string()]
}
