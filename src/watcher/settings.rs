use crate::watcher::file::settings::FForchaFileWatcherSettings;
use serde::Deserialize;

#[derive(Deserialize)]
pub struct FForchaWatcherSettings {
    #[serde(default = "default_file_watchers")]
    pub file: Vec<FForchaFileWatcherSettings>,
}

impl Default for FForchaWatcherSettings {
    fn default() -> Self {
        Self {
            file: default_file_watchers(),
        }
    }
}

fn default_file_watchers() -> Vec<FForchaFileWatcherSettings> {
    vec![FForchaFileWatcherSettings::default()]
}
