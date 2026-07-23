use crate::watcher::file::settings::FForchaFileWatcherSettings;
use serde::Deserialize;
use std::time::Duration;

#[derive(Deserialize, Debug)]
pub struct FForchaWatcherSettings {
    #[serde(default = "default_exit_on_watcher_failure")]
    pub exit_on_watcher_failure: bool,

    #[serde(default = "default_watcher_action_debounce_duration")]
    pub watcher_action_debounce_duration: Duration,

    #[serde(default = "default_file_watchers")]
    pub file: Vec<FForchaFileWatcherSettings>,
}

impl Default for FForchaWatcherSettings {
    fn default() -> Self {
        Self {
            exit_on_watcher_failure: default_exit_on_watcher_failure(),
            watcher_action_debounce_duration: default_watcher_action_debounce_duration(),
            file: default_file_watchers(),
        }
    }
}

fn default_exit_on_watcher_failure() -> bool {
    true
}

fn default_watcher_action_debounce_duration() -> Duration {
    Duration::from_secs(5)
}

fn default_file_watchers() -> Vec<FForchaFileWatcherSettings> {
    vec![FForchaFileWatcherSettings::default()]
}
