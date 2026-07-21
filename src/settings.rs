use crate::watcher::settings::FForchaWatcherSettings;
use serde::Deserialize;

#[derive(Deserialize)]
pub struct FForchaSettings {
    #[serde(default = "default_watcher")]
    pub watcher: FForchaWatcherSettings,
}

fn default_watcher() -> FForchaWatcherSettings {
    FForchaWatcherSettings::default()
}
