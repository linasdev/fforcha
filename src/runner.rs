use crate::error::FForchaError;
use crate::settings::FForchaSettings;
use crate::watcher::FForchaWatcher;
use crate::watcher::file::FForchaFileWatcher;
use futures::StreamExt;
use futures::stream::SelectAll;
use log::error;

pub async fn run(settings: &FForchaSettings) -> Result<(), FForchaError> {
    let mut watchers = Vec::<Box<dyn FForchaWatcher>>::new();

    for file_watcher_settings in settings.watcher.file.iter() {
        let file_watcher = FForchaFileWatcher::new(file_watcher_settings);
        watchers.push(Box::new(file_watcher));
    }

    let mut combined_watcher_action_stream = SelectAll::new();
    for watcher in watchers.into_iter() {
        let watcher_action_stream = watcher.watch().await?;
        combined_watcher_action_stream.push(watcher_action_stream);
    }

    combined_watcher_action_stream
        .for_each(|watcher_action| async move {
            error!("Watcher action: {:?}", watcher_action);
        })
        .await;

    Ok(())
}
