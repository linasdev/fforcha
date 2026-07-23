use crate::error::FForchaError;
use crate::settings::FForchaSettings;
use crate::watcher::FForchaWatcher;
use crate::watcher::file::FForchaFileWatcher;
use futures::StreamExt;
use futures::stream::SelectAll;
use log::{error, info, warn};
use std::collections::HashMap;
use tokio::select;
use tokio::sync::mpsc;
use tokio_util::time::DelayQueue;

pub async fn run(settings: &FForchaSettings) -> Result<(), FForchaError> {
    let mut watchers = Vec::<Box<dyn FForchaWatcher>>::new();
    let mut watcher_index = 0;

    for file_watcher_settings in settings.watcher.file.iter() {
        let file_watcher = FForchaFileWatcher::new(file_watcher_settings, watcher_index);
        watchers.push(Box::new(file_watcher));
        watcher_index += 1;
    }

    let mut combined_watcher_action_stream = SelectAll::new();
    for watcher in watchers.iter() {
        let watcher_action_stream = watcher.watch().await?;
        combined_watcher_action_stream.push(watcher_action_stream);
    }

    let exit_on_watcher_failure = settings.watcher.exit_on_watcher_failure;
    let watcher_action_debounce_duration = settings.watcher.watcher_action_debounce_duration;
    let mut queued_watcher_action_map = HashMap::new();
    let mut watcher_action_delay_queue = DelayQueue::new();
    let (watcher_action_sender, mut watcher_action_receiver) = mpsc::unbounded_channel();
    let watcher_action_debouncer_handle = tokio::spawn(async move {
        loop {
            select! {
                watcher_action_result_option = combined_watcher_action_stream.next() => {
                    match watcher_action_result_option {
                        Some(Ok(next_watcher_action)) => {
                            let watcher_action_key = next_watcher_action.key();
                            if let Some((watcher_action, delay_queue_key)) = queued_watcher_action_map.get_mut(&watcher_action_key) {
                                watcher_action_delay_queue.reset(&delay_queue_key, watcher_action_debounce_duration);
                                *watcher_action = next_watcher_action;
                            } else {
                                let delay_queue_key = watcher_action_delay_queue.insert(watcher_action_key.clone(), watcher_action_debounce_duration);
                                queued_watcher_action_map.insert(watcher_action_key, (next_watcher_action, delay_queue_key));
                            }
                        }
                        Some(Err(error_with_index)) => {
                            if exit_on_watcher_failure {
                                error!("Exiting because of watcher error: {:?}", error_with_index.0);
                                break Err(error_with_index.0.into());
                            }

                            warn!("Restarting watcher because of watcher error: {:?}", error_with_index.0);
                            if let Some(watcher) = watchers.get(error_with_index.1) {
                                match watcher.watch().await {
                                    Ok(watcher_action_stream) => {
                                        combined_watcher_action_stream.push(watcher_action_stream);
                                        info!("Successfully restarted watcher");
                                    }
                                    Err(error) => {
                                        warn!("Failed to restart watcher with error: {:?}", error);
                                        continue;
                                    }
                                }
                            } else {
                                error!("Failed to restart non-existing watcher with index: {}", error_with_index.1);
                            }
                        }
                        None => {
                            info!("All watchers finished, exiting");
                            break Ok(());
                        }
                    }
                }
                Some(debounced_watcher_action_key) = watcher_action_delay_queue.next() => {
                    let debounced_watcher_action_key = debounced_watcher_action_key.into_inner();
                    if let Some((watcher_action, _)) = queued_watcher_action_map.remove(&debounced_watcher_action_key) {
                        if watcher_action_sender.send(watcher_action).is_err() {
                            error!("Failed to send watcher action through channel, exiting");
                            break Err(FForchaError::WatcherActionChannelClosed);
                        }
                    }
                }
            }
        }
    });

    while let Some(watcher_action) = watcher_action_receiver.recv().await {
        info!("Received watcher action: {watcher_action:?}");
    }

    watcher_action_debouncer_handle
        .await
        .expect("Failed to join with watcher action debouncer")?;

    Ok(())
}
