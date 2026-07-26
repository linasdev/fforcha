use crate::asset::FForchaAsset;
use crate::watcher::FForchaWatcher;
use crate::watcher::action::{FForchaWatcherAction, FForchaWatcherActionStream};
use crate::watcher::error::{FForchaWatcherError, FForchaWatcherErrorWithIndex};
use crate::watcher::file::FForchaFileWatcher;
use crate::watcher::settings::FForchaWatcherSettings;
use futures::StreamExt;
use futures::stream::SelectAll;
use log::{error, info, warn};
use std::collections::HashMap;
use tokio::select;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::time::DelayQueue;

pub struct FForchaWatcherRunner {
    settings: FForchaWatcherSettings,
    watchers: Vec<Box<dyn FForchaWatcher>>,
}

impl FForchaWatcherRunner {
    pub fn new(settings: FForchaWatcherSettings) -> Self {
        Self {
            settings,
            watchers: vec![],
        }
    }

    pub fn prepare_watchers(&mut self) {
        self.watchers.clear();

        let mut watcher_index = 0;
        for file_watcher_settings in self.settings.file.iter() {
            let file_watcher = FForchaFileWatcher::new(file_watcher_settings, watcher_index);
            self.watchers.push(Box::new(file_watcher));
            watcher_index += 1;
        }
    }

    pub async fn restart_watcher_with_index(
        &self,
        watcher_index: usize,
    ) -> Result<FForchaWatcherActionStream<Box<dyn FForchaAsset>>, FForchaWatcherError> {
        if let Some(watcher) = self.watchers.get(watcher_index) {
            Ok(watcher.watch().await?)
        } else {
            Err(FForchaWatcherError::WatcherDoesNotExist)
        }
    }

    pub async fn run(
        mut self,
    ) -> Result<
        (
            JoinHandle<Result<(), FForchaWatcherError>>,
            mpsc::UnboundedReceiver<FForchaWatcherAction<Box<dyn FForchaAsset>>>,
        ),
        FForchaWatcherError,
    > {
        info!("Starting FForcha watcher runner");

        self.prepare_watchers();

        let mut combined_watcher_action_stream = SelectAll::new();
        for watcher in self.watchers.iter() {
            let watcher_action_stream = watcher.watch().await?;
            combined_watcher_action_stream.push(watcher_action_stream);
        }

        let exit_on_watcher_failure = self.settings.exit_on_watcher_failure;
        let always_restart_watchers = self.settings.always_restart_watchers;
        let watcher_restart_delay = self.settings.watcher_restart_delay;
        let watcher_action_debounce_duration = self.settings.watcher_action_debounce_duration;
        let mut queued_watcher_actions = HashMap::new();
        let mut watcher_action_delay_queue = DelayQueue::new();
        let mut watcher_restart_delay_queue = DelayQueue::new();
        let mut combined_watcher_action_stream_finished = false;
        let (watcher_action_sender, watcher_action_receiver) = mpsc::unbounded_channel();
        let watcher_runner_join_handle = tokio::spawn(async move {
            loop {
                select! {
                    Some(watcher_index) = watcher_restart_delay_queue.next(), if !watcher_restart_delay_queue.is_empty() => {
                        let watcher_index = watcher_index.into_inner();

                        warn!("Restarting watcher");

                        match self.restart_watcher_with_index(watcher_index).await {
                            Ok(watcher_action_stream) => {
                                combined_watcher_action_stream.push(watcher_action_stream);
                                info!("Successfully restarted watcher");
                            }
                            Err(error) => {
                                warn!("Failed to restart watcher with error: {:?}", error);

                                if always_restart_watchers {
                                    watcher_restart_delay_queue.insert(watcher_index, watcher_restart_delay);
                                }
                            }
                        }
                    }
                    Some(debounced_watcher_action_key) = watcher_action_delay_queue.next(), if !watcher_action_delay_queue.is_empty() => {
                        let debounced_watcher_action_key = debounced_watcher_action_key.into_inner();
                        if let Some((watcher_action, _)) = queued_watcher_actions.remove(&debounced_watcher_action_key) {
                            if watcher_action_sender.send(watcher_action).is_err() {
                                error!("Failed to send watcher action through channel, exiting");
                                break Err(FForchaWatcherError::WatcherActionChannelClosed);
                            }
                        }
                    }
                    watcher_action_result_option = combined_watcher_action_stream.next(), if !combined_watcher_action_stream.is_empty() => {
                        match watcher_action_result_option {
                            Some(Ok(next_watcher_action)) => {
                                combined_watcher_action_stream_finished = false;

                                let watcher_action_key = next_watcher_action.key();
                                if let Some((watcher_action, delay_queue_key)) = queued_watcher_actions.get_mut(&watcher_action_key) {
                                    watcher_action_delay_queue.reset(&delay_queue_key, watcher_action_debounce_duration);
                                    *watcher_action = next_watcher_action;
                                } else {
                                    let delay_queue_key = watcher_action_delay_queue.insert(watcher_action_key.clone(), watcher_action_debounce_duration);
                                    queued_watcher_actions.insert(watcher_action_key, (next_watcher_action, delay_queue_key));
                                }
                            }
                            Some(Err(FForchaWatcherErrorWithIndex(error, watcher_index))) => {
                                if exit_on_watcher_failure {
                                    error!("Exiting because of watcher error: {error:?}");
                                    break Err(error);
                                }

                                warn!("Restarting watcher because of watcher error: {error:?}");

                                match self.restart_watcher_with_index(watcher_index).await {
                                    Ok(watcher_action_stream) => {
                                        combined_watcher_action_stream.push(watcher_action_stream);
                                        info!("Successfully restarted watcher");
                                    }
                                    Err(error) => {
                                        warn!("Failed to restart watcher with error: {:?}", error);

                                        if always_restart_watchers {
                                            watcher_restart_delay_queue.insert(watcher_index, watcher_restart_delay);
                                        }
                                    }
                                }
                            }
                            None if !combined_watcher_action_stream_finished => {
                                combined_watcher_action_stream_finished = true;
                                if watcher_restart_delay_queue.is_empty() {
                                    info!("All watchers finished, exiting");
                                    break Ok(());
                                }

                                info!("All watchers finished, waiting for restart");
                            }
                            None => {}
                        }
                    }
                }
            }
        });

        Ok((watcher_runner_join_handle, watcher_action_receiver))
    }
}
