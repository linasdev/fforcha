use crate::asset::FForchaAsset;
use crate::common::task::FForchaTask;
use log::{debug, warn};
use scc::HashMap;
use std::collections::BTreeSet;
use std::fmt;
use std::fmt::Debug;
use std::ops::DerefMut;
use std::sync::Arc;
use tokio::select;
use tokio::sync::{Mutex, Semaphore, mpsc, oneshot};

pub mod runner;

pub struct FForchaServerQueue {
    asset_key_to_task_ids: HashMap<String, Vec<String>>,
    task_id_to_asset_key: HashMap<String, String>,
    tasks: HashMap<String, FForchaTask>,
    cancel_senders: HashMap<String, oneshot::Sender<()>>,
    requeue_sender: mpsc::UnboundedSender<String>,
    requeue_receiver: Arc<Mutex<mpsc::UnboundedReceiver<String>>>,
    available_task_ids: Arc<Mutex<BTreeSet<String>>>,
    semaphore: Semaphore,
}

impl FForchaServerQueue {
    pub fn new() -> Self {
        let (requeue_sender, requeue_receiver) = mpsc::unbounded_channel();

        Self {
            asset_key_to_task_ids: HashMap::new(),
            task_id_to_asset_key: HashMap::new(),
            tasks: HashMap::new(),
            cancel_senders: HashMap::new(),
            requeue_sender,
            requeue_receiver: Arc::new(Mutex::new(requeue_receiver)),
            available_task_ids: Arc::new(Mutex::new(BTreeSet::new())),
            semaphore: Semaphore::new(0),
        }
    }

    pub async fn push(&self, task: FForchaTask) {
        let asset_key = task.input_asset().key();
        let task_id = task.id();

        if let Err(_) = self.tasks.insert_async(task_id.clone(), task).await {
            warn!(
                "Skipping queueing duplicate task in server queue (task_id: {task_id}, asset_key: {asset_key})"
            );
            return;
        }

        self.asset_key_to_task_ids
            .entry_async(asset_key.clone())
            .await
            .or_insert_with(|| vec![])
            .push(task_id.clone());

        self.task_id_to_asset_key
            .upsert_async(task_id.clone(), asset_key.clone())
            .await;

        self.available_task_ids.lock().await.insert(task_id.clone());
        self.semaphore.add_permits(1);

        debug!("Pushed task to server queue (task_id: {task_id}, asset_key: {asset_key})");
    }

    pub async fn remove(&self, task_id: String) {
        let existed = self.available_task_ids.lock().await.remove(&task_id);
        if existed {
            if let Ok(permit) = self.semaphore.try_acquire() {
                permit.forget();
            }

            if let Some((_, task)) = self.tasks.remove_async(&task_id).await {
                debug!(
                    "Removed still-available task from server queue (task_id: {task_id}, asset_key: {})",
                    task.input_asset().key()
                );
            }

            self.clean_up_maps_for_task(&task_id).await;
            return;
        }

        if let Some((_, cancel_sender)) = self.cancel_senders.remove_async(&task_id).await {
            if cancel_sender.send(()).is_err() {
                warn!("Failed to send cancel signal across channel");
            }
        }

        if let Some((_, task)) = self.tasks.remove_async(&task_id).await {
            debug!(
                "Removed in-progress task from server queue (task_id: {task_id}, asset_key: {})",
                task.input_asset().key()
            );
        }

        self.clean_up_maps_for_task(&task_id).await;
    }

    pub async fn remove_by_input_asset_key(&self, input_asset_key: String) {
        debug!("Removing all tasks with asset key: {input_asset_key}");

        if let Some((_, task_ids)) = self
            .asset_key_to_task_ids
            .remove_async(&input_asset_key)
            .await
        {
            for task_id in task_ids.into_iter() {
                self.remove(task_id).await;
            }
        }
    }

    pub async fn start_pop(&self) -> FForchaServerQueueTaskPermit {
        loop {
            let mut re_queued = false;
            let task_id = {
                let mut requeue_receiver = self.requeue_receiver.lock().await;

                select! {
                    biased;

                    Some(task_id) = requeue_receiver.recv() => {
                        re_queued = true;
                        self.cancel_senders.remove_async(&task_id).await;
                        task_id
                    }
                    semaphore_result = self.semaphore.acquire() => {
                        semaphore_result.expect("Failed to acquire semaphore").forget();

                        match self.available_task_ids.lock().await.pop_first() {
                            Some(task_id) => task_id,
                            None => continue,
                        }
                    }
                }
            };

            if let Some(task) = self.tasks.get_async(&task_id).await {
                let asset_key = task.input_asset().key();
                if re_queued {
                    debug!(
                        "Re-queued task to server queue (task_id: {task_id}, asset_key: {asset_key})"
                    );
                }

                let (cancel_sender, cancel_receiver) = oneshot::channel();

                if let Some(cancel_sender) = self
                    .cancel_senders
                    .upsert_async(task_id.clone(), cancel_sender)
                    .await
                {
                    if cancel_sender.send(()).is_err() {
                        warn!("Failed to send cancel signal across channel");
                    }
                }

                debug!(
                    "Started popping task from server queue (task_id: {task_id}, asset_key: {asset_key})"
                );
                break FForchaServerQueueTaskPermit {
                    completed: false,
                    task: task.clone(),
                    cancel_receiver,
                    requeue_sender: self.requeue_sender.clone(),
                };
            }
        }
    }

    pub async fn complete_pop(&self, mut permit: FForchaServerQueueTaskPermit) {
        let task_id = permit.task.id();
        self.cancel_senders.remove_async(&task_id).await;

        if let Some((_, task)) = self.tasks.remove_async(&task_id).await {
            debug!(
                "Popped task from server queue (task_id: {task_id}, asset_key: {})",
                task.input_asset().key()
            );
        }

        self.clean_up_maps_for_task(&task_id).await;

        permit.completed = true;
    }

    async fn clean_up_maps_for_task(&self, task_id: &str) {
        if let Some((_, asset_key)) = self.task_id_to_asset_key.remove_async(task_id).await {
            if let Some(mut task_ids) = self.asset_key_to_task_ids.get_async(&asset_key).await {
                task_ids
                    .deref_mut()
                    .retain(|current_task_id| current_task_id != &task_id);

                if task_ids.is_empty() {
                    let _ = task_ids.remove_entry();
                }
            }
        }
    }
}

pub struct FForchaServerQueueTaskPermit {
    completed: bool,
    task: FForchaTask,
    cancel_receiver: oneshot::Receiver<()>,
    requeue_sender: mpsc::UnboundedSender<String>,
}

impl FForchaServerQueueTaskPermit {
    pub fn task(&self) -> &FForchaTask {
        &self.task
    }

    pub fn task_and_cancel_receiver(&mut self) -> (&FForchaTask, &mut oneshot::Receiver<()>) {
        (&self.task, &mut self.cancel_receiver)
    }
}

impl Drop for FForchaServerQueueTaskPermit {
    fn drop(&mut self) {
        if !self.completed && self.requeue_sender.send(self.task.id()).is_err() {
            warn!("Failed to send requeue signal across channel");
        }
    }
}

impl Debug for FForchaServerQueueTaskPermit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FForchaServerQueueTaskPermit")
            .field("task", &self.task)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::asset::file::FForchaFileAsset;
    use googletest::prelude::*;
    use nanoid::nanoid;
    use std::hash::Hash;
    use std::ops::Deref;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_push_task_to_queue_when_queue_is_empty() {
        let target = FForchaServerQueue::new();

        let input_asset = FForchaFileAsset::new("/path/to/file.ext".into());
        let task = FForchaTask::new(Arc::new(input_asset));

        target.push(task.clone()).await;

        let asset_key_to_task_ids = get_vec_of_hash_map(&target.asset_key_to_task_ids);
        let task_id_to_asset_key = get_vec_of_hash_map(&target.task_id_to_asset_key);
        let tasks = get_vec_of_hash_map(&target.tasks);
        assert_that!(
            asset_key_to_task_ids,
            unordered_elements_are![(eq("file:///path/to/file.ext"), elements_are![&task.id()])]
        );
        assert_that!(
            task_id_to_asset_key,
            unordered_elements_are![(eq(&task.id()), eq("file:///path/to/file.ext"))]
        );
        assert_that!(
            tasks,
            unordered_elements_are![(
                eq(&task.id()),
                predicate(|t: &FForchaTask| {
                    t.id() == task.id() && t.input_asset().key() == "file:///path/to/file.ext"
                })
            ),]
        );
        assert_that!(target.cancel_senders.len(), eq(0));
        assert_that!(
            target.available_task_ids.lock().await.deref(),
            unordered_elements_are![eq(&task.id())]
        );
        assert_that!(target.semaphore.available_permits(), eq(1));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_push_task_to_queue_when_queue_is_not_empty() {
        let queued_input_asset = FForchaFileAsset::new("/path/to/queued-file.ext".into());
        let queued_task = FForchaTask::new(Arc::new(queued_input_asset));
        let target = FForchaServerQueue::new();
        target.push(queued_task.clone()).await;

        let input_asset = FForchaFileAsset::new("/path/to/file.ext".into());
        let task = FForchaTask::new(Arc::new(input_asset));

        target.push(task.clone()).await;

        let asset_key_to_task_ids = get_vec_of_hash_map(&target.asset_key_to_task_ids);
        let task_id_to_asset_key = get_vec_of_hash_map(&target.task_id_to_asset_key);
        let tasks = get_vec_of_hash_map(&target.tasks);
        assert_that!(
            asset_key_to_task_ids,
            unordered_elements_are![
                (
                    eq("file:///path/to/queued-file.ext"),
                    elements_are![&queued_task.id()]
                ),
                (eq("file:///path/to/file.ext"), elements_are![&task.id()]),
            ]
        );
        assert_that!(
            task_id_to_asset_key,
            unordered_elements_are![
                (eq(&queued_task.id()), eq("file:///path/to/queued-file.ext")),
                (eq(&task.id()), eq("file:///path/to/file.ext")),
            ]
        );
        assert_that!(
            tasks,
            unordered_elements_are![
                (
                    eq(&queued_task.id()),
                    predicate(|t: &FForchaTask| {
                        t.id() == queued_task.id()
                            && t.input_asset().key() == "file:///path/to/queued-file.ext"
                    })
                ),
                (
                    eq(&task.id()),
                    predicate(|t: &FForchaTask| {
                        t.id() == task.id() && t.input_asset().key() == "file:///path/to/file.ext"
                    })
                ),
            ]
        );
        assert_that!(target.cancel_senders.len(), eq(0));
        assert_that!(
            target.available_task_ids.lock().await.deref(),
            unordered_elements_are![eq(&queued_task.id()), eq(&task.id())]
        );
        assert_that!(target.semaphore.available_permits(), eq(2));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_remove_task_from_queue_when_task_exists_and_is_not_popped() {
        let queued_input_asset = FForchaFileAsset::new("/path/to/queued-file.ext".into());
        let queued_task = FForchaTask::new(Arc::new(queued_input_asset));
        let target = FForchaServerQueue::new();
        target.push(queued_task.clone()).await;

        target.remove(queued_task.id()).await;

        let asset_key_to_task_ids = get_vec_of_hash_map(&target.asset_key_to_task_ids);
        let task_id_to_asset_key = get_vec_of_hash_map(&target.task_id_to_asset_key);
        let tasks = get_vec_of_hash_map(&target.tasks);
        assert_that!(asset_key_to_task_ids, is_empty());
        assert_that!(task_id_to_asset_key, is_empty());
        assert_that!(tasks, is_empty());
        assert_that!(target.cancel_senders.len(), eq(0));
        assert_that!(target.available_task_ids.lock().await.deref(), is_empty());
        assert_that!(target.semaphore.available_permits(), eq(0));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_remove_task_from_queue_when_task_exists_and_is_popped() {
        let queued_input_asset = FForchaFileAsset::new("/path/to/queued-file.ext".into());
        let queued_task = FForchaTask::new(Arc::new(queued_input_asset));
        let target = FForchaServerQueue::new();
        target.push(queued_task.clone()).await;
        let mut permit = target.start_pop().await;
        let join_handle = tokio::spawn(async move {
            let (_, cancel_receiver) = permit.task_and_cancel_receiver();
            cancel_receiver
                .await
                .expect("Failed to receive cancel signal");
        });

        target.remove(queued_task.id()).await;

        let asset_key_to_task_ids = get_vec_of_hash_map(&target.asset_key_to_task_ids);
        let task_id_to_asset_key = get_vec_of_hash_map(&target.task_id_to_asset_key);
        let tasks = get_vec_of_hash_map(&target.tasks);
        assert_that!(asset_key_to_task_ids, is_empty());
        assert_that!(task_id_to_asset_key, is_empty());
        assert_that!(tasks, is_empty());
        assert_that!(target.cancel_senders.len(), eq(0));
        assert_that!(target.available_task_ids.lock().await.deref(), is_empty());
        assert_that!(target.semaphore.available_permits(), eq(0));

        join_handle.await.expect("Failed to join task");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_not_remove_task_from_queue_when_task_does_not_exist() {
        let target = FForchaServerQueue::new();

        target.remove(nanoid!()).await;

        let asset_key_to_task_ids = get_vec_of_hash_map(&target.asset_key_to_task_ids);
        let task_id_to_asset_key = get_vec_of_hash_map(&target.task_id_to_asset_key);
        let tasks = get_vec_of_hash_map(&target.tasks);
        assert_that!(asset_key_to_task_ids, is_empty());
        assert_that!(task_id_to_asset_key, is_empty());
        assert_that!(tasks, is_empty());
        assert_that!(target.cancel_senders.len(), eq(0));
        assert_that!(target.available_task_ids.lock().await.deref(), is_empty());
        assert_that!(target.semaphore.available_permits(), eq(0));
    }

    fn get_vec_of_hash_map<K: Eq + Hash + Clone + Debug, V: Clone + Debug>(
        map: &HashMap<K, V>,
    ) -> Vec<(K, V)> {
        let mut drained = vec![];

        map.retain_sync(|key, value| {
            drained.push((key.clone(), value.clone()));
            false
        });

        drained
    }
}
