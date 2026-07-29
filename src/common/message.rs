use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug)]
#[serde(tag = "type", content = "data", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum FForchaMessage {
    Acknowledgement {
        task_id: Option<String>,
    },
    TaskAssignment {
        task_id: String,
        input_asset_key: String,
    },
    TaskProgress {
        task_id: String,
        progress: f32,
    },
    TaskCompletion {
        task_id: String,
    },
}
