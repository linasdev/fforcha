use crate::asset::FForchaAsset;
use nanoid::nanoid;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct FForchaTask {
    id: String,
    input_asset: Arc<dyn FForchaAsset>,
}

impl FForchaTask {
    pub fn new(input_asset: Arc<dyn FForchaAsset>) -> Self {
        Self {
            id: nanoid!(),
            input_asset,
        }
    }

    pub fn id(&self) -> String {
        self.id.clone()
    }

    pub fn input_asset(&self) -> Arc<dyn FForchaAsset> {
        self.input_asset.clone()
    }
}
