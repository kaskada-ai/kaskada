use std::sync::Arc;

use crate::StateBackend;

pub struct StateStore {
    backend: Arc<dyn StateBackend>,
}

impl StateStore {
    pub fn new(backend: Arc<dyn StateBackend>) -> Self {
        StateStore { backend }
    }
}

/// All state mutations for a given partition happen as part of a batch.
pub struct StateBatch {
    partition_id: u8,
}

pub struct ValueState<T> {
    value: Option<T>,
}
