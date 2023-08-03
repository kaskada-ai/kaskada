use arrow::array::{new_null_array, Array, ArrayRef, AsArray};

use crate::{ComputeStore, StateToken, StoreKey};

/// Token used for collecting structs
#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct CollectStructToken {
    /// Stores the state for in-memory usage.
    ///
    /// Stores a vec of `ListArrays`, where the index of each list corresponds to the
    /// entity index. Each list should contain struct types.
    #[serde(with = "sparrow_arrow::serde::array_ref")]
    pub state: Vec<Option<ArrayRef>>,
}

impl StateToken for CollectStructToken {
    fn restore(&mut self, key: &StoreKey, store: &ComputeStore) -> anyhow::Result<()> {
        if let Some(state) = store.get(key)? {
            let state: CollectStructToken = state;
            self.state = state.state;
        };
        Ok(())
    }

    fn store(&self, key: &StoreKey, store: &ComputeStore) -> anyhow::Result<()> {
        store.put(key, &self)
    }
}

impl CollectStructToken {
    pub fn new(state: Vec<Option<ArrayRef>>) -> Self {
        Self { state }
    }

    pub fn resize(&mut self, len: usize) {
        if len >= self.state.len() {
            self.state.resize(len + 1, None);
        }
    }

    pub fn set_state(&mut self, index: usize, new_state: Option<ArrayRef>) {
        self.state[index] = new_state
    }
}
