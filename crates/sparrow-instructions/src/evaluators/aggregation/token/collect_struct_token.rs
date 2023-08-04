use std::collections::{BTreeMap, VecDeque};

use arrow::array::{new_null_array, Array, ArrayRef, AsArray};
use hashbrown::HashMap;

use crate::{ComputeStore, StateToken, StoreKey};

/// Token used for collecting structs
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct CollectStructToken {
    /// Stores the state for in-memory usage.
    ///
    /// A [ListArray] comprised of lists of structs for each entity.
    #[serde(with = "sparrow_arrow::serde::array_ref")]
    pub state: ArrayRef,
    /// TODO: does this make sense
    /// This is internally mutated, is that okay?
    pub entity_take_indices: BTreeMap<u32, VecDeque<u32>>,
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
    pub fn new(state: ArrayRef) -> Self {
        Self {
            state,
            // entity_take_indices: HashMap::new(),
            entity_take_indices: BTreeMap::new(),
        }
    }

    pub fn resize(&mut self, len: usize) -> anyhow::Result<()> {
        let diff = len - self.state.len();

        let null_array = new_null_array(self.state.data_type(), diff);
        let null_array = null_array.as_ref().as_list::<i32>();
        let new_state = arrow::compute::concat(&[&self.state, null_array])?;
        self.state = new_state.clone();
        Ok(())
    }

    pub fn set_state(&mut self, new_state: ArrayRef) {
        self.state = new_state;
    }
}
