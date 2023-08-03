use arrow::array::{new_null_array, Array, ArrayRef, AsArray, MapArray};

use crate::{ComputeStore, StateToken, StoreKey};

/// Token used for map accumulators
///
/// Map accumulators are serialized as [ArrayRef], working directly with
/// Arrow.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct ListAccumToken {
    /// Stores the state for in-memory usage.
    #[serde(with = "sparrow_arrow::serde::array_ref")]
    pub accum: ArrayRef,
}

impl StateToken for ListAccumToken {
    fn restore(&mut self, key: &StoreKey, store: &ComputeStore) -> anyhow::Result<()> {
        if let Some(state) = store.get(key)? {
            let state: ListAccumToken = state;
            self.accum = state.accum;
        };
        Ok(())
    }

    fn store(&self, key: &StoreKey, store: &ComputeStore) -> anyhow::Result<()> {
        store.put(key, &self)
    }
}

impl ListAccumToken {
    pub fn new(accum: ArrayRef) -> Self {
        Self { accum }
    }

    pub fn array(&self) -> &MapArray {
        self.accum.as_map()
    }

    /// Concat nulls to the end of the current accumulator to grow the size.
    pub fn resize(&mut self, len: usize) -> anyhow::Result<()> {
        let diff = len - self.accum.len();

        let null_array = new_null_array(self.accum.data_type(), diff);
        let null_array = null_array.as_ref().as_list::<i32>();
        let new_state = arrow::compute::concat(&[&self.accum, null_array])?;
        self.accum = new_state.clone();
        Ok(())
    }

    pub fn value_is_null(&mut self, key: u32) -> bool {
        self.accum.is_null(key as usize)
    }

    pub fn set_state(&mut self, new_state: ArrayRef) {
        self.accum = new_state
    }
}
