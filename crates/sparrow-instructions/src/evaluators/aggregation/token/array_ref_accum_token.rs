use arrow::array::{new_empty_array, new_null_array, Array, ArrayRef};
use arrow_schema::DataType;

use crate::{ComputeStore, StateToken, StoreKey};

/// Token used for accumualting an ArrayRef.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct ArrayRefAccumToken {
    /// Stores the state for in-memory usage.
    #[serde(with = "sparrow_arrow::serde::array_ref")]
    pub accum: ArrayRef,
}

impl StateToken for ArrayRefAccumToken {
    fn restore(&mut self, key: &StoreKey, store: &ComputeStore) -> anyhow::Result<()> {
        if let Some(state) = store.get(key)? {
            let state: ArrayRefAccumToken = state;
            self.accum = state.accum;
        };
        Ok(())
    }

    fn store(&self, key: &StoreKey, store: &ComputeStore) -> anyhow::Result<()> {
        store.put(key, &self)
    }
}

impl ArrayRefAccumToken {
    pub fn new(accum: ArrayRef) -> Self {
        Self { accum }
    }

    pub fn empty(data_type: &DataType) -> Self {
        Self {
            accum: new_empty_array(data_type),
        }
    }

    pub fn array(&self) -> &dyn Array {
        self.accum.as_ref()
    }

    /// Concat nulls to the end of the current accumulator to grow the size.
    pub fn resize(&mut self, len: usize) -> anyhow::Result<()> {
        let diff = len - self.accum.len();

        let null_array = new_null_array(self.accum.data_type(), diff);
        let new_state = arrow::compute::concat(&[&self.accum, null_array.as_ref()])?;
        self.accum = new_state.clone();
        Ok(())
    }

    pub fn value_is_null(&mut self, key: usize) -> bool {
        self.accum.is_null(key)
    }

    pub fn set_state(&mut self, new_state: ArrayRef) {
        self.accum = new_state
    }
}
