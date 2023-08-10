use std::sync::Arc;

use arrow::array::{new_empty_array, new_null_array, Array, ArrayRef, AsArray};
use arrow_schema::{DataType, Field, TimeUnit};

use crate::{ComputeStore, StateToken, StoreKey};

/// Token used for collecting structs
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct CollectStructToken {
    /// Stores the state for in-memory usage.
    ///
    /// A [ListArray] comprised of lists of structs for each entity.
    #[serde(with = "sparrow_arrow::serde::array_ref")]
    pub state: ArrayRef,
    /// Stores the times of the state values.
    ///
    /// A [ListArray] comprised of lists of timestamps for each entity.
    ///
    /// This array is only used when we have a `trailing` window.
    /// Likely this should be separated into a different implementation.
    #[serde(with = "sparrow_arrow::serde::array_ref")]
    pub times: ArrayRef,
}

impl StateToken for CollectStructToken {
    fn restore(&mut self, key: &StoreKey, store: &ComputeStore) -> anyhow::Result<()> {
        if let Some(state) = store.get(key)? {
            let state: CollectStructToken = state;
            self.state = state.state;
        };

        // TODO: restore times
        panic!("time restoration not implemented")
    }

    fn store(&self, key: &StoreKey, store: &ComputeStore) -> anyhow::Result<()> {
        store.put(key, &self)
    }
}

impl CollectStructToken {
    pub fn new(state: ArrayRef) -> Self {
        let field_ref = Arc::new(Field::new(
            "item",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ));
        let times_type = DataType::List(field_ref);
        Self {
            state,
            times: new_empty_array(&times_type),
        }
    }

    pub fn new_with_time(state: ArrayRef, times: ArrayRef) -> Self {
        Self { state, times }
    }

    pub fn resize(&mut self, len: usize) -> anyhow::Result<()> {
        let diff = len - self.state.len();

        // Resize the state
        let null_array = new_null_array(self.state.data_type(), diff);
        let null_array = null_array.as_ref().as_list::<i32>();
        let new_state = arrow::compute::concat(&[&self.state, null_array])?;
        self.state = new_state.clone();

        // Resize the times
        let null_array = new_null_array(self.times.data_type(), diff);
        let null_array = null_array.as_ref().as_list::<i32>();
        let new_times = arrow::compute::concat(&[&self.times, null_array])?;
        self.times = new_times.clone();

        Ok(())
    }

    pub fn set_state_and_time(&mut self, new_state: ArrayRef, new_times: ArrayRef) {
        self.state = new_state;
        self.times = new_times;
    }

    pub fn set_state(&mut self, new_state: ArrayRef) {
        self.state = new_state;
    }
}
