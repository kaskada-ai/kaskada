use std::collections::VecDeque;
use std::sync::Arc;

use arrow::array::{new_null_array, Array, ArrayRef, AsArray, PrimitiveArray, UInt32Array};
use arrow::datatypes::ArrowPrimitiveType;
use itertools::izip;
use sparrow_arrow::downcast::downcast_primitive_array;

use crate::{ComputeStore, StateToken, StoreKey};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct LagStructToken {
    /// Lists of structs
    #[serde(with = "sparrow_arrow::serde::array_ref")]
    pub state: ArrayRef,
}

impl StateToken for LagStructToken {
    fn restore(&mut self, key: &StoreKey, store: &ComputeStore) -> anyhow::Result<()> {
        if let Some(token) = store.get(key)? {
            let token: LagStructToken = token;
            self.state = token.state;
        };
        Ok(())
    }

    fn store(&self, key: &StoreKey, store: &ComputeStore) -> anyhow::Result<()> {
        store.put(key, &self)
    }
}

impl LagStructToken {
    pub fn new(state: ArrayRef) -> Self {
        Self { state }
    }

    /// Concat nulls to the end of the current accumulator to grow the size.
    pub fn resize(&mut self, len: usize) -> anyhow::Result<()> {
        let diff = len - self.state.len();

        let null_array = new_null_array(self.state.data_type(), diff);
        let null_array = null_array.as_ref().as_list::<i32>();
        let new_state = arrow::compute::concat(&[&self.state, null_array])?;
        self.state = new_state.clone();
        Ok(())
    }

    pub fn set_state(&mut self, new_state: ArrayRef) {
        self.state = new_state
    }
}
