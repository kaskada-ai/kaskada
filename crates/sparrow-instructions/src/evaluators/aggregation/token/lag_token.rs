use arrow::datatypes::ArrowPrimitiveType;
use serde::de::DeserializeOwned;
use serde::Serialize;
use sparrow_kernels::lag::LagPrimitive;

use crate::{ComputeStore, StateToken, StoreKey};

/// State token used for the lag operator.
impl<T> StateToken for LagPrimitive<T>
where
    T: ArrowPrimitiveType,
    T::Native: Serialize + DeserializeOwned + Copy,
{
    fn restore(&mut self, key: &StoreKey, store: &ComputeStore) -> anyhow::Result<()> {
        if let Some(state) = store.get(key)? {
            self.state = state;
        } else {
            self.state = vec![];
        }

        Ok(())
    }

    fn store(&self, key: &StoreKey, store: &ComputeStore) -> anyhow::Result<()> {
        store.put(key, &self.state)
    }
}
