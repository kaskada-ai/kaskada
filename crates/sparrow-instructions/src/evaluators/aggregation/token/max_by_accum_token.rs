use std::marker::PhantomData;

use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{ComputeStore, StateToken, StoreKey};

/// Token used for max by accumulators.
///
/// Max By tokens store the max measured value and the output value
/// associated with the max measured value.
#[derive(Default)]
pub struct MaxByAccumToken<T1, T2> {
    /// Stores the state of the max measured value.
    measured: Vec<Option<T1>>,
    /// Stores the state of the values associated with the max measured value.
    values: Vec<Option<T2>>,
    // _phantom: PhantomData<fn(T) -> T>,
}

impl<T1, T2> StateToken for MaxByAccumToken<T1, T2>
where
    T1: Serialize + DeserializeOwned,
    T2: Serialize + DeserializeOwned,
{
    fn restore(&mut self, key: &StoreKey, store: &ComputeStore) -> anyhow::Result<()> {
        store.get_to_vec(key, &mut self.measured)
        // TODO: Add extension for getting values
        // store.get_to_vec(key, &mut self.values)
    }

    fn store(&self, key: &StoreKey, store: &ComputeStore) -> anyhow::Result<()> {
        store.put(key, &self.measured)
        // TODO: Add extension for storing values
    }
}

impl<T1, T2> MaxByAccumToken<T1, T2>
where
    T1: Clone,
    T2: Clone,
{
    pub fn resize(&mut self, len: usize) {
        self.measured.resize(len, None);
        self.values.resize(len, None);
    }

    pub fn get_value(&mut self, key: u32) -> anyhow::Result<Option<T2>> {
        Ok(self.values[key as usize].clone())
    }

    pub fn put_measured(&mut self, key: u32, measure: Option<T1>) -> anyhow::Result<()> {
        self.measured[key as usize] = measure;
        Ok(())
    }

    pub fn put_output_value(&mut self, key: u32, value: Option<T2>) -> anyhow::Result<()> {
        self.values[key as usize] = value;
        Ok(())
    }
}
