use std::marker::PhantomData;

use crate::{ComputeStore, StateToken, StoreKey};

/// Token used for primitive accumulators.
///
/// Primitive accumulators are stored as `[passId, instId] -> vec<T>`
/// where the index of `T` indicates the entity indices.
///
/// Note that this pattern is intended for use only by primitive
/// accumulators that do not apply significant memory pressure by
/// storing the entire accum in memory. For other aggregations, the
/// initial intention is to read/modify/write each individual value into
/// persistent storage.
#[derive(Default)]
pub struct PrimitiveAccumToken<T> {
    /// Stores the state for in-memory usage.
    accum: Vec<T>,

    _phantom: PhantomData<fn(T) -> T>,
}

impl<T> StateToken for PrimitiveAccumToken<T>
where
    Vec<T>: serde::ser::Serialize + serde::de::DeserializeOwned + std::fmt::Debug,
{
    fn restore(&mut self, key: &StoreKey, store: &ComputeStore) -> anyhow::Result<()> {
        store.get_to_vec(key, &mut self.accum)
    }

    fn store(&self, key: &StoreKey, store: &ComputeStore) -> anyhow::Result<()> {
        store.put(key, &self.accum)
    }
}

impl<T> PrimitiveAccumToken<T> {
    pub fn get_primitive_accum(&mut self) -> anyhow::Result<Vec<T>> {
        Ok(std::mem::take(&mut self.accum))
    }

    pub fn put_primitive_accum(&mut self, accum: Vec<T>) -> anyhow::Result<()> {
        self.accum = accum;
        Ok(())
    }
}
