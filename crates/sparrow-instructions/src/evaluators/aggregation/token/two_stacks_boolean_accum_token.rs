use crate::aggregation::two_stacks::TwoStacks;
use crate::{AggFn, ComputeStore, StateToken, StoreKey};

/// Key used for windowed boolean accumulators using two-stacks
/// implementation.
///
/// Stored as `[passId, instId, entity_index] -> TwoStacks<AggF>`
pub struct TwoStacksBooleanAccumToken<AggF>
where
    AggF: AggFn,
{
    /// Stores the state for in-memory usage.
    accum: Vec<TwoStacks<AggF>>,
}

impl<AggF> StateToken for TwoStacksBooleanAccumToken<AggF>
where
    AggF: AggFn,
    Vec<TwoStacks<AggF>>: serde::ser::Serialize + serde::de::DeserializeOwned,
{
    fn restore(&mut self, key: &StoreKey, store: &ComputeStore) -> anyhow::Result<()> {
        store.get_to_vec(key, &mut self.accum)
    }

    fn store(&self, key: &StoreKey, store: &ComputeStore) -> anyhow::Result<()> {
        store.put(key, &self.accum)
    }
}

impl<AggF> TwoStacksBooleanAccumToken<AggF>
where
    AggF: AggFn,
{
    pub(crate) fn new() -> Self {
        Self { accum: Vec::new() }
    }

    pub(crate) fn get_boolean_accum(&mut self) -> anyhow::Result<Vec<TwoStacks<AggF>>> {
        Ok(std::mem::take(&mut self.accum))
    }

    pub(crate) fn put_boolean_accum(&mut self, accum: Vec<TwoStacks<AggF>>) -> anyhow::Result<()> {
        self.accum = accum;
        Ok(())
    }
}
