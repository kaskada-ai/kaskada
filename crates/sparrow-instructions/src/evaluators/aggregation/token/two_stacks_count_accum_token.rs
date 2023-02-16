use crate::aggregation::two_stacks::TwoStacks;
use crate::{AggFn, ComputeStore, StateToken, StoreKey};

/// Key used for windowed count accumulators using two-stacks
/// implementation.
///
/// Stored as `[passId, instId] -> Vec<TwoStacks<AggF>>`
pub struct TwoStacksCountAccumToken<AggF>
where
    AggF: AggFn,
{
    /// Stores the state.
    accum: Vec<TwoStacks<AggF>>,
}

impl<AggF> StateToken for TwoStacksCountAccumToken<AggF>
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

impl<AggF> TwoStacksCountAccumToken<AggF>
where
    AggF: AggFn,
{
    pub(crate) fn new() -> Self {
        Self { accum: Vec::new() }
    }

    pub(crate) fn resize(&mut self, len: usize, initial_windows: i64) {
        self.accum.resize(len, TwoStacks::new(initial_windows));
    }

    pub(crate) fn get_value(&mut self, entity_index: u32) -> AggF::AccT {
        self.accum[entity_index as usize].accum_value()
    }

    pub(crate) fn add_value(&mut self, entity_index: u32, value: AggF::InT) {
        self.accum[entity_index as usize].add_input(&value)
    }

    pub(crate) fn evict_window(&mut self, entity_index: u32) {
        self.accum[entity_index as usize].evict()
    }
}
