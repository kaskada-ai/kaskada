use crate::aggregation::two_stacks::TwoStacks;
use crate::{AggFn, ComputeStore, StateToken, StoreKey};

/// Key used for windowed string accumulators using two-stacks
/// implementation.
///
/// Stored as `[passId, instId, entity_index] -> TwoStacks<AggF>`
pub struct TwoStacksStringAccumToken<AggF>
where
    AggF: AggFn,
{
    /// Stores the state.
    accum: Vec<TwoStacks<AggF>>,
}

impl<AggF> StateToken for TwoStacksStringAccumToken<AggF>
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

impl<AggF> TwoStacksStringAccumToken<AggF>
where
    AggF: AggFn,
{
    pub(crate) fn new() -> Self {
        Self { accum: Vec::new() }
    }

    pub(crate) fn resize(&mut self, len: usize, initial_windows: i64) {
        self.accum.resize(len, TwoStacks::new(initial_windows));
    }

    pub(crate) fn get_value(&mut self, key: u32) -> anyhow::Result<Option<TwoStacks<AggF>>> {
        Ok(Some(self.accum[key as usize].clone()))
    }

    pub(crate) fn put_value(&mut self, key: u32, input: TwoStacks<AggF>) -> anyhow::Result<()> {
        self.accum[key as usize] = input;
        Ok(())
    }
}
