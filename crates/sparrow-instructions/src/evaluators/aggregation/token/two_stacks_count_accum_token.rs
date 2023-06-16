use crate::aggregation::two_stacks::TwoStacks;
use crate::AggFn;

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
