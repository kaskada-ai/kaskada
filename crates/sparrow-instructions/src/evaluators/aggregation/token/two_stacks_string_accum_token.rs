use crate::aggregation::two_stacks::TwoStacks;
use crate::AggFn;

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
