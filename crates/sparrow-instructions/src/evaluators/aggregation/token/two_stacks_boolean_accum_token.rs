use crate::aggregation::two_stacks::TwoStacks;
use crate::AggFn;

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
