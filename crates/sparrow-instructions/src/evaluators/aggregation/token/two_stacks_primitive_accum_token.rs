use crate::{AggFn, TwoStacks};

/// Key used for windowed primitive accumulators using two-stacks
/// implementation.
///
/// Stored as `[pass_id, inst_id] -> Vec<T>`
///
/// Note that this pattern is intended for use only by primitive
/// accumulators that do not apply significant memory pressure by
/// storing the entire accum in memory. For other aggregations, the
/// initial intention is to read/modify/write each individual value into
/// persistent storage.
pub struct TwoStacksPrimitiveAccumToken<AggF>
where
    AggF: AggFn,
{
    /// Stores the state for in-memory usage.
    accum: Vec<TwoStacks<AggF>>,
}

impl<AggF> TwoStacksPrimitiveAccumToken<AggF>
where
    AggF: AggFn,
{
    pub(crate) fn new() -> Self {
        Self { accum: Vec::new() }
    }

    pub(crate) fn get_primitive_accum(&mut self) -> anyhow::Result<Vec<TwoStacks<AggF>>> {
        Ok(std::mem::take(&mut self.accum))
    }

    pub(crate) fn put_primitive_accum(
        &mut self,
        accum: Vec<TwoStacks<AggF>>,
    ) -> anyhow::Result<()> {
        self.accum = accum;
        Ok(())
    }
}
