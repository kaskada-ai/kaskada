use arrow_array::ArrayRef;
use sparrow_arrow::Batch;

use crate::values::WorkAreaValue;

/// Information about an in-progress batch used for evaluation.
pub(super) struct WorkArea<'a> {
    pub input: &'a Batch,
    pub expressions: Vec<ArrayRef>,
}

impl<'a> WorkArea<'a> {
    /// Create a work area for processing the given batch.
    ///
    /// Arguments:
    /// - `input`: The [Batch] to process
    /// - `expressions`: The number of expressions processed.
    pub fn with_capacity(input: &'a Batch, expressions: usize) -> Self {
        assert!(!input.is_empty());
        Self {
            input,
            expressions: Vec::with_capacity(expressions),
        }
    }

    /// Return the [ArrayRef] for the given input index.
    pub fn input_column(&self, index: usize) -> &ArrayRef {
        self.input.record_batch().expect("non empty").column(index)
    }

    /// Return the [Value] for the given expression index.
    pub fn expression<R: WorkAreaValue>(&self, index: R) -> R::Array<'_> {
        index.access(&self.expressions)
    }

    /// Return the number of rows in the current work area.
    pub fn num_rows(&self) -> usize {
        self.input.num_rows()
    }
}
