use arrow_array::ArrayRef;

use crate::work_area::WorkArea;
use crate::Error;

/// Trait for evaluating an individual expression node.
pub(super) trait Evaluator: Send + Sync {
    /// Evaluate the function with the given runtime info.
    fn evaluate(&self, work_area: &WorkArea<'_>) -> error_stack::Result<ArrayRef, Error>;
}
