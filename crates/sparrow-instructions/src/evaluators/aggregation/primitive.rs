//! Primitive aggregation evaluators.

mod arrow_agg_evaluator;
mod max_by_evaluator;
mod two_stacks_arrow_agg_evaluator;

pub use arrow_agg_evaluator::*;
pub use max_by_evaluator::*;
