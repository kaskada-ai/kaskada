use arrow_schema::DataType;
use index_vec::IndexVec;

use crate::{Expr, ExprId};

index_vec::define_index_type! {
    /// The identifier (index) of a step.
    pub struct StepId = u32;

    DISPLAY_FORMAT = "{}";
}

/// A single step in the physical plan.
///
/// Each step corresponds to a specific relational operator.
/// Many [kinds of steps](StepKind) include [expressions](Exprs)
/// to specify columns -- for instance the columns to compute in
/// a projection or the condition to use for a selection.
/// Conceptually, steps describe how batches are produced while
/// expressions describe columns inside a batch.
///
/// During execution, each step receives a partitioned stream
/// of ordered batches and produces a partitioned stream of
/// ordered batches. Steps never operate between partitions --
/// instead, operations like `with_key` for a given partition
/// produce output destined for multiple partitions based
/// on the newly computed keys.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Step {
    /// The ID of the step.
    pub id: StepId,
    /// The kind of step being performed.
    pub kind: StepKind,
    /// Inputs to this step.
    pub inputs: Vec<StepId>,
    /// The data type produced by this step.
    pub result_type: DataType,
    /// Expressions used in the step, if any.
    ///
    /// The final expression in the vector is considered the "result" of executing
    /// the expressions.
    ///
    /// When expressions are executed, what inputs to the expressions are and
    /// how the output is used depends on the StepKinds. See specific StepKinds
    /// for whether expressions are allowed, how they are interpreted, and any
    /// other restrictions.
    pub exprs: IndexVec<ExprId, Expr>,
}

/// The kinds of steps that can occur in the physical plan.
#[derive(
    Clone,
    Debug,
    serde::Serialize,
    serde::Deserialize,
    strum_macros::IntoStaticStr,
    PartialEq,
    Eq,
    Hash,
)]
#[serde(rename_all = "snake_case")]

pub enum StepKind {
    /// Read the given source.
    Read {
        source_id: uuid::Uuid,
    },
    /// Merge the given relations.
    Merge,
    /// Apply stateless projections to columns in the table.
    ///
    /// The output includes the same rows as the input, but with columns
    /// projected as configured.
    ///
    /// Expressions in the step are used to compute the projected columns. The number
    /// of expressions output should be the same as the fields in the step schema.
    Project,
    /// Filter the results based on a boolean predicate.
    ///
    /// Expressions in the step are used to compute the predicate. There should be a
    /// single output producing a boolean value.
    Filter,
    /// A step that repartitions the output.
    ///
    /// Expressions in the step are used to compute the partition keys. Each output
    /// corresponds to a part of the key.
    Repartition {
        num_partitions: usize,
    },
    Error,
}
