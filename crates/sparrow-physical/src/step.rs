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

impl std::fmt::Display for Step {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            id, kind, inputs, ..
        } = self;
        write!(f, "Step {id}: kind {kind} reads {:?}", inputs)
    }
}

/// The kinds of steps that can occur in the physical plan.
#[derive(
    Clone,
    Copy,
    Debug,
    serde::Serialize,
    serde::Deserialize,
    strum_macros::IntoStaticStr,
    PartialEq,
    Eq,
    Hash,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]

pub enum StepKind {
    /// Read the given source.
    Read {
        source_uuid: uuid::Uuid,
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

impl StepKind {
    /// Return true if the step is implemented as a transform (rather than an operation).
    ///
    /// This should generally be true for steps that take a single input and
    /// effectively perform a "flat map" over the batches. Specifically, they
    /// may change the number of rows in a batch (or omit the batch entirely)
    /// and they may change the columns in the batch but they should not accept
    /// multiple inputs or need to interact with scheduling.
    ///
    /// Examples:
    ///
    /// - `project` is a transform because it computes new columns for each row in
    ///   every batch
    /// - `filter` is a transform because it removes rows from each batch, and omits
    ///   empty batches
    /// - `merge` is not a transform because it accepts multiple inputs
    /// - `shift` depends on how we choose to implement it. If it is implemented as
    ///   a stateful transform that just buffers and emits as processing proceeds
    ///   through time, then it would be a transform. If we find ways to implement
    ///   it more efficiently by implementing the pipeline interface, then it may
    ///   not be a transform.
    pub fn is_transform(&self) -> bool {
        matches!(self, StepKind::Project | StepKind::Filter)
    }
}

impl std::fmt::Display for StepKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StepKind::Read {
                source_uuid: source_id,
            } => write!(f, "read({source_id})"),
            StepKind::Repartition { num_partitions } => write!(f, "repartition({num_partitions})"),
            _ => {
                let name: &'static str = self.into();
                write!(f, "{name}")
            }
        }
    }
}
