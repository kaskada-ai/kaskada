use arrow_schema::SchemaRef;

use crate::Exprs;

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
/// During execution, each step receives an partitioned stream
/// of ordered batches and produces a partitioned stream of
/// ordered batches. Steps never operate between partitions --
/// instead, operations like `with_key` for a given partition
/// produce output to destined for multiple partitions based
/// on the newly computed keys.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Step {
    /// The kind of step being performed.
    pub kind: StepKind,
    /// Inputs to this step.
    pub inputs: Vec<StepId>,
    /// The schema for this step.
    pub schema: SchemaRef,
}

/// The kinds of stesp that can occur in the physical plan.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]

pub enum StepKind {
    /// Scan the given table.
    Scan {
        table_name: String,
    },
    /// Merge the given relations.
    Merge,
    /// Apply a projection to adjust columns in the table.
    ///
    /// The output includes the same rows as the input, but with columns
    /// projected as configured.
    Project {
        /// Expressions to compute the projection.
        ///
        /// The length of the outputs should be the same as the fields in the schema.
        exprs: Exprs,
    },
    /// Filter the results based on a boolean predicate.
    Filter {
        /// Expressions to apply to compute the predicate.
        ///
        /// There should be a single output producing a boolean value.
        exprs: Exprs,
    },
    /// A step that repartitions the output.
    Repartition {
        num_partitions: usize,
        /// Expressions to compute the keys.
        ///
        /// Each output corresponds to a part of the key.
        keys: Exprs,
    },
    Error,
}
