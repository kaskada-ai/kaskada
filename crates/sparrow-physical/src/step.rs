use arrow_schema::SchemaRef;

use crate::Exprs;

/// The identifier (index) of a step.

#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
#[serde(transparent)]
pub struct StepId(pub usize);

impl From<usize> for StepId {
    fn from(value: usize) -> Self {
        StepId(value)
    }
}

/// A single step in the physical plan.
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
