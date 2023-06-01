use arrow_schema::SchemaRef;

/// The identifier (index) of a step.

#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
#[serde(transparent)]
pub struct StepId(usize);

impl From<usize> for StepId {
    fn from(value: usize) -> Self {
        StepId(value)
    }
}

/// A single step in the physical plan.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Step {
    pub id: StepId,
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
        /// Expressions to apply to compute additional input columns.
        exprs: Vec<crate::Expr>,
        /// Indices of expressions to use for the output.
        ///
        /// The length should be the same as the number of fields in the schema.
        outputs: Vec<usize>,
    },
    /// Filter the results based on a boolean predicate.
    Filter {
        /// Expressions to apply to compute the predicate.
        ///
        /// The last expression should be the boolean predicate.
        exprs: Vec<crate::Expr>,
    },
    /// A step that repartitions the output.
    Repartition {
        num_partitions: usize,
        /// Expressions to apply to compute columns which may be referenced by `keys`.
        exprs: Vec<crate::Expr>,
        /// Indices of expression columns representing the keys.
        keys: Vec<usize>,
    },
    Error,
}
