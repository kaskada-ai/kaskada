use sparrow_api::kaskada::v1alpha::operation_input_ref::Interpolation;
use sparrow_instructions::{InstKind, InstOp};

use crate::dfg::{DfgExpr, Expression, Operation, StepKind};

/// Information about how each node needs to be interpolated.
pub(super) struct Interpolations {
    /// The interpolation for each node in a DFG expression.
    ///
    /// The interpolation for a node with a given `egg::Id`
    /// will be stored in this vector at `usize::from(id)`.
    ///
    /// Specifically, the length of this vector is the same
    /// as the length of the DFG expression.
    node_interpolations: Vec<Interpolation>,
}

impl Interpolations {
    pub fn try_new(expr: &DfgExpr) -> anyhow::Result<Self> {
        let mut node_interpolations = Vec::with_capacity(expr.len());
        for id in expr.ids() {
            let (kind, children) = expr.node(id);
            let interpolation = match kind {
                StepKind::Operation(Operation::Empty) => Interpolation::Null,
                StepKind::Operation(Operation::Scan { .. }) => Interpolation::Null,
                StepKind::Operation(
                    Operation::MergeJoin
                    | Operation::LookupRequest
                    | Operation::LookupResponse
                    | Operation::WithKey,
                ) => {
                    // The result of merge join, lookup or with-key is treated as "as-of".
                    // When a value is transformed to the joined domain it will be
                    // as-of if the value is as-of, and `null` if the value is `null`.
                    Interpolation::AsOf
                }
                StepKind::Operation(
                    Operation::Select | Operation::ShiftTo | Operation::ShiftUntil,
                ) => {
                    // Any result transformed to a select/shift-to/shift-until becomes discrete
                    // at the specific times selected/shifted to.
                    Interpolation::Null
                }
                StepKind::Operation(Operation::Tick(_)) => {
                    // Ticks are similar to merges.
                    Interpolation::Null
                }
                StepKind::Expression(Expression::Literal(_) | Expression::LateBound(_)) => {
                    // Literals and late bounds values are interpolated, as they should
                    // always be able to be present in subsequent instructions.
                    Interpolation::AsOf
                }
                // TODO: `collect` should be in it's own special grouping,
                // or we should just start calling it an aggregation everywhere.
                StepKind::Expression(Expression::Inst(InstKind::Simple(inst)))
                    if inst.is_aggregation() || inst.name() == "collect" =>
                {
                    Interpolation::AsOf
                }
                StepKind::Expression(Expression::Inst(InstKind::Simple(InstOp::TimeOf))) => {
                    // TimeOf should always produce a discrete value
                    Interpolation::Null
                }
                StepKind::Expression(_) => {
                    // Expressions skip the operations (last child) when inferring interpolation
                    // as they occur after the operation is applied.
                    infer_interpolation(&node_interpolations, &children[0..children.len() - 1])
                }
                StepKind::Transform => {
                    // Transforms include the operation since they apply the operation to the input.
                    // e.g. (transform value select_op) - if `value` is `as-of`, the
                    // interpolation of the result is still `null` due to `select_op` being
                    // `null`.
                    infer_interpolation(&node_interpolations, children)
                }
                StepKind::Window(_) => {
                    anyhow::bail!("Window arguments should be flattened in the DFG")
                }
                StepKind::Error => {
                    anyhow::bail!("Encountered error node in DFG while creating plan")
                }
            };
            node_interpolations.push(interpolation);
        }

        Ok(Self {
            node_interpolations,
        })
    }

    pub fn interpolation(&self, id: egg::Id) -> Interpolation {
        self.node_interpolations[usize::from(id)]
    }
}

fn infer_interpolation(
    node_interpolations: &[Interpolation],
    children: &[egg::Id],
) -> Interpolation {
    let mut interpolations = children
        .iter()
        .map(|child| node_interpolations[usize::from(*child)]);
    if interpolations.all(|i| i == Interpolation::AsOf) {
        Interpolation::AsOf
    } else {
        Interpolation::Null
    }
}
