use sparrow_api::kaskada::v1alpha::PerEntityBehavior;
use sparrow_instructions::{InstKind, InstOp};

use crate::dfg::{DfgExpr, Expression, Operation, StepKind};
use crate::CompilerOptions;

/// Return `true` if incremental should be enabled for the given query.
///
/// This currently depends on the `experimental` options.
#[allow(clippy::if_same_then_else)]
pub(super) fn is_incremental_enabled(dfg: &DfgExpr, options: &CompilerOptions) -> bool {
    if !options.experimental {
        // Incremental is currently experimental (opt-in)
        false
    } else if options.per_entity_behavior != PerEntityBehavior::Final {
        // Only "final" results are currently supported by incremental.
        // All results *could* be supported if there is a changed since time
        // specified.
        false
    } else {
        dfg.ids()
            .map(|id| dfg.kind(id))
            .all(is_incremental_supported)
    }
}

fn is_incremental_supported(step: &StepKind) -> bool {
    match step {
        StepKind::Operation(Operation::Empty | Operation::MergeJoin) => true,
        StepKind::Expression(Expression::Inst(InstKind::Simple(InstOp::Collect))) => true,
        StepKind::Operation(Operation::Scan { .. } | Operation::Select | Operation::Tick(_)) => {
            true
        }
        StepKind::Expression(Expression::Literal(_) | Expression::LateBound(_)) => true,
        StepKind::Expression(Expression::Inst(_)) => true,
        StepKind::Transform => true,
        StepKind::Window(_) => true,
        StepKind::Operation(Operation::WithKey) => true,
        StepKind::Operation(Operation::ShiftTo) => true,
        StepKind::Operation(Operation::ShiftUntil) => true,
        StepKind::Operation(Operation::LookupRequest | Operation::LookupResponse) => true,
        StepKind::Error => false,
    }
}
