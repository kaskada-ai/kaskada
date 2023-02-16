use anyhow::Context;
use sparrow_core::debug_println;

use crate::dfg::{DfgExpr, Operation, StepKind};

/// Wrapped vector containing the index of the operation each node is in.
///
/// This uses the information already present in the DFG associating each
/// expression node with a corresponding operation node to determine the
/// operation each expression is in.
///
/// The result allows determining for each node ID (whether it is an
/// operation or an expression) the index of the operation plan the node
/// is executed in.
///
/// One tricky detail is that some expression may be associated with the
/// `Operation::None` which indicates literal (or late bound) operations.
/// Such expressions should not be directly planned -- instead they are
/// transformed to an actual operation first.
pub(super) struct OperationSchedule {
    // The operation assigned to each node.
    operation_for_node: Vec<Option<u32>>,
    num_operations: usize,
}

impl OperationSchedule {
    pub fn try_new(expr: &DfgExpr) -> anyhow::Result<Self> {
        let mut next_operation = 0;

        let mut operation_for_node = Vec::with_capacity(expr.len());
        for id in expr.ids() {
            let (kind, children) = expr.node(id);
            let operation = match kind {
                StepKind::Operation(Operation::Empty) => None,
                StepKind::Operation(_) => {
                    // Operations are scheduled for their own operation.
                    // Since they are already topologically sorted, we just
                    // put them in the next operation.
                    let assigned_operation = next_operation;
                    next_operation += 1;
                    Some(assigned_operation)
                }

                StepKind::Expression(_) | StepKind::Transform | StepKind::Window(_) => {
                    // Expressions and transforms include the operation they are in as their last
                    // argument. So, we just need to find that ID and figure out
                    // which operation it is scheduled in.
                    let operation = *children.last().context("expression must have one arg")?;

                    // Get the operation index that the referenced operation was assigned to.
                    // This will be the operation of this expression node.
                    *operation_for_node
                        .get(usize::from(operation))
                        .context("undefined operation")?
                }
                StepKind::Error => anyhow::bail!("Unexpected error while creating plan"),
            };

            debug_println!(
                crate::plan::DBG_PRINT_PLAN,
                "Scheduled {id:?}={kind:?}({children:?} for operation {operation:?}"
            );
            operation_for_node.push(operation);
        }

        Ok(Self {
            operation_for_node,
            num_operations: next_operation as usize,
        })
    }

    pub fn num_operations(&self) -> usize {
        self.num_operations
    }

    /// Return the operation this expression is scheduled in.
    ///
    /// Returns `None` for literals and other expressions that are
    /// outside of a specific operation.
    pub fn operation_opt(&self, id: egg::Id) -> Option<u32> {
        self.operation_for_node[usize::from(id)]
    }

    /// Return the operation this expression is scheduled in.
    ///
    /// Returns `Err` for literals and other expressions that are
    /// outside of a specific operation.
    pub fn operation(&self, id: egg::Id) -> anyhow::Result<u32> {
        self.operation_opt(id).context("unexpected literal")
    }
}
