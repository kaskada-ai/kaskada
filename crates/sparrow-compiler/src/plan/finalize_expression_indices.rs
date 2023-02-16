use anyhow::Context;
use sparrow_api::kaskada::v1alpha::{expression_plan, OperationInputRef, OperationPlan};

/// Finalize operation inputs within each operation.
///
/// During creation, the indices are set to refer to operations and expressions
/// by absolute index (within all operations and all expressions within each
/// operation). For execution, we often want "relative" indices. For an
/// expression, this means determining the index within the "expressions output
/// by the producing operation" rather than all expressions within the producing
/// operation. This relative index cannot be determined until the entire plan
/// has been completed, since expressions may be marked as output by later parts
/// of builing the plan, which would require adjusting all indices.
pub(super) fn finalize_expression_indices(
    mut operations: Vec<OperationPlan>,
) -> anyhow::Result<Vec<OperationPlan>> {
    // First, build a lookup vector for each operation, mapping index to
    // output-index.
    let finalizer = ExpressionIndexFinalizer::new(&operations);

    // Then, update the `OperationInput` expressions in each operation.
    for (op_index, operation) in operations.iter_mut().enumerate() {
        let operator = operation.operator.as_mut().context("missing operator")?;

        // Fill in the output index within operators.
        for input_index in 0..operator.operation_input_ref_len() {
            let operation_input_ref = operator.operation_input_ref_mut(input_index)?;
            finalizer
                .finalize_input_ref(operation_input_ref)
                .with_context(|| {
                    format!("Finalizing input {input_index} of operation {op_index}")
                })?;
        }

        // Fill in the output index with "input" expressions
        for (expr_index, expression) in operation.expressions.iter_mut().enumerate() {
            let input = expression.operator.as_mut().context("missing operator")?;
            if let expression_plan::Operator::Input(input) = input {
                finalizer.finalize_input_ref(input).with_context(|| {
                    format!("Finalizing expression {expr_index} of operation {op_index}")
                })?;
            }
        }
    }

    Ok(operations)
}

/// Helper for finalizing the operation and expression indices in the plan.
///
/// This has to happen *after* the plan is completed since the index of an
/// output expression may change if earlier expressions are marked as output.
struct ExpressionIndexFinalizer {
    operation_output_indices: Vec<Vec<Option<u32>>>,
}

impl ExpressionIndexFinalizer {
    fn new(operations: &[OperationPlan]) -> Self {
        let operation_output_indices: Vec<Vec<Option<u32>>> = operations
            .iter()
            .map(|operation| {
                let mut next_output = 0;
                operation
                    .expressions
                    .iter()
                    .map(|expression| {
                        if expression.output {
                            let index = next_output;
                            next_output += 1;
                            Some(index)
                        } else {
                            None
                        }
                    })
                    .collect()
            })
            .collect();
        Self {
            operation_output_indices,
        }
    }

    fn finalize_input_ref(&self, input: &mut OperationInputRef) -> anyhow::Result<()> {
        use sparrow_api::kaskada::v1alpha::operation_input_ref::{Column, KeyColumn};
        input.input_column = match input.column.as_ref() {
            Some(Column::KeyColumn(key_column)) => match KeyColumn::from_i32(*key_column) {
                None | Some(KeyColumn::Unspecified) => {
                    anyhow::bail!("Unrecognized key column: {key_column}")
                }
                Some(KeyColumn::Time) => 0,
                Some(KeyColumn::Subsort) => 1,
                Some(KeyColumn::KeyHash) => 2,
            },
            Some(Column::ProducerExpression(expression_index)) => {
                let operation_index = input.producing_operation as usize;
                let expression_index = *expression_index as usize;

                let operation = self
                    .operation_output_indices
                    .get(operation_index)
                    .with_context(|| format!("Invalid operation {operation_index}"))?;
                let expression = operation.get(expression_index).with_context(|| {
                    format!(
                        "Invalid expression {operation_index}.{expression_index} (operation only \
                         has {} expressions)",
                        operation.len()
                    )
                })?;

                // Get information about the exported (output) index
                let column = expression.with_context(|| {
                    format!("expression {operation_index}.{expression_index} is not output")
                })?;

                // The actual column is offset by the 3 key columns.
                3 + column
            }
            Some(Column::ScanRecord(())) => 0,
            Some(Column::Tick(())) => 0,
            None => anyhow::bail!("Reference missing column"),
        };

        Ok(())
    }
}
