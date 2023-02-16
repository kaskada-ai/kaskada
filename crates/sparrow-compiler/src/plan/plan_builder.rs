use anyhow::Context;
use itertools::Itertools;
use sparrow_api::kaskada::v1alpha::DataType;
use sparrow_api::kaskada::v1alpha::{
    expression_plan, operation_input_ref, operation_plan, ComputePlan, ExpressionPlan,
    OperationPlan, PerEntityBehavior,
};

use crate::dfg::DfgExpr;
use crate::plan::finalize_expression_indices::finalize_expression_indices;
use crate::plan::interpolations::Interpolations;
use crate::plan::operation_schedule::OperationSchedule;

/// A wrapper around the operation protos being built.
///
/// This provides book-keeping for the plan creation process, as
/// described below.
pub(super) struct PlanBuilder {
    /// The operation each expression node is in.
    pub(super) schedule: OperationSchedule,
    /// The interpolation for each expression node.
    pub(super) interpolations: Interpolations,
    /// Information about how the result of each node may be accessed.
    ///
    /// This is used when converting the arguments to a node into
    /// corresponding value reference protos within the plan.
    values: Vec<ValueRef>,
    /// The operation plans that are being built.
    operation_plans: Vec<OperationPlan>,
}

impl PlanBuilder {
    pub fn try_new(expr: &DfgExpr) -> anyhow::Result<Self> {
        let schedule = OperationSchedule::try_new(expr)?;
        let interpolations = Interpolations::try_new(expr)?;
        let values = Vec::with_capacity(expr.len());
        let operation_plans = Vec::with_capacity(schedule.num_operations());
        Ok(Self {
            schedule,
            interpolations,
            values,
            operation_plans,
        })
    }

    /// Adds an ID that should not be compiled directly.
    ///
    /// This is useful for operations and expressions in the DFG which should
    /// not be directly referenced in the plan. For instance, the empty
    /// operation or late-bound values should not be directly referenced.
    /// Intsead, things referencing the empty operation (like late bound
    /// values) should be transformed into an actual operation to provide
    /// the domain.
    pub fn add_invalid(&mut self, id: egg::Id, reason: &'static str) {
        debug_assert_eq!(self.values.len(), usize::from(id));
        self.values.push(ValueRef::Invalid(reason));
    }

    /// Adds an operation to the plan.
    pub fn add_operation(
        &mut self,
        id: egg::Id,
        operation_index: u32,
        operator: operation_plan::Operator,
    ) -> anyhow::Result<&mut OperationPlan> {
        let operation_plan = OperationPlan {
            expressions: vec![],
            operator: Some(operator),
        };

        debug_assert_eq!(self.operation_plans.len() as u32, operation_index);
        self.operation_plans.push(operation_plan);

        debug_assert_eq!(self.values.len(), usize::from(id));
        self.values.push(ValueRef::Operation(operation_index));

        Ok(self.operation_plans.last_mut().unwrap())
    }

    pub fn get_expression_type(
        &self,
        operation_index: u32,
        expression_index: u32,
    ) -> anyhow::Result<&DataType> {
        self.expressions(operation_index)?
            .get(expression_index as usize)
            .context("missing expression index")?
            .result_type
            .as_ref()
            .context("missing result_type")
    }

    /// Adds an expression without binding it to a DFG node.
    ///
    /// This should be used when adding "administrative" expressions, such
    /// as plumbing the value of an input through multiple nested merges.
    ///
    /// Returns the index of the added expression within the operation.
    pub fn add_unbound_expression(
        &mut self,
        operation_index: u32,
        operator: expression_plan::Operator,
        arguments: Vec<u32>,
        result_type: DataType,
        output: bool,
    ) -> anyhow::Result<u32> {
        // TODO: Consider using a map to de-duplicate expressions added multiple times.
        // This shouldn't arise from nodes directly from the DFG, but could happen
        // when adding "administrative" expressions.

        if cfg!(debug_assertions) {
            if let expression_plan::Operator::Input(input) = &operator {
                match input.column.as_ref().context("missing column")? {
                    operation_input_ref::Column::KeyColumn(_) => {}
                    operation_input_ref::Column::ProducerExpression(expression) => {
                        anyhow::ensure!(
                            self.expressions(input.producing_operation)?[*expression as usize]
                                .output,
                            "Expression {}.{} ({:?} is not output",
                            input.producing_operation,
                            expression,
                            self.expressions(input.producing_operation)?[*expression as usize],
                        )
                    }

                    operation_input_ref::Column::ScanRecord(_) => {}
                    operation_input_ref::Column::Tick(_) => {}
                }

                let operation_inputs: Vec<_> = self.operation_plans[operation_index as usize]
                    .operator()?
                    .input_ops_iter()
                    .collect();

                anyhow::ensure!(
                    operation_inputs.contains(&input.producing_operation),
                    "Input {input:?} must be from a registered input of the operation \
                     ({operation_inputs:?})"
                )
            }
        }

        let operation_plan = self
            .operation_plans
            .get_mut(operation_index as usize)
            .context("operation plan")?;
        let expression_index = operation_plan.expressions.len();

        operation_plan.expressions.push(ExpressionPlan {
            arguments,
            result_type: Some(result_type),
            output,
            operator: Some(operator),
        });
        Ok(expression_index as u32)
    }

    /// Adds an expression wit the given operator and arguments to the plan.
    ///
    /// Returns the index of the expression that was added.
    pub fn add_expression(
        &mut self,
        id: egg::Id,
        operation_index: u32,
        operator: expression_plan::Operator,
        arguments: Vec<u32>,
        result_type: DataType,
    ) -> anyhow::Result<u32> {
        let expression_index = self
            .add_unbound_expression(operation_index, operator, arguments, result_type, false)
            .with_context(|| format!("adding expression {id:?}"))?;

        debug_assert_eq!(self.values.len(), usize::from(id));
        self.values.push(ValueRef::Expression {
            operation_index,
            expression_index,
        });

        Ok(expression_index)
    }

    /// Resolve arguments within the given operation to expression indices.
    pub(super) fn resolve_arguments(
        &self,
        operation_index: u32,
        arguments: &[egg::Id],
    ) -> anyhow::Result<Vec<u32>> {
        arguments
            .iter()
            .map(|id| self.expression_ref(operation_index, *id))
            .try_collect()
    }

    /// Get the expression plans for the given operation index.
    pub(super) fn expressions(&self, operation_index: u32) -> anyhow::Result<&[ExpressionPlan]> {
        self.operation_plans
            .get(operation_index as usize)
            .context("operation index out-of-bounds")
            .map(|operation| operation.expressions.as_ref())
    }

    /// Return the expression index of the given `id` within the
    /// `expected_operation_index`.
    ///
    /// Returns an error if the value is not associated with the given
    /// operation.
    fn expression_ref(&self, expected_operation_index: u32, id: egg::Id) -> anyhow::Result<u32> {
        let value = self
            .values
            .get(usize::from(id))
            .with_context(|| format!("missing expression ref for {id:?}"))?;

        match value {
            ValueRef::Invalid(reason) => {
                anyhow::bail!("Unable to access value of {id:?}: {reason}")
            }
            ValueRef::Operation(_) => {
                // HACK: Some operations shouldn't be referenced. But some, like
                // `ScanTable` or `Select` may be referenced to access their results.
                // For now, we know that their result is available as `expression 0`.
                //
                // This should probably be cleaned up when we straighten out how to
                // implement the `transform` (domain change), since it relates to how
                // the input to an operation is created.
                Ok(0)
            }
            ValueRef::Expression {
                operation_index,
                expression_index,
            } => {
                anyhow::ensure!(
                    *operation_index == expected_operation_index,
                    "Unable to access value of {id:?} from operation {} in operation {}",
                    operation_index,
                    expected_operation_index
                );

                Ok(*expression_index)
            }
        }
    }

    /// Record the given expression index as exported.
    pub(super) fn mark_output(&mut self, operation_index: u32, expression_index: u32) {
        self.operation_plans[operation_index as usize].expressions[expression_index as usize]
            .output = true;
    }

    /// Record an expression as exported, and return the expression index it
    /// will be exported with.
    pub(super) fn get_output(&mut self, operation_index: u32, id: egg::Id) -> anyhow::Result<u32> {
        let expression_index = self.expression_ref(operation_index, id)?;
        self.mark_output(operation_index, expression_index);

        Ok(expression_index)
    }

    pub fn finish(
        mut self,
        per_entity_behavior: PerEntityBehavior,
        output_id: egg::Id,
        primary_grouping: String,
        primary_grouping_key_type: DataType,
    ) -> anyhow::Result<ComputePlan> {
        // Make sure the expression for the output is exported, and is
        // the last expression in the last operation. Anything computed
        // *after* the output is ready would be wasted. Execution also
        // uses the last column in the last operation to determine the
        // output batches. It should also (currently) be the *only*
        // exported expression in the last operation.

        // NOTE: We currently only support plans in a specified domain (operation),
        // since we need to know which rows to output.
        let output_operation = self
            .schedule
            .operation(output_id)
            .context("output not in an operation")?;
        let output_expression = self.get_output(output_operation, output_id)?;

        let output_operation = output_operation as usize;

        anyhow::ensure!(
            output_operation == self.operation_plans.len() - 1,
            "Output should be in the last operation",
        );
        anyhow::ensure!(
            output_expression as usize
                == self.operation_plans[output_operation].expressions.len() - 1,
            "Output should be last expression in last operation"
        );

        // During construction, only the absolute operation and expression indices
        // were populated. Finalize the input references by setting the relative
        // indices. This populates the `producing_expression_output_index` to the
        // index within only the output expressions, and `operation_input_index`
        // to the index of the producing operation within the inputs to the
        // consuming operation.
        let operations = finalize_expression_indices(self.operation_plans)?;

        Ok(ComputePlan {
            per_entity_behavior: per_entity_behavior as i32,
            operations,
            primary_grouping,
            primary_grouping_key_type: Some(primary_grouping_key_type),
        })
    }
}

enum ValueRef {
    /// Values that should not be referenced by anything.
    ///
    /// This includes a string indicating the source of the invalid value
    /// used for better error reporting.
    Invalid(&'static str),
    /// Operation values that should only be referenced by other operations.
    Operation(u32),
    /// The result of the expression at `index` in the given `operation`.
    Expression {
        operation_index: u32,
        expression_index: u32,
    },
}
