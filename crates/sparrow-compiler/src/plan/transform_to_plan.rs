//! This file contains the code for adding `transform` nodes to the plan.
//!
//! These nodes are particularly tricky because they require understanding
//! the different operations, and how to access the transformed value within
//! the resulting operation.

use bit_set::BitSet;
use sparrow_api::kaskada::v1alpha::operation_input_ref::{Column, Interpolation};
use sparrow_api::kaskada::v1alpha::DataType;
use sparrow_api::kaskada::v1alpha::{expression_plan, operation_plan, OperationInputRef};

use crate::dfg::{DfgExpr, Expression, StepKind};
use crate::plan::plan_builder::PlanBuilder;

/// Book-keeping necessary for converting transforms to the plan.
pub(super) struct TransformToPlan<'a> {
    expr: &'a DfgExpr,

    /// Details about the operations available "through" this operation.
    available_inputs: Vec<AvailableInputs>,
}

impl<'a> TransformToPlan<'a> {
    pub(super) fn new(expr: &'a DfgExpr) -> Self {
        Self {
            expr,
            available_inputs: Vec::new(),
        }
    }

    /// Registers an operation with `TransformToPlan`.
    ///
    /// This must be called for each operation, before any transforms
    /// or other operations may reference it.
    pub(super) fn register_operation(
        &mut self,
        operation_index: u32,
        operator: &operation_plan::Operator,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(operation_index as usize == self.available_inputs.len());

        let input: AvailableInputs = match operator {
            operation_plan::Operator::Merge(merge) => {
                let left = self.get_available_input(merge.left);
                let right = self.get_available_input(merge.right);

                AvailableInputs::new(vec![left, right])
            }
            operation_plan::Operator::LookupRequest(lookup_request) => {
                let input = self.get_available_input(lookup_request.primary_operation);
                AvailableInputs::new(vec![input])
            }
            _ => {
                // Other instructions don't "pass through" inputs.
                AvailableInputs::empty()
            }
        };
        self.available_inputs.push(input);
        Ok(())
    }

    fn get_available_input(&self, input: u32) -> AvailableInput {
        self.available_inputs[input as usize].create_input(input)
    }

    /// Implement a transform (from the DFG) within the plan.
    ///
    /// For most cases, this involves taking a value from a producing operation
    /// and making it available within a consuming operation. The producing
    /// operation is the operation associated with the `value`. The
    /// consuming operation is the operation associated with the `transform`.
    ///
    /// Adding this to a plan involves (1) ensuring the expression that computes
    /// the `value` is part of the output for the producing operation and (2)
    /// adding an expression to the consuming operation to represent the
    /// `transform`.
    ///
    /// In general, the value must come directly from the producing operation --
    /// there should not be any `transform` step that carries a value through
    /// multiple operations other than through multiple implicit merge-joins.
    /// Those are handled specially.
    pub(super) fn dfg_to_plan(
        &self,
        plan_builder: &mut PlanBuilder,
        transform_id: egg::Id,
        consuming_operation_index: u32,
        value: egg::Id,
    ) -> anyhow::Result<u32> {
        if let StepKind::Expression(Expression::LateBound(late_bound)) = self.expr.kind(value) {
            // For a late bound value, no transformation should be necessary.
            // Instead, we just add an expression to the resulting operation
            // (determined from the operation index of the transform).
            plan_builder.add_expression(
                transform_id,
                consuming_operation_index,
                expression_plan::Operator::LateBound(*late_bound as i32),
                vec![],
                late_bound.data_type()?.try_into()?,
            )
        } else {
            // The operation (index) that produced the value.
            let producing_operation_index = plan_builder.schedule.operation(value)?;
            anyhow::ensure!(
                producing_operation_index < consuming_operation_index,
                "Transform {transform_id:?}: Cannot transform {value:?} from \
                 {producing_operation_index} to {consuming_operation_index}"
            );

            // The expression (index) within the source operation.
            // This will make sure it is exported as well.
            let producing_expression_index =
                plan_builder.get_output(producing_operation_index, value)?;

            let result_type = plan_builder
                .get_expression_type(producing_operation_index, producing_expression_index)?
                .clone();

            let interpolation = plan_builder.interpolations.interpolation(value);

            let direct_input = self.get_direct_input(
                plan_builder,
                producing_operation_index,
                producing_expression_index,
                consuming_operation_index,
                interpolation,
                &result_type,
            )?;
            // Add an expression to the consuming operation that inputs from the
            // producing operation with the given index.
            plan_builder.add_expression(
                transform_id,
                consuming_operation_index,
                expression_plan::Operator::Input(direct_input),
                vec![],
                result_type,
            )
        }
    }

    /// Return the input reference for the given expression in the producing
    /// operation.
    ///
    /// If the `producing_operation` is a direct input of `consuming_operation`
    /// returns the `OperationInputRef` used for referencing the `value` from
    /// the `producing_operation` within the `consuming_operation`.
    ///
    /// If the `producing_operation` is an indirect input, then the value
    /// is plumbed through the operations in between the `producing_operation`
    /// and the `consuming_operation`, so that it is directly available.
    pub(crate) fn get_direct_input(
        &self,
        plan_builder: &mut PlanBuilder,
        producing_operation: u32,
        producing_expression: u32,
        consuming_operation: u32,
        interpolation: Interpolation,
        result_type: &DataType,
    ) -> anyhow::Result<OperationInputRef> {
        let input = &self.available_inputs[consuming_operation as usize];

        #[allow(clippy::if_same_then_else)]
        let (direct_producing_operation, producer_expression) = if input.available.is_empty() {
            // If the consumer is not a merge, then the producer should be a direct input.
            plan_builder.mark_output(producing_operation, producing_expression);
            (producing_operation, producing_expression)
        } else if input.get_direct_index(producing_operation).is_some() {
            // This is a direct input (left or right) of the consuming merge.
            plan_builder.mark_output(producing_operation, producing_expression);
            (producing_operation, producing_expression)
        } else if let Some(indirect) = input.get_indirect_index(producing_operation) {
            // This item is available in the merge, but indirectly.
            let direct_producing_operation = input.available[indirect].direct;

            // We need to recursively call `get_direct_input` to get it directly.
            let direct_input = self.get_direct_input(
                plan_builder,
                producing_operation,
                producing_expression,
                direct_producing_operation,
                interpolation,
                result_type,
            )?;
            let expression = plan_builder.add_unbound_expression(
                direct_producing_operation,
                expression_plan::Operator::Input(direct_input),
                vec![],
                result_type.clone(),
                true,
            )?;

            (direct_producing_operation, expression)
        } else {
            anyhow::bail!(
                "Producing operation {producing_operation} is not available in \
                 {consuming_operation}"
            )
        };

        let interpolation = interpolation as i32;

        Ok(OperationInputRef {
            producing_operation: direct_producing_operation,
            column: Some(Column::ProducerExpression(producer_expression)),
            // This field filled in during plan finalization
            input_column: u32::MAX,
            interpolation,
        })
    }
}

/// Information about an input "passed through" this operation.
///
/// Values that are directly (or indirectly) available may be implicitly
/// carried through operations by `get_direct_input`.
#[derive(Debug)]
struct AvailableInput {
    /// The direct input operation this describes.
    direct: u32,
    /// The operation indices available indirect through this direct input.
    indirect: BitSet,
}

/// Information about all inputs "passed through" this operation.
#[derive(Debug)]
#[repr(transparent)]
struct AvailableInputs {
    /// Information about inputs to this operation.
    available: Vec<AvailableInput>,
}

impl AvailableInputs {
    fn empty() -> Self {
        Self { available: vec![] }
    }

    fn new(available: Vec<AvailableInput>) -> Self {
        Self { available }
    }

    fn create_input(&self, direct: u32) -> AvailableInput {
        let mut indirect = BitSet::new();
        for input in &self.available {
            indirect.union_with(&input.indirect);
            indirect.insert(input.direct as usize);
        }

        AvailableInput { direct, indirect }
    }

    /// Return the index of the input the `operation` is directly available on.
    fn get_direct_index(&self, operation: u32) -> Option<usize> {
        self.available
            .iter()
            .position(|input| input.direct == operation)
    }

    /// Return the index of the input the `operation` is indirectly available
    /// on.
    fn get_indirect_index(&self, operation: u32) -> Option<usize> {
        debug_assert!(
            self.get_direct_index(operation).is_none(),
            "Operation should not be directly available when calling get_indirect_index"
        );

        self.available
            .iter()
            .position(|input| input.indirect.contains(operation as usize))
    }
}
