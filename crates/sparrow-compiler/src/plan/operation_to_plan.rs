use anyhow::Context;
use operation_input_ref::Interpolation;
use operation_plan::LookupResponseOperation;
use sparrow_api::kaskada::v1alpha::data_type::{self, PrimitiveType};
use sparrow_api::kaskada::v1alpha::operation_input_ref::{self, Column};
use sparrow_api::kaskada::v1alpha::operation_plan::{
    shift_to_operation, LookupRequestOperation, ScanOperation, SelectOperation, ShiftToOperation,
    ShiftUntilOperation, TickOperation, WithKeyOperation,
};
use sparrow_api::kaskada::v1alpha::{
    expression_plan, operation_plan, OperationInputRef, OperationPlan, SlicePlan,
};
use sparrow_api::kaskada::v1alpha::{DataType, Schema};
use sparrow_arrow::scalar_value::ScalarValue;

use super::transform_to_plan::TransformToPlan;
use crate::dfg::{DfgExpr, Expression, Operation, StepKind};
use crate::plan::plan_builder::PlanBuilder;
use crate::DataContext;

/// DataType protobuf representing a list of u64.
#[static_init::dynamic]
static LIST_U64_DATA_TYPE: DataType = DataType {
    kind: Some(data_type::Kind::List(Box::new(data_type::List {
        // Note: The fields here must match the default fields used when creating
        // types during type inference, otherwise schema validation will fail.
        name: "item".to_owned(),
        item_type: Some(Box::new(DataType {
            kind: Some(data_type::Kind::Primitive(
                data_type::PrimitiveType::U64 as i32,
            )),
        })),
        nullable: true,
    }))),
};

/// Create the `OperationPlan` for the given operation.
pub(super) fn dfg_to_plan<'plan>(
    transform_to_plan: &TransformToPlan<'plan>,
    data_context: &DataContext,
    plan_builder: &'plan mut PlanBuilder,
    id: egg::Id,
    operation: &Operation,
    children: &[egg::Id],
    expr: &DfgExpr,
) -> anyhow::Result<&'plan mut OperationPlan> {
    let operation_index = plan_builder.schedule.operation(id)?;

    match operation {
        Operation::Empty => anyhow::bail!("Encountered `Empty` operation"),
        Operation::Scan { table_id, slice } => {
            let table_info = data_context.table_info(*table_id).context("table_info")?;
            let table_name = table_info.name();
            let schema: Schema = table_info.schema().as_ref().try_into()?;
            let slice_plan = SlicePlan {
                table_name: table_name.to_owned(),
                slice: slice.clone(),
            };

            let result_type = DataType::new_struct(schema.fields.clone());

            let operation = plan_builder.add_operation(
                id,
                operation_index,
                operation_plan::Operator::Scan(ScanOperation {
                    table_id: Some(table_id.as_proto()),
                    schema: Some(schema),
                    slice_plan: Some(slice_plan),
                }),
            )?;

            // The records read from the table will be available in column 0 of input 0.
            // We *could* map each column separately, but that would require mapping
            // field names to indices, and also eliminating the `fieldref` instructions
            // from the plan. This is easier while moving between old execution plans
            // and new execution plans. We may want to revisit.
            //
            // This is also likely to change
            operation
                .expressions
                .push(sparrow_api::kaskada::v1alpha::ExpressionPlan {
                    arguments: vec![],
                    result_type: Some(result_type),
                    output: false,
                    operator: Some(expression_plan::Operator::Input(OperationInputRef {
                        // Rows read by a scan operation are treated as coming from the scan
                        // operation itself. This "cyclic" reference is used
                        // to indicate that it is OK for the scan operation
                        // to reference itself.
                        producing_operation: operation_index,
                        column: Some(Column::ScanRecord(())),
                        input_column: u32::MAX,
                        interpolation: operation_input_ref::Interpolation::Null as i32,
                    })),
                });

            Ok(operation)
        }
        Operation::MergeJoin => {
            let left_operation = plan_builder.schedule.operation(children[0])?;
            let right_operation = plan_builder.schedule.operation(children[1])?;
            plan_builder.add_operation(
                id,
                operation_index,
                operation_plan::Operator::Merge(operation_plan::MergeOperation {
                    left: left_operation,
                    right: right_operation,
                }),
            )
        }

        Operation::LookupRequest => {
            let key = children[0];
            let key_operation = plan_builder.schedule.operation(key)?;
            let key_expression = plan_builder.get_output(key_operation, key)?;

            let operation = plan_builder.add_operation(
                id,
                operation_index,
                operation_plan::Operator::LookupRequest(LookupRequestOperation {
                    primary_operation: key_operation,
                    foreign_key_hash: Some(OperationInputRef {
                        producing_operation: key_operation,
                        // Populated by finalization.
                        input_column: u32::MAX,
                        column: Some(operation_input_ref::Column::ProducerExpression(
                            key_expression,
                        )),
                        interpolation: operation_input_ref::Interpolation::AsOf as i32,
                    }),
                }),
            )?;

            operation
                .expressions
                .push(sparrow_api::kaskada::v1alpha::ExpressionPlan {
                    arguments: vec![],
                    result_type: Some(LIST_U64_DATA_TYPE.clone()),
                    output: true,
                    operator: Some(expression_plan::Operator::Input(OperationInputRef {
                        producing_operation: key_operation,
                        input_column: 2,
                        column: Some(operation_input_ref::Column::KeyColumn(
                            operation_input_ref::KeyColumn::KeyHash as i32,
                        )),
                        interpolation: operation_input_ref::Interpolation::Null as i32,
                    })),
                });
            Ok(operation)
        }
        Operation::LookupResponse => {
            let request_operation = plan_builder.schedule.operation(children[0])?;
            let merged_operation = plan_builder.schedule.operation(children[1])?;

            // HACK / to clean up. This is a bit weird. We'd *like* to get the input ref
            // to use in the lookup response operation... but we haven't added it yet.
            // So instead, we get the input ref to use to get the key *in the merged
            // operation*.
            let requesting_key = transform_to_plan.get_direct_input(
                plan_builder,
                request_operation,
                0,
                merged_operation,
                Interpolation::Null,
                &LIST_U64_DATA_TYPE,
            )?;
            let requesting_key = plan_builder.add_unbound_expression(
                merged_operation,
                expression_plan::Operator::Input(requesting_key),
                vec![],
                LIST_U64_DATA_TYPE.clone(),
                true,
            )?;

            plan_builder.add_operation(
                id,
                operation_index,
                operation_plan::Operator::LookupResponse(LookupResponseOperation {
                    foreign_operation: merged_operation,
                    requesting_key_hash: Some(OperationInputRef {
                        producing_operation: merged_operation,
                        input_column: u32::MAX,
                        column: Some(operation_input_ref::Column::ProducerExpression(
                            requesting_key,
                        )),
                        interpolation: operation_input_ref::Interpolation::Null as i32,
                    }),
                }),
            )
        }

        Operation::WithKey => {
            let new_key = children[0];
            // The `key` is the first argument of the `with_key` signature. However,
            // in the DFG, the `key_operation` is actually the `key_value` transformed
            // to the merged domain of the `key_op` and `value_op`.
            // i.e. (transform ?key_value (merge_join ?key_op ?value_op))
            let key_operation = plan_builder.schedule.operation(new_key)?;
            let key_expression = plan_builder.get_output(key_operation, new_key)?;

            let grouping = children[1];
            let grouping = match expr.kind(grouping) {
                StepKind::Expression(Expression::Literal(ScalarValue::Utf8(Some(grouping)))) => {
                    grouping.to_owned()
                }
                unexpected => {
                    anyhow::bail!("Unexpected non-const expression for grouping {unexpected:?}");
                }
            };

            let interpolation = plan_builder.interpolations.interpolation(new_key) as i32;

            plan_builder.add_operation(
                id,
                operation_index,
                operation_plan::Operator::WithKey(WithKeyOperation {
                    input: key_operation,

                    new_key: Some(OperationInputRef {
                        producing_operation: key_operation,
                        column: Some(Column::ProducerExpression(key_expression)),
                        // Filled in during plan finalization.
                        input_column: u32::MAX,
                        interpolation,
                    }),

                    grouping,
                }),
            )
        }
        Operation::Select => {
            let condition = children[0];
            let condition_operation = plan_builder.schedule.operation(condition)?;
            let condition_expression = plan_builder.get_output(condition_operation, condition)?;

            let interpolation = plan_builder.interpolations.interpolation(condition) as i32;

            plan_builder.add_operation(
                id,
                operation_index,
                operation_plan::Operator::Select(SelectOperation {
                    input: condition_operation,
                    condition: Some(OperationInputRef {
                        producing_operation: condition_operation,
                        column: Some(Column::ProducerExpression(condition_expression)),
                        // Filled in during plan finalization.
                        input_column: u32::MAX,
                        interpolation,
                    }),
                }),
            )
        }
        Operation::ShiftTo => {
            let time = children[0];
            let time_operation = plan_builder.schedule.operation(time)?;
            let time_expression = plan_builder.get_output(time_operation, time)?;
            let interpolation = plan_builder.interpolations.interpolation(time) as i32;

            plan_builder.add_operation(
                id,
                operation_index,
                operation_plan::Operator::ShiftTo(ShiftToOperation {
                    input: time_operation,
                    time: Some(shift_to_operation::Time::Computed(OperationInputRef {
                        producing_operation: time_operation,
                        // Filled in during plan finalization.
                        input_column: u32::MAX,
                        column: Some(Column::ProducerExpression(time_expression)),
                        interpolation,
                    })),
                }),
            )
        }
        Operation::ShiftUntil => {
            let condition = children[0];
            let condition_operation = plan_builder.schedule.operation(condition)?;
            let condition_expression = plan_builder.get_output(condition_operation, condition)?;
            let interpolation = plan_builder.interpolations.interpolation(condition) as i32;

            plan_builder.add_operation(
                id,
                operation_index,
                operation_plan::Operator::ShiftUntil(ShiftUntilOperation {
                    input: condition_operation,
                    condition: Some(OperationInputRef {
                        producing_operation: condition_operation,
                        column: Some(Column::ProducerExpression(condition_expression)),
                        // Filled in during plan finalization.
                        input_column: u32::MAX,
                        interpolation,
                    }),
                }),
            )
        }
        Operation::Tick(behavior) => {
            let result_type = DataType::new_primitive(PrimitiveType::Bool);

            let behavior = *behavior as i32;
            let input = plan_builder.schedule.operation(children[0])?;
            let operator = operation_plan::Operator::Tick(TickOperation { behavior, input });

            let operation = plan_builder.add_operation(id, operation_index, operator)?;

            operation
                .expressions
                .push(sparrow_api::kaskada::v1alpha::ExpressionPlan {
                    arguments: vec![],
                    result_type: Some(result_type),
                    output: false,
                    operator: Some(expression_plan::Operator::Input(OperationInputRef {
                        producing_operation: operation_index,
                        column: Some(Column::Tick(())),
                        input_column: 0,
                        interpolation: operation_input_ref::Interpolation::Null as i32,
                    })),
                });
            Ok(operation)
        }
    }
}
