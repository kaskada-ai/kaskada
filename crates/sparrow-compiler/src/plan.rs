use arrow::datatypes::DataType;
use sparrow_api::kaskada::v1alpha::{ComputePlan, PerEntityBehavior};
use sparrow_core::debug_println;

use crate::dfg::{DfgExpr, Operation, StepKind};
use crate::plan::plan_builder::PlanBuilder;
use crate::DataContext;

mod expression_to_plan;
mod finalize_expression_indices;
mod interpolations;
mod operation_schedule;
mod operation_to_plan;
mod plan_builder;
mod transform_to_plan;

/// A constant to easily enable debug prints int he plan code.
///
/// This will be compiled away when `false`.
const DBG_PRINT_PLAN: bool = false;

/// Extracts a `ComputePlan` proto from a `DfgExpr`.
///
/// TODO: The `DataContext` is used to get the table name from an ID, which is
/// only necessary to create the `slice_plan` because it uses a name instead of
/// an ID.
pub fn extract_plan_proto(
    data_context: &DataContext,
    expr: DfgExpr,
    per_entity_behavior: PerEntityBehavior,
    primary_grouping: String,
    primary_grouping_key_type: &DataType,
) -> anyhow::Result<ComputePlan> {
    // TODO: Projection pushdown?
    // TODO: Slice analysis?

    let mut transform_to_plan = transform_to_plan::TransformToPlan::new(&expr);

    let mut plan_builder = PlanBuilder::try_new(&expr)?;

    for id in expr.ids() {
        let (kind, children) = expr.node(id);

        let operation_index = plan_builder.schedule.operation_opt(id);

        debug_println!(
            DBG_PRINT_PLAN,
            "Adding {id:?}={kind:?}({children:?}) to plan in operation {operation_index:?}",
        );

        let operation_index = if let Some(operation_index) = operation_index {
            operation_index
        } else {
            // We don't add nodes associated with the empty operation to the plan.
            // These should correspond to expressions that weren't dependent on
            // any data, so they should be literals, late values, etc. which are
            // are all added as expressions and transformed into the appropriate
            // domain.
            debug_println!(
                DBG_PRINT_PLAN,
                "Skipping node {id:?}={kind:?} outside operation"
            );
            plan_builder.add_invalid(id, "Node in the empty operation");
            continue;
        };

        match kind {
            StepKind::Operation(Operation::Empty) => {
                anyhow::bail!(
                    "Empty operation should have been skipped earlier, but was in operation index \
                     {operation_index}"
                )
            }
            StepKind::Operation(operation) => {
                let operation = operation_to_plan::dfg_to_plan(
                    &transform_to_plan,
                    data_context,
                    &mut plan_builder,
                    id,
                    operation,
                    children,
                    &expr,
                )?;
                // Add the generated operator to the transform infor.
                transform_to_plan.register_operation(operation_index, operation.operator()?)?;
            }
            StepKind::Expression(expression) => {
                // The last child is the operation. Everything else is an argument.
                let arguments = &children[0..children.len() - 1];

                let expression_index = expression_to_plan::dfg_to_plan(
                    &mut plan_builder,
                    id,
                    operation_index,
                    expression,
                    arguments,
                )?;
                debug_println!(
                    DBG_PRINT_PLAN,
                    "... Added as {operation_index}.{expression_index}"
                );
            }
            StepKind::Transform => {
                let expression_index = transform_to_plan.dfg_to_plan(
                    &mut plan_builder,
                    id,
                    operation_index,
                    children[0],
                )?;
                debug_println!(
                    DBG_PRINT_PLAN,
                    "... Added as {operation_index}.{expression_index}"
                );
            }
            StepKind::Error => {
                anyhow::bail!("Encountered error in DFG, unable to create plan")
            }
            StepKind::Window(_) => todo!(),
        };
    }

    let output_id = egg::Id::from(expr.len() - 1);
    let primary_grouping_key_type = primary_grouping_key_type.try_into()?;
    plan_builder.finish(
        per_entity_behavior,
        output_id,
        primary_grouping,
        primary_grouping_key_type,
    )
}

#[cfg(test)]
mod tests {
    use sparrow_api::kaskada::v1alpha::compile_request::ExpressionKind;
    use sparrow_api::kaskada::v1alpha::SliceRequest;
    use sparrow_api::kaskada::v1alpha::{
        CompileRequest, ComputePlan, FeatureSet, PerEntityBehavior,
    };

    use crate::{DataContext, InternalCompileOptions};

    #[tokio::test]
    async fn test_simple_plan() {
        insta::assert_yaml_snapshot!(create_plan(
            "simple_plan",
            &DataContext::for_test(),
            FeatureSet::new("{ x: Table1.x_i64 + 10 }", vec![]),
            None,
            PerEntityBehavior::All,
        )
        .await
        .unwrap());
    }

    #[tokio::test]
    async fn test_two_tables() {
        insta::assert_yaml_snapshot!(create_plan(
            "two_tables",
            &DataContext::for_test(),
            FeatureSet::new("{ x: Table1.x_i64 + Table2.x_i64 }", vec![]),
            None,
            PerEntityBehavior::All,
        )
        .await
        .unwrap());
    }

    #[tokio::test]
    async fn test_three_tables() {
        insta::assert_yaml_snapshot!(create_plan(
            "three_tables",
            &DataContext::for_test(),
            FeatureSet::new("{ x: Table1.x_i64 + Table2.x_i64 + Table3.x_i64 }", vec![]),
            None,
            PerEntityBehavior::All,
        )
        .await
        .unwrap());
    }

    #[tokio::test]
    async fn test_three_tables_merged_differently() {
        // TODO: Simplify redundant merges
        //
        // This plan currently contains unnecessary merges because it doesn't re-order
        // the the operations. Specifically, the simplifier doesn't know that we
        // can just merge everything to get the merged set of Table1, Table2,
        // and Table3, and then do all the math in that merged domain. This is
        // probably OK for now (doesn't affect correctness, only wasted work).
        insta::assert_yaml_snapshot!(create_plan(
            "three_tables_merged_differently",
            &DataContext::for_test(),
            FeatureSet::new(
                "{ x: (Table1.x_i64 + Table2.x_i64) + Table3.x_i64, y: Table1.x_i64 + \
                 (Table2.x_i64 + Table3.x_i64) }",
                vec![]
            ),
            None,
            PerEntityBehavior::All,
        )
        .await
        .unwrap());
    }

    async fn create_plan(
        name: &str,
        data_context: &DataContext,
        feature_set: FeatureSet,
        slice_request: Option<SliceRequest>,
        per_entity_behavior: PerEntityBehavior,
    ) -> anyhow::Result<ComputePlan> {
        let mut test_output_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        test_output_dir.push("tests");
        test_output_dir.push("test-output");
        test_output_dir.push("plan");

        if !test_output_dir.exists() {
            std::fs::create_dir_all(&test_output_dir).unwrap()
        }

        let response = crate::compile_proto(
            CompileRequest {
                tables: data_context.proto_tables()?,
                feature_set: Some(feature_set),
                slice_request,
                expression_kind: ExpressionKind::Complete as i32,
                experimental: false,
                per_entity_behavior: per_entity_behavior as i32,
            },
            InternalCompileOptions {
                store_final_dfg: Some(test_output_dir.join(format!("{name}_final_dfg.dot"))),
                store_plan_graph: Some(test_output_dir.join(format!("{name}_plan.dot"))),
                ..InternalCompileOptions::default()
            },
        )
        .await
        .map_err(|e| e.into_error())?;

        if let Some(plan) = response.plan {
            Ok(plan)
        } else {
            Err(anyhow::anyhow!(
                "No plan produced. Diagnostics {:?}",
                response.fenl_diagnostics
            ))
        }
    }
}
