use anyhow::Context;
use sparrow_api::kaskada::v1alpha::DataType;
use sparrow_api::kaskada::v1alpha::{expression_plan, ExpressionPlan};
use sparrow_arrow::scalar_value::ScalarValue;
use sparrow_instructions::InstKind;
use sparrow_syntax::{ArgVec, FenlType};

use crate::dfg::Expression;
use crate::plan::plan_builder::PlanBuilder;

/// Return the expression operator corresponding to the expression.
///
/// Returns the expression index added to the plan.
pub(super) fn dfg_to_plan(
    plan_builder: &mut PlanBuilder,
    id: egg::Id,
    operation_index: u32,
    expression: &Expression,
    arguments: &[egg::Id],
) -> anyhow::Result<u32> {
    let arguments = plan_builder.resolve_arguments(operation_index, arguments)?;

    let (operator, result_type) = match expression {
        Expression::Literal(literal) => {
            let operator = expression_plan::Operator::Literal(literal.into());
            (operator, DataType::try_from(&literal.data_type())?)
        }
        Expression::LateBound(late_bound) => {
            let operator = expression_plan::Operator::LateBound(*late_bound as i32);
            (operator, late_bound.data_type()?.try_into()?)
        }
        Expression::Inst(inst) => {
            let name = match inst {
                InstKind::Simple(inst) => inst.name().to_owned(),
                InstKind::FieldRef => "field_ref".to_owned(),
                InstKind::Record => "record".to_owned(),
                InstKind::Cast(_) => "cast".to_owned(),
                InstKind::Udf(udf) => udf.uuid().to_string(),
            };

            let result_type =
                infer_result_type(inst, &arguments, plan_builder.expressions(operation_index)?)?;

            let operator = expression_plan::Operator::Instruction(name);
            (operator, result_type)
        }
    };

    plan_builder.add_expression(id, operation_index, operator, arguments, result_type)
}

/// Given an expression (operator and arguments) return the result type.
///
/// This uses information about previous expressions to determine the types
/// of each argument.
fn infer_result_type(
    inst_kind: &InstKind,
    arguments: &[u32],
    expressions: &[ExpressionPlan],
) -> anyhow::Result<DataType> {
    let mut argument_types = ArgVec::with_capacity(arguments.len());
    let mut argument_literals = ArgVec::with_capacity(arguments.len());

    for argument in arguments {
        let expression = expressions
            .get(*argument as usize)
            .context("missing expression for argument")?;

        // This approach of using the `ExpressionPlan` as the source of truth
        // requires extra conversions to/from protobuf. But it minimizes
        // the need to manage redundant information (eg., a vector of FenlType)
        // associated with each node.
        let argument_type = expression
            .result_type
            .as_ref()
            .context("expression missing result type")?;

        let argument_fenl_type =
            FenlType::try_from(argument_type).context("convert expression result type")?;
        argument_types.push(argument_fenl_type);

        // Type checking currently requires literal values.
        // So, for any argument that comes from a literal, this populates
        // a literal value containing that.
        //
        // For many cases, this shouldn't be necessary to determine the *type*.
        // However, type-checking is also verifying the *signature*.
        // So the constants are used to determine things like
        // whether the field name actually exists (eg., in a field ref)
        // and whether the argument to `sliding` or `lag` is actually
        // a constant.
        if let Some(expression_plan::Operator::Literal(literal)) = &expression.operator {
            let data_type = arrow::datatypes::DataType::try_from(argument_type)?;
            let literal: ScalarValue = literal.try_into_scalar_value(&data_type)?;
            argument_literals.push(Some(literal));
        } else {
            argument_literals.push(None);
        }
    }

    let result_type =
        crate::types::instruction::typecheck_inst(inst_kind, argument_types, &argument_literals)?;
    DataType::try_from(&result_type).context("unable to encode result type")
}
