use anyhow::Context;
use itertools::Itertools;
use sparrow_arrow::scalar_value::ScalarValue;
use sparrow_instructions::{
    ColumnarValue, ComputeStore, Evaluator, GroupingIndices, RuntimeInfo, StaticArg, StaticInfo,
};
use sparrow_instructions::{InstKind, InstOp, ValueRef};

use crate::types::instruction::typecheck_inst;

/// Apply an operation to literal values and return the result as a literal
/// value.
pub(super) fn evaluate_constant(
    kind: &InstKind,
    inputs: Vec<ScalarValue>,
) -> anyhow::Result<ScalarValue> {
    if let InstKind::Simple(inst_op) = &kind {
        match inst_op {
            // This would fail later *anyway*. But it would do so in a much
            // harder to debug way. We handle it here to provide a clearer
            // error.
            InstOp::TimeOf => {
                return Err(anyhow::anyhow!("Cannot constant evaluate 'time_of'"));
            }

            // Handle aggregations specially, since we won't be able to execute them.
            //
            // These definitions may at first seem a little weird. The idea is
            // a literal is never new, so an aggregation applied to a literal behaves
            // as if there was never anything in the aggregation. For `count_if`, the
            // empty behavior is `0`. For other aggregations, it is `null`.
            //
            // It may turn out to need more thinking, but we're sticking with it for
            // now to fix various panics caused by not having *some* behavior defined.
            InstOp::CountIf => return Ok(ScalarValue::UInt32(Some(0))),
            InstOp::First => return Ok(inputs[0].null()),
            InstOp::Last => return Ok(inputs[0].null()),
            InstOp::Max => return Ok(inputs[0].null()),
            InstOp::Mean => return Ok(ScalarValue::Float64(None)),
            InstOp::Min => return Ok(inputs[0].null()),
            InstOp::Sum => return Ok(inputs[0].null()),
            InstOp::Variance => return Ok(ScalarValue::Float64(None)),

            // Handle instructions for which the default `null` behavior of
            // "null if any input is null" are incorrect.
            InstOp::Coalesce => {
                let result = inputs
                    .into_iter()
                    .find(|input| !input.is_null())
                    .unwrap_or(ScalarValue::Null);
                return Ok(result);
            }
            InstOp::LogicalAnd => {
                let mut input = inputs.into_iter();
                if let Some((a, b)) = input.next_tuple() {
                    let a = match a {
                        ScalarValue::Null | ScalarValue::Boolean(None) => None,
                        ScalarValue::Boolean(Some(x)) => Some(x),
                        unexpected => anyhow::bail!(
                            "Expected boolean arguments for logical and, but got {unexpected:?}"
                        ),
                    };
                    let b = match b {
                        ScalarValue::Null | ScalarValue::Boolean(None) => None,
                        ScalarValue::Boolean(Some(x)) => Some(x),
                        unexpected => anyhow::bail!(
                            "Expected boolean arguments for logical and, but got {unexpected:?}"
                        ),
                    };

                    return match (a, b) {
                        (Some(true), Some(true)) => Ok(ScalarValue::Boolean(Some(true))),
                        (Some(false), _) | (_, Some(false)) => {
                            Ok(ScalarValue::Boolean(Some(false)))
                        }
                        (_, _) => Ok(ScalarValue::Boolean(None)),
                    };
                } else {
                    anyhow::bail!("Expected 2 args for logical and, but got {input:?}")
                }
            }
            InstOp::LogicalOr => {
                let mut input = inputs.into_iter();
                if let Some((a, b)) = input.next_tuple() {
                    let a = match a {
                        ScalarValue::Null | ScalarValue::Boolean(None) => None,
                        ScalarValue::Boolean(Some(x)) => Some(x),
                        unexpected => anyhow::bail!(
                            "Expected boolean arguments for logical and, but got {unexpected:?}"
                        ),
                    };
                    let b = match b {
                        ScalarValue::Null | ScalarValue::Boolean(None) => None,
                        ScalarValue::Boolean(Some(x)) => Some(x),
                        unexpected => anyhow::bail!(
                            "Expected boolean arguments for logical and, but got {unexpected:?}"
                        ),
                    };

                    return match (a, b) {
                        (Some(false), Some(false)) => Ok(ScalarValue::Boolean(Some(false))),
                        (Some(true), _) | (_, Some(true)) => Ok(ScalarValue::Boolean(Some(true))),
                        (_, _) => Ok(ScalarValue::Boolean(None)),
                    };
                } else {
                    anyhow::bail!("Expected 2 args for logical and, but got {input:?}")
                }
            }
            InstOp::Substring => {
                anyhow::bail!("Constant evaluation for substring not yet implemented")
            }
            InstOp::Clamp => {
                anyhow::bail!("Constant evaluation for clamp not yet implemented")
            }

            // Other instructions
            _ => {
                if inputs
                    .iter()
                    .any(|input| matches!(input, ScalarValue::Null))
                {
                    // Handle the null buffer.
                    return Ok(ScalarValue::Null);
                } else {
                    // Fall through to actually run the instruction.
                }
            }
        }
    }

    let args: Vec<StaticArg> = inputs
        .iter()
        .map(|i| StaticArg {
            value_ref: ValueRef::Literal(i.clone()),
            data_type: i.data_type(),
        })
        .collect();

    let argument_types = args.iter().map(|i| i.data_type.clone().into()).collect();
    let argument_literals: Vec<_> = inputs.into_iter().map(Some).collect();
    let result_type = typecheck_inst(kind, argument_types, &argument_literals)?;
    let result_type = result_type.arrow_type().with_context(|| {
        format!(
            "Expected result of literal instruction to have concrete type, but got {result_type:?}"
        )
    })?;

    let info = StaticInfo::new(kind, args, result_type);

    let mut evaluator: Box<dyn Evaluator> = sparrow_instructions::create_evaluator(info)
        .with_context(|| format!("Unable to create executor for {kind:?}"))?;

    let result = evaluator
        .evaluate(&ConstEvaluator)
        .with_context(|| format!("Failed to constant evaluate {kind:?}"))?;

    let result = ScalarValue::from_singleton_array(result.as_ref()).with_context(|| {
        format!(
            "Expected scalar value as result of constant evaluation of {kind:?}, but got {result:?}"
        )
    })?;

    Ok(result)
}

struct ConstEvaluator;

impl RuntimeInfo for ConstEvaluator {
    fn value(&self, arg: &ValueRef) -> anyhow::Result<ColumnarValue> {
        match arg {
            ValueRef::Literal(literal) => {
                // Materialize here rather than relying on normal materialization of
                // `ColumnarValue::Literal` to avoid performance warnings.
                Ok(ColumnarValue::Array(literal.to_singleton_array()))
            }
            non_literal => {
                panic!("Accessing non-literal '{non_literal:?}' during constant evalution")
            }
        }
    }

    fn grouping(&self) -> &GroupingIndices {
        panic!("Accessing grouping indices during constant evaluation")
    }

    fn time_column(&self) -> ColumnarValue {
        panic!("Accessing time column during constant evaluation")
    }

    fn storage(&self) -> Option<&ComputeStore> {
        panic!("Accessing storage during constant evaluation")
    }

    fn num_rows(&self) -> usize {
        1
    }
}

#[cfg(test)]
mod tests {
    use sparrow_arrow::scalar_value::ScalarValue;
    use sparrow_instructions::{InstKind, InstOp};
    use strum::IntoEnumIterator;

    #[test]
    #[allow(clippy::print_stdout)]
    fn test_const_eval_null() {
        // Regression test covering a failure caused by the inability to constant
        // evaluate on null arguments
        let mut failing_instructions = Vec::new();
        for inst_op in InstOp::iter() {
            if matches!(inst_op, InstOp::TimeOf | InstOp::Clamp | InstOp::Substring) {
                continue;
            }

            let kind = InstKind::Simple(inst_op);
            let inputs = vec![ScalarValue::Null; inst_op.signature().parameters().len()];

            if let Err(e) = super::evaluate_constant(&kind, inputs) {
                println!("Failed to evaluate '{inst_op}': {e:?}");
                failing_instructions.push(inst_op);
            }
        }

        assert!(
            failing_instructions.is_empty(),
            "Instructions failed on `null`: {failing_instructions:?}"
        );
    }
}
