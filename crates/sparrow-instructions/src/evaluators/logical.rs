use std::sync::Arc;

use crate::ValueRef;
use arrow::array::{Array, ArrayData, ArrayRef, BooleanArray};
use arrow::buffer::bitwise_bin_op_helper;
use arrow::datatypes::DataType;

use crate::{Evaluator, EvaluatorFactory, RuntimeInfo, StaticInfo};

/// Evaluator for the `not` instruction.
pub(super) struct NotEvaluator {
    input: ValueRef,
}

impl Evaluator for NotEvaluator {
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let input = info.value(&self.input)?.boolean_array()?;
        let result = arrow::compute::not(input.as_ref())?;
        Ok(Arc::new(result))
    }
}

impl EvaluatorFactory for NotEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let input = info.unpack_argument()?;
        Ok(Box::new(Self { input }))
    }
}

/// Evaluator for the `logical_or` instruction.
///
/// Note this explicitly specifies the use of Kleene behavior
/// for three-valued logic.
///
/// Returns `true` if either side is `true`.
pub(super) struct LogicalOrKleeneEvaluator {
    lhs: ValueRef,
    rhs: ValueRef,
}

impl Evaluator for LogicalOrKleeneEvaluator {
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let lhs = info.value(&self.lhs)?.boolean_array()?;
        let rhs = info.value(&self.rhs)?.boolean_array()?;
        let result = arrow::compute::or_kleene(lhs.as_ref(), rhs.as_ref())?;

        // TODO: https://github.com/apache/arrow-rs/issues/1498 is fixed, which should
        // allow us to avoid deep copying the array here.
        let result: BooleanArray = result.iter().collect();

        Ok(Arc::new(result))
    }
}

impl EvaluatorFactory for LogicalOrKleeneEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let (lhs, rhs) = info.unpack_arguments()?;
        Ok(Box::new(Self { lhs, rhs }))
    }
}

/// Evaluator for the `logical_and` instruction.
///
/// Note this explicitly specifies the use of Kleene behavior
/// for three-valued logic.
///
/// Returns `false` if either side is `false`.
pub(super) struct LogicalAndKleeneEvaluator {
    lhs: ValueRef,
    rhs: ValueRef,
}

impl Evaluator for LogicalAndKleeneEvaluator {
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let lhs = info.value(&self.lhs)?.boolean_array()?;
        let rhs = info.value(&self.rhs)?.boolean_array()?;
        let result = arrow::compute::and_kleene(lhs.as_ref(), rhs.as_ref())?;

        // TODO: https://github.com/apache/arrow-rs/issues/1498 is fixed, which should
        // allow us to avoid deep copying the array here.
        let result: BooleanArray = result.iter().collect();
        Ok(Arc::new(result))
    }
}

impl EvaluatorFactory for LogicalAndKleeneEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let (lhs, rhs) = info.unpack_arguments()?;
        Ok(Box::new(Self { lhs, rhs }))
    }
}

/// Evaluator for the `null_if` instruction.
pub(super) struct NullIfEvaluator {
    condition: ValueRef,
    value: ValueRef,
}

impl Evaluator for NullIfEvaluator {
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        // We should be able to do this without creating two temporary arrays.
        // Basically, we want the to call `nullif` with the condition being true if
        // either it was `null` or it was `true` in the original. We could do this
        // with a variant of `arrow::compute::prep_null_mask_filter` that went
        // `null` to `true` instead of `null` to `false`.
        let value = info.value(&self.value)?.array_ref()?;
        let condition = info.value(&self.condition)?.boolean_array()?;
        let result = if let Some(condition_null) = condition.nulls() {
            // If there are nulls, then we need to create an argument for `nullif`
            // that corresponds to `condition | condition_null`. This is equivalent
            // to `condition | !condition_valid`.
            let offset = condition.offset();
            let len = condition.len();
            let buffer = bitwise_bin_op_helper(
                condition.values().inner(),
                offset,
                condition_null.buffer(),
                offset,
                len,
                |condition, condition_valid| condition | !condition_valid,
            );
            let array_data = ArrayData::builder(DataType::Boolean)
                .len(condition.len())
                .add_buffer(buffer);
            // SAFETY: The `bitwise_bin_op_helper` produced an array of the proper length.
            let array_data = unsafe { array_data.build_unchecked() };
            let condition = BooleanArray::from(array_data);

            arrow::compute::nullif(&value, &condition)?
        } else {
            // If there are no nulls, then the condition can be used directly.
            arrow::compute::nullif(&value, &condition)?
        };

        Ok(result)
    }
}

impl EvaluatorFactory for NullIfEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let (condition, value) = info.unpack_arguments()?;
        Ok(Box::new(Self { condition, value }))
    }
}

/// Evaluator for the `if` instruction.
pub(super) struct IfEvaluator {
    condition: ValueRef,
    value: ValueRef,
}

impl Evaluator for IfEvaluator {
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        // To implement `if` we need to negate the condition. We could avoid this by
        // 1. Having a special `if` kernel
        // 2. Teaching the optimizer to prefer `null_if` in cases where the `not` node
        //    is already in the DFG.
        let condition = info.value(&self.condition)?.boolean_array()?;
        let condition = if let Some(condition_null) = condition.nulls() {
            // The result of `if` is null, if
            // 1. The `condition` is null.
            // 2. The `condition` is false.
            //
            // To accomplish this, we call `nullif` with an input that contains
            // * `true` if the input condition was `null` or `false`
            // * `false` if the input condition was `true`.
            //
            // This is `!(condition && condition_valid)`.
            let offset = condition.offset();
            let len = condition.len();
            let buffer = bitwise_bin_op_helper(
                condition.values().inner(),
                offset,
                condition_null.buffer(),
                offset,
                len,
                |condition, condition_valid| !(condition & condition_valid),
            );
            let array_data = ArrayData::builder(DataType::Boolean)
                .len(condition.len())
                .add_buffer(buffer);
            // SAFETY: The `bitwise_bin_op_helper` produced an array of the proper length.
            let array_data = unsafe { array_data.build_unchecked() };

            BooleanArray::from(array_data)
        } else {
            arrow::compute::not(&condition)?
        };
        let value = info.value(&self.value)?.array_ref()?;
        let result = arrow::compute::nullif(value.as_ref(), &condition)?;
        Ok(result)
    }
}

impl EvaluatorFactory for IfEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let (condition, value) = info.unpack_arguments()?;
        Ok(Box::new(Self { condition, value }))
    }
}
