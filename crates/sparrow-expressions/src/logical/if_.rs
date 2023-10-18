use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BooleanArray};
use arrow_data::ArrayData;
use arrow_schema::DataType;
use error_stack::{IntoReport, ResultExt};

use sparrow_interfaces::expression::{
    ArrayRefValue, BooleanValue, Error, Evaluator, StaticInfo, WorkArea,
};

inventory::submit!(sparrow_interfaces::expression::EvaluatorFactory {
    name: "if",
    create: &create
});

/// Evaluator for the `if` instruction.
struct IfEvaluator {
    condition: BooleanValue,
    value: ArrayRefValue,
}

impl Evaluator for IfEvaluator {
    fn evaluate(&self, info: &WorkArea<'_>) -> error_stack::Result<ArrayRef, Error> {
        let condition = info.expression(self.condition);
        let value = info.expression(self.value);

        let result = if_(condition, value)?;
        Ok(Arc::new(result))
    }
}

fn create(info: StaticInfo<'_>) -> error_stack::Result<Box<dyn Evaluator>, Error> {
    let (condition, value) = info.unpack_arguments()?;
    Ok(Box::new(IfEvaluator {
        condition: condition.boolean()?,
        value: value.array_ref(),
    }))
}

fn if_(condition: &BooleanArray, value: &ArrayRef) -> error_stack::Result<ArrayRef, Error> {
    // To implement `if` we need to negate the condition. We could avoid this by
    // 1. Having a special `if` kernel
    // 2. Teaching the optimizer to prefer `null_if` in cases where the `not` node
    //    is already in the DFG.
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
        let buffer = arrow_buffer::bitwise_bin_op_helper(
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
        arrow_arith::boolean::not(condition)
            .into_report()
            .change_context(Error::ExprEvaluation)?
    };

    arrow_select::nullif::nullif(value.as_ref(), &condition)
        .into_report()
        .change_context(Error::ExprEvaluation)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{BooleanArray, Int64Array};

    #[test]
    fn test_if_no_null_conditions() {
        let condition = BooleanArray::new(vec![true, false, true, false].into(), None);
        let value = Int64Array::from(vec![Some(5), Some(6), None, None]);
        let value: ArrayRef = Arc::new(value);

        let result = if_(&condition, &value).unwrap();
        assert_eq!(
            result.as_ref(),
            &Int64Array::from(vec![Some(5), None, None, None])
        )
    }

    #[test]
    fn test_if_null_conditions() {
        let condition = BooleanArray::new(
            vec![true, false, true, false, true, false, true, false].into(),
            Some(vec![true, true, true, true, false, false, false, false].into()),
        );
        let value = Int64Array::from(vec![
            Some(5),
            Some(6),
            None,
            None,
            Some(7),
            Some(8),
            None,
            None,
        ]);
        let value: ArrayRef = Arc::new(value);

        let result = if_(&condition, &value).unwrap();
        assert_eq!(
            result.as_ref(),
            &Int64Array::from(vec![Some(5), None, None, None, None, None, None, None])
        )
    }
}
