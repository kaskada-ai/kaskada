use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BooleanArray};
use arrow_buffer::bitwise_bin_op_helper;
use arrow_data::ArrayData;
use arrow_schema::DataType;
use error_stack::{IntoReport, ResultExt};

use sparrow_interfaces::expression::{
    ArrayRefValue, BooleanValue, Error, Evaluator, StaticInfo, WorkArea,
};

inventory::submit!(sparrow_interfaces::expression::EvaluatorFactory {
    name: "null_if",
    create: &create
});

/// Evaluator for the `null_if` instruction.
struct NullIfEvaluator {
    condition: BooleanValue,
    value: ArrayRefValue,
}

impl Evaluator for NullIfEvaluator {
    fn evaluate(&self, info: &WorkArea<'_>) -> error_stack::Result<ArrayRef, Error> {
        let condition = info.expression(self.condition);
        let value = info.expression(self.value);

        let result = null_if(condition, value)?;
        Ok(Arc::new(result))
    }
}

fn create(info: StaticInfo<'_>) -> error_stack::Result<Box<dyn Evaluator>, Error> {
    let (condition, value) = info.unpack_arguments()?;
    Ok(Box::new(NullIfEvaluator {
        condition: condition.boolean()?,
        value: value.array_ref(),
    }))
}

fn null_if(condition: &BooleanArray, value: &ArrayRef) -> error_stack::Result<ArrayRef, Error> {
    // We should be able to do this without creating two temporary arrays.
    // Basically, we want the to call `nullif` with the condition being true if
    // either it was `null` or it was `true` in the original. We could do this
    // with a variant of `arrow::compute::prep_null_mask_filter` that went
    // `null` to `true` instead of `null` to `false`.
    if let Some(condition_null) = condition.nulls() {
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
        arrow_select::nullif::nullif(&value, &condition)
            .into_report()
            .change_context(Error::ExprEvaluation)
    } else {
        // If there are no nulls, then the condition can be used directly.
        arrow_select::nullif::nullif(&value, condition)
            .into_report()
            .change_context(Error::ExprEvaluation)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{BooleanArray, Int64Array};

    #[test]
    fn test_null_if_no_null_conditions() {
        let condition = BooleanArray::new(vec![true, false, true, false].into(), None);
        let value = Int64Array::from(vec![Some(5), Some(6), None, None]);
        let value: ArrayRef = Arc::new(value);

        let result = null_if(&condition, &value).unwrap();
        assert_eq!(
            result.as_ref(),
            &Int64Array::from(vec![None, Some(6), None, None])
        )
    }

    #[test]
    fn test_null_if_null_conditions() {
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

        let result = null_if(&condition, &value).unwrap();
        assert_eq!(
            result.as_ref(),
            &Int64Array::from(vec![None, Some(6), None, None, None, None, None, None])
        )
    }
}
