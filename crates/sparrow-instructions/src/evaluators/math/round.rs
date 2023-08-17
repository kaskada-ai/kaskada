use std::sync::Arc;

use crate::ValueRef;
use anyhow::anyhow;
use arrow::array::{ArrayRef, Float32Array, Float64Array};
use arrow::compute::kernels::arity::unary;
use arrow::datatypes::DataType;
use sparrow_arrow::downcast::downcast_primitive_array;

use crate::{Evaluator, EvaluatorFactory, RuntimeInfo, StaticInfo};

/// Evaluator for Round.
pub(in crate::evaluators) struct RoundEvaluator {
    input: ValueRef,
}

impl EvaluatorFactory for RoundEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let input = info.unpack_argument()?;
        Ok(Box::new(Self { input }))
    }
}

impl Evaluator for RoundEvaluator {
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let input = info.value(&self.input)?.array_ref()?;
        let result = round(&input)?;
        Ok(result)
    }
}

/// Rounds each element in the input `ArrayRef` to the nearest integer.
/// Half-way cases are rounded away from `0`.
/// When passed an integer array, clones the `ArrayRef` without allocating a new
/// array.
///
/// # Errors
/// When passed a non-numeric array type.
fn round(array: &ArrayRef) -> anyhow::Result<ArrayRef> {
    match array.data_type() {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            // Integers are already rounded.
            Ok(array.clone())
        }
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            // Integers are already rounded.
            Ok(array.clone())
        }
        DataType::Float32 => {
            let input_array: &Float32Array = downcast_primitive_array(array.as_ref())?;
            let result_array: Float32Array = unary(input_array, |x| x.round());
            Ok(Arc::new(result_array))
        }
        DataType::Float64 => {
            let input_array: &Float64Array = downcast_primitive_array(array.as_ref())?;
            let result_array: Float64Array = unary(input_array, |x| x.round());
            Ok(Arc::new(result_array))
        }
        unsupported => Err(anyhow!("`round` doesn't support type {:?}", unsupported)),
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Float32Array, Int32Array};

    use super::*;

    #[test]
    fn test_round_int32() {
        let array: ArrayRef = Arc::new(Int32Array::from(vec![Some(5), Some(7), Some(1), None]));
        let actual = round(&array).unwrap();
        assert_eq!(
            actual.as_ref(),
            &Int32Array::from(vec![Some(5), Some(7), Some(1), None])
        );
    }

    #[test]
    fn test_round_float32() {
        let array: ArrayRef = Arc::new(Float32Array::from(vec![
            Some(5.6),
            Some(-7.5),
            Some(3.0),
            Some(1.5),
            None,
        ]));
        let actual = round(&array).unwrap();
        assert_eq!(
            actual.as_ref(),
            &Float32Array::from(vec![Some(6.0), Some(-8.0), Some(3.0), Some(2.0), None])
        );
    }

    #[test]
    fn test_round_float64() {
        let array: ArrayRef = Arc::new(Float32Array::from(vec![
            Some(5.6),
            Some(-7.5),
            Some(3.0),
            Some(1.5),
            None,
        ]));
        let actual = round(&array).unwrap();
        assert_eq!(
            actual.as_ref(),
            &Float32Array::from(vec![Some(6.0), Some(-8.0), Some(3.0), Some(2.0), None])
        );
    }
}
