use std::sync::Arc;

use crate::ValueRef;
use anyhow::anyhow;
use arrow::array::{ArrayRef, Float32Array, Float64Array};
use arrow::compute::kernels::arity::unary;
use arrow::datatypes::DataType;
use sparrow_arrow::downcast::downcast_primitive_array;

use crate::{Evaluator, EvaluatorFactory, RuntimeInfo, StaticInfo};

/// Evaluator for the ceiling instruction.
pub(in crate::evaluators) struct CeilEvaluator {
    input: ValueRef,
}

impl Evaluator for CeilEvaluator {
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let input = info.value(&self.input)?.array_ref()?;
        let result = ceil(&input)?;
        Ok(result)
    }
}

impl EvaluatorFactory for CeilEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let input = info.unpack_argument()?;
        Ok(Box::new(Self { input }))
    }
}

/// Evaluator for the floor instruction.
pub(in crate::evaluators) struct FloorEvaluator {
    input: ValueRef,
}

impl EvaluatorFactory for FloorEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let input = info.unpack_argument()?;
        Ok(Box::new(Self { input }))
    }
}

impl Evaluator for FloorEvaluator {
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let input = info.value(&self.input)?.array_ref()?;
        let result = floor(&input)?;
        Ok(result)
    }
}

/// Return the ceiling of a numeric array.
///
/// # Errors
/// When passed a non-numeric array type.
fn ceil(array: &ArrayRef) -> anyhow::Result<ArrayRef> {
    match array.data_type() {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            // Integers are already their own ceiling.
            Ok(array.clone())
        }

        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            // Integers are already their own ceiling.
            Ok(array.clone())
        }
        DataType::Float32 => {
            let input_array: &Float32Array = downcast_primitive_array(array.as_ref())?;
            let result_array: Float32Array = unary(input_array, |x| x.ceil());
            Ok(Arc::new(result_array))
        }
        DataType::Float64 => {
            let input_array: &Float64Array = downcast_primitive_array(array.as_ref())?;
            let result_array: Float64Array = unary(input_array, |x| x.ceil());
            Ok(Arc::new(result_array))
        }
        unsupported => Err(anyhow!(
            "ceil kernel doesn't support type {:?}",
            unsupported
        )),
    }
}

/// Return the floor of a numeric array.
///
/// # Errors
/// When passed a non-numeric array type.
fn floor(array: &ArrayRef) -> anyhow::Result<ArrayRef> {
    match array.data_type() {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            // Integers are already their own floor.
            Ok(array.clone())
        }

        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            // Integers are already their own floor.
            Ok(array.clone())
        }
        DataType::Float32 => {
            let input_array: &Float32Array = downcast_primitive_array(array.as_ref())?;
            let result_array: Float32Array = unary(input_array, |x| x.floor());
            Ok(Arc::new(result_array))
        }
        DataType::Float64 => {
            let input_array: &Float64Array = downcast_primitive_array(array.as_ref())?;
            let result_array: Float64Array = unary(input_array, |x| x.floor());
            Ok(Arc::new(result_array))
        }
        unsupported => Err(anyhow!(
            "floor kernel doesn't support type {:?}",
            unsupported
        )),
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::Float32Array;

    use super::*;

    #[test]
    fn test_ceil_float32() {
        let array: ArrayRef = Arc::new(Float32Array::from(vec![Some(5.6), Some(7.7), None]));
        let actual = ceil(&array).unwrap();
        assert_eq!(
            actual.as_ref(),
            &Float32Array::from(vec![Some(6.0), Some(8.0), None])
        );
    }

    #[test]
    fn test_ceil_float64() {
        let array: ArrayRef = Arc::new(Float64Array::from(vec![Some(5.6), Some(7.7), None]));
        let actual = ceil(&array).unwrap();
        assert_eq!(
            actual.as_ref(),
            &Float64Array::from(vec![Some(6.0), Some(8.0), None])
        );
    }

    #[test]
    fn test_floor_float32() {
        let array: ArrayRef = Arc::new(Float32Array::from(vec![
            Some(5.6),
            Some(7.7),
            Some(1.0),
            None,
        ]));
        let actual = floor(&array).unwrap();
        assert_eq!(
            actual.as_ref(),
            &Float32Array::from(vec![Some(5.0), Some(7.0), Some(1.0), None])
        );
    }

    #[test]
    fn test_floor_float64() {
        let array: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(5.6),
            Some(7.7),
            Some(1.0),
            None,
        ]));
        let actual = floor(&array).unwrap();
        assert_eq!(
            actual.as_ref(),
            &Float64Array::from(vec![Some(5.0), Some(7.0), Some(1.0), None])
        );
    }
}
