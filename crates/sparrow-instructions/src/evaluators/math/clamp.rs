use std::marker::PhantomData;
use std::sync::Arc;

use crate::ValueRef;
use arrow::array::{ArrayRef, PrimitiveArray};
use arrow::datatypes::ArrowNumericType;
use itertools::izip;

use crate::{Evaluator, EvaluatorFactory, RuntimeInfo, StaticInfo};

/// Evaluator for the `clamp` instruction.
pub(in crate::evaluators) struct ClampEvaluator<T: ArrowNumericType> {
    input: ValueRef,
    min: ValueRef,
    max: ValueRef,
    // Use the type parameter and indicate it is invariant.
    _phantom: PhantomData<fn(T) -> T>,
}

impl<T: ArrowNumericType> EvaluatorFactory for ClampEvaluator<T> {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let (input, min, max) = info.unpack_arguments()?;

        Ok(Box::new(Self {
            input,
            min,
            max,
            _phantom: PhantomData,
        }))
    }
}

impl<T: ArrowNumericType> Evaluator for ClampEvaluator<T> {
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let data = info.value(&self.input)?.primitive_array()?;
        let min = info.value(&self.min)?.primitive_array()?;
        let max = info.value(&self.max)?.primitive_array()?;
        let result = clamp::<T>(data.as_ref(), min.as_ref(), max.as_ref())?;
        Ok(result)
    }
}

/// Clamp the elements of `array` to a minimum of the elements of `min`, and a
/// maximum of the elements of `max`.
///
/// An array value that falls between the min and max clamp values will remain
/// unchanged. An array value of `null` will remain `null` regardless of the
/// clamp values. The resulting array will also contain a `null` element if the
/// corresponding `min` value exceeds the corresponding `max` value.
///
/// If the `min` or `max` are `null`, then no clamping on that side will be
/// performed.
///
/// # Errors
/// When passed arrays of differing length.
fn clamp<T>(
    array: &PrimitiveArray<T>,
    min: &PrimitiveArray<T>,
    max: &PrimitiveArray<T>,
) -> anyhow::Result<ArrayRef>
where
    T: ArrowNumericType,
    T::Native: PartialOrd,
{
    anyhow::ensure!(
        array.len() == min.len(),
        "Data array and min array must be the same length."
    );
    anyhow::ensure!(
        array.len() == max.len(),
        "Data array and max array must be the same length."
    );

    let result: PrimitiveArray<T> = izip!(array.iter(), min.iter(), max.iter())
        .map(|(value, min, max)| match (value, min, max) {
            (None, _, _) => None,
            (_, Some(min), Some(max)) if min > max => None,
            (Some(value), Some(min), _) if value < min => Some(min),
            (Some(value), _, Some(max)) if value > max => Some(max),
            (Some(value), _, _) => Some(value),
        })
        .collect();

    Ok(Arc::new(result))
}

#[cfg(test)]
mod tests {
    use arrow::array::Int32Array;
    use arrow::datatypes::Int32Type;

    use super::*;

    #[test]
    fn clamp_mismatched_inputs() {
        let input: PrimitiveArray<Int32Type> = Int32Array::from(vec![Some(1)]);
        let min: PrimitiveArray<Int32Type> = Int32Array::from(vec![Some(1), Some(1)]);
        let max: PrimitiveArray<Int32Type> = Int32Array::from(vec![Some(1)]);
        clamp::<Int32Type>(&input, &min, &max).expect_err("");

        let input: PrimitiveArray<Int32Type> = Int32Array::from(vec![Some(1)]);
        let min: PrimitiveArray<Int32Type> = Int32Array::from(vec![Some(1)]);
        let max: PrimitiveArray<Int32Type> = Int32Array::from(vec![Some(1), Some(1)]);
        clamp::<Int32Type>(&input, &min, &max).expect_err("");
    }

    #[test]
    fn clamp_null_input() {
        let input: PrimitiveArray<Int32Type> = Int32Array::from(vec![None, None, None, None, None]);
        let min: PrimitiveArray<Int32Type> =
            Int32Array::from(vec![None, Some(1), None, Some(1), Some(5)]);
        let max: PrimitiveArray<Int32Type> =
            Int32Array::from(vec![None, Some(5), Some(5), None, Some(1)]);
        let actual = clamp::<Int32Type>(&input, &min, &max).unwrap();
        assert_eq!(
            actual.as_ref(),
            &Int32Array::from(vec![None, None, None, None, None,])
        );
    }

    #[test]
    fn clamp_invalid_bounds() {
        let input: PrimitiveArray<Int32Type> = Int32Array::from(vec![Some(3)]);
        let min: PrimitiveArray<Int32Type> = Int32Array::from(vec![Some(5)]);
        let max: PrimitiveArray<Int32Type> = Int32Array::from(vec![Some(1)]);
        let actual = clamp::<Int32Type>(&input, &min, &max).unwrap();
        assert_eq!(actual.as_ref(), &Int32Array::from(vec![None]));
    }

    #[test]
    fn clamp_to_min() {
        let input: PrimitiveArray<Int32Type> = Int32Array::from(vec![Some(0), Some(0)]);
        let min: PrimitiveArray<Int32Type> = Int32Array::from(vec![Some(1), Some(1)]);
        let max: PrimitiveArray<Int32Type> = Int32Array::from(vec![None, Some(5)]);
        let actual = clamp::<Int32Type>(&input, &min, &max).unwrap();
        assert_eq!(actual.as_ref(), &Int32Array::from(vec![Some(1), Some(1)]));
    }

    #[test]
    fn clamp_to_max() {
        let input: PrimitiveArray<Int32Type> = Int32Array::from(vec![Some(9), Some(9)]);
        let min: PrimitiveArray<Int32Type> = Int32Array::from(vec![None, Some(1)]);
        let max: PrimitiveArray<Int32Type> = Int32Array::from(vec![Some(5), Some(5)]);
        let actual = clamp::<Int32Type>(&input, &min, &max).unwrap();
        assert_eq!(actual.as_ref(), &Int32Array::from(vec![Some(5), Some(5)]));
    }

    #[test]
    fn clamp_inside_bounds() {
        let input: PrimitiveArray<Int32Type> =
            Int32Array::from(vec![Some(3), Some(3), Some(3), Some(3)]);
        let min: PrimitiveArray<Int32Type> = Int32Array::from(vec![None, None, Some(1), Some(1)]);
        let max: PrimitiveArray<Int32Type> = Int32Array::from(vec![None, Some(5), None, Some(5)]);
        let actual = clamp::<Int32Type>(&input, &min, &max).unwrap();
        assert_eq!(
            actual.as_ref(),
            &Int32Array::from(vec![Some(3), Some(3), Some(3), Some(3)])
        );
    }
}
