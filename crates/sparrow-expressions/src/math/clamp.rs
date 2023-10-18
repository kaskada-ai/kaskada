use std::sync::Arc;

use arrow_array::{ArrayRef, ArrowNumericType, PrimitiveArray};
use itertools::izip;

use sparrow_interfaces::expression::{Error, Evaluator, PrimitiveValue, StaticInfo, WorkArea};

inventory::submit!(sparrow_interfaces::expression::EvaluatorFactory {
    name: "clamp",
    create: &crate::macros::create_primitive_evaluator!(0, create, ordered)
});

/// Evaluator for Round.
struct ClampEvaluator<T: ArrowNumericType> {
    input: PrimitiveValue<T>,
    min: PrimitiveValue<T>,
    max: PrimitiveValue<T>,
}

impl<T: ArrowNumericType> Evaluator for ClampEvaluator<T> {
    fn evaluate(&self, info: &WorkArea<'_>) -> error_stack::Result<ArrayRef, Error> {
        let data = info.expression(self.input);
        let min = info.expression(self.min);
        let max = info.expression(self.max);
        let result = clamp::<T>(data, min, max)?;
        Ok(Arc::new(result))
    }
}
fn create<T: ArrowNumericType>(
    info: StaticInfo<'_>,
) -> error_stack::Result<Box<dyn Evaluator>, Error> {
    let (input, min, max) = info.unpack_arguments()?;

    Ok(Box::new(ClampEvaluator::<T> {
        input: input.primitive()?,
        min: min.primitive()?,
        max: max.primitive()?,
    }))
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
) -> error_stack::Result<PrimitiveArray<T>, Error>
where
    T: ArrowNumericType,
    T::Native: PartialOrd,
{
    error_stack::ensure!(
        array.len() == min.len(),
        Error::MismatchedLengths {
            a_label: "array",
            a_len: array.len(),
            b_label: "min",
            b_len: min.len()
        }
    );
    error_stack::ensure!(
        array.len() == max.len(),
        Error::MismatchedLengths {
            a_label: "array",
            a_len: array.len(),
            b_label: "max",
            b_len: max.len()
        }
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

    Ok(result)
}

#[cfg(test)]
mod tests {
    use arrow_array::types::Int32Type;
    use arrow_array::Int32Array;

    use super::*;

    #[test]
    fn clamp_mismatched_inputs() {
        let input: PrimitiveArray<Int32Type> = Int32Array::from(vec![Some(1)]);
        let min: PrimitiveArray<Int32Type> = Int32Array::from(vec![Some(1), Some(1)]);
        let max: PrimitiveArray<Int32Type> = Int32Array::from(vec![Some(1)]);
        let _ = clamp::<Int32Type>(&input, &min, &max).expect_err("");

        let input: PrimitiveArray<Int32Type> = Int32Array::from(vec![Some(1)]);
        let min: PrimitiveArray<Int32Type> = Int32Array::from(vec![Some(1)]);
        let max: PrimitiveArray<Int32Type> = Int32Array::from(vec![Some(1), Some(1)]);
        let _ = clamp::<Int32Type>(&input, &min, &max).expect_err("");
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
            &actual,
            &Int32Array::from(vec![None, None, None, None, None,])
        );
    }

    #[test]
    fn clamp_invalid_bounds() {
        let input: PrimitiveArray<Int32Type> = Int32Array::from(vec![Some(3)]);
        let min: PrimitiveArray<Int32Type> = Int32Array::from(vec![Some(5)]);
        let max: PrimitiveArray<Int32Type> = Int32Array::from(vec![Some(1)]);
        let actual = clamp::<Int32Type>(&input, &min, &max).unwrap();
        assert_eq!(&actual, &Int32Array::from(vec![None]));
    }

    #[test]
    fn clamp_to_min() {
        let input: PrimitiveArray<Int32Type> = Int32Array::from(vec![Some(0), Some(0)]);
        let min: PrimitiveArray<Int32Type> = Int32Array::from(vec![Some(1), Some(1)]);
        let max: PrimitiveArray<Int32Type> = Int32Array::from(vec![None, Some(5)]);
        let actual = clamp::<Int32Type>(&input, &min, &max).unwrap();
        assert_eq!(&actual, &Int32Array::from(vec![Some(1), Some(1)]));
    }

    #[test]
    fn clamp_to_max() {
        let input: PrimitiveArray<Int32Type> = Int32Array::from(vec![Some(9), Some(9)]);
        let min: PrimitiveArray<Int32Type> = Int32Array::from(vec![None, Some(1)]);
        let max: PrimitiveArray<Int32Type> = Int32Array::from(vec![Some(5), Some(5)]);
        let actual = clamp::<Int32Type>(&input, &min, &max).unwrap();
        assert_eq!(&actual, &Int32Array::from(vec![Some(5), Some(5)]));
    }

    #[test]
    fn clamp_inside_bounds() {
        let input: PrimitiveArray<Int32Type> =
            Int32Array::from(vec![Some(3), Some(3), Some(3), Some(3)]);
        let min: PrimitiveArray<Int32Type> = Int32Array::from(vec![None, None, Some(1), Some(1)]);
        let max: PrimitiveArray<Int32Type> = Int32Array::from(vec![None, Some(5), None, Some(5)]);
        let actual = clamp::<Int32Type>(&input, &min, &max).unwrap();
        assert_eq!(
            &actual,
            &Int32Array::from(vec![Some(3), Some(3), Some(3), Some(3)])
        );
    }
}
