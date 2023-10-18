use std::sync::Arc;

use arrow_array::{ArrayRef, ArrowNumericType, PrimitiveArray};
use num::Float;

use sparrow_interfaces::expression::{Error, Evaluator, PrimitiveValue, StaticInfo, WorkArea};

inventory::submit!(sparrow_interfaces::expression::EvaluatorFactory {
    name: "floor",
    create: &crate::macros::create_primitive_evaluator!(0, create, float)
});

/// Evaluator for the `floor` expression.
///
/// Returns the largest integer less than or equal to a number.
struct FloorEvaluator<T: ArrowNumericType>
where
    T::Native: Float,
{
    input: PrimitiveValue<T>,
}

impl<T> Evaluator for FloorEvaluator<T>
where
    T: ArrowNumericType,
    T::Native: Float,
{
    fn evaluate(&self, info: &WorkArea<'_>) -> error_stack::Result<ArrayRef, Error> {
        let input = info.expression(self.input);
        let result = floor(input);
        Ok(Arc::new(result))
    }
}

fn create<T>(info: StaticInfo<'_>) -> error_stack::Result<Box<dyn Evaluator>, Error>
where
    T: ArrowNumericType,
    T::Native: Float,
{
    let input = info.unpack_argument()?;
    Ok(Box::new(FloorEvaluator::<T> {
        input: input.primitive()?,
    }))
}

/// The floor function.
///
/// Returns the largest integer less than or equal to a number.
fn floor<T>(array: &PrimitiveArray<T>) -> PrimitiveArray<T>
where
    T: ArrowNumericType,
    T::Native: Float,
{
    array.unary(|b| b.floor())
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_relative_eq;
    use arrow_array::types::Float64Type;
    use arrow_array::Float64Array;
    use itertools::izip;

    #[test]
    fn test_floor_float64() {
        let input: PrimitiveArray<Float64Type> =
            Float64Array::from(vec![Some(1.1), Some(5.2), None, Some(-5.5)]);
        let expected = Float64Array::from(vec![Some(1.0), Some(5.0), None, Some(-6.0)]);
        let actual = floor(&input);
        for (a, b) in izip!(actual.iter(), expected.iter()) {
            if a.is_none() && b.is_none() {
                continue;
            }
            assert_relative_eq!(a.unwrap(), b.unwrap());
        }
    }
}
