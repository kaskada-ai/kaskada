use std::sync::Arc;

use arrow_array::{ArrayRef, ArrowNumericType, PrimitiveArray};
use num::Float;

use sparrow_interfaces::expression::{Error, Evaluator, PrimitiveValue, StaticInfo, WorkArea};

inventory::submit!(sparrow_interfaces::expression::EvaluatorFactory {
    name: "ceil",
    create: &crate::macros::create_primitive_evaluator!(0, create, float)
});

/// Evaluator for the `ceil` expression.
///
/// Returns the smallest integer greater than or equal to a number.
struct CeilEvaluator<T: ArrowNumericType>
where
    T::Native: Float,
{
    input: PrimitiveValue<T>,
}

impl<T> Evaluator for CeilEvaluator<T>
where
    T: ArrowNumericType,
    T::Native: Float,
{
    fn evaluate(&self, info: &WorkArea<'_>) -> error_stack::Result<ArrayRef, Error> {
        let input = info.expression(self.input);
        let result = ceil(input);
        Ok(Arc::new(result))
    }
}

fn create<T>(info: StaticInfo<'_>) -> error_stack::Result<Box<dyn Evaluator>, Error>
where
    T: ArrowNumericType,
    T::Native: Float,
{
    let input = info.unpack_argument()?;
    Ok(Box::new(CeilEvaluator::<T> {
        input: input.primitive()?,
    }))
}

/// The ceiling function.
///
/// Returns the smallest integer greater than or equal to a number.
fn ceil<T>(array: &PrimitiveArray<T>) -> PrimitiveArray<T>
where
    T: ArrowNumericType,
    T::Native: Float,
{
    array.unary(|b| b.ceil())
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_relative_eq;
    use arrow_array::types::Float64Type;
    use arrow_array::Float64Array;
    use itertools::izip;

    #[test]
    fn test_ceil_float64() {
        let input: PrimitiveArray<Float64Type> =
            Float64Array::from(vec![Some(1.1), Some(5.2), None, Some(-5.5)]);
        let expected = Float64Array::from(vec![Some(2.0), Some(6.0), None, Some(-5.0)]);
        let actual = ceil(&input);
        for (a, b) in izip!(actual.iter(), expected.iter()) {
            if a.is_none() && b.is_none() {
                continue;
            }
            assert_relative_eq!(a.unwrap(), b.unwrap());
        }
    }
}
