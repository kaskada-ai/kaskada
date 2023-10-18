use std::sync::Arc;

use arrow_array::{ArrayRef, ArrowNumericType, PrimitiveArray};
use num::Float;

use sparrow_interfaces::expression::{Error, Evaluator, PrimitiveValue, StaticInfo, WorkArea};

inventory::submit!(sparrow_interfaces::expression::EvaluatorFactory {
    name: "round",
    create: &crate::macros::create_primitive_evaluator!(0, create, float)
});

/// Evaluator for the `round` expression.
///
/// Returns the nearest integer to a number. Round half-way cases away from
/// `0.0`.
struct RoundEvaluator<T>
where
    T::Native: Float,
    T: ArrowNumericType,
{
    input: PrimitiveValue<T>,
}

impl<T> Evaluator for RoundEvaluator<T>
where
    T: ArrowNumericType,
    T::Native: Float,
{
    fn evaluate(&self, info: &WorkArea<'_>) -> error_stack::Result<ArrayRef, Error> {
        let input = info.expression(self.input);
        let result = round(input);
        Ok(Arc::new(result))
    }
}

fn create<T>(info: StaticInfo<'_>) -> error_stack::Result<Box<dyn Evaluator>, Error>
where
    T: ArrowNumericType,
    T::Native: Float,
{
    let input = info.unpack_argument()?;
    Ok(Box::new(RoundEvaluator::<T> {
        input: input.primitive()?,
    }))
}

/// The round function.
///
/// Returns the nearest integer to a number. Round half-way cases away from
/// `0.0`.
fn round<T>(array: &PrimitiveArray<T>) -> PrimitiveArray<T>
where
    T: ArrowNumericType,
    T::Native: Float,
{
    array.unary(|b| b.round())
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_relative_eq;
    use arrow_array::types::Float64Type;
    use arrow_array::Float64Array;
    use itertools::izip;

    #[test]
    fn test_round_float64() {
        let input: PrimitiveArray<Float64Type> =
            Float64Array::from(vec![Some(1.1), Some(5.7), None, Some(-5.5), Some(6.5)]);
        let expected = Float64Array::from(vec![Some(1.0), Some(6.0), None, Some(-6.0), Some(7.0)]);
        let actual = round(&input);
        for (a, b) in izip!(actual.iter(), expected.iter()) {
            if a.is_none() && b.is_none() {
                continue;
            }
            assert_relative_eq!(a.unwrap(), b.unwrap());
        }
    }
}
