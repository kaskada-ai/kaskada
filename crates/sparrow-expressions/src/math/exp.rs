use std::sync::Arc;

use arrow_array::{ArrayRef, ArrowNumericType, PrimitiveArray};
use num::Float;

use sparrow_interfaces::expression::{Error, Evaluator, PrimitiveValue, StaticInfo, WorkArea};

inventory::submit!(sparrow_interfaces::expression::EvaluatorFactory {
    name: "exp",
    create: &crate::macros::create_primitive_evaluator!(0, create, float)
});

/// Evaluator for the `exp` expression.
///
/// The exponential function, or inverse log. Returns e^(input).
struct ExpEvaluator<T: ArrowNumericType>
where
    T::Native: Float,
{
    input: PrimitiveValue<T>,
}

impl<T> Evaluator for ExpEvaluator<T>
where
    T: ArrowNumericType,
    T::Native: Float,
{
    fn evaluate(&self, info: &WorkArea<'_>) -> error_stack::Result<ArrayRef, Error> {
        let input = info.expression(self.input);
        let result = exp(input);
        Ok(Arc::new(result))
    }
}

fn create<T>(info: StaticInfo<'_>) -> error_stack::Result<Box<dyn Evaluator>, Error>
where
    T: ArrowNumericType,
    T::Native: Float,
{
    let input = info.unpack_argument()?;
    Ok(Box::new(ExpEvaluator::<T> {
        input: input.primitive()?,
    }))
}

/// The exponential function, or inverse log. Returns e^(input).
fn exp<T>(array: &PrimitiveArray<T>) -> PrimitiveArray<T>
where
    T: ArrowNumericType,
    T::Native: Float,
{
    array.unary(|b| b.exp())
}

#[cfg(test)]
mod tests {
    use approx::assert_relative_eq;
    use arrow_array::types::Float64Type;
    use arrow_array::Float64Array;
    use itertools::izip;
    use num::traits::Pow;

    use super::*;

    #[test]
    fn test_exp_float64() {
        let input: PrimitiveArray<Float64Type> =
            Float64Array::from(vec![Some(1.0), Some(5.0), Some(0.0)]);
        let e: f64 = std::f64::consts::E;
        let expected =
            Float64Array::from(vec![Some(std::f64::consts::E), Some(e.pow(5.0)), Some(1.0)]);
        let actual = exp(&input);
        for (a, b) in izip!(actual.iter(), expected.iter()) {
            assert_relative_eq!(a.unwrap(), b.unwrap());
        }
    }
}
