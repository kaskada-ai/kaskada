use std::convert::Infallible;
use std::marker::PhantomData;
use std::sync::Arc;

use crate::ValueRef;
use arrow::array::{ArrayRef, PrimitiveArray};
use arrow::compute::kernels::arity::unary;
use arrow::datatypes::ArrowNumericType;
use num::traits::Float;

use crate::{Evaluator, EvaluatorFactory, RuntimeInfo, StaticInfo};

/// Evaluator for Exp.
pub(in crate::evaluators) struct ExpEvaluator<T: ArrowNumericType>
where
    T::Native: num::Float,
{
    input: ValueRef,
    // Use the type parameter and indicate it is invariant.
    _phantom: PhantomData<fn(T) -> T>,
}

impl<T: ArrowNumericType> Evaluator for ExpEvaluator<T>
where
    T::Native: num::Float,
{
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let input = info.value(&self.input)?.primitive_array::<T>()?;
        let result = exp(&input)?;
        Ok(Arc::new(result))
    }
}

impl<T: ArrowNumericType> EvaluatorFactory for ExpEvaluator<T>
where
    T::Native: num::Float,
{
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let input = info.unpack_argument()?;
        Ok(Box::new(Self {
            input,
            _phantom: PhantomData,
        }))
    }
}

/// The exponential function, or inverse log. Returns e^(input).
///
/// # Errors
/// When passed a non-numeric array type.
fn exp<T>(array: &PrimitiveArray<T>) -> Result<PrimitiveArray<T>, Infallible>
where
    T: ArrowNumericType,
    T::Native: Float,
{
    Ok(unary(array, |b| b.exp()))
}

#[cfg(test)]
mod tests {
    use approx::assert_relative_eq;
    use arrow::array::Float64Array;
    use arrow::datatypes::Float64Type;
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
        let actual = exp(&input).unwrap();
        for (a, b) in izip!(actual.iter(), expected.iter()) {
            assert_relative_eq!(a.unwrap(), b.unwrap());
        }
    }
}
