use std::marker::PhantomData;
use std::sync::Arc;

use crate::ValueRef;
use arrow::array::{ArrayRef, PrimitiveArray};
use arrow::compute::math_op;
use arrow::datatypes::{ArrowNativeTypeOp, ArrowNumericType};
use num::traits::Pow;
use sparrow_arrow::scalar_value::NativeFromScalar;

use crate::{Evaluator, EvaluatorFactory, RuntimeInfo, StaticInfo};

/// Evaluator for Powf.
pub(in crate::evaluators) struct PowfEvaluator<T: ArrowNumericType> {
    base: ValueRef,
    exp: ValueRef,
    // Use the type parameter and indicate it is invariant.
    _phantom: PhantomData<fn(T) -> T>,
}

impl<T: ArrowNumericType> Evaluator for PowfEvaluator<T>
where
    T: NativeFromScalar,
    T::Native: ArrowNativeTypeOp + num::pow::Pow<T::Native, Output = T::Native>,
{
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let base = info.value(&self.base)?.primitive_array::<T>()?;
        let exp = info.value(&self.exp)?;
        let result = match exp.try_primitive_literal::<T>() {
            Ok(Some(exp_literal)) => Arc::new(powf_scalar(base.as_ref(), exp_literal)?),
            Ok(None) => {
                // Raised to the null power is always null
                arrow::array::new_null_array(&T::DATA_TYPE, base.len())
            }
            Err(_) => {
                let exp = exp.primitive_array::<T>()?;
                Arc::new(powf(base.as_ref(), exp.as_ref())?)
            }
        };
        Ok(result)
    }
}

/// Raise array with floating point values to the power of a scalar.
fn powf_scalar<T>(array: &PrimitiveArray<T>, raise: T::Native) -> anyhow::Result<PrimitiveArray<T>>
where
    T: ArrowNumericType,
    T::Native: Pow<T::Native, Output = T::Native>,
{
    // TODO: Update this to use `unary_opt` and `null` on overflow.
    Ok(array.unary(|x| x.pow(raise)))
}

impl<T: ArrowNumericType> EvaluatorFactory for PowfEvaluator<T>
where
    T: NativeFromScalar,
    T::Native: ArrowNativeTypeOp + num::pow::Pow<T::Native, Output = T::Native>,
{
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let (base, exp) = info.unpack_arguments()?;
        Ok(Box::new(Self {
            base,
            exp,
            _phantom: PhantomData,
        }))
    }
}

/// Raise the elements of base to the power of the elements of exp. Returns
/// a(n)^b(n) for all n in the input arrays. This is provided as a supplement to
/// the built-in Arrow kernel which operates on a base array and scalar
/// exponent. By convention 0^0 returns 1.
///
/// # Errors
/// When passed arrays of differing length.
/// When passed base or exponent arrays containing non-float types.
fn powf<T>(base: &PrimitiveArray<T>, exp: &PrimitiveArray<T>) -> anyhow::Result<PrimitiveArray<T>>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp + Pow<T::Native, Output = T::Native>,
{
    let result = math_op(base, exp, |b, e| b.pow(e))?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use arrow::array::Float32Array;
    use arrow::datatypes::Float32Type;

    use super::*;

    #[test]
    fn test_powf_float32() {
        let left: PrimitiveArray<Float32Type> =
            Float32Array::from(vec![Some(1.0), Some(5.0), Some(0.0), None]);
        let right: PrimitiveArray<Float32Type> =
            Float32Array::from(vec![Some(5.0), Some(2.0), Some(0.0), None]);
        let actual = powf::<Float32Type>(&left, &right).unwrap();
        assert_eq!(
            &actual,
            &Float32Array::from(vec![Some(1.0), Some(25.0), Some(1.0), None])
        );
    }
}
