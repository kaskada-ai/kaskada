use std::sync::Arc;

use arrow_array::{ArrayRef, ArrowNumericType, PrimitiveArray};
use error_stack::{IntoReport, ResultExt};
use num::traits::Pow;

use sparrow_interfaces::expression::{Error, Evaluator, PrimitiveValue, StaticInfo, WorkArea};

inventory::submit!(sparrow_interfaces::expression::EvaluatorFactory {
    name: "powf",
    create: &crate::macros::create_primitive_evaluator!(
        0,
        create,
        // Currently, the signature for powf forces f64, so we only instantiate that case.
        (arrow_schema::DataType::Float64, Float64Type)
    )
});

/// Evaluator for `powf`.
///
/// Returns `base[n]^exp[n]` for all n in the input arrays.
struct PowfEvaluator<T: ArrowNumericType> {
    base: PrimitiveValue<T>,
    exp: PrimitiveValue<T>,
}

impl<T> Evaluator for PowfEvaluator<T>
where
    T: ArrowNumericType,
    T::Native: num::pow::Pow<T::Native, Output = T::Native>,
{
    fn evaluate(&self, work_area: &WorkArea<'_>) -> error_stack::Result<ArrayRef, Error> {
        let base = work_area.expression(self.base);
        let exp = work_area.expression(self.exp);
        let result = powf(base, exp)?;
        Ok(Arc::new(result))
    }
}

fn create<T>(info: StaticInfo<'_>) -> error_stack::Result<Box<dyn Evaluator>, Error>
where
    T: ArrowNumericType,
    T::Native: num::pow::Pow<T::Native, Output = T::Native>,
{
    let (base, exp) = info.unpack_arguments()?;
    Ok(Box::new(PowfEvaluator::<T> {
        base: base.primitive()?,
        exp: exp.primitive()?,
    }))
}

/// Raise the elements of base to the power of the elements of exp.
///
/// Returns `base[n]^exp[n]` for all n in the input arrays.
///
/// This is provided as a supplement to the built-in Arrow kernel which operates
/// on a base array and scalar exponent. By convention 0^0 returns 1.
///
/// # Errors
/// When passed arrays of differing length.
/// When passed base or exponent arrays containing non-float types.
fn powf<T>(
    base: &PrimitiveArray<T>,
    exp: &PrimitiveArray<T>,
) -> error_stack::Result<PrimitiveArray<T>, Error>
where
    T: ArrowNumericType,
    T::Native: num::pow::Pow<T::Native, Output = T::Native>,
{
    let result = arrow_arith::arity::binary(base, exp, |b, e| b.pow(e))
        .into_report()
        .change_context(Error::ExprEvaluation)?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use arrow_array::types::Float32Type;
    use arrow_array::Float32Array;

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
