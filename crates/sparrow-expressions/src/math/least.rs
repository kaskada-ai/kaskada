use std::sync::Arc;

use arrow_array::{ArrayRef, ArrowNativeTypeOp, ArrowNumericType, PrimitiveArray};
use error_stack::{IntoReport, ResultExt};

use sparrow_interfaces::expression::{Error, Evaluator, PrimitiveValue, StaticInfo, WorkArea};

inventory::submit!(sparrow_interfaces::expression::EvaluatorFactory {
    name: "least",
    create: &crate::macros::create_primitive_evaluator!(0, create, number)
});

/// Evaluator for the `least` (min) instruction.
struct LeastEvaluator<T: ArrowNumericType> {
    lhs: PrimitiveValue<T>,
    rhs: PrimitiveValue<T>,
}

impl<T: ArrowNumericType> Evaluator for LeastEvaluator<T> {
    fn evaluate(&self, info: &WorkArea<'_>) -> error_stack::Result<ArrayRef, Error> {
        let lhs = info.expression(self.lhs);
        let rhs = info.expression(self.rhs);
        let result = arg_min(lhs, rhs)?;
        Ok(Arc::new(result))
    }
}
fn create<T: ArrowNumericType>(
    info: StaticInfo<'_>,
) -> error_stack::Result<Box<dyn Evaluator>, Error> {
    let (lhs, rhs) = info.unpack_arguments()?;
    Ok(Box::new(LeastEvaluator::<T> {
        lhs: lhs.primitive()?,
        rhs: rhs.primitive()?,
    }))
}

/// Return the per-element minimum of two numeric arrays. If a < b, a is
/// returned. Otherwise b.
///
/// Note that this operates on types implementing the PartialOrd trait, which
/// can have possibly surprising behavior when ordering certain floating-point
/// types e.g. NaN. See <https://doc.rust-lang.org/std/cmp/trait.PartialOrd.html#tymethod.partial_cmp> for specifics.
///
/// # Errors
/// When passed arrays of differing length.
fn arg_min<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> error_stack::Result<PrimitiveArray<T>, Error>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
    arrow_arith::arity::binary(left, right, |a, b| if a < b { a } else { b })
        .into_report()
        .change_context(Error::ExprEvaluation)
}

#[cfg(test)]
mod tests {
    use arrow_array::types::{Float32Type, Int32Type};
    use arrow_array::{Float32Array, Int32Array};

    use super::*;

    #[test]
    fn test_arg_min_int32() {
        let left: PrimitiveArray<Int32Type> = Int32Array::from(vec![Some(1), Some(5), None]);
        let right: PrimitiveArray<Int32Type> = Int32Array::from(vec![Some(0), Some(6), None]);
        let actual = arg_min::<Int32Type>(&left, &right).unwrap();
        assert_eq!(&actual, &Int32Array::from(vec![Some(0), Some(5), None]));
    }

    #[test]
    fn test_arg_min_float32() {
        let left: PrimitiveArray<Float32Type> =
            Float32Array::from(vec![Some(1.0), Some(-5.1), None]);
        let right: PrimitiveArray<Float32Type> =
            Float32Array::from(vec![Some(0.5), Some(-5.0), None]);
        let actual = arg_min::<Float32Type>(&left, &right).unwrap();
        assert_eq!(
            &actual,
            &Float32Array::from(vec![Some(0.5), Some(-5.1), None])
        );
    }
}
