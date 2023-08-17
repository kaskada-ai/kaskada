use std::marker::PhantomData;
use std::sync::Arc;

use crate::ValueRef;
use arrow::array::Array;
use arrow::array::ArrayRef;
use arrow::datatypes::{ArrowNativeTypeOp, ArrowNumericType, DataType};
use sparrow_arrow::scalar_value::NativeFromScalar;

use crate::evaluators::macros::create_typed_evaluator;
use crate::{Evaluator, EvaluatorFactory, RuntimeInfo, StaticInfo};

pub(super) struct EqEvaluatorFactory;
pub(super) struct NeqEvaluatorFactory;

impl EvaluatorFactory for EqEvaluatorFactory {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        create_typed_evaluator!(
            &info.args[0].data_type,
            NumericEqEvaluator,
            DynEqEvaluator,
            DynEqEvaluator,
            DynEqEvaluator,
            BoolEqEvaluator,
            StringEqEvaluator,
            info
        )
    }
}

impl EvaluatorFactory for NeqEvaluatorFactory {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        create_typed_evaluator!(
            &info.args[0].data_type,
            NumericNeqEvaluator,
            DynNeqEvaluator,
            DynNeqEvaluator,
            DynNeqEvaluator,
            BoolNeqEvaluator,
            StringNeqEvaluator,
            info
        )
    }
}

/// Evaluator for `eq` on using `eq_dyn`
struct DynEqEvaluator {
    lhs: ValueRef,
    rhs: ValueRef,
}

impl Evaluator for DynEqEvaluator {
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let lhs = info.value(&self.lhs)?.array_ref()?;
        let rhs = info.value(&self.rhs)?.array_ref()?;
        let result = arrow::compute::eq_dyn(lhs.as_ref(), rhs.as_ref())?;
        Ok(Arc::new(result))
    }
}

impl EvaluatorFactory for DynEqEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let (lhs, rhs) = info.unpack_arguments()?;
        Ok(Box::new(Self { lhs, rhs }))
    }
}

/// Evaluator for `neq` using `neq_dyn`
struct DynNeqEvaluator {
    lhs: ValueRef,
    rhs: ValueRef,
}

impl Evaluator for DynNeqEvaluator {
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let lhs = info.value(&self.lhs)?.array_ref()?;
        let rhs = info.value(&self.rhs)?.array_ref()?;
        let result = arrow::compute::neq_dyn(lhs.as_ref(), rhs.as_ref())?;
        Ok(Arc::new(result))
    }
}

impl EvaluatorFactory for DynNeqEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let (lhs, rhs) = info.unpack_arguments()?;
        Ok(Box::new(Self { lhs, rhs }))
    }
}

/// Evaluator for `eq` on booleans.
struct BoolEqEvaluator {
    lhs: ValueRef,
    rhs: ValueRef,
}

impl Evaluator for BoolEqEvaluator {
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let lhs = info.value(&self.lhs)?;
        let rhs = info.value(&self.rhs)?;

        let result = if let Ok(lhs_literal) = lhs.try_boolean_literal() {
            let rhs = rhs.boolean_array()?;
            if let Some(lhs_literal) = lhs_literal {
                Arc::new(arrow::compute::eq_bool_scalar(rhs.as_ref(), lhs_literal)?)
            } else {
                // The LHS is `null`, which in Kleene logic is "unknown".
                // The result of equality is thus always "unknown" (`null`).
                arrow::array::new_null_array(&DataType::Boolean, rhs.len())
            }
        } else if let Ok(rhs_literal) = rhs.try_boolean_literal() {
            let lhs = lhs.boolean_array()?;
            if let Some(rhs_literal) = rhs_literal {
                Arc::new(arrow::compute::eq_bool_scalar(lhs.as_ref(), rhs_literal)?)
            } else {
                // The RHS is `null`, which in Kleene logic is "unknown".
                // The result of equality is thus always "unknown" (`null`).
                arrow::array::new_null_array(&DataType::Boolean, lhs.len())
            }
        } else {
            let lhs = lhs.boolean_array()?;
            let rhs = rhs.boolean_array()?;
            Arc::new(arrow::compute::eq_bool(lhs.as_ref(), rhs.as_ref())?)
        };

        Ok(result)
    }
}

impl EvaluatorFactory for BoolEqEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let (lhs, rhs) = info.unpack_arguments()?;
        Ok(Box::new(Self { lhs, rhs }))
    }
}

/// Evaluator for `eq` on primitive types.
struct NumericEqEvaluator<T: ArrowNumericType> {
    lhs: ValueRef,
    rhs: ValueRef,
    // Use the type parameter and indicate it is invariant.
    _phantom: PhantomData<fn(T) -> T>,
}

impl<T: ArrowNumericType + NativeFromScalar> Evaluator for NumericEqEvaluator<T>
where
    T::Native: ArrowNativeTypeOp,
{
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        // TODO: Create a `NumericEqScalarEvaluator` for this case and
        // make the switch at evaluator creation time.

        let lhs = info.value(&self.lhs)?;
        let rhs = info.value(&self.rhs)?;

        let result = if let Ok(lhs_literal) = lhs.try_primitive_literal::<T>() {
            let rhs = rhs.primitive_array()?;
            if let Some(lhs_literal) = lhs_literal {
                Arc::new(arrow::compute::eq_scalar::<T>(rhs.as_ref(), lhs_literal)?)
            } else {
                // The LHS is `null`, which in Kleene logic is "unknown".
                // The result of equality is thus always "unknown" (`null`).
                arrow::array::new_null_array(&DataType::Boolean, rhs.len())
            }
        } else if let Ok(rhs_literal) = rhs.try_primitive_literal::<T>() {
            let lhs = lhs.primitive_array()?;
            if let Some(rhs_literal) = rhs_literal {
                Arc::new(arrow::compute::eq_scalar::<T>(lhs.as_ref(), rhs_literal)?)
            } else {
                // The RHS is `null`, which in Kleene logic is "unknown".
                // The result of equality is thus always "unknown" (`null`).
                arrow::array::new_null_array(&DataType::Boolean, lhs.len())
            }
        } else {
            let lhs = lhs.primitive_array()?;
            let rhs = rhs.primitive_array()?;
            Arc::new(arrow::compute::eq::<T>(lhs.as_ref(), rhs.as_ref())?)
        };

        Ok(result)
    }
}

impl<T: ArrowNumericType + NativeFromScalar> EvaluatorFactory for NumericEqEvaluator<T>
where
    T::Native: ArrowNativeTypeOp,
{
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let (lhs, rhs) = info.unpack_arguments()?;
        Ok(Box::new(Self {
            lhs,
            rhs,
            _phantom: PhantomData,
        }))
    }
}

/// Evaluator for `eq` on strings types.
struct StringEqEvaluator {
    lhs: ValueRef,
    rhs: ValueRef,
}

impl Evaluator for StringEqEvaluator {
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let lhs = info.value(&self.lhs)?;
        let rhs = info.value(&self.rhs)?;

        let result = if let Ok(lhs_literal) = lhs.try_string_literal() {
            let rhs = rhs.string_array()?;
            if let Some(lhs_literal) = lhs_literal {
                Arc::new(arrow::compute::eq_utf8_scalar::<i32>(
                    rhs.as_ref(),
                    lhs_literal,
                )?)
            } else {
                // The LHS is `null`, which in Kleene logic is "unknown".
                // The result of equality is thus always "unknown" (`null`).
                arrow::array::new_null_array(&DataType::Boolean, rhs.len())
            }
        } else if let Ok(rhs_literal) = rhs.try_string_literal() {
            let lhs = lhs.string_array()?;
            if let Some(rhs_literal) = rhs_literal {
                Arc::new(arrow::compute::eq_utf8_scalar::<i32>(
                    lhs.as_ref(),
                    rhs_literal,
                )?)
            } else {
                // The RHS is `null`, which in Kleene logic is "unknown".
                // The result of equality is thus always "unknown" (`null`).
                arrow::array::new_null_array(&DataType::Boolean, lhs.len())
            }
        } else {
            let lhs = lhs.string_array()?;
            let rhs = rhs.string_array()?;
            Arc::new(arrow::compute::eq_utf8::<i32>(lhs.as_ref(), rhs.as_ref())?)
        };

        Ok(result)
    }
}

impl EvaluatorFactory for StringEqEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let (lhs, rhs) = info.unpack_arguments()?;
        Ok(Box::new(Self { lhs, rhs }))
    }
}

/// Evaluator for `neq` on booleans.
struct BoolNeqEvaluator {
    lhs: ValueRef,
    rhs: ValueRef,
}

impl Evaluator for BoolNeqEvaluator {
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let lhs = info.value(&self.lhs)?;
        let rhs = info.value(&self.rhs)?;

        let result = if let Ok(lhs_literal) = lhs.try_boolean_literal() {
            let rhs = rhs.boolean_array()?;
            if let Some(lhs_literal) = lhs_literal {
                Arc::new(arrow::compute::neq_bool_scalar(rhs.as_ref(), lhs_literal)?)
            } else {
                // The LHS is `null`, which in Kleene logic is "unknown".
                // The result of equality is thus always "unknown" (`null`).
                arrow::array::new_null_array(&DataType::Boolean, rhs.len())
            }
        } else if let Ok(rhs_literal) = rhs.try_boolean_literal() {
            let lhs = lhs.boolean_array()?;
            if let Some(rhs_literal) = rhs_literal {
                Arc::new(arrow::compute::neq_bool_scalar(lhs.as_ref(), rhs_literal)?)
            } else {
                // The RHS is `null`, which in Kleene logic is "unknown".
                // The result of equality is thus always "unknown" (`null`).
                arrow::array::new_null_array(&DataType::Boolean, lhs.len())
            }
        } else {
            let lhs = lhs.boolean_array()?;
            let rhs = rhs.boolean_array()?;
            Arc::new(arrow::compute::neq_bool(lhs.as_ref(), rhs.as_ref())?)
        };

        Ok(result)
    }
}

impl EvaluatorFactory for BoolNeqEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let (lhs, rhs) = info.unpack_arguments()?;
        Ok(Box::new(Self { lhs, rhs }))
    }
}

/// Evaluator for `neq` on primitive types.
struct NumericNeqEvaluator<T: ArrowNumericType> {
    lhs: ValueRef,
    rhs: ValueRef,
    // Use the type parameter and indicate it is invariant.
    _phantom: PhantomData<fn(T) -> T>,
}

impl<T: ArrowNumericType + NativeFromScalar> Evaluator for NumericNeqEvaluator<T>
where
    T::Native: ArrowNativeTypeOp,
{
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        // TODO: Create a `NumericEqScalarEvaluator` for this case and
        // make the switch at evaluator creation time.

        let lhs = info.value(&self.lhs)?;
        let rhs = info.value(&self.rhs)?;

        let result = if let Ok(lhs_literal) = lhs.try_primitive_literal::<T>() {
            let rhs = rhs.primitive_array()?;
            if let Some(lhs_literal) = lhs_literal {
                Arc::new(arrow::compute::neq_scalar::<T>(rhs.as_ref(), lhs_literal)?)
            } else {
                // The LHS is `null`, which in Kleene logic is "unknown".
                // The result of equality is thus always "unknown" (`null`).
                arrow::array::new_null_array(&DataType::Boolean, rhs.len())
            }
        } else if let Ok(rhs_literal) = rhs.try_primitive_literal::<T>() {
            let lhs = lhs.primitive_array()?;
            if let Some(rhs_literal) = rhs_literal {
                Arc::new(arrow::compute::neq_scalar::<T>(lhs.as_ref(), rhs_literal)?)
            } else {
                // The RHS is `null`, which in Kleene logic is "unknown".
                // The result of equality is thus always "unknown" (`null`).
                arrow::array::new_null_array(&DataType::Boolean, lhs.len())
            }
        } else {
            let lhs = lhs.primitive_array()?;
            let rhs = rhs.primitive_array()?;
            Arc::new(arrow::compute::neq::<T>(lhs.as_ref(), rhs.as_ref())?)
        };

        Ok(result)
    }
}

impl<T: ArrowNumericType + NativeFromScalar> EvaluatorFactory for NumericNeqEvaluator<T>
where
    T::Native: ArrowNativeTypeOp,
{
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let (lhs, rhs) = info.unpack_arguments()?;
        Ok(Box::new(Self {
            lhs,
            rhs,
            _phantom: PhantomData,
        }))
    }
}

/// Evaluator for `neq` on strings types.
struct StringNeqEvaluator {
    lhs: ValueRef,
    rhs: ValueRef,
}

impl Evaluator for StringNeqEvaluator {
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let lhs = info.value(&self.lhs)?;
        let rhs = info.value(&self.rhs)?;

        let result = if let Ok(lhs_literal) = lhs.try_string_literal() {
            let rhs = rhs.string_array()?;
            if let Some(lhs_literal) = lhs_literal {
                Arc::new(arrow::compute::neq_utf8_scalar::<i32>(
                    rhs.as_ref(),
                    lhs_literal,
                )?)
            } else {
                // The LHS is `null`, which in Kleene logic is "unknown".
                // The result of equality is thus always "unknown" (`null`).
                arrow::array::new_null_array(&DataType::Boolean, rhs.len())
            }
        } else if let Ok(rhs_literal) = rhs.try_string_literal() {
            let lhs = lhs.string_array()?;
            if let Some(rhs_literal) = rhs_literal {
                Arc::new(arrow::compute::neq_utf8_scalar::<i32>(
                    lhs.as_ref(),
                    rhs_literal,
                )?)
            } else {
                // The RHS is `null`, which in Kleene logic is "unknown".
                // The result of equality is thus always "unknown" (`null`).
                arrow::array::new_null_array(&DataType::Boolean, lhs.len())
            }
        } else {
            let lhs = lhs.string_array()?;
            let rhs = rhs.string_array()?;
            Arc::new(arrow::compute::neq_utf8::<i32>(lhs.as_ref(), rhs.as_ref())?)
        };

        Ok(result)
    }
}

impl EvaluatorFactory for StringNeqEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let (lhs, rhs) = info.unpack_arguments()?;
        Ok(Box::new(Self { lhs, rhs }))
    }
}
