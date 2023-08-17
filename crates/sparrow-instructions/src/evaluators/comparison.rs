use std::marker::PhantomData;
use std::sync::Arc;

use crate::ValueRef;
use anyhow::anyhow;
use arrow::array::ArrayRef;
use arrow::datatypes::{ArrowNativeTypeOp, ArrowNumericType};
use sparrow_arrow::scalar_value::NativeFromScalar;

use crate::{Evaluator, EvaluatorFactory, RuntimeInfo, StaticInfo};

/// Evaluator for the `gt` instruction.
pub(super) struct GtEvaluator<T: ArrowNumericType> {
    lhs: ValueRef,
    rhs: ValueRef,
    // Use the type parameter and indicate it is invariant.
    _phantom: PhantomData<fn(T) -> T>,
}

impl<T> Evaluator for GtEvaluator<T>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let lhs = info.value(&self.lhs)?.primitive_array()?;
        let rhs = info.value(&self.rhs)?.primitive_array()?;

        let result = arrow::compute::gt::<T>(lhs.as_ref(), rhs.as_ref())?;
        Ok(Arc::new(result))
    }
}

impl<T> EvaluatorFactory for GtEvaluator<T>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp,
{
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let (lhs, rhs) = info.unpack_arguments()?;
        anyhow::ensure!(
            !lhs.is_literal() && !rhs.is_literal(),
            "GtEvaluator should not receive literal arguments but got {:?}, {:?}",
            lhs,
            rhs
        );
        Ok(Box::new(Self {
            lhs,
            rhs,
            _phantom: PhantomData,
        }))
    }
}

/// Evaluator for the `gt` instruction when the rhs is scalar.
///
/// `create_evaluator` can convert `scalar > value` to `value < scalar`
/// to use a scalar evaluator when necessary.
pub(super) struct GtScalarEvaluator<T: ArrowNumericType> {
    lhs: ValueRef,
    rhs: T::Native,
    // Use the type parameter and indicate it is invariant.
    _phantom: PhantomData<fn(T) -> T>,
}

impl<T: ArrowNumericType + NativeFromScalar> Evaluator for GtScalarEvaluator<T>
where
    T::Native: ArrowNativeTypeOp,
{
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let lhs = info.value(&self.lhs)?.primitive_array()?;

        let result = arrow::compute::gt_scalar::<T>(lhs.as_ref(), self.rhs)?;
        Ok(Arc::new(result))
    }
}

impl<T: ArrowNumericType + NativeFromScalar> EvaluatorFactory for GtScalarEvaluator<T>
where
    T::Native: ArrowNativeTypeOp,
{
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let (lhs, rhs) = info.unpack_arguments()?;
        let rhs = rhs.literal_value().ok_or_else(|| {
            anyhow!(
                "GtScalarEvaluator expects RHS to be scalar, but was {:?}",
                rhs
            )
        })?;
        let rhs = T::native_from_scalar(rhs)?
            .ok_or_else(|| anyhow!("Unexpected null on RHS of GtScalar"))?;
        Ok(Box::new(Self {
            lhs,
            rhs,
            _phantom: PhantomData,
        }))
    }
}

/// Evaluator for the `lt` instruction.
pub(super) struct LtEvaluator<T: ArrowNumericType> {
    lhs: ValueRef,
    rhs: ValueRef,
    // Use the type parameter and indicate it is invariant.
    _phantom: PhantomData<fn(T) -> T>,
}

impl<T: ArrowNumericType> Evaluator for LtEvaluator<T>
where
    T::Native: ArrowNativeTypeOp,
{
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let lhs = info.value(&self.lhs)?.primitive_array()?;
        let rhs = info.value(&self.rhs)?.primitive_array()?;

        let result = arrow::compute::lt::<T>(lhs.as_ref(), rhs.as_ref())?;
        Ok(Arc::new(result))
    }
}

impl<T: ArrowNumericType> EvaluatorFactory for LtEvaluator<T>
where
    T::Native: ArrowNativeTypeOp,
{
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let (lhs, rhs) = info.unpack_arguments()?;
        anyhow::ensure!(
            !lhs.is_literal() && !rhs.is_literal(),
            "LtEvaluator should not receive literal arguments but got {:?}, {:?}",
            lhs,
            rhs
        );
        Ok(Box::new(Self {
            lhs,
            rhs,
            _phantom: PhantomData,
        }))
    }
}

/// Evaluator for the `lt` instruction when the rhs is scalar.
///
/// `create_evaluator` can convert `scalar < value` to `value > scalar`
/// to use a scalar evaluator when necessary.
pub(super) struct LtScalarEvaluator<T: ArrowNumericType> {
    lhs: ValueRef,
    rhs: T::Native,
    // Use the type parameter and indicate it is invariant.
    _phantom: PhantomData<fn(T) -> T>,
}

impl<T: ArrowNumericType + NativeFromScalar> Evaluator for LtScalarEvaluator<T>
where
    T::Native: ArrowNativeTypeOp,
{
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let lhs = info.value(&self.lhs)?.primitive_array()?;

        let result = arrow::compute::lt_scalar::<T>(lhs.as_ref(), self.rhs)?;
        Ok(Arc::new(result))
    }
}

impl<T: ArrowNumericType + NativeFromScalar> EvaluatorFactory for LtScalarEvaluator<T>
where
    T::Native: ArrowNativeTypeOp,
{
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let (lhs, rhs) = info.unpack_arguments()?;
        let rhs = rhs.literal_value().ok_or_else(|| {
            anyhow!(
                "LtScalarEvaluator expects RHS to be scalar, but was {:?}",
                rhs
            )
        })?;
        let rhs = T::native_from_scalar(rhs)?
            .ok_or_else(|| anyhow!("Unexpected null on RHS of LtScalar"))?;
        Ok(Box::new(Self {
            lhs,
            rhs,
            _phantom: PhantomData,
        }))
    }
}

/// Evaluator for the `gte` instruction.
pub(super) struct GteEvaluator<T: ArrowNumericType> {
    lhs: ValueRef,
    rhs: ValueRef,
    // Use the type parameter and indicate it is invariant.
    _phantom: PhantomData<fn(T) -> T>,
}

impl<T: ArrowNumericType> Evaluator for GteEvaluator<T>
where
    T::Native: ArrowNativeTypeOp,
{
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let lhs = info.value(&self.lhs)?.primitive_array()?;
        let rhs = info.value(&self.rhs)?.primitive_array()?;

        let result = arrow::compute::gt_eq::<T>(lhs.as_ref(), rhs.as_ref())?;
        Ok(Arc::new(result))
    }
}

impl<T: ArrowNumericType> EvaluatorFactory for GteEvaluator<T>
where
    T::Native: ArrowNativeTypeOp,
{
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let (lhs, rhs) = info.unpack_arguments()?;
        anyhow::ensure!(
            !lhs.is_literal() && !rhs.is_literal(),
            "GteEvaluator should not receive literal arguments but got {:?}, {:?}",
            lhs,
            rhs
        );
        Ok(Box::new(Self {
            lhs,
            rhs,
            _phantom: PhantomData,
        }))
    }
}

/// Evaluator for the `gte` instruction when the rhs is scalar.
///
/// `create_evaluator` can convert `scalar >= value` to `value <= scalar`
/// to use a scalar evaluator when necessary.
pub(super) struct GteScalarEvaluator<T: ArrowNumericType> {
    lhs: ValueRef,
    rhs: T::Native,
    // Use the type parameter and indicate it is invariant.
    _phantom: PhantomData<fn(T) -> T>,
}

impl<T: ArrowNumericType + NativeFromScalar> Evaluator for GteScalarEvaluator<T>
where
    T::Native: ArrowNativeTypeOp,
{
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let lhs = info.value(&self.lhs)?.primitive_array()?;

        let result = arrow::compute::gt_eq_scalar::<T>(lhs.as_ref(), self.rhs)?;
        Ok(Arc::new(result))
    }
}

impl<T: ArrowNumericType + NativeFromScalar> EvaluatorFactory for GteScalarEvaluator<T>
where
    T::Native: ArrowNativeTypeOp,
{
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let (lhs, rhs) = info.unpack_arguments()?;
        let rhs = rhs.literal_value().ok_or_else(|| {
            anyhow!(
                "GteScalarEvaluator expects RHS to be scalar, but was {:?}",
                rhs
            )
        })?;
        let rhs = T::native_from_scalar(rhs)?
            .ok_or_else(|| anyhow!("Unexpected null on RHS of GteScalar"))?;
        Ok(Box::new(Self {
            lhs,
            rhs,
            _phantom: PhantomData,
        }))
    }
}

/// Evaluator for the `lte` instruction.
pub(super) struct LteEvaluator<T: ArrowNumericType> {
    lhs: ValueRef,
    rhs: ValueRef,
    // Use the type parameter and indicate it is invariant.
    _phantom: PhantomData<fn(T) -> T>,
}

impl<T: ArrowNumericType> Evaluator for LteEvaluator<T>
where
    T::Native: ArrowNativeTypeOp,
{
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let lhs = info.value(&self.lhs)?.primitive_array()?;
        let rhs = info.value(&self.rhs)?.primitive_array()?;

        let result = arrow::compute::lt_eq::<T>(lhs.as_ref(), rhs.as_ref())?;
        Ok(Arc::new(result))
    }
}

impl<T: ArrowNumericType> EvaluatorFactory for LteEvaluator<T>
where
    T::Native: ArrowNativeTypeOp,
{
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let (lhs, rhs) = info.unpack_arguments()?;
        anyhow::ensure!(
            !lhs.is_literal() && !rhs.is_literal(),
            "LteEvaluator should not receive literal arguments but got {:?}, {:?}",
            lhs,
            rhs
        );
        Ok(Box::new(Self {
            lhs,
            rhs,
            _phantom: PhantomData,
        }))
    }
}

/// Evaluator for the `lte` instruction when the rhs is scalar.
///
/// `create_evaluator` can convert `scalar <= value` to `value >= scalar`
/// to use a scalar evaluator when necessary.
pub(super) struct LteScalarEvaluator<T: ArrowNumericType> {
    lhs: ValueRef,
    rhs: T::Native,
    // Use the type parameter and indicate it is invariant.
    _phantom: PhantomData<fn(T) -> T>,
}

impl<T: ArrowNumericType + NativeFromScalar> Evaluator for LteScalarEvaluator<T>
where
    T::Native: ArrowNativeTypeOp,
{
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let lhs = info.value(&self.lhs)?.primitive_array()?;

        let result = arrow::compute::lt_eq_scalar::<T>(lhs.as_ref(), self.rhs)?;
        Ok(Arc::new(result))
    }
}

impl<T: ArrowNumericType + NativeFromScalar> EvaluatorFactory for LteScalarEvaluator<T>
where
    T::Native: ArrowNativeTypeOp,
{
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let (lhs, rhs) = info.unpack_arguments()?;
        let rhs = rhs.literal_value().ok_or_else(|| {
            anyhow!(
                "LteScalarEvaluator expects RHS to be scalar, but was {:?}",
                rhs
            )
        })?;
        let rhs = T::native_from_scalar(rhs)?
            .ok_or_else(|| anyhow!("Unexpected null on RHS of LteScalar"))?;
        Ok(Box::new(Self {
            lhs,
            rhs,
            _phantom: PhantomData,
        }))
    }
}
