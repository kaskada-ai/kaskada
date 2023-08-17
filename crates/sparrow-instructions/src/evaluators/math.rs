use std::marker::PhantomData;
use std::sync::Arc;

use crate::ValueRef;
use arrow::array::ArrayRef;
use arrow::datatypes::{ArrowNativeTypeOp, ArrowNumericType};
use sparrow_arrow::scalar_value::NativeFromScalar;

mod clamp;
mod exp;
mod floor_ceil;
mod min_max;
mod powf;
mod round;

pub(super) use clamp::*;
pub(super) use exp::*;
pub(super) use floor_ceil::*;
pub(super) use min_max::*;
pub(super) use powf::*;
pub(super) use round::*;

use crate::evaluators::{Evaluator, RuntimeInfo};
use crate::{EvaluatorFactory, StaticInfo};

/// Evaluator for unary negation.
pub(super) struct NegEvaluator<T: ArrowNumericType>
where
    T::Native: std::ops::Neg<Output = T::Native>,
{
    input: ValueRef,
    // Use the type parameter and indicate it is invariant.
    _phantom: PhantomData<fn(T) -> T>,
}

impl<T> Evaluator for NegEvaluator<T>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp + std::ops::Neg<Output = T::Native>,
{
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let input = info.value(&self.input)?.primitive_array()?;
        let result = arrow::compute::negate::<T>(input.as_ref())?;
        Ok(Arc::new(result))
    }
}

impl<T> EvaluatorFactory for NegEvaluator<T>
where
    T: ArrowNumericType,
    T::Native: ArrowNativeTypeOp + std::ops::Neg<Output = T::Native>,
{
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let input = info.unpack_argument()?;
        Ok(Box::new(Self {
            input,
            _phantom: PhantomData,
        }))
    }
}

/// Evaluator for addition.
pub(super) struct AddEvaluator<T: ArrowNumericType> {
    lhs: ValueRef,
    rhs: ValueRef,
    // Use the type parameter and indicate it is invariant.
    _phantom: PhantomData<fn(T) -> T>,
}

impl<T: ArrowNumericType> Evaluator for AddEvaluator<T>
where
    T::Native: ArrowNativeTypeOp,
{
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let lhs = info.value(&self.lhs)?.primitive_array()?;
        let rhs = info.value(&self.rhs)?.primitive_array()?;
        let result = arrow::compute::add::<T>(lhs.as_ref(), rhs.as_ref())?;
        Ok(Arc::new(result))
    }
}

impl<T: ArrowNumericType> EvaluatorFactory for AddEvaluator<T>
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

/// Evaluator for subtraction.
pub(super) struct SubEvaluator<T: ArrowNumericType> {
    lhs: ValueRef,
    rhs: ValueRef,
    // Use the type parameter and indicate it is invariant.
    _phantom: PhantomData<fn(T) -> T>,
}

impl<T: ArrowNumericType> Evaluator for SubEvaluator<T>
where
    T::Native: ArrowNativeTypeOp,
{
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let lhs = info.value(&self.lhs)?.primitive_array()?;
        let rhs = info.value(&self.rhs)?.primitive_array()?;
        let result = arrow::compute::subtract::<T>(lhs.as_ref(), rhs.as_ref())?;
        Ok(Arc::new(result))
    }
}

impl<T: ArrowNumericType> EvaluatorFactory for SubEvaluator<T>
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

/// Evaluator for multiplication.
pub(super) struct MulEvaluator<T: ArrowNumericType> {
    lhs: ValueRef,
    rhs: ValueRef,
    // Use the type parameter and indicate it is invariant.
    _phantom: PhantomData<fn(T) -> T>,
}

impl<T: ArrowNumericType> Evaluator for MulEvaluator<T>
where
    T::Native: ArrowNativeTypeOp,
{
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let lhs = info.value(&self.lhs)?.primitive_array()?;
        let rhs = info.value(&self.rhs)?.primitive_array()?;
        let result = arrow::compute::multiply::<T>(lhs.as_ref(), rhs.as_ref())?;
        Ok(Arc::new(result))
    }
}

impl<T: ArrowNumericType> EvaluatorFactory for MulEvaluator<T>
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

/// Evaluator for division.
pub(super) struct DivEvaluator<T: ArrowNumericType>
where
    T::Native: ArrowNativeTypeOp,
    T: NativeFromScalar,
{
    lhs: ValueRef,
    rhs: ValueRef,
    // Use the type parameter and indicate it is invariant.
    _phantom: PhantomData<fn(T) -> T>,
}

impl<T: ArrowNumericType> Evaluator for DivEvaluator<T>
where
    T::Native: ArrowNativeTypeOp + num::One,
    T: NativeFromScalar,
{
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let lhs = info.value(&self.lhs)?.primitive_array()?;
        let rhs = info.value(&self.rhs)?;
        let result = match rhs.try_primitive_literal::<T>() {
            Ok(Some(rhs)) if !rhs.is_zero() => {
                // Division by a literal
                Arc::new(arrow::compute::divide_scalar(lhs.as_ref(), rhs)?)
            }
            Ok(None | Some(_)) => {
                // TODO: Simplification opportunity
                // Division by 0 and division by null produce a null result.
                arrow::array::new_null_array(&T::DATA_TYPE, lhs.len())
            }

            Err(_) => {
                // Division by a non-literal.
                // Null out cases where the rhs is zero.
                let rhs = rhs.primitive_array()?;
                Arc::new(arrow::compute::divide_opt::<T>(lhs.as_ref(), rhs.as_ref())?)
            }
        };

        Ok(result)
    }
}

impl<T: ArrowNumericType> EvaluatorFactory for DivEvaluator<T>
where
    T::Native: ArrowNativeTypeOp + num::One,
    T: NativeFromScalar,
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
