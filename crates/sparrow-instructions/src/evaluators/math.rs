use std::marker::PhantomData;
use std::sync::Arc;

use crate::ValueRef;
use arrow::datatypes::{ArrowNativeTypeOp, ArrowNumericType};
use arrow_array::ArrayRef;
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

use crate::evaluators::{check_numeric, Evaluator, RuntimeInfo};
use crate::{EvaluatorFactory, StaticInfo};

/// Evaluator for unary negation.
pub(super) struct NegEvaluator {
    input: ValueRef,
}

impl Evaluator for NegEvaluator {
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let input = info.value(&self.input)?.array_ref()?;
        let result = arrow_arith::numeric::neg_wrapping(input.as_ref())?;
        Ok(Arc::new(result))
    }
}

impl EvaluatorFactory for NegEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        check_numeric("neg", info.args[0].data_type())?;
        let input = info.unpack_argument()?;
        Ok(Box::new(Self { input }))
    }
}

/// Evaluator for addition.
pub(super) struct AddEvaluator {
    lhs: ValueRef,
    rhs: ValueRef,
}

impl Evaluator for AddEvaluator {
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let lhs = info.value(&self.lhs)?.array_ref()?;
        let rhs = info.value(&self.rhs)?.array_ref()?;
        let result = arrow_arith::numeric::add_wrapping(&lhs, &rhs)?;
        Ok(Arc::new(result))
    }
}

impl EvaluatorFactory for AddEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let lhs = info.args[0].data_type();
        let rhs = info.args[1].data_type();
        anyhow::ensure!(
            lhs == rhs,
            "Argument types for add must match, but got {lhs:?} and {rhs:?}"
        );
        check_numeric("add", lhs)?;

        let (lhs, rhs) = info.unpack_arguments()?;
        Ok(Box::new(Self { lhs, rhs }))
    }
}

/// Evaluator for subtraction.
pub(super) struct SubEvaluator {
    lhs: ValueRef,
    rhs: ValueRef,
}

impl Evaluator for SubEvaluator {
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let lhs = info.value(&self.lhs)?.array_ref()?;
        let rhs = info.value(&self.rhs)?.array_ref()?;
        let result = arrow_arith::numeric::sub(&lhs, &rhs)?;
        Ok(Arc::new(result))
    }
}

impl EvaluatorFactory for SubEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let lhs = info.args[0].data_type();
        let rhs = info.args[1].data_type();
        anyhow::ensure!(
            lhs == rhs,
            "Argument types for sub must match, but got {lhs:?} and {rhs:?}"
        );
        check_numeric("sub", lhs)?;

        let (lhs, rhs) = info.unpack_arguments()?;
        Ok(Box::new(Self { lhs, rhs }))
    }
}

/// Evaluator for multiplication.
pub(super) struct MulEvaluator {
    lhs: ValueRef,
    rhs: ValueRef,
}

impl Evaluator for MulEvaluator {
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let lhs = info.value(&self.lhs)?.array_ref()?;
        let rhs = info.value(&self.rhs)?.array_ref()?;
        let result = arrow_arith::numeric::mul(&lhs, &rhs)?;
        Ok(Arc::new(result))
    }
}

impl EvaluatorFactory for MulEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let lhs = info.args[0].data_type();
        let rhs = info.args[1].data_type();
        anyhow::ensure!(
            lhs == rhs,
            "Argument types for mul must match, but got {lhs:?} and {rhs:?}"
        );
        check_numeric("mul", lhs)?;

        let (lhs, rhs) = info.unpack_arguments()?;
        Ok(Box::new(Self { lhs, rhs }))
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
        let lhs = info.value(&self.lhs)?.array_ref()?;
        let rhs = info.value(&self.rhs)?;
        let result = match rhs.try_primitive_literal::<T>() {
            Ok(Some(rhs)) if !rhs.is_zero() => {
                // Division by a literal
                let rhs = arrow_array::PrimitiveArray::<T>::new_scalar(rhs);
                let result = arrow_arith::numeric::div(&lhs, &rhs)?;
                Arc::new(result)
            }
            Ok(None | Some(_)) => {
                // TODO: Simplification opportunity
                // Division by 0 and division by null produce a null result.
                arrow::array::new_null_array(&T::DATA_TYPE, lhs.len())
            }

            Err(_) => {
                // Division by a non-literal.
                // TODO: Null out cases where the rhs is zero.
                let rhs = rhs.array_ref()?;
                let result = arrow_arith::numeric::div(&lhs, &rhs)?;
                Arc::new(result)
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
