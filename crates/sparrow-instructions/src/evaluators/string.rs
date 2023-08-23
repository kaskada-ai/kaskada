use std::sync::Arc;

use crate::ValueRef;
use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use sparrow_arrow::downcast::downcast_primitive_array;

use crate::{Evaluator, EvaluatorFactory, RuntimeInfo, StaticInfo};

/// Evaluator for the `len` function.
pub(super) struct LenEvaluator {
    input: ValueRef,
}

impl Evaluator for LenEvaluator {
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let input = info.value(&self.input)?.array_ref()?;
        let result = arrow::compute::kernels::length::length(input.as_ref())?;
        Ok(result)
    }
}

impl EvaluatorFactory for LenEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let input = info.unpack_argument()?;
        Ok(Box::new(Self { input }))
    }
}

/// Evaluator for the `upper` function.
pub(super) struct UpperEvaluator {
    input: ValueRef,
}

impl Evaluator for UpperEvaluator {
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let input = info.value(&self.input)?.string_array()?;
        let result = sparrow_kernels::string::upper(input.as_ref())?;
        Ok(Arc::new(result))
    }
}

impl EvaluatorFactory for UpperEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let input = info.unpack_argument()?;
        Ok(Box::new(Self { input }))
    }
}

/// Evaluator for the `lower` function.
pub(super) struct LowerEvaluator {
    input: ValueRef,
}

impl Evaluator for LowerEvaluator {
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let input = info.value(&self.input)?.string_array()?;
        let result = sparrow_kernels::string::lower(input.as_ref())?;
        Ok(Arc::new(result))
    }
}

impl EvaluatorFactory for LowerEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let input = info.unpack_argument()?;
        Ok(Box::new(Self { input }))
    }
}

/// Evaluator for the `substring` function.
pub(super) struct SubstringEvaluator {
    string: ValueRef,
    start: ValueRef,
    end: ValueRef,
}

impl Evaluator for SubstringEvaluator {
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let string = info.value(&self.string)?.string_array()?;

        // TODO: Specialize this for the case where start/end are literal
        // numbers or `null`. Avoid the `cast` if possible.
        let start = info.value(&self.start)?.array_ref()?;
        let start = arrow::compute::cast(&start, &DataType::Int32)?;
        let start = downcast_primitive_array(start.as_ref())?;

        let end = info.value(&self.end)?.array_ref()?;
        let end = arrow::compute::cast(&end, &DataType::Int32)?;
        let end = downcast_primitive_array(end.as_ref())?;

        let result = sparrow_kernels::string::substring(string.as_ref(), start, end)?;
        Ok(Arc::new(result))
    }
}

impl EvaluatorFactory for SubstringEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let (string, start, end) = info.unpack_arguments()?;
        Ok(Box::new(Self { string, start, end }))
    }
}
