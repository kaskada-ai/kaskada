use std::sync::Arc;

use crate::ValueRef;
use arrow::array::ArrayRef;

use crate::{Evaluator, EvaluatorFactory, RuntimeInfo, StaticInfo};

/// Evaluator for `eq`.
pub(super) struct EqEvaluator {
    lhs: ValueRef,
    rhs: ValueRef,
}

impl Evaluator for EqEvaluator {
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let lhs = info.value(&self.lhs)?.array_ref()?;
        let rhs = info.value(&self.rhs)?.array_ref()?;
        let result = arrow_ord::cmp::eq(&lhs, &rhs)?;
        Ok(Arc::new(result))
    }
}

impl EvaluatorFactory for EqEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let (lhs, rhs) = info.unpack_arguments()?;
        Ok(Box::new(Self { lhs, rhs }))
    }
}

/// Evaluator for `neq`.
pub(super) struct NeqEvaluator {
    lhs: ValueRef,
    rhs: ValueRef,
}

impl Evaluator for NeqEvaluator {
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let lhs = info.value(&self.lhs)?.array_ref()?;
        let rhs = info.value(&self.rhs)?.array_ref()?;
        let result = arrow_ord::cmp::neq(&lhs, &rhs)?;
        Ok(Arc::new(result))
    }
}

impl EvaluatorFactory for NeqEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let (lhs, rhs) = info.unpack_arguments()?;
        Ok(Box::new(Self { lhs, rhs }))
    }
}
