use arrow::array::ArrayRef;

use crate::ValueRef;
use arrow_schema::DataType;
use std::sync::Arc;

use crate::{Evaluator, EvaluatorFactory, StaticInfo};

/// Evaluator for `len` on lists.
///
/// Produces the length of the list.
#[derive(Debug)]
pub(in crate::evaluators) struct ListLenEvaluator {
    list: ValueRef,
}

impl EvaluatorFactory for ListLenEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let input_type = info.args[0].data_type.clone();
        match input_type {
            DataType::List(_) => (),
            other => anyhow::bail!("expected list type, saw {:?}", other),
        };

        let list = info.unpack_argument()?;
        Ok(Box::new(Self { list }))
    }
}

impl Evaluator for ListLenEvaluator {
    fn evaluate(&mut self, info: &dyn crate::RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let input = info.value(&self.list)?.array_ref()?;
        let result = arrow::compute::kernels::length::length(input.as_ref())?;
        Ok(Arc::new(result))
    }
}
