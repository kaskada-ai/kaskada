use std::sync::Arc;

use arrow_array::ArrayRef;
use error_stack::ResultExt;

use sparrow_interfaces::expression::{ArrayRefValue, Error, Evaluator, StaticInfo, WorkArea};

inventory::submit!(sparrow_interfaces::expression::EvaluatorFactory {
    name: "hash",
    create: &create
});

/// Evaluator for `hash`.
struct HashEvaluator {
    input: ArrayRefValue,
}

impl Evaluator for HashEvaluator {
    fn evaluate(&self, info: &WorkArea<'_>) -> error_stack::Result<ArrayRef, Error> {
        let input = info.expression(self.input);
        let hashes =
            sparrow_arrow::hash::hash(input.as_ref()).change_context(Error::ExprEvaluation)?;

        Ok(Arc::new(hashes))
    }
}

fn create(info: StaticInfo<'_>) -> error_stack::Result<Box<dyn Evaluator>, Error> {
    let input = info.unpack_argument()?;
    Ok(Box::new(HashEvaluator {
        input: input.array_ref(),
    }))
}
