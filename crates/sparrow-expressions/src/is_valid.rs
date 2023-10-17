use std::sync::Arc;

use arrow_array::ArrayRef;
use error_stack::{IntoReport, ResultExt};

use sparrow_interfaces::expression::{ArrayRefValue, Error, Evaluator, StaticInfo, WorkArea};

inventory::submit!(sparrow_interfaces::expression::EvaluatorFactory {
    name: "is_valid",
    create: &create
});

/// Evaluator for `is_valid`.
struct IsValidEvaluator {
    input: ArrayRefValue,
}

impl Evaluator for IsValidEvaluator {
    fn evaluate(&self, info: &WorkArea<'_>) -> error_stack::Result<ArrayRef, Error> {
        let input = info.expression(self.input);
        let result = arrow_arith::boolean::is_not_null(input.as_ref())
            .into_report()
            .change_context(Error::ExprEvaluation)?;
        Ok(Arc::new(result))
    }
}

fn create(info: StaticInfo<'_>) -> error_stack::Result<Box<dyn Evaluator>, Error> {
    let input = info.unpack_argument()?;
    Ok(Box::new(IsValidEvaluator {
        input: input.array_ref(),
    }))
}
