use std::sync::Arc;

use arrow_array::ArrayRef;
use error_stack::{IntoReport, ResultExt};

use sparrow_interfaces::expression::{BooleanValue, Error, Evaluator, StaticInfo, WorkArea};

inventory::submit!(sparrow_interfaces::expression::EvaluatorFactory {
    name: "not",
    create: &create
});

/// Evaluator for unary logical negation.
struct NotEvaluator {
    input: BooleanValue,
}

impl Evaluator for NotEvaluator {
    fn evaluate(&self, info: &WorkArea<'_>) -> error_stack::Result<ArrayRef, Error> {
        let input = info.expression(self.input);
        let result = arrow_arith::boolean::not(input)
            .into_report()
            .change_context(Error::ExprEvaluation)?;
        Ok(Arc::new(result))
    }
}

fn create(info: StaticInfo<'_>) -> error_stack::Result<Box<dyn Evaluator>, Error> {
    let input = info.unpack_argument()?;
    Ok(Box::new(NotEvaluator {
        input: input.boolean()?,
    }))
}
