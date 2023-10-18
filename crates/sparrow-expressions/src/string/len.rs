use std::sync::Arc;

use arrow_array::ArrayRef;
use error_stack::{IntoReport, ResultExt};

use sparrow_interfaces::expression::{Error, Evaluator, StaticInfo, StringValue, WorkArea};

inventory::submit!(sparrow_interfaces::expression::EvaluatorFactory {
    name: "len",
    create: &create
});

/// Evaluator for string length (`len`).
struct LenEvaluator {
    input: StringValue,
}

impl Evaluator for LenEvaluator {
    fn evaluate(&self, info: &WorkArea<'_>) -> error_stack::Result<ArrayRef, Error> {
        let input = info.expression(self.input);
        let result = arrow_string::length::length(input)
            .into_report()
            .change_context(Error::ExprEvaluation)?;
        Ok(Arc::new(result))
    }
}

fn create(info: StaticInfo<'_>) -> error_stack::Result<Box<dyn Evaluator>, Error> {
    let input = info.unpack_argument()?;
    Ok(Box::new(LenEvaluator {
        input: input.string()?,
    }))
}
