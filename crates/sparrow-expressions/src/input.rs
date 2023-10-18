use arrow_array::ArrayRef;

use sparrow_interfaces::expression::{Error, Evaluator, WorkArea};

inventory::submit!(sparrow_interfaces::expression::EvaluatorFactory {
    name: "input",
    create: &create,
});

/// Evaluator for referencing an input column.
struct InputEvaluator;

impl Evaluator for InputEvaluator {
    fn evaluate(&self, info: &WorkArea<'_>) -> error_stack::Result<ArrayRef, Error> {
        Ok(info.input_column().clone())
    }
}

fn create(
    _info: sparrow_interfaces::expression::StaticInfo<'_>,
) -> error_stack::Result<Box<dyn Evaluator>, Error> {
    Ok(Box::new(InputEvaluator))
}
