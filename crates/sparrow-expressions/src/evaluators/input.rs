use arrow_array::ArrayRef;

use crate::evaluator::Evaluator;
use crate::work_area::WorkArea;
use crate::Error;

inventory::submit!(crate::evaluators::EvaluatorFactory {
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

fn create(info: super::StaticInfo<'_>) -> error_stack::Result<Box<dyn Evaluator>, Error> {
    error_stack::ensure!(
        info.input_type == info.result_type,
        Error::InvalidResultType {
            expected: info.result_type.clone(),
            actual: info.input_type.clone(),
        }
    );

    Ok(Box::new(InputEvaluator))
}
