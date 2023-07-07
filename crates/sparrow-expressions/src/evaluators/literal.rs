use arrow_array::ArrayRef;
use sparrow_arrow::scalar_value::ScalarValue;

use crate::evaluator::Evaluator;
use crate::work_area::WorkArea;
use crate::Error;

inventory::submit!(crate::evaluators::EvaluatorFactory {
    name: "literal",
    create: &create
});

/// Evaluator for a scalar.
struct LiteralEvaluator {
    scalar: ScalarValue,
}

impl Evaluator for LiteralEvaluator {
    fn evaluate(&self, info: &WorkArea<'_>) -> error_stack::Result<ArrayRef, Error> {
        Ok(self.scalar.to_array(info.num_rows()))
    }
}

fn create(info: super::StaticInfo<'_>) -> error_stack::Result<Box<dyn Evaluator>, Error> {
    let scalar = info.literal()?.clone();
    error_stack::ensure!(
        &scalar.data_type() == info.result_type,
        Error::InvalidLiteralType {
            expected: info.result_type.clone(),
            actual: scalar.data_type()
        }
    );

    Ok(Box::new(LiteralEvaluator { scalar }))
}
