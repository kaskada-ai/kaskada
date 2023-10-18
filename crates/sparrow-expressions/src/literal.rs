use arrow_array::ArrayRef;
use sparrow_arrow::scalar_value::ScalarValue;

use sparrow_interfaces::expression::{Error, Evaluator, WorkArea};

inventory::submit!(sparrow_interfaces::expression::EvaluatorFactory {
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

fn create(
    info: sparrow_interfaces::expression::StaticInfo<'_>,
) -> error_stack::Result<Box<dyn Evaluator>, Error> {
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
