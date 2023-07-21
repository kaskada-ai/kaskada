use arrow_array::ArrayRef;

use crate::evaluator::Evaluator;
use crate::work_area::WorkArea;
use crate::Error;

inventory::submit!(crate::evaluators::EvaluatorFactory {
    name: "column",
    create: &create,
});

/// Evaluator for column reference (`.c`)..
struct ColumnEvaluator {
    column: usize,
}

impl Evaluator for ColumnEvaluator {
    fn evaluate(&self, info: &WorkArea<'_>) -> error_stack::Result<ArrayRef, Error> {
        Ok(info.input_column(self.column).clone())
    }
}

fn create(info: super::StaticInfo<'_>) -> error_stack::Result<Box<dyn Evaluator>, Error> {
    let name = info.literal_string()?;
    let (column, field) = info
        .input_schema
        .column_with_name(name)
        .expect("missing column");
    error_stack::ensure!(
        field.data_type() == info.result_type,
        Error::InvalidResultType {
            expected: field.data_type().clone(),
            actual: info.result_type.clone()
        }
    );

    Ok(Box::new(ColumnEvaluator { column }))
}
