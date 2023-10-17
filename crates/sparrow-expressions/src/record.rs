use std::sync::Arc;

use arrow_array::StructArray;
use arrow_schema::{DataType, Fields};
use itertools::Itertools;

use sparrow_interfaces::expression::{ArrayRefValue, Error, Evaluator, StaticInfo, WorkArea};

inventory::submit!(sparrow_interfaces::expression::EvaluatorFactory {
    name: "record",
    create: &create
});

/// Evaluator for record creation.
struct RecordEvaluator {
    inputs: Vec<ArrayRefValue>,
    fields: Fields,
}

impl Evaluator for RecordEvaluator {
    fn evaluate(
        &self,
        work_area: &WorkArea<'_>,
    ) -> error_stack::Result<arrow_array::ArrayRef, Error> {
        let arrays = self
            .inputs
            .iter()
            .map(|input| work_area.expression(*input).clone())
            .collect();
        Ok(Arc::new(StructArray::new(
            self.fields.clone(),
            arrays,
            None,
        )))
    }
}

fn create(info: StaticInfo<'_>) -> error_stack::Result<Box<dyn Evaluator>, Error> {
    let DataType::Struct(fields) = info.result_type else {
        error_stack::bail!(Error::InvalidNonStructResultType {
            actual: info.result_type.clone()
        })
    };

    error_stack::ensure!(
        info.args.len() == fields.len(),
        Error::InvalidArgumentCount {
            name: info.name.clone(),
            expected: fields.len(),
            actual: info.args.len()
        }
    );

    let inputs = info
        .args
        .iter()
        .zip(fields)
        .map(|(arg, field)| {
            error_stack::ensure!(
                arg.data_type == field.data_type(),
                Error::InvalidArgumentType {
                    expected: field.data_type().clone(),
                    actual: arg.data_type.clone()
                }
            );
            Ok(arg.array_ref())
        })
        .try_collect()?;

    Ok(Box::new(RecordEvaluator {
        inputs,
        fields: fields.clone(),
    }))
}
