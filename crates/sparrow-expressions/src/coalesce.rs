use arrow_array::{ArrayRef, BooleanArray};
use error_stack::{IntoReport, ResultExt};

use sparrow_interfaces::expression::{ArrayRefValue, Error, Evaluator, StaticInfo};

inventory::submit!(sparrow_interfaces::expression::EvaluatorFactory {
    name: "coalesce",
    create: &create
});

/// Evaluator for `coalesce`.
struct CoalesceEvaluator {
    inputs: Vec<ArrayRefValue>,
}

impl Evaluator for CoalesceEvaluator {
    fn evaluate(
        &self,
        work_area: &sparrow_interfaces::expression::WorkArea<'_>,
    ) -> error_stack::Result<ArrayRef, Error> {
        let mut inputs = self.inputs.iter().copied();

        let result = inputs.next().expect("at least one value for coalesce");
        let mut result = work_area.expression(result).clone();
        for input in inputs {
            match result.nulls() {
                None => {
                    // Result contains no nulls, so short-circuit the coalesce.
                    break;
                }
                Some(nulls) if nulls.null_count() == 0 => {
                    // Result contains no nulls, so short-circuit the coalesce.
                    break;
                }
                Some(result_nulls) => {
                    let input = work_area.expression(input);

                    // Check the null bits on the next input to see if it is worth
                    // running the zip.
                    if let Some(input_nulls) = input.nulls() {
                        // used = !result_null & input_null (recalling that null is true when valid)
                        let used = (&!result_nulls.inner()) & input_nulls.inner();
                        if used.count_set_bits() == 0 {
                            // If there are no cases where `result_nulls` is false (invalid) and
                            // `input_nulls` is true (valid), then no values from `input` will be
                            // used.
                            continue;
                        }
                    }

                    // If there are nulls in the result, then we replace them with
                    // the value from the next input.
                    let use_input = BooleanArray::new(result_nulls.inner().clone(), None);
                    result = arrow_select::zip::zip(&use_input, input.as_ref(), result.as_ref())
                        .into_report()
                        .change_context(Error::ExprEvaluation)?;
                }
            }
        }

        Ok(result)
    }
}

fn create(info: StaticInfo<'_>) -> error_stack::Result<Box<dyn Evaluator>, Error> {
    error_stack::ensure!(
        !info.args.is_empty(),
        Error::NoArguments {
            name: info.name.clone(),
            actual: info.args.len()
        }
    );

    for arg in &info.args {
        error_stack::ensure!(
            info.result_type == arg.data_type,
            Error::InvalidArgumentType {
                expected: info.result_type.clone(),
                actual: arg.data_type.clone()
            }
        )
    }

    let inputs = info.args.iter().map(|arg| arg.array_ref()).collect();
    Ok(Box::new(CoalesceEvaluator { inputs }))
}
