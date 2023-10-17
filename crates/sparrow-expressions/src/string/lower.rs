use std::sync::Arc;

use arrow_array::{ArrayRef, StringArray};

use sparrow_interfaces::expression::{Error, Evaluator, StaticInfo, StringValue, WorkArea};

inventory::submit!(sparrow_interfaces::expression::EvaluatorFactory {
    name: "lower",
    create: &create
});

/// Evaluator for string to lowercase (`lower`).
struct LowerEvaluator {
    input: StringValue,
}

impl Evaluator for LowerEvaluator {
    fn evaluate(&self, info: &WorkArea<'_>) -> error_stack::Result<ArrayRef, Error> {
        let input = info.expression(self.input);
        let result = lower(input);
        Ok(Arc::new(result))
    }
}

fn create(info: StaticInfo<'_>) -> error_stack::Result<Box<dyn Evaluator>, Error> {
    let input = info.unpack_argument()?;
    Ok(Box::new(LowerEvaluator {
        input: input.string()?,
    }))
}

fn lower(input: &StringArray) -> StringArray {
    input
        .iter()
        .map(|opt_s| opt_s.map(|s| s.to_lowercase()))
        .collect()
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_lower_string() {
        let array = StringArray::from(vec![
            Some("hELLo"),
            None,
            Some("WORLD"),
            Some("hello"),
            None,
        ]);
        let expected = StringArray::from(vec![
            Some("hello"),
            None,
            Some("world"),
            Some("hello"),
            None,
        ]);

        let actual = lower(&array);
        assert_eq!(expected, actual);
    }
}
