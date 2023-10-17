use std::sync::Arc;

use arrow_array::{ArrayRef, StringArray};

use sparrow_interfaces::expression::{Error, Evaluator, StaticInfo, StringValue, WorkArea};

inventory::submit!(sparrow_interfaces::expression::EvaluatorFactory {
    name: "upper",
    create: &create
});

/// Evaluator for string to uppercase (`upper`).
pub struct UpperEvaluator {
    input: StringValue,
}

impl Evaluator for UpperEvaluator {
    fn evaluate(&self, info: &WorkArea<'_>) -> error_stack::Result<ArrayRef, Error> {
        let input = info.expression(self.input);
        let result = upper(input);
        Ok(Arc::new(result))
    }
}

fn create(info: StaticInfo<'_>) -> error_stack::Result<Box<dyn Evaluator>, Error> {
    let input = info.unpack_argument()?;
    Ok(Box::new(UpperEvaluator {
        input: input.string()?,
    }))
}

fn upper(input: &StringArray) -> StringArray {
    input
        .iter()
        .map(|opt_s| opt_s.map(|s| s.to_uppercase()))
        .collect()
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_upper_string() {
        let array = StringArray::from(vec![
            Some("hELLo"),
            None,
            Some("WORLD"),
            Some("hello"),
            None,
        ]);
        let expected = StringArray::from(vec![
            Some("HELLO"),
            None,
            Some("WORLD"),
            Some("HELLO"),
            None,
        ]);

        let actual = upper(&array);
        assert_eq!(expected, actual);
    }
}
