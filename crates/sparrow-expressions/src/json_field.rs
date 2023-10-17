use std::sync::Arc;

use arrow_array::StringArray;

use sparrow_interfaces::expression::{Error, Evaluator, StringValue, WorkArea};

inventory::submit!(sparrow_interfaces::expression::EvaluatorFactory {
    name: "json_field",
    create: &create
});

/// Evaluator for `json_field` expressions.
///
/// This evaluator expects a `string` input and `field` name.
/// It parses the `string` into a `json` object, then outputs the
/// value of the `field` name as a `string`, or `null` if the `field`
/// does not exist.
struct JsonFieldEvaluator {
    input: StringValue,
    field_name: String,
}

impl Evaluator for JsonFieldEvaluator {
    fn evaluate(
        &self,
        work_area: &WorkArea<'_>,
    ) -> error_stack::Result<arrow_array::ArrayRef, Error> {
        let input = work_area.expression(self.input);
        let result = json_field(input, &self.field_name);
        Ok(Arc::new(result))
    }
}

fn create(
    info: sparrow_interfaces::expression::StaticInfo<'_>,
) -> error_stack::Result<Box<dyn Evaluator>, Error> {
    let field_name = info.literal_string()?.to_owned();
    let input = info.unpack_argument()?;

    Ok(Box::new(JsonFieldEvaluator {
        input: input.string()?,
        field_name,
    }))
}

fn json_field(input: &StringArray, field_name: &str) -> StringArray {
    input
        .iter()
        .map(|s| {
            // If the string is not json-formatted, return None rather than error.
            // It's possible that rows before a certain time are simply malformed, rather
            // than this indicating a user error. In that case, we
            // shouldn't assume wrongdoing, but rather produce a `null`
            // that the user may be able to recover from.
            //
            // This is a good candidate for runtime warnings.
            s.and_then(|s| serde_json::from_str(s).ok())
        })
        .map(|json| {
            json.and_then(|json: serde_json::Value| {
                let value = &json[field_name];
                if value.is_null() {
                    // No value - field does not exist in this json object.
                    None
                } else if let Some(s) = value.as_str() {
                    // If the value is a string, use this representation.
                    // Note: This is to prevent the additional quote padding that
                    // would be added if doing `value.to_string()` on a string type.
                    Some(s.to_string())
                } else {
                    // Otherwise, convert to a string
                    Some(value.to_string())
                }
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {

    use super::*;

    fn json_formatted_input() -> StringArray {
        let json = vec![
            "{ \"a\": 10, \"b\": \"dog\", \"c\": 3.3 }",
            "{ \"a\": 4, \"b\": \"cat\", \"c\": 5.2 }",
            "null",
            "{ \"a\": 40 }",
            "{ \"a\": 11, \"c\": 2.1 }",
            "{ \"a\": 6, \"b\": \"bird\" }",
            "{ \"a\": 2, \"b\": \"cat\", \"c\": 4.6 }",
        ];
        StringArray::from(json)
    }

    #[test]
    fn test_json_field_a() {
        assert_eq!(
            json_field(&json_formatted_input(), "a"),
            StringArray::from(vec![
                Some("10"),
                Some("4"),
                None,
                Some("40"),
                Some("11"),
                Some("6"),
                Some("2")
            ])
        );
    }

    #[test]
    fn test_json_field_b() {
        assert_eq!(
            json_field(&json_formatted_input(), "b"),
            StringArray::from(vec![
                Some("dog"),
                Some("cat"),
                None,
                None,
                None,
                Some("bird"),
                Some("cat")
            ])
        );
    }

    #[test]
    fn test_json_field_c() {
        assert_eq!(
            json_field(&json_formatted_input(), "c"),
            StringArray::from(vec![
                Some("3.3"),
                Some("5.2"),
                None,
                None,
                Some("2.1"),
                None,
                Some("4.6")
            ])
        );
    }

    #[test]
    fn test_json_field_d() {
        let null_str: Option<&'static str> = None;
        assert_eq!(
            json_field(&json_formatted_input(), "d"),
            StringArray::from(vec![
                null_str, null_str, null_str, null_str, null_str, null_str, null_str
            ])
        );
    }
}
