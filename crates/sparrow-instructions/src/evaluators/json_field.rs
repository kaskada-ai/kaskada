use std::sync::Arc;

use crate::ValueRef;
use anyhow::Context;
use arrow::array::{Array, ArrayRef, StringArray};
use owning_ref::ArcRef;
use sparrow_arrow::scalar_value::ScalarValue;

use crate::{Evaluator, EvaluatorFactory, RuntimeInfo, StaticInfo};

/// Evaluator for the `json_field` function.
///
/// This evaluator expects a `string` input and `field` name.
/// It parses the `string` into a `json` object, then outputs the
/// value of the `field` name as a `string`, or `null` if the `field`
/// does not exist.
pub(super) struct JsonFieldEvaluator {
    json_string: ValueRef,
    field_name: String,
}

impl Evaluator for JsonFieldEvaluator {
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let strings: ArcRef<dyn Array, StringArray> =
            info.value(&self.json_string)?.string_array()?;

        let jsons = Self::parse_to_json(strings)?;
        let result: StringArray = self.values_from_field(jsons)?;
        Ok(Arc::new(result))
    }
}

impl JsonFieldEvaluator {
    fn parse_to_json(
        strings: ArcRef<dyn Array, StringArray>,
    ) -> anyhow::Result<Vec<Option<serde_json::Value>>> {
        strings
            .iter()
            .map(|s| {
                if let Some(s) = s {
                    // If the string is not json-formatted, return None rather than error.
                    // It's possible that rows before a certain time are simply malformed, rather
                    // than this indicating a user error. In that case, we
                    // shouldn't assume wrongdoing, but rather produce a `null`
                    // that the user may be able to recover from.
                    //
                    // This is a good candidate for runtime warnings.
                    Ok(serde_json::from_str(s).ok())
                } else {
                    Ok(None)
                }
            })
            .collect::<anyhow::Result<Vec<Option<serde_json::Value>>>>()
    }

    fn values_from_field(
        &self,
        jsons: Vec<Option<serde_json::Value>>,
    ) -> anyhow::Result<StringArray> {
        let result: StringArray = jsons
            .iter()
            .map(|json| {
                if let Some(json) = json {
                    let value = &json[&self.field_name];
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
                } else {
                    None
                }
            })
            .collect();
        Ok(result)
    }
}

impl EvaluatorFactory for JsonFieldEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let (json_string, field_name) = info.unpack_arguments()?;
        let field_name = match field_name {
            ValueRef::Literal(ScalarValue::Utf8(s)) => s.context("Expected non-null field name")?,
            unexpected => {
                anyhow::bail!("Expected literal utf8 for field name, saw {:?}", unexpected)
            }
        };
        Ok(Box::new(Self {
            json_string,
            field_name,
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{InstKind, InstOp};
    use arrow::array::{GenericStringArray, StringArray};
    use arrow::datatypes::DataType;
    use owning_ref::OwningRef;
    use sparrow_arrow::downcast::downcast_string_array;

    use super::*;
    use crate::StaticArg;

    fn json_formatted_input() -> StringArray {
        let json = vec![
            "{ \"a\": 10, \"b\": \"dog\", \"c\": 3.3 }",
            "{ \"a\": 4, \"b\": \"cat\", \"c\": 5.2 }",
            "{ \"a\": 40 }",
            "{ \"a\": 11, \"c\": 2.1 }",
            "{ \"a\": 6, \"b\": \"bird\" }",
            "{ \"a\": 2, \"b\": \"cat\", \"c\": 4.6 }",
        ];
        StringArray::from(json)
    }

    #[test]
    fn test_parse_to_json_successful() {
        let array: Arc<dyn Array> = Arc::new(json_formatted_input());
        let input: OwningRef<
            std::sync::Arc<(dyn arrow::array::Array + 'static)>,
            GenericStringArray<i32>,
        > = ArcRef::new(array)
            .try_map(|a| downcast_string_array(a))
            .unwrap();
        let result = JsonFieldEvaluator::parse_to_json(input).unwrap();
        result.iter().for_each(|value| assert!(value.is_some()));
    }

    #[test]
    fn test_parse_fields_out() {
        let array: Arc<dyn Array> = Arc::new(json_formatted_input());
        let input: OwningRef<
            std::sync::Arc<(dyn arrow::array::Array + 'static)>,
            GenericStringArray<i32>,
        > = ArcRef::new(array)
            .try_map(|a| downcast_string_array(a))
            .unwrap();
        let result = JsonFieldEvaluator::parse_to_json(input).unwrap();

        let value = result.first().unwrap().as_ref();
        let unwrapped = &value.unwrap()["a"].as_i64().unwrap();
        assert_eq!(unwrapped, &10);

        let value = result.last().unwrap().as_ref();
        let unwrapped = &value.unwrap()["b"].as_str().unwrap();
        assert_eq!(unwrapped, &"cat");

        let unwrapped = &value.unwrap()["c"].as_f64().unwrap();
        assert_eq!(unwrapped, &4.6);

        let value = result[2].as_ref().unwrap();
        let unwrapped = &value["c"];
        assert!(unwrapped.is_null())
    }

    #[test]
    fn test_creating_with_valid_field_name_type() {
        let node = ValueRef::Inst(0u32);
        let field_name = "field_name";
        let node_field_name = ValueRef::Literal(ScalarValue::Utf8(Some(field_name.to_owned())));

        let args = vec![
            StaticArg {
                value_ref: node,
                data_type: DataType::Utf8,
            },
            StaticArg {
                value_ref: node_field_name,
                data_type: DataType::Utf8,
            },
        ];

        let info = StaticInfo::new(&InstKind::Simple(InstOp::JsonField), args, &DataType::Utf8);
        JsonFieldEvaluator::try_new(info).unwrap_or_else(|e| {
            panic!("Expected to create evaluator with literal utf8 field name: {e}")
        });
    }

    #[test]
    fn test_creating_fails_with_invalid_field_name_type() {
        let node = ValueRef::Inst(0u32);
        // Invalid non-literal field name
        let node_field_name = ValueRef::Inst(1u32);

        let args = vec![
            StaticArg {
                value_ref: node,
                data_type: DataType::Utf8,
            },
            StaticArg {
                value_ref: node_field_name,
                data_type: DataType::Utf8,
            },
        ];

        let info = StaticInfo::new(&InstKind::Simple(InstOp::JsonField), args, &DataType::Utf8);
        assert!(JsonFieldEvaluator::try_new(info).is_err());
    }
}
