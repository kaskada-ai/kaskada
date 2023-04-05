use arrow::array::{ArrayRef, ArrowPrimitiveType, PrimitiveArray, StringBuilder};
use arrow::datatypes::{
    Date32Type, Float32Type, Float64Type, Int32Type, Int64Type, TimestampMicrosecondType,
    TimestampMillisecondType,
};
use arrow::error::ArrowError;
use avro_rs::types::Value;
use std::sync::Arc;

/// Converts a nested Vec of Avro values into a Vec of Arrow arrays.
///
/// The avro_to_arrow function takes a nested vector of Avro values, where each inner vector
/// represents a row and contains tuples with a string field name and the corresponding Avro Value,
/// and returns a Result containing a vector of Arrow arrays (Vec<ArrayRef>) if the
/// conversion is successful, or an ArrowError if an error occurs during conversion.
pub fn avro_to_arrow(values: Vec<Vec<(String, Value)>>) -> Result<Vec<ArrayRef>, ArrowError> {
    (0..values[0].len())
        .map(|i| avro_to_arrow_field(&values, i))
        .collect()
}

fn build_avro_primitive_array<T>(
    values: &&Vec<Vec<(String, Value)>>,
    field_index: usize,
) -> Result<ArrayRef, ArrowError>
where
    T: ArrowPrimitiveType + AvroParser,
{
    values
        .iter()
        .enumerate()
        .map(|(row_index, row)| {
            let (_, value) = &row[field_index];
            if let Value::Null = value {
                return Ok(None);
            }
            let parsed = T::parse_avro_value(value);

            match parsed {
                Some(e) => Ok(Some(e)),
                None => Err(ArrowError::ParseError(format!(
                    "Error while parsing value {:?} for column {} at line {}",
                    value, field_index, row_index
                ))),
            }
        })
        .collect::<Result<PrimitiveArray<T>, ArrowError>>()
        .map(|e| Arc::new(e) as ArrayRef)
}

fn build_avro_utf8_array(
    values: &Vec<Vec<(String, Value)>>,
    field_index: usize,
) -> Result<ArrayRef, ArrowError> {
    let mut builder = StringBuilder::new();

    for row in values {
        let (_, value) = &row[field_index];
        match value {
            Value::String(s) => builder.append_value(s),
            Value::Null => builder.append_null(),
            _ => {
                return Err(ArrowError::ParseError(format!(
                    "Error while parsing value {:?} for column {}",
                    value, field_index
                )));
            }
        };
    }

    Ok(Arc::new(builder.finish()) as ArrayRef)
}

fn avro_to_arrow_field(
    values: &Vec<Vec<(String, Value)>>,
    field_index: usize,
) -> Result<ArrayRef, ArrowError> {
    // Infer the Arrow type based on the Avro type of the first value in the column.
    let (_, first_value) = &values[0][field_index];
    match first_value {
        Value::Int(_) => build_avro_primitive_array::<Int32Type>(&values, field_index),
        Value::Long(_) => build_avro_primitive_array::<Int64Type>(&values, field_index),
        Value::Float(_) => build_avro_primitive_array::<Float32Type>(&values, field_index),
        Value::Double(_) => build_avro_primitive_array::<Float64Type>(&values, field_index),
        Value::TimestampMillis(_) => {
            build_avro_primitive_array::<TimestampMillisecondType>(&values, field_index)
        }
        Value::TimestampMicros(_) => {
            build_avro_primitive_array::<TimestampMicrosecondType>(&values, field_index)
        }
        Value::Date(_) => build_avro_primitive_array::<Date32Type>(&values, field_index),
        Value::String(_) => build_avro_utf8_array(values, field_index),
        _ => Err(ArrowError::ParseError(format!(
            "Unsupported Avro type {:?} for column {:?}",
            first_value, field_index
        ))),
    }
}

pub trait AvroParser: ArrowPrimitiveType {
    fn parse_avro_value(value: &Value) -> Option<Self::Native>;
}

impl AvroParser for Int32Type {
    fn parse_avro_value(value: &Value) -> Option<Self::Native> {
        match value {
            Value::Int(i) => Some(*i),
            _ => None,
        }
    }
}

impl AvroParser for Int64Type {
    fn parse_avro_value(value: &Value) -> Option<Self::Native> {
        match value {
            Value::Long(l) => Some(*l),
            _ => None,
        }
    }
}

impl AvroParser for Float32Type {
    fn parse_avro_value(value: &Value) -> Option<Self::Native> {
        match value {
            Value::Float(f) => Some(*f),
            _ => None,
        }
    }
}

impl AvroParser for Float64Type {
    fn parse_avro_value(value: &Value) -> Option<Self::Native> {
        match value {
            Value::Double(d) => Some(*d),
            _ => None,
        }
    }
}

impl AvroParser for TimestampMillisecondType {
    fn parse_avro_value(value: &Value) -> Option<Self::Native> {
        match value {
            Value::TimestampMillis(t) => Some(*t),
            _ => None,
        }
    }
}

impl AvroParser for TimestampMicrosecondType {
    fn parse_avro_value(value: &Value) -> Option<Self::Native> {
        match value {
            Value::TimestampMicros(t) => Some(*t),
            _ => None,
        }
    }
}

impl AvroParser for Date32Type {
    fn parse_avro_value(value: &Value) -> Option<Self::Native> {
        match value {
            Value::Date(d) => Some(*d),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int32Array, StringArray};
    use avro_rs::types::Value;

    #[test]
    fn test_avro_to_arrow() {
        let avro_values = vec![
            vec![
                ("field1".to_string(), Value::Int(1)),
                ("field2".to_string(), Value::String("hello".to_string())),
            ],
            vec![
                ("field1".to_string(), Value::Int(2)),
                ("field2".to_string(), Value::String("world".to_string())),
            ],
        ];

        let result = avro_to_arrow(avro_values).unwrap();
        assert_eq!(result.len(), 2);

        let int_array = result[0].as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(int_array.value(0), 1);
        assert_eq!(int_array.value(1), 2);

        let str_array = result[1].as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(str_array.value(0), "hello");
        assert_eq!(str_array.value(1), "world");
    }
}
