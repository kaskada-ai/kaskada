use avro_rs::types::Value;
use arrow::array::{ArrayRef, ArrowPrimitiveType, PrimitiveArray};
use arrow::error::ArrowError;
use arrow::datatypes::{Float64Type, Int32Type, TimestampMillisecondType};
use std::sync::Arc;

pub fn avro_to_arrow(values: Vec<Box<Vec<(String, Value)>>>) -> Result<Vec<ArrayRef>, ArrowError> {
    (0..values[0].len())
        .map(|i| avro_to_arrow_field(&values, i))
        .collect()
}

fn build_avro_primitive_array<T>(
    values: &Vec<Box<Vec<(String, Value)>>>,
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

fn avro_to_arrow_field(
    values: &Vec<Box<Vec<(String, Value)>>>,
    field_index: usize,
) -> Result<ArrayRef, ArrowError> {
    let _t = TimestampMillisecondType::DATA_TYPE;
    // Infer the Arrow type based on the Avro type of the first value in the column.
    let (_, first_value) = &values[0][field_index];
    match first_value {
        Value::Int(_) => build_avro_primitive_array::<Int32Type>(&values, field_index),
        Value::Double(_) => build_avro_primitive_array::<Float64Type>(&values, field_index),
        Value::TimestampMillis(_) => {
            build_avro_primitive_array::<TimestampMillisecondType>(&values, field_index)
        }
        _ => Err(ArrowError::ParseError(format!(
            "Unsupported Avro type {:?} for column {:?}",
            first_value, field_index
        ))),
    }
}
