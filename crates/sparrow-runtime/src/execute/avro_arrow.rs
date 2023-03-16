use avro_rs::types::Value;
use arrow::array::{ArrayRef, ArrowPrimitiveType, PrimitiveArray, StringBuilder};
use arrow::error::ArrowError;
use arrow::datatypes::{Date32Type, Date64Type, DurationSecondType, Float16Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, TimestampMicrosecondType, TimestampMillisecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type, Utf8Type};
use std::sync::Arc;

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
    let _t = TimestampMillisecondType::DATA_TYPE;
    // Infer the Arrow type based on the Avro type of the first value in the column.
    let (_, first_value) = &values[0][field_index];
    match first_value {
        Value::Int(_) => build_avro_primitive_array::<Int32Type>(&values, field_index),
        Value::Long(_) => build_avro_primitive_array::<Int64Type>(&values, field_index),
        Value::Float(_) => build_avro_primitive_array::<Float32Type>(&values, field_index),
        Value::Double(_) => build_avro_primitive_array::<Float64Type>(&values, field_index),
        Value::TimestampMillis(_) => {
            build_avro_primitive_array::<TimestampMillisecondType>(&values, field_index)
        },
        Value::TimestampMicros(_) => {
            build_avro_primitive_array::<TimestampMicrosecondType>(&values, field_index)
        },
        Value::Date(_) => build_avro_primitive_array::<Date32Type>(&values, field_index),
        Value::String(_) => build_avro_utf8_array(&values, field_index),
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
