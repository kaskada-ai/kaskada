use super::Error;
use arrow::datatypes::{DataType, Field, IntervalUnit, Schema, TimeUnit};
use avro_schema::schema::{
    Field as AvroField, Fixed, FixedLogical, IntLogical, LongLogical, Record, Schema as AvroSchema,
};
use error_stack::ResultExt;
use itertools::Itertools;
use std::collections::HashMap;

#[derive(derive_more::Display, Debug)]
#[display(fmt = "in field '{_0}'")]
struct FieldName(String);

/// Converts an arrow [`Schema`] to an Avro [`AvroSchema`].
pub fn to_avro_schema(schema: &Schema) -> error_stack::Result<AvroSchema, Error> {
    to_avro_record_schema(schema).map(AvroSchema::Record)
}

/// Converts an arrow [`Schema`] to an Avro [`Record`].
fn to_avro_record_schema(schema: &Schema) -> error_stack::Result<Record, Error> {
    let mut name_counter: i32 = 0;
    let fields: Vec<_> = schema
        .fields
        .iter()
        .map(|f| arrow_to_avro(f, &mut name_counter))
        .try_collect()?;

    Ok(Record {
        name: "Record".to_string(),
        namespace: None,
        doc: None,
        aliases: vec![],
        fields,
    })
}

fn arrow_to_avro(field: &Field, name_counter: &mut i32) -> error_stack::Result<AvroField, Error> {
    let schema = type_to_schema(field.data_type(), field.is_nullable(), name_counter)
        .attach_printable_lazy(|| FieldName(field.name().to_owned()))?;
    Ok(AvroField::new(field.name(), schema))
}

fn type_to_schema(
    data_type: &DataType,
    is_nullable: bool,
    name_counter: &mut i32,
) -> error_stack::Result<AvroSchema, Error> {
    // Nullable types are represented in Avro as the union of `null` and the type.
    Ok(if is_nullable {
        AvroSchema::Union(vec![
            AvroSchema::Null,
            type_to_schema_non_nullable(data_type, name_counter)?,
        ])
    } else {
        type_to_schema_non_nullable(data_type, name_counter)?
    })
}

fn record_name(name_counter: &mut i32) -> String {
    *name_counter += 1;
    format!("r{name_counter}")
}

fn type_to_schema_non_nullable(
    data_type: &DataType,
    name_counter: &mut i32,
) -> error_stack::Result<AvroSchema, Error> {
    Ok(match data_type {
        DataType::Null => AvroSchema::Null,
        DataType::Boolean => AvroSchema::Boolean,
        DataType::Int32 => AvroSchema::Int(None),
        DataType::Int64 => AvroSchema::Long(None),
        DataType::Float32 => AvroSchema::Float,
        DataType::Float64 => AvroSchema::Double,
        DataType::Binary => AvroSchema::Bytes(None),
        DataType::LargeBinary => AvroSchema::Bytes(None),
        DataType::Utf8 => AvroSchema::String(None),
        DataType::LargeUtf8 => AvroSchema::String(None),
        DataType::Struct(fields) => AvroSchema::Record(Record::new(
            record_name(name_counter),
            fields
                .iter()
                .map(|f| arrow_to_avro(f, name_counter))
                .try_collect()?,
        )),
        DataType::Date32 => AvroSchema::Int(Some(IntLogical::Date)),
        DataType::Time32(TimeUnit::Millisecond) => AvroSchema::Int(Some(IntLogical::Time)),
        DataType::Time64(TimeUnit::Microsecond) => AvroSchema::Long(Some(LongLogical::Time)),
        DataType::Timestamp(TimeUnit::Millisecond, None) => {
            AvroSchema::Long(Some(LongLogical::LocalTimestampMillis))
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            AvroSchema::Long(Some(LongLogical::LocalTimestampMicros))
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            let mut fixed = Fixed::new("", 12);
            fixed.logical = Some(FixedLogical::Duration);
            AvroSchema::Fixed(fixed)
        }
        DataType::FixedSizeBinary(size) => AvroSchema::Fixed(Fixed::new("", (*size) as usize)),
        other => error_stack::bail!(Error::UnimplementedArrow(other.clone())),
    })
}

/// Converts an Avro [`AvroSchema`] to an arrow [`Schema`].
pub fn from_avro_schema(avro_schema: &AvroSchema) -> error_stack::Result<Schema, Error> {
    let mut names: HashMap<&str, usize> = HashMap::new();
    let fields = match avro_schema {
        AvroSchema::Record(record) => record
            .fields
            .iter()
            .map(|field| avro_to_arrow(field, &mut names))
            .collect::<Result<Vec<_>, _>>()?,
        _ => error_stack::bail!(Error::UnsupportedAvro(avro_schema.clone())),
    };
    Ok(Schema::new(fields))
}

fn avro_to_arrow(
    field: &AvroField,
    names: &mut HashMap<&str, usize>,
) -> error_stack::Result<Field, Error> {
    let name = field.name.as_str();
    let (data_type, nullable) = schema_to_type(&field.schema, names)?;
    Ok(Field::new(name, data_type, nullable))
}

/// returns (DataType, is_nullable)
fn schema_to_type(
    schema: &AvroSchema,
    names: &mut HashMap<&str, usize>,
) -> error_stack::Result<(DataType, bool), Error> {
    Ok(match schema {
        AvroSchema::Null => (DataType::Null, false),
        AvroSchema::Boolean => (DataType::Boolean, false),
        AvroSchema::Int(None) => (DataType::Int32, false),
        AvroSchema::Long(None) => (DataType::Int64, false),
        AvroSchema::Float => (DataType::Float32, false),
        AvroSchema::Double => (DataType::Float64, false),
        AvroSchema::Int(Some(IntLogical::Date)) => (DataType::Date32, false),
        AvroSchema::Int(Some(IntLogical::Time)) => (DataType::Time32(TimeUnit::Millisecond), false),
        AvroSchema::Long(Some(LongLogical::Time)) => {
            (DataType::Time64(TimeUnit::Microsecond), false)
        }
        AvroSchema::Long(Some(LongLogical::LocalTimestampMillis)) => {
            (DataType::Timestamp(TimeUnit::Millisecond, None), false)
        }
        AvroSchema::Long(Some(LongLogical::LocalTimestampMicros)) => {
            (DataType::Timestamp(TimeUnit::Microsecond, None), false)
        }
        AvroSchema::Bytes(None) => (DataType::Binary, false),
        AvroSchema::String(None) => (DataType::Utf8, false),
        AvroSchema::Fixed(fixed) => (DataType::FixedSizeBinary(fixed.size as i32), false),
        AvroSchema::Record(record) => (record_to_type(record, names)?, false),
        AvroSchema::Union(union) => {
            if union.len() == 2 && union.contains(&AvroSchema::Null) {
                let non_null_schema = union.iter().find(|&s| s != &AvroSchema::Null).unwrap();
                (schema_to_type(non_null_schema, names)?.0, true)
            } else {
                error_stack::bail!(Error::UnimplementedAvro(schema.clone()));
            }
        }
        _ => error_stack::bail!(Error::UnimplementedAvro(schema.clone())),
    })
}

fn record_to_type(
    record: &Record,
    names: &mut HashMap<&str, usize>,
) -> error_stack::Result<DataType, Error> {
    let fields = record
        .fields
        .iter()
        .map(|field| avro_to_arrow(field, names))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(DataType::Struct(fields.into()))
}

#[cfg(test)]
mod tests {
    use super::to_avro_schema;
    use crate::avro::from_avro_schema;
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn test_simple_struct_to_avro() {
        let arrow_schema = simple_arrow_schema();
        let avro_schema = to_avro_schema(&arrow_schema).unwrap();
        insta::assert_json_snapshot!(&avro_schema);
    }

    fn simple_arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Float64, false),
            Field::new("c", DataType::Utf8, true),
        ])
    }

    #[test]
    fn test_nested_struct_to_avro() {
        let arrow_schema = nested_arrow_schema();
        let avro_schema = to_avro_schema(&arrow_schema).unwrap();
        insta::assert_json_snapshot!(&avro_schema);
    }

    fn nested_arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Float64, false),
            Field::new("c", DataType::Utf8, true),
            Field::new(
                "d",
                DataType::Struct(
                    vec![
                        Field::new("a", DataType::Int64, true),
                        Field::new("b", DataType::Float64, false),
                    ]
                    .into(),
                ),
                false,
            ),
        ])
    }

    #[test]
    fn test_nullable_struct_to_avro() {
        let arrow_schema = nullable_arrow_schema();
        let avro_schema = to_avro_schema(&arrow_schema).unwrap();
        insta::assert_json_snapshot!(&avro_schema);
    }

    fn nullable_arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new(
                "b",
                DataType::Struct(
                    vec![
                        Field::new("c", DataType::Utf8, false),
                        Field::new("d", DataType::Int32, true),
                    ]
                    .into(),
                ),
                true,
            ),
        ])
    }

    fn test_round_trip(arrow_schema: Schema) {
        let avro_schema = to_avro_schema(&arrow_schema).unwrap();
        let result = from_avro_schema(&avro_schema).unwrap();
        assert_eq!(result, arrow_schema)
    }

    #[test]
    fn test_simple_round_trip() {
        test_round_trip(simple_arrow_schema());
    }

    #[test]
    fn test_nested_round_trip() {
        test_round_trip(nested_arrow_schema());
    }

    #[test]
    fn test_nullable_round_trip() {
        test_round_trip(nullable_arrow_schema());
    }
}
