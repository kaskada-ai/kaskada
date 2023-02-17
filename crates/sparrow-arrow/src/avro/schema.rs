use super::Error;
use arrow::datatypes::{DataType, Field, IntervalUnit, Schema, TimeUnit};
use avro_schema::schema::{
    Field as AvroField, Fixed, FixedLogical, IntLogical, LongLogical, Record, Schema as AvroSchema,
};
use error_stack::ResultExt;
use itertools::Itertools;

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
        .map(|f| field_to_field(f, &mut name_counter))
        .try_collect()?;

    Ok(Record {
        name: "Record".to_string(),
        namespace: None,
        doc: None,
        aliases: vec![],
        fields,
    })
}

fn field_to_field(field: &Field, name_counter: &mut i32) -> error_stack::Result<AvroField, Error> {
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
                .map(|f| field_to_field(f, name_counter))
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
        other => error_stack::bail!(Error::Unimplemented(other.clone())),
    })
}

#[cfg(test)]
mod tests {
    use super::to_avro_schema;
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn test_simple_struct_to_avro() {
        let arrow_schema = Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Float64, false),
            Field::new("c", DataType::Utf8, true),
        ]);

        let avro_schema = to_avro_schema(&arrow_schema).unwrap();
        insta::assert_json_snapshot!(&avro_schema);
    }

    #[test]
    fn test_nested_struct_to_avro() {
        let arrow_schema = Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Float64, false),
            Field::new("c", DataType::Utf8, true),
            Field::new(
                "d",
                DataType::Struct(vec![
                    Field::new("a", DataType::Int64, true),
                    Field::new("b", DataType::Float64, false),
                ]),
                false,
            ),
        ]);

        let avro_schema = to_avro_schema(&arrow_schema).unwrap();
        insta::assert_json_snapshot!(&avro_schema);
    }
}
