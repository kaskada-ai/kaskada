use std::sync::Arc;

use arrow::datatypes::ArrowPrimitiveType;
use itertools::Itertools;
use sparrow_syntax::FenlType;
use thiserror::Error;

// use crate::kaskada::sparrow::v1alpha::schema;
use crate::kaskada::v1alpha::{data_type, schema, DataType, Schema};

impl DataType {
    pub fn new_struct(fields: Vec<schema::Field>) -> Self {
        Self {
            kind: Some(data_type::Kind::Struct(Schema { fields })),
        }
    }
    /// Creates a new map from the given fields.
    ///
    /// `fields` should have two elements, the first being the key type
    /// and the second being the value type.
    pub fn new_map(name: &str, ordered: bool, fields: Vec<schema::Field>) -> Self {
        debug_assert!(fields.len() == 2);
        let key = &fields[0];
        let value = &fields[1];
        Self {
            kind: Some(data_type::Kind::Map(Box::new(data_type::Map {
                name: name.to_owned(),
                ordered,
                key_name: key.name.clone(),
                key_type: Some(Box::new(
                    key.data_type.as_ref().expect("data type to exist").clone(),
                )),
                key_is_nullable: key.nullable,
                value_name: value.name.clone(),
                value_type: Some(Box::new(
                    value
                        .data_type
                        .as_ref()
                        .expect("data type to exist")
                        .clone(),
                )),
                value_is_nullable: value.nullable,
            }))),
        }
    }

    pub fn new_list(name: &str, field: schema::Field) -> Self {
        Self {
            kind: Some(data_type::Kind::List(Box::new(data_type::List {
                name: name.to_owned(),
                item_type: Some(Box::new(
                    field
                        .data_type
                        .as_ref()
                        .expect("data type to exist")
                        .clone(),
                )),
                nullable: field.nullable,
            }))),
        }
    }

    pub fn new_primitive(primitive: data_type::PrimitiveType) -> Self {
        Self {
            kind: Some(data_type::Kind::Primitive(primitive as i32)),
        }
    }
}

fn fields_to_arrow(
    fields: &[schema::Field],
) -> Result<Vec<arrow::datatypes::Field>, ConversionError<DataType>> {
    fields
        .iter()
        .map(|field| {
            let data_type = field
                .data_type
                .as_ref()
                .unwrap_or_else(|| unreachable!("Unexpected missing data type: {:?}", field));
            let field_type = arrow::datatypes::DataType::try_from(data_type)
                .map_err(|e| e.with_prepend_field(field.name.clone()))?;
            Ok(arrow::datatypes::Field::new(&field.name, field_type, true))
        })
        .try_collect()
}

/// Error returned when we couldn't convert a proto to an arrow DataType.
#[derive(Debug, PartialEq, Eq, Error)]
pub struct ConversionError<T: std::fmt::Debug> {
    /// The path to this type (through outer structs).
    fields: Vec<String>,
    /// The data type we couldn't convert.
    data_type: T,
}

impl<T: std::fmt::Debug> ConversionError<T> {
    fn new_unsupported(data_type: T) -> Self {
        Self {
            fields: Vec::new(),
            data_type,
        }
    }

    fn with_prepend_field(mut self, field: String) -> Self {
        self.fields.push(field);
        self
    }

    fn map_data_type<T2: std::fmt::Debug>(self, f: impl FnOnce(T) -> T2) -> ConversionError<T2> {
        ConversionError {
            fields: self.fields,
            data_type: f(self.data_type),
        }
    }
}

impl<T: std::fmt::Debug> std::fmt::Display for ConversionError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.fields.is_empty() {
            write!(f, "Unsupported conversion from '{:?}'", self.data_type)
        } else {
            write!(
                f,
                "Unsupported conversion from '{:?}' for field '{}'",
                self.data_type,
                self.fields.iter().rev().format(".")
            )
        }
    }
}

impl TryFrom<&arrow::datatypes::DataType> for DataType {
    type Error = ConversionError<arrow::datatypes::DataType>;

    fn try_from(value: &arrow::datatypes::DataType) -> Result<Self, Self::Error> {
        use data_type::PrimitiveType;
        match value {
            arrow::datatypes::DataType::Date32 => {
                Ok(DataType::new_primitive(PrimitiveType::Date32))
            }
            arrow::datatypes::DataType::Null => Ok(DataType::new_primitive(PrimitiveType::Null)),
            arrow::datatypes::DataType::Boolean => Ok(DataType::new_primitive(PrimitiveType::Bool)),
            arrow::datatypes::DataType::Int8 => Ok(DataType::new_primitive(PrimitiveType::I8)),
            arrow::datatypes::DataType::Int16 => Ok(DataType::new_primitive(PrimitiveType::I16)),
            arrow::datatypes::DataType::Int32 => Ok(DataType::new_primitive(PrimitiveType::I32)),
            arrow::datatypes::DataType::Int64 => Ok(DataType::new_primitive(PrimitiveType::I64)),
            arrow::datatypes::DataType::UInt8 => Ok(DataType::new_primitive(PrimitiveType::U8)),
            arrow::datatypes::DataType::UInt16 => Ok(DataType::new_primitive(PrimitiveType::U16)),
            arrow::datatypes::DataType::UInt32 => Ok(DataType::new_primitive(PrimitiveType::U32)),
            arrow::datatypes::DataType::UInt64 => Ok(DataType::new_primitive(PrimitiveType::U64)),
            arrow::datatypes::DataType::Float16 => Ok(DataType::new_primitive(PrimitiveType::F16)),
            arrow::datatypes::DataType::Float32 => Ok(DataType::new_primitive(PrimitiveType::F32)),
            arrow::datatypes::DataType::Float64 => Ok(DataType::new_primitive(PrimitiveType::F64)),
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Second, None) => {
                Ok(DataType::new_primitive(PrimitiveType::TimestampSecond))
            }
            arrow::datatypes::DataType::Timestamp(
                arrow::datatypes::TimeUnit::Millisecond,
                None,
            ) => Ok(DataType::new_primitive(PrimitiveType::TimestampMillisecond)),
            arrow::datatypes::DataType::Timestamp(
                arrow::datatypes::TimeUnit::Microsecond,
                None,
            ) => Ok(DataType::new_primitive(PrimitiveType::TimestampMicrosecond)),
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None) => {
                Ok(DataType::new_primitive(PrimitiveType::TimestampNanosecond))
            }
            arrow::datatypes::DataType::Duration(arrow::datatypes::TimeUnit::Second) => {
                Ok(DataType::new_primitive(PrimitiveType::DurationSecond))
            }
            arrow::datatypes::DataType::Duration(arrow::datatypes::TimeUnit::Millisecond) => {
                Ok(DataType::new_primitive(PrimitiveType::DurationMillisecond))
            }
            arrow::datatypes::DataType::Duration(arrow::datatypes::TimeUnit::Microsecond) => {
                Ok(DataType::new_primitive(PrimitiveType::DurationMicrosecond))
            }
            arrow::datatypes::DataType::Duration(arrow::datatypes::TimeUnit::Nanosecond) => {
                Ok(DataType::new_primitive(PrimitiveType::DurationNanosecond))
            }
            arrow::datatypes::DataType::Interval(arrow::datatypes::IntervalUnit::DayTime) => {
                Ok(DataType::new_primitive(PrimitiveType::IntervalDayTime))
            }
            arrow::datatypes::DataType::Interval(arrow::datatypes::IntervalUnit::YearMonth) => {
                Ok(DataType::new_primitive(PrimitiveType::IntervalYearMonth))
            }
            arrow::datatypes::DataType::Utf8 => Ok(DataType::new_primitive(PrimitiveType::String)),
            arrow::datatypes::DataType::LargeUtf8 => {
                Ok(DataType::new_primitive(PrimitiveType::LargeString))
            }
            arrow::datatypes::DataType::Struct(fields) => {
                let fields = fields
                    .iter()
                    .map(|field| -> Result<schema::Field, Self::Error> {
                        let name = field.name().to_owned();
                        match field.data_type().try_into() {
                            Ok(data_type) => Ok(schema::Field {
                                name,
                                data_type: Some(data_type),
                                nullable: field.is_nullable(),
                            }),
                            Err(err) => Err(err.with_prepend_field(name)),
                        }
                    })
                    .try_collect()?;
                Ok(DataType::new_struct(fields))
            }
            // Note: the `ordered` field may let us specialize the implementation
            // to use binary search in the future.
            arrow::datatypes::DataType::Map(s, is_ordered) => {
                // [DataType::Map] is represented as a list of structs with two fields: `key` and `value`
                let arrow::datatypes::DataType::Struct(fields) = s.data_type() else {
                    // unexpected - maps should always contain a struct
                    return Err(ConversionError::new_unsupported(s.data_type().clone()));
                };

                debug_assert!(fields.len() == 2, "expect two fields in map");
                let key = &fields[0];
                let value = &fields[1];
                let key = schema::Field {
                    name: key.name().to_owned(),
                    data_type: Some(key.data_type().try_into().map_err(
                        |err: ConversionError<arrow::datatypes::DataType>| {
                            err.with_prepend_field("key".to_owned())
                        },
                    )?),
                    nullable: key.is_nullable(),
                };
                let value = schema::Field {
                    name: value.name().to_owned(),
                    data_type: Some(value.data_type().try_into().map_err(
                        |err: ConversionError<arrow::datatypes::DataType>| {
                            err.with_prepend_field("value".to_owned())
                        },
                    )?),
                    nullable: value.is_nullable(),
                };

                Ok(DataType::new_map(s.name(), *is_ordered, vec![key, value]))
            }
            arrow::datatypes::DataType::List(field) => {
                let name = field.name();
                let field = schema::Field {
                    name: name.to_owned(),
                    data_type: Some(field.data_type().try_into().map_err(
                        |err: ConversionError<arrow::datatypes::DataType>| {
                            err.with_prepend_field("list item".to_owned())
                        },
                    )?),
                    nullable: field.is_nullable(),
                };

                Ok(DataType::new_list(name, field))
            }

            unsupported => Err(ConversionError::new_unsupported(unsupported.clone())),
        }
    }
}

impl TryFrom<&DataType> for FenlType {
    type Error = ConversionError<DataType>;
    fn try_from(
        value: &DataType,
    ) -> Result<Self, <sparrow_syntax::FenlType as TryFrom<&DataType>>::Error> {
        match &value.kind {
            Some(data_type::Kind::Window(())) => Ok(FenlType::Window),
            None => Ok(FenlType::Error),
            _ => {
                let concrete = arrow::datatypes::DataType::try_from(value)?;
                Ok(FenlType::Concrete(concrete))
            }
        }
    }
}

impl TryFrom<&FenlType> for DataType {
    type Error = ConversionError<FenlType>;

    fn try_from(value: &FenlType) -> Result<Self, Self::Error> {
        match value {
            FenlType::Collection(_, _) => Err(ConversionError::new_unsupported(value.clone())),
            FenlType::Concrete(data_type) => {
                data_type
                    .try_into()
                    .map_err(|e: ConversionError<arrow::datatypes::DataType>| {
                        e.map_data_type(FenlType::Concrete)
                    })
            }
            FenlType::TypeRef(_) => Err(ConversionError::new_unsupported(value.clone())),
            FenlType::Window => Ok(Self {
                kind: Some(data_type::Kind::Window(())),
            }),
            FenlType::Json => Ok(Self {
                kind: Some(data_type::Kind::Primitive(
                    data_type::PrimitiveType::Bool as i32,
                )),
            }),
            FenlType::Error => Ok(Self { kind: None }),
        }
    }
}

impl TryFrom<&DataType> for arrow::datatypes::DataType {
    type Error = ConversionError<DataType>;
    fn try_from(value: &DataType) -> Result<Self, Self::Error> {
        match &value.kind {
            Some(data_type::Kind::Primitive(primitive)) => {
                use data_type::PrimitiveType;
                match PrimitiveType::from_i32(*primitive) {
                    Some(PrimitiveType::Date32) => Ok(arrow::datatypes::DataType::Date32),
                    Some(PrimitiveType::Null) => Ok(arrow::datatypes::DataType::Null),
                    Some(PrimitiveType::Bool) => Ok(arrow::datatypes::DataType::Boolean),
                    Some(PrimitiveType::I8) => Ok(arrow::datatypes::DataType::Int8),
                    Some(PrimitiveType::I16) => Ok(arrow::datatypes::DataType::Int16),
                    Some(PrimitiveType::I32) => Ok(arrow::datatypes::DataType::Int32),
                    Some(PrimitiveType::I64) => Ok(arrow::datatypes::DataType::Int64),
                    Some(PrimitiveType::U8) => Ok(arrow::datatypes::DataType::UInt8),
                    Some(PrimitiveType::U16) => Ok(arrow::datatypes::DataType::UInt16),
                    Some(PrimitiveType::U32) => Ok(arrow::datatypes::DataType::UInt32),
                    Some(PrimitiveType::U64) => Ok(arrow::datatypes::DataType::UInt64),
                    Some(PrimitiveType::F16) => Ok(arrow::datatypes::DataType::Float16),
                    Some(PrimitiveType::F32) => Ok(arrow::datatypes::DataType::Float32),
                    Some(PrimitiveType::F64) => Ok(arrow::datatypes::DataType::Float64),
                    Some(PrimitiveType::String) => Ok(arrow::datatypes::DataType::Utf8),
                    Some(PrimitiveType::LargeString) => Ok(arrow::datatypes::DataType::LargeUtf8),
                    Some(PrimitiveType::IntervalDayTime) => {
                        Ok(arrow::datatypes::DataType::Interval(
                            arrow::datatypes::IntervalUnit::DayTime,
                        ))
                    }
                    Some(PrimitiveType::IntervalYearMonth) => {
                        Ok(arrow::datatypes::DataType::Interval(
                            arrow::datatypes::IntervalUnit::YearMonth,
                        ))
                    }
                    Some(PrimitiveType::DurationSecond) => {
                        Ok(arrow::datatypes::DurationSecondType::DATA_TYPE)
                    }
                    Some(PrimitiveType::DurationMillisecond) => {
                        Ok(arrow::datatypes::DurationMillisecondType::DATA_TYPE)
                    }
                    Some(PrimitiveType::DurationMicrosecond) => {
                        Ok(arrow::datatypes::DurationMicrosecondType::DATA_TYPE)
                    }
                    Some(PrimitiveType::DurationNanosecond) => {
                        Ok(arrow::datatypes::DurationNanosecondType::DATA_TYPE)
                    }
                    Some(PrimitiveType::TimestampSecond) => {
                        Ok(arrow::datatypes::TimestampSecondType::DATA_TYPE)
                    }
                    Some(PrimitiveType::TimestampMillisecond) => {
                        Ok(arrow::datatypes::TimestampMillisecondType::DATA_TYPE)
                    }
                    Some(PrimitiveType::TimestampMicrosecond) => {
                        Ok(arrow::datatypes::TimestampMicrosecondType::DATA_TYPE)
                    }
                    Some(PrimitiveType::TimestampNanosecond) => {
                        Ok(arrow::datatypes::TimestampNanosecondType::DATA_TYPE)
                    }
                    Some(PrimitiveType::Json) => {
                        Err(ConversionError::new_unsupported(value.clone()))
                    }
                    None | Some(PrimitiveType::Unspecified) => {
                        Err(ConversionError::new_unsupported(value.clone()))
                    }
                }
            }
            Some(data_type::Kind::Struct(schema)) => Ok(arrow::datatypes::DataType::Struct(
                fields_to_arrow(&schema.fields)?.into(),
            )),
            Some(data_type::Kind::List(list)) => match list.item_type.as_ref() {
                Some(item_type) => {
                    let item_type = arrow::datatypes::DataType::try_from(item_type.as_ref())
                        .map_err(|e| e.with_prepend_field("list item".to_owned()))?;
                    let item_type =
                        arrow::datatypes::Field::new(list.name.clone(), item_type, list.nullable);
                    Ok(arrow::datatypes::DataType::List(Arc::new(item_type)))
                }
                None => Err(ConversionError::new_unsupported(value.clone())),
            },
            Some(data_type::Kind::Map(map)) => {
                match (map.key_type.as_ref(), map.value_type.as_ref()) {
                    (Some(key), Some(value)) => {
                        let key = arrow::datatypes::DataType::try_from(key.as_ref())
                            .map_err(|e| e.with_prepend_field("map key".to_owned()))?;
                        let value = arrow::datatypes::DataType::try_from(value.as_ref())
                            .map_err(|e| e.with_prepend_field("map value".to_owned()))?;

                        let fields = arrow::datatypes::Fields::from(vec![
                            arrow::datatypes::Field::new(
                                map.key_name.clone(),
                                key,
                                map.key_is_nullable,
                            ),
                            arrow::datatypes::Field::new(
                                map.value_name.clone(),
                                value,
                                map.value_is_nullable,
                            ),
                        ]);
                        let s = arrow::datatypes::Field::new(
                            map.name.clone(),
                            arrow::datatypes::DataType::Struct(fields),
                            false,
                        );
                        Ok(arrow::datatypes::DataType::Map(Arc::new(s), map.ordered))
                    }
                    _ => Err(ConversionError::new_unsupported(value.clone())),
                }
            }
            None | Some(data_type::Kind::Window(_)) => {
                Err(ConversionError::new_unsupported(value.clone()))
            }
        }
    }
}

impl Schema {
    /// Convert this Schema protobuf to an arrow `Schema`.
    ///
    /// This should generally be placed inside an `Arc` to avoid deep
    /// copying the schema.
    pub fn as_arrow_schema(&self) -> Result<arrow::datatypes::Schema, ConversionError<DataType>> {
        let fields = fields_to_arrow(&self.fields);
        fields.map(arrow::datatypes::Schema::new)
    }
}

impl TryFrom<&arrow::datatypes::Schema> for Schema {
    type Error = ConversionError<arrow::datatypes::DataType>;

    fn try_from(value: &arrow::datatypes::Schema) -> Result<Self, Self::Error> {
        let fields = value
            .fields()
            .iter()
            .map(|field| -> Result<schema::Field, Self::Error> {
                let name = field.name().to_owned();
                match field.data_type().try_into() {
                    Ok(data_type) => Ok(schema::Field {
                        name,
                        data_type: Some(data_type),
                        nullable: field.is_nullable(),
                    }),
                    Err(err) => Err(err.with_prepend_field(name)),
                }
            })
            .try_collect()?;
        Ok(Schema { fields })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_data_type_round_trip(expected: &arrow::datatypes::DataType) {
        let proto = DataType::try_from(expected).unwrap();
        let actual = arrow::datatypes::DataType::try_from(&proto).unwrap();
        assert_eq!(expected, &actual)
    }

    fn assert_schema_round_trip(expected: &arrow::datatypes::Schema) {
        let proto = Schema::try_from(expected).unwrap();
        let actual = proto.as_arrow_schema().unwrap();
        assert_eq!(expected, &actual)
    }

    #[test]
    fn test_primitive_types_proto_round_trip() {
        let primitive_types = [
            arrow::datatypes::DataType::Null,
            arrow::datatypes::DataType::Boolean,
            arrow::datatypes::DataType::Int16,
            arrow::datatypes::DataType::Int32,
            arrow::datatypes::DataType::Int64,
            arrow::datatypes::DataType::UInt16,
            arrow::datatypes::DataType::UInt32,
            arrow::datatypes::DataType::UInt64,
            arrow::datatypes::DataType::Float32,
            arrow::datatypes::DataType::Float64,
            arrow::datatypes::DataType::Utf8,
            arrow::datatypes::DataType::LargeUtf8,
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Second, None),
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            arrow::datatypes::DataType::Duration(arrow::datatypes::TimeUnit::Second),
            arrow::datatypes::DataType::Duration(arrow::datatypes::TimeUnit::Millisecond),
            arrow::datatypes::DataType::Duration(arrow::datatypes::TimeUnit::Microsecond),
            arrow::datatypes::DataType::Duration(arrow::datatypes::TimeUnit::Nanosecond),
            arrow::datatypes::DataType::Interval(arrow::datatypes::IntervalUnit::DayTime),
            arrow::datatypes::DataType::Interval(arrow::datatypes::IntervalUnit::YearMonth),
        ];

        for primitive_type in primitive_types {
            assert_data_type_round_trip(&primitive_type)
        }
    }

    #[test]
    fn test_struct_round_trip() {
        let inner_struct_type = arrow::datatypes::DataType::Struct(
            vec![
                arrow::datatypes::Field::new("a", arrow::datatypes::DataType::Int64, true),
                arrow::datatypes::Field::new("b", arrow::datatypes::DataType::Utf8, true),
            ]
            .into(),
        );

        assert_data_type_round_trip(&inner_struct_type);

        let outer_struct_type = arrow::datatypes::DataType::Struct(
            vec![
                arrow::datatypes::Field::new("a", inner_struct_type, true),
                arrow::datatypes::Field::new("b", arrow::datatypes::DataType::Utf8, true),
            ]
            .into(),
        );

        assert_data_type_round_trip(&outer_struct_type);
    }

    #[test]
    fn test_schema_round_trip() {
        // Schema with primitive fields.
        let arrow_schema = arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("a", arrow::datatypes::DataType::Int64, true),
            arrow::datatypes::Field::new("b", arrow::datatypes::DataType::Utf8, true),
        ]);
        assert_schema_round_trip(&arrow_schema);

        // Schema with struct field.
        let inner_struct_type = arrow::datatypes::DataType::Struct(
            vec![
                arrow::datatypes::Field::new("a", arrow::datatypes::DataType::Int64, true),
                arrow::datatypes::Field::new("b", arrow::datatypes::DataType::Utf8, true),
            ]
            .into(),
        );
        let arrow_schema = arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("a", inner_struct_type, true),
            arrow::datatypes::Field::new("b", arrow::datatypes::DataType::Utf8, true),
        ]);
        assert_schema_round_trip(&arrow_schema);
    }

    #[test]
    fn test_unsupported_datatype() {
        let err = DataType::try_from(&arrow::datatypes::DataType::FixedSizeBinary(1)).unwrap_err();
        assert_eq!(
            err,
            ConversionError {
                fields: vec![],
                data_type: arrow::datatypes::DataType::FixedSizeBinary(1)
            }
        );
        assert_eq!(
            &err.to_string(),
            "Unsupported conversion from 'FixedSizeBinary(1)'"
        );
    }

    #[test]
    fn test_unsupported_nested_struct() {
        let inner_struct_type = arrow::datatypes::DataType::Struct(
            vec![
                arrow::datatypes::Field::new("a", arrow::datatypes::DataType::Int64, true),
                arrow::datatypes::Field::new(
                    "b",
                    arrow::datatypes::DataType::FixedSizeBinary(1),
                    true,
                ),
            ]
            .into(),
        );
        let outer_struct_type = arrow::datatypes::DataType::Struct(
            vec![
                arrow::datatypes::Field::new("x", inner_struct_type, true),
                arrow::datatypes::Field::new("y", arrow::datatypes::DataType::Utf8, true),
            ]
            .into(),
        );
        let err = DataType::try_from(&outer_struct_type).unwrap_err();
        assert_eq!(
            err,
            ConversionError {
                fields: vec!["b".to_owned(), "x".to_owned()],
                data_type: arrow::datatypes::DataType::FixedSizeBinary(1),
            }
        );
        assert_eq!(
            &err.to_string(),
            "Unsupported conversion from 'FixedSizeBinary(1)' for field 'x.b'"
        );
    }

    #[test]
    fn test_unsupported_nested_schema() {
        let inner_struct_type = arrow::datatypes::DataType::Struct(
            vec![
                arrow::datatypes::Field::new("a", arrow::datatypes::DataType::Int64, true),
                arrow::datatypes::Field::new(
                    "b",
                    arrow::datatypes::DataType::FixedSizeBinary(1),
                    true,
                ),
            ]
            .into(),
        );
        let outer_schema = arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("x", inner_struct_type, true),
            arrow::datatypes::Field::new("y", arrow::datatypes::DataType::Utf8, true),
        ]);
        let err = Schema::try_from(&outer_schema).unwrap_err();
        assert_eq!(
            err,
            ConversionError {
                fields: vec!["b".to_owned(), "x".to_owned()],
                data_type: arrow::datatypes::DataType::FixedSizeBinary(1),
            }
        );
        assert_eq!(
            &err.to_string(),
            "Unsupported conversion from 'FixedSizeBinary(1)' for field 'x.b'"
        );
    }
}
