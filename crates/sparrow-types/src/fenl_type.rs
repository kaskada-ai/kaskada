use std::sync::Arc;

use arrow_schema::{DataType, Field, Fields, TimeUnit};
use hashbrown::HashMap;
use itertools::Itertools;

use crate::TypeVariable;

/// Types used within signatures and during instantiation.
///
/// These need to allow type variables in any location
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FenlType {
    pub(crate) type_constructor: TypeConstructor,
    pub(crate) type_args: Vec<FenlType>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TypeConstructor {
    Concrete(DataType),
    List,
    Map(bool),
    Generic(TypeVariable),
}

impl std::fmt::Display for FenlType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.type_constructor {
            TypeConstructor::Concrete(data_type) => {
                write!(f, "{}", data_type.display())
            }
            TypeConstructor::List => {
                write!(f, "list<{}>", self.type_args[0])
            }
            TypeConstructor::Map(is_sorted) => {
                write!(
                    f,
                    "map<{}, {}, {is_sorted}>",
                    self.type_args[0], self.type_args[1]
                )
            }
            TypeConstructor::Generic(type_variable) => {
                write!(f, "{}", type_variable)
            }
        }
    }
}

impl From<DataType> for FenlType {
    fn from(value: DataType) -> Self {
        match value {
            DataType::List(item) => Self {
                type_constructor: TypeConstructor::List,
                type_args: vec![item.data_type().into()],
            },
            DataType::Map(key_value, sorted) => {
                let DataType::Struct(key_value) = key_value.data_type() else {
                    panic!("Unexpected: Map type with non-struct field: {key_value:?}");
                };
                assert_eq!(key_value.len(), 2);
                Self {
                    type_constructor: TypeConstructor::Map(sorted),
                    type_args: vec![
                        key_value[0].data_type().into(),
                        key_value[1].data_type().into(),
                    ],
                }
            }
            other => Self {
                type_constructor: TypeConstructor::Concrete(other),
                type_args: vec![],
            },
        }
    }
}

impl From<&DataType> for FenlType {
    fn from(value: &DataType) -> Self {
        match value {
            DataType::List(item) => Self {
                type_constructor: TypeConstructor::List,
                type_args: vec![item.data_type().into()],
            },
            DataType::Map(key_value, sorted) => {
                let DataType::Struct(key_value) = key_value.data_type() else {
                    panic!("Unexpected: Map type with non-struct field: {key_value:?}");
                };
                assert_eq!(key_value.len(), 2);
                Self {
                    type_constructor: TypeConstructor::Map(*sorted),
                    type_args: vec![
                        key_value[0].data_type().into(),
                        key_value[1].data_type().into(),
                    ],
                }
            }
            other => Self {
                type_constructor: TypeConstructor::Concrete(other.clone()),
                type_args: vec![],
            },
        }
    }
}

impl FenlType {
    pub fn generic(type_variable: impl Into<TypeVariable>) -> Self {
        Self {
            type_constructor: TypeConstructor::Generic(type_variable.into()),
            type_args: Vec::new(),
        }
    }

    pub fn has_generic(&self) -> bool {
        if matches!(self.type_constructor, TypeConstructor::Generic(_)) {
            true
        } else {
            self.type_args.iter().any(|arg| arg.has_generic())
        }
    }

    pub fn generics(&self) -> hashbrown::HashSet<&TypeVariable> {
        let mut result = hashbrown::HashSet::new();
        self.add_generics(&mut result);
        result
    }

    pub(crate) fn add_generics<'a>(&'a self, generics: &mut hashbrown::HashSet<&'a TypeVariable>) {
        match &self.type_constructor {
            TypeConstructor::Generic(type_variable) => {
                generics.insert(type_variable);
            }
            _ => {
                for arg in &self.type_args {
                    arg.add_generics(generics);
                }
            }
        }
    }

    pub fn instantiate(
        &self,
        solutions: Option<&HashMap<TypeVariable, DataType>>,
    ) -> Option<DataType> {
        match &self.type_constructor {
            TypeConstructor::Generic(type_var) => solutions.and_then(|a| a.get(type_var).cloned()),
            TypeConstructor::Concrete(data_type) => Some(data_type.clone()),
            TypeConstructor::List => {
                debug_assert_eq!(self.type_args.len(), 1);
                let Some(item) = self.type_args[0].instantiate(solutions) else {
                    return None;
                };
                let item = Arc::new(Field::new("item", item, true));
                Some(DataType::List(item))
            }
            TypeConstructor::Map(sorted) => {
                debug_assert_eq!(self.type_args.len(), 2);
                let Some(key) = self.type_args[0].instantiate(solutions) else {
                    return None;
                };
                let Some(value) = self.type_args[1].instantiate(solutions) else {
                    return None;
                };

                let key = Field::new("key", key, false);
                let value = Field::new("value", value, true);

                let key_value = DataType::Struct(vec![key, value].into());
                let key_value = Arc::new(Field::new("entries", key_value, false));
                Some(DataType::Map(key_value, *sorted))
            }
        }
    }

    /// Return true if the type is concrete and equal to `expected`.
    pub fn is_concrete(&self, expected: &DataType) -> bool {
        matches!(&self.type_constructor, TypeConstructor::Concrete(actual) if actual == expected)
    }
}

pub trait DisplayFenlType {
    type DisplayType;
    fn display(self) -> Self::DisplayType;
}

impl<'a> DisplayFenlType for &'a DataType {
    type DisplayType = DisplayDataType<'a>;

    fn display(self) -> Self::DisplayType {
        DisplayDataType(self)
    }
}

/// A wrapper for formatting DataTypes.
pub struct DisplayDataType<'a>(pub &'a DataType);

impl<'a> std::fmt::Display for DisplayDataType<'a> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            DataType::Null => fmt.write_str("null"),
            DataType::Boolean => fmt.write_str("bool"),
            DataType::Date32 => fmt.write_str("date32"),
            DataType::Date64 => fmt.write_str("date64"),
            DataType::Int8 => fmt.write_str("i8"),
            DataType::Int16 => fmt.write_str("i16"),
            DataType::Int32 => fmt.write_str("i32"),
            DataType::Int64 => fmt.write_str("i64"),
            DataType::UInt8 => fmt.write_str("u8"),
            DataType::UInt16 => fmt.write_str("u16"),
            DataType::UInt32 => fmt.write_str("u32"),
            DataType::UInt64 => fmt.write_str("u64"),
            DataType::Float16 => fmt.write_str("f16"),
            DataType::Float32 => fmt.write_str("f32"),
            DataType::Float64 => fmt.write_str("f64"),
            DataType::Timestamp(unit, _) => fmt.write_str(match unit {
                TimeUnit::Second => "timestamp_s",
                TimeUnit::Millisecond => "timestamp_ms",
                TimeUnit::Microsecond => "timestamp_us",
                TimeUnit::Nanosecond => "timestamp_ns",
            }),
            DataType::Time32(_) => todo!(),
            DataType::Time64(_) => todo!(),
            DataType::Duration(_) => todo!(),
            DataType::Interval(_) => todo!(),
            DataType::Binary => fmt.write_str("bytes"),
            DataType::FixedSizeBinary(size) => write!(fmt, "fixed_bytes[{size}]"),
            DataType::LargeBinary => fmt.write_str("large_bytes"),
            DataType::Utf8 => fmt.write_str("text"),
            DataType::LargeUtf8 => fmt.write_str("longtext"),
            DataType::List(item) => {
                write!(fmt, "list<{}>", item.data_type().display())
            }
            DataType::FixedSizeList(_, _) => todo!(),
            DataType::LargeList(_) => todo!(),
            DataType::Struct(fields) => {
                write!(fmt, "{}", DisplayStruct(fields))
            }
            DataType::Union(_, _) => todo!(),
            DataType::Map(field, sorted) => {
                let DataType::Struct(fields) = field.data_type() else {
                    panic!("Unexpected: Map type with non-struct field");
                };
                assert_eq!(fields.len(), 2);
                write!(
                    fmt,
                    "map<{}, {}, {sorted}>",
                    fields[0].data_type().display(),
                    fields[1].data_type().display(),
                )
            }
            DataType::Decimal256(_, _) => todo!(),
            DataType::Dictionary(_, _) => todo!(),
            DataType::Decimal128(_, _) => todo!(),
            DataType::RunEndEncoded(_, _) => todo!(),
        }
    }
}

// Creates a struct that can be given a reference to set of fields
/// A wrapper for formatting structs.
struct DisplayStruct<'a>(pub &'a Fields);
impl<'a> std::fmt::Display for DisplayStruct<'a> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            fmt,
            "{{{}}}",
            self.0.iter().format_with(", ", |field, f| {
                f(&format_args!(
                    "{}: {}",
                    field.name(),
                    DisplayDataType(field.data_type())
                ))
            })
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::Field;

    use super::*;

    #[test]
    fn test_equality() {
        assert_eq!(
            FenlType::from(DataType::Int32),
            FenlType::from(DataType::Int32)
        );
        assert_ne!(
            FenlType::from(DataType::Int32),
            FenlType::from(DataType::Int64)
        );

        let nullable_int32_field = Arc::new(Field::new("item", DataType::Int32, true));
        assert_eq!(
            FenlType::from(DataType::List(nullable_int32_field.clone())),
            FenlType::from(DataType::List(nullable_int32_field.clone()))
        );

        assert_eq!(
            FenlType::from(DataType::List(nullable_int32_field.clone())),
            FenlType {
                type_constructor: TypeConstructor::List,
                type_args: vec![FenlType::from(DataType::Int32)],
            }
        );
        assert_ne!(
            FenlType::from(DataType::List(nullable_int32_field.clone())),
            FenlType {
                type_constructor: TypeConstructor::List,
                type_args: vec![FenlType::from(DataType::Int64)],
            }
        );
    }
}
