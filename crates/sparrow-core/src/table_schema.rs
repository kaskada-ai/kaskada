use std::ops::Deref;
use std::sync::Arc;

use arrow::datatypes::{
    ArrowPrimitiveType, DataType, Field, FieldRef, Schema, SchemaRef, TimestampNanosecondType,
};
use static_init::dynamic;

/// A wrapper around a SchemaRef for the standard temporal table schema.
///
/// Specifically, all tables uses the same schema, consisting of these three
/// fields in addition to the data in the table:
///
/// - `_time` - The occurrence time associated with each row.
/// - `_subsort` - The subsort ID, used for ordering within each time.
/// - `_key_hash` - The hash of the primary key, used for comparisons.
///
/// This should be used in cases when the schema *must* include those fields. In
/// cases where the schema doesn't (or may not) include those fields, we instead
/// use `SchemaRef` directly.
#[repr(transparent)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TableSchema(SchemaRef);

#[dynamic]
static DEFAULT_SCHEMA: TableSchema = TableSchema::from_data_fields(vec![]).unwrap();

impl TableSchema {
    pub const TIME: &'static str = "_time";
    pub const SUBSORT: &'static str = "_subsort";
    pub const KEY_HASH: &'static str = "_key_hash";
    pub const NUM_KEY_COLUMNS: usize = 3;

    /// Returns the default `TableSchema`, which includes the [time, subsort,
    /// key_hash].
    pub fn default_schema() -> &'static TableSchema {
        &DEFAULT_SCHEMA
    }

    /// Create a `TableSchema` from a `SchemaRef` that already has the key
    /// fields.
    ///
    /// Use `from_data_schema` if the columns aren't already present.
    pub fn from_sparrow_schema(schema: SchemaRef) -> anyhow::Result<Self> {
        let fields = schema.fields();

        anyhow::ensure!(
            fields.len() >= 3,
            "Must have at least 3 fields, but was {:?}",
            fields
        );
        anyhow::ensure!(
            fields[0].name() == Self::TIME,
            "First field must be '{}', but was '{}'",
            Self::TIME,
            fields[0].name(),
        );
        anyhow::ensure!(
            fields[1].name() == Self::SUBSORT,
            "Second field must be '{}', but was '{}'",
            Self::SUBSORT,
            fields[1].name(),
        );
        anyhow::ensure!(
            fields[2].name() == Self::KEY_HASH,
            "Third field must be '{}', but was '{}'",
            Self::KEY_HASH,
            fields[2].name(),
        );
        Ok(Self(schema))
    }

    /// Create a `TableSchema` containing the given *data* fields.
    ///
    /// Returns an error if the data fields already contain the "key fields".
    pub fn from_data_fields(
        data_fields: impl IntoIterator<Item = FieldRef>,
    ) -> anyhow::Result<Self> {
        let iter = data_fields.into_iter();
        let mut result_fields = Vec::with_capacity(3 + iter.size_hint().0);
        result_fields.push(Arc::new(Field::new(
            Self::TIME,
            TimestampNanosecondType::DATA_TYPE,
            false,
        )));
        result_fields.push(Arc::new(Field::new(Self::SUBSORT, DataType::UInt64, false)));
        result_fields.push(Arc::new(Field::new(
            Self::KEY_HASH,
            DataType::UInt64,
            false,
        )));

        for field in iter {
            anyhow::ensure!(
                field.name() != Self::TIME
                    && field.name() != Self::SUBSORT
                    && field.name() != Self::KEY_HASH,
                "Data fields cannot use reserved name '{}'",
                field.name()
            );
            result_fields.push(field)
        }

        Ok(Self(Arc::new(Schema::new(result_fields))))
    }

    /// Create a `TableSchema` from a `SchemaRef` that doesn't already contain
    /// the key fields.
    ///
    /// This prepends the key columns. Use `from_sparrow_schema` if the columns
    /// are already present.
    pub fn try_from_data_schema<T: Deref<Target = Schema>>(schema: T) -> anyhow::Result<Self> {
        Self::from_data_fields(schema.fields().iter().cloned())
    }

    /// Return a reference to the Arrow schema associated with this table.
    pub fn schema_ref(&self) -> &SchemaRef {
        &self.0
    }

    /// Return the number of data columns in this table.
    pub fn num_data_columns(&self) -> usize {
        self.0.fields().len() - 3
    }

    /// Total number of columns, including the 3 key columns.
    pub fn num_columns(&self) -> usize {
        self.0.fields().len()
    }

    /// Return the given data column from this schema.
    pub fn data_field(&self, i: usize) -> &Field {
        &self.data_fields()[i]
    }

    /// Return the data fields.
    pub fn data_fields(&self) -> &[FieldRef] {
        &self.0.fields()[3..]
    }

    /// Returns the data columns of the current table schema as a struct.
    pub fn as_struct_type(&self) -> DataType {
        DataType::Struct(self.data_fields().into())
    }
}
