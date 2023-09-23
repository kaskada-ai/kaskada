use arrow::datatypes::Schema;
use avro_rs::types::Value;
use hashbrown::HashSet;

pub(crate) mod kafka;
pub(crate) mod pulsar;

pub struct AvroWrapper {
    user_record: Value,
    projected_record: Value,
}

#[derive(derive_more::Display, Debug)]
pub enum DeserializeError {
    #[display(fmt = "error reading Avro record")]
    Avro,
    #[display(fmt = "unsupported Avro value")]
    UnsupportedType,
    #[display(fmt = "internal error")]
    InternalError,
}

impl error_stack::Context for DeserializeError {}

#[derive(Debug)]
struct DeserializeErrorWrapper(error_stack::Report<DeserializeError>);

impl std::fmt::Display for DeserializeErrorWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl std::error::Error for DeserializeErrorWrapper {}

impl From<error_stack::Report<DeserializeError>> for DeserializeErrorWrapper {
    fn from(error: error_stack::Report<DeserializeError>) -> Self {
        DeserializeErrorWrapper(error)
    }
}

/// Determine needed indices given a file schema and projected schema.
fn get_columns_to_read(file_schema: &Schema, projected_schema: &Schema) -> Vec<usize> {
    let needed_columns: HashSet<_> = projected_schema
        .fields()
        .iter()
        .map(|field| field.name())
        .collect();

    let mut columns = Vec::with_capacity(3 + needed_columns.len());

    for (index, column) in file_schema.fields().iter().enumerate() {
        if needed_columns.contains(column.name()) {
            columns.push(index)
        }
    }

    columns
}
