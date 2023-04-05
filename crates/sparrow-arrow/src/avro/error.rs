use arrow::datatypes::DataType;

#[derive(derive_more::Display, Debug)]
pub enum Error {
    #[display(fmt = "unimplemented conversion from Arrow type {_0:?} to Avro")]
    UnimplementedArrow(DataType),
    #[display(fmt = "unimplemented conversion from Avro type {_0:?} to Arrow")]
    UnimplementedAvro(avro_schema::schema::Schema),
    #[display(fmt = "unsupported Avro top-level schema type {_0:?}")]
    UnsupportedAvro(avro_schema::schema::Schema),
}

impl error_stack::Context for Error {}
