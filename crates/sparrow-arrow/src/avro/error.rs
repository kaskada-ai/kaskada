use arrow::datatypes::DataType;

#[derive(derive_more::Display, Debug)]
pub enum Error {
    #[display(fmt = "unimplemented conversion from Arrow type {_0:?} to Avro")]
    Unimplemented(DataType),
}

impl error_stack::Context for Error {}
