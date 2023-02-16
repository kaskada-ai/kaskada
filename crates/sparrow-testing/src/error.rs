#[derive(Debug, derive_more::Display)]
#[display(fmt = "error in test code")]
pub struct Error;

impl error_stack::Context for Error {}

pub type Result<T> = error_stack::Result<T, Error>;
