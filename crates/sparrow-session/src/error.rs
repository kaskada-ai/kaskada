use itertools::Itertools;
use sparrow_compiler::NearestMatches;

#[derive(derive_more::Display, Debug)]
pub enum Error {
    #[display(fmt = "failed to create table '{name}'")]
    CreateTable { name: String },
    #[display(fmt = "failed to encode schema for table '{_0}'")]
    SchemaForTable(String),
    #[display(fmt = "invalid expression")]
    Invalid,
    #[display(fmt = "invalid url: {_0}")]
    InvalidUrl(String),
    #[display(fmt = "error opening file: {_0}")]
    OpeningFile(String),
    #[display(fmt = "error reading file")]
    ReadingFile,
    #[display(fmt = "no function named '{name}'; nearest matches are: {nearest}")]
    NoSuchFunction {
        name: String,
        nearest: NearestMatches<String>,
    },
    #[display(fmt = "{}", "_0.iter().join(\"\n\")")]
    Errors(Vec<String>),
    #[display(fmt = "failed to prepare batch")]
    Prepare,
    #[display(fmt = "internal error: {_0}")]
    Internal(String),
    #[display(fmt = "compile query")]
    Compile,
    #[display(fmt = "execute query")]
    Execute,
    #[display(fmt = "execution failed")]
    ExecutionFailed,
}

impl error_stack::Context for Error {}

impl Error {
    pub fn internal() -> Self {
        Error::Internal("no additional context".to_owned())
    }

    pub fn internal_msg(msg: String) -> Self {
        Error::Internal(msg)
    }
}
