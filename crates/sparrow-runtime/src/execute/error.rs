use sparrow_core::ErrorCode;

#[derive(derive_more::Display, Debug)]
pub enum Error {
    #[display(fmt = "compute request missing '{_0}'")]
    MissingField(&'static str),
    #[display(fmt = "unspecified per-entity behavior")]
    UnspecifiedPerEntityBehavior,
    #[display(fmt = "invalid batch input bounds")]
    InvalidBounds,
    #[display(fmt = "internal compute error: {_0}")]
    Internal(&'static str),
    #[display(fmt = "invalid operation: {_0}")]
    InvalidOperation(String),
    #[display(fmt = "invalid destination")]
    InvalidDestination,
    #[display(fmt = "failed to pre-process next input for operation")]
    PreprocessNextInput,
    #[display(fmt = "output '{output}' is not supported")]
    UnsupportedOutput { output: &'static str },
}

macro_rules! invalid_operation {
    ($fmt:expr) => {
        crate::execute::Error::InvalidOperation($fmt.to_owned())
    };
    ($fmt:expr, $($arg:expr),+) => {
        crate::execute::Error::InvalidOperation(format!($fmt, $($arg),+))
    };
}

pub(crate) use invalid_operation;

impl Error {
    pub fn internal() -> Self {
        Error::Internal("no additional context")
    }

    pub fn internal_msg(msg: &'static str) -> Self {
        Error::Internal(msg)
    }

    pub fn invalid_operation(msg: String) -> Self {
        Error::InvalidOperation(msg)
    }
}

impl error_stack::Context for Error {}

impl ErrorCode for Error {
    fn error_code(&self) -> tonic::Code {
        match self {
            Error::MissingField(_) => tonic::Code::InvalidArgument,
            _ => tonic::Code::Internal,
        }
    }
}
