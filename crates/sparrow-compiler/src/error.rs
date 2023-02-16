use sparrow_core::ErrorCode;

#[derive(derive_more::Display, Debug)]
pub enum Error {
    #[display(fmt = "compile request missing '{_0}'")]
    MissingField(&'static str),
    #[display(fmt = "compilation error")]
    CompileError,
    #[display(fmt = "query compilation error: {_0}")]
    Internal(&'static str),
    #[display(fmt = "failed to extract plan protos")]
    ExtractPlanProto,
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
