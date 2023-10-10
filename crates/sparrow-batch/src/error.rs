#[derive(derive_more::Display, Debug)]
pub enum Error {
    #[display(fmt = "internal error: {}", _0)]
    Internal(String),
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
