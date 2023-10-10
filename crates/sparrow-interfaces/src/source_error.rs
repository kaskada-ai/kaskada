#[non_exhaustive]
#[derive(derive_more::Display, Debug)]
pub enum SourceError {
    #[display(fmt = "internal error: {}", _0)]
    Internal(&'static str),
    #[display(fmt = "failed to add in-memory batch")]
    Add,
    #[display(fmt = "receiver lagged")]
    ReceiverLagged,
}

impl error_stack::Context for SourceError {}

impl SourceError {
    pub fn internal() -> Self {
        SourceError::Internal("no additional context")
    }

    pub fn internal_msg(msg: &'static str) -> Self {
        SourceError::Internal(msg)
    }
}
