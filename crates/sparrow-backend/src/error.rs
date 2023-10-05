use std::borrow::Cow;

#[derive(derive_more::Display, Debug)]
pub enum Error {
    #[display(fmt = "no instruction named '{_0}'")]
    NoSuchInstruction(String),
    #[display(fmt = "invalid logical plan: {_0}")]
    InvalidLogicalPlan(Cow<'static, str>),
    #[display(fmt = "internal error: {_0}")]
    Internal(Cow<'static, str>),
}

impl Error {
    pub fn invalid_logical_plan(message: impl Into<Cow<'static, str>>) -> Self {
        Self::InvalidLogicalPlan(message.into())
    }

    pub fn internal(message: impl Into<Cow<'static, str>>) -> Self {
        Self::Internal(message.into())
    }
}

impl error_stack::Context for Error {}
