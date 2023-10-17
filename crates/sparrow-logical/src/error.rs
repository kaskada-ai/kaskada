use std::borrow::Cow;

use crate::{ExprRef, Grouping};
use arrow_schema::DataType;

use sparrow_types::DisplayFenlType;

#[derive(derive_more::Display, Debug)]
pub enum Error {
    #[display(fmt = "internal error: {_0}")]
    Internal(Cow<'static, str>),
    #[display(fmt = "invalid non-struct type: {}", "_0.display()")]
    InvalidNonStructType(DataType),
    #[display(fmt = "invalid non-string literal: {_0:?}")]
    InvalidNonStringLiteral(ExprRef),
    // TODO: Include nearest matches?
    #[display(fmt = "invalid field name '{name}'")]
    InvalidFieldName { name: String },
    #[display(fmt = "invalid types")]
    InvalidTypes,
    #[display(fmt = "incompatible groupings {_0:?}")]
    IncompatibleGroupings(Vec<Grouping>),
    #[display(fmt = "invalid function: '{_0}'")]
    InvalidFunction(String),
}

impl Error {
    pub(crate) fn internal(message: impl Into<Cow<'static, str>>) -> Self {
        Self::Internal(message.into())
    }
}

impl error_stack::Context for Error {}
