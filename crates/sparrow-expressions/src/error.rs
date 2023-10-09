use std::borrow::Cow;

use arrow_schema::DataType;
use sparrow_arrow::scalar_value::ScalarValue;

#[derive(derive_more::Display, Debug)]
pub enum Error {
    #[display(fmt = "invalid argument type: expected {expected:?} but was {actual:?}")]
    InvalidArgumentType {
        expected: DataType,
        actual: DataType,
    },
    #[display(fmt = "invalid result type: expected {expected:?} but was {actual:?}")]
    InvalidResultType {
        expected: DataType,
        actual: DataType,
    },
    #[display(fmt = "invalid argument type: expected struct but was {actual:?}")]
    InvalidNonStructArgumentType { actual: DataType },
    #[display(fmt = "invalid result type: expected struct but was {actual:?}")]
    InvalidNonStructResultType { actual: DataType },
    #[display(fmt = "invalid literal argument: expected {expected} but was '{actual:?}")]
    InvalidLiteral {
        expected: &'static str,
        actual: ScalarValue,
    },
    #[display(fmt = "'{name:?}' evaluator expected {expected} arguments but got {actual}")]
    InvalidArgumentCount {
        name: Cow<'static, str>,
        expected: usize,
        actual: usize,
    },
    #[display(fmt = "'{name:?}' evaluator expected {expected} literals but got {actual}")]
    InvalidLiteralCount {
        name: Cow<'static, str>,
        expected: usize,
        actual: usize,
    },
    #[display(fmt = "invalid literal argument type: expected {expected:?} but was {actual:?}")]
    InvalidLiteralType {
        expected: DataType,
        actual: DataType,
    },
    #[display(fmt = "'{name:?}' evaluator expected at least 1 argument but got {actual}")]
    NoArguments {
        name: Cow<'static, str>,
        actual: usize,
    },
    #[display(fmt = "'{name:?}' evaluator doesn't support type {actual:?}")]
    UnsupportedArgumentType {
        name: Cow<'static, str>,
        actual: DataType,
    },
    #[display(fmt = "'{name:?}' evaluator doesn't support result type {result_type:?}")]
    UnsupportedResultType {
        name: Cow<'static, str>,
        result_type: DataType,
    },
    #[display(fmt = "'{name:?}' evaluator produced type {actual:?} but expected {expected:?}")]
    UnexpectedResultType {
        name: Cow<'static, str>,
        actual: DataType,
        expected: DataType,
    },
    #[display(fmt = "no evaluator for '{_0}'")]
    NoEvaluator(Cow<'static, str>),
    #[display(fmt = "error during expression evaluation'")]
    ExprEvaluation,
    #[display(fmt = "mismatched lengths: {a_label} length {a_len}, {b_label} length {b_len}")]
    MismatchedLengths {
        a_label: &'static str,
        a_len: usize,
        b_label: &'static str,
        b_len: usize,
    },
    #[display(fmt = "no field named '{field_name}' in struct '{fields:?}")]
    NoSuchField {
        field_name: String,
        fields: arrow_schema::Fields,
    },
    #[display(fmt = "unsupported: {_0}")]
    Unsupported(Cow<'static, str>),
}

impl error_stack::Context for Error {}
