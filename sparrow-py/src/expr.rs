use std::sync::Arc;

use itertools::Itertools;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;

/// Kaskada expression node.
#[derive(Clone)]
#[pyclass]
pub(crate) struct Expr(Arc<ExprInfo>);

impl std::fmt::Debug for Expr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Expr")
            .field("kind", &self.0.kind)
            .field("args", &self.0.args)
            .finish()
    }
}

impl std::fmt::Display for Expr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0.args.is_empty() {
            write!(f, "({})", self.0.kind)
        } else {
            write!(f, "({} {})", self.0.kind, self.0.args.iter().format(" "))
        }
    }
}

pub(crate) struct ExprInfo {
    kind: String,
    args: Vec<Arg>,
    data_type: String,
}

#[pymethods]
impl Expr {
    #[new]
    #[pyo3(signature = (kind, args))]
    fn new(kind: String, args: Vec<Arg>) -> PyResult<Self> {
        if &kind == "typeerror" {
            return Err(PyTypeError::new_err("Illegal types"));
        }
        Ok(Self(Arc::new(ExprInfo {
            kind,
            args,
            data_type: "some_random_type".to_owned(),
        })))
    }

    fn __repr__(&self) -> String {
        format!("{self:?}")
    }

    fn __str__(&self) -> String {
        format!("{self}")
    }

    fn data_type_string(&self) -> &str {
        &self.0.data_type
    }
}

#[derive(FromPyObject, Debug)]
enum Arg {
    Expr(Expr),
    LiteralUInt(u64),
    LiteralInt(i64),
    LiteralFloat(f64),
    LiteralString(String),
}

impl std::fmt::Display for Arg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Expr(e) => e.fmt(f),
            Self::LiteralUInt(n) => write!(f, "{n}u64"),
            Self::LiteralInt(n) => write!(f, "{n}i64"),
            Self::LiteralFloat(n) => write!(f, "{n}f64"),
            Self::LiteralString(s) => write!(f, "{s:?}"),
        }
    }
}
