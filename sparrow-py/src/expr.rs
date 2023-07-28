use arrow::pyarrow::ToPyArrow;
use itertools::Itertools;
use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use sparrow_compiler::query_builder::{Error, Literal, QueryBuilder};
use sparrow_compiler::AstDfgRef;
use sparrow_syntax::FenlType;

use crate::session::Session;

const ENABLE_DIAGNOSTIC_PRINTS: bool = false;

/// Kaskada expression node.
#[derive(Clone)]
#[pyclass(subclass)]
pub(crate) struct Expr {
    pub node: AstDfgRef,
    pub session: Session,
}

#[pymethods]
impl Expr {
    /// Create a new expression.
    ///
    /// This creates a new expression based on the `operation` and `args` provided.
    #[new]
    #[pyo3(signature = (session, operation, args))]
    fn new(session: Session, operation: String, args: Vec<Arg>) -> PyResult<Self> {
        if &operation == "typeerror" {
            return Err(PyTypeError::new_err("Illegal types"));
        }

        // DO NOT SUBMIT: Error if they don't all use the same session.
        let mut session_mut = session.session_mut()?;
        let args: Vec<_> = args
            .into_iter()
            .map(|arg| {
                arg.into_ast_dfg_ref(&mut session_mut.query_builder)
                    // DO NOT SUBMIT: Better error handling.
                    .map_err(|_| PyRuntimeError::new_err("unable to create literal"))
            })
            .collect::<PyResult<_>>()?;
        if ENABLE_DIAGNOSTIC_PRINTS {
            println!(
                "{operation}({})",
                args.iter()
                    .format_with(", ", |arg, f| f(&format_args!("{}", arg.value())))
            );
        }
        let node = match session_mut.query_builder.add_expr(&operation, args) {
            Ok(node) => node,
            Err(e) => {
                // DO NOT SUBMIT: Better error handling.
                return Err(PyValueError::new_err(e.to_string()));
            }
        };
        std::mem::drop(session_mut);

        if ENABLE_DIAGNOSTIC_PRINTS {
            println!("... {:?}", node.value());
        }
        Ok(Self { node, session })
    }

    /// Return true if the two expressions are equivalent.
    ///
    /// Pyo3 doesn't currently support `__eq__` and we don't want to do `__richcmp__`.
    /// This is mostly only used for testing, so it seems ok.
    fn equivalent(&self, other: &Expr) -> PyResult<bool> {
        Ok(self.node.equivalent(&other.node) && self.session == other.session)
    }

    /// Return the session this expression is in.
    fn session(&self) -> Session {
        self.session.clone()
    }

    /// Return the `pyarrow` type of the resulting expression.
    fn data_type(&self, py: Python) -> PyResult<Option<PyObject>> {
        match self.node.value_type() {
            FenlType::Concrete(t) => Ok(Some(t.to_pyarrow(py)?)),
            _ => Ok(None),
        }
    }

    /// Return the Fenl type of the expression, as a string.
    fn data_type_string(&self) -> String {
        self.node.value_type().to_string()
    }
}

#[derive(FromPyObject)]
enum Arg {
    Expr(Expr),
    LiteralUInt(u64),
    LiteralInt(i64),
    LiteralFloat(f64),
    LiteralString(String),
}

impl Arg {
    fn into_ast_dfg_ref(
        self,
        query_builder: &mut QueryBuilder,
    ) -> error_stack::Result<AstDfgRef, Error> {
        match self {
            Self::Expr(e) => Ok(e.node),
            Self::LiteralUInt(n) => query_builder.add_literal(Literal::UInt64Literal(n)),
            Self::LiteralInt(n) => query_builder.add_literal(Literal::Int64Literal(n)),
            Self::LiteralFloat(n) => query_builder.add_literal(Literal::Float64Literal(n)),
            Self::LiteralString(s) => query_builder.add_literal(Literal::StringLiteral(s)),
        }
    }
}
