use crate::error::Error;
use crate::execution::Execution;
use crate::session::Session;
use arrow::pyarrow::ToPyArrow;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use sparrow_session::{Expr as RustExpr, Literal, Session as RustSession};

/// Kaskada expression node.
#[derive(Clone)]
#[pyclass(subclass)]
pub(crate) struct Expr {
    pub rust_expr: RustExpr,
    pub session: Session,
}

#[pymethods]
impl Expr {
    /// Create a new expression.
    ///
    /// This creates a new expression based on the `operation` and `args` provided.
    #[new]
    #[pyo3(signature = (session, operation, args))]
    fn new(session: Session, operation: String, args: Vec<Option<Arg>>) -> PyResult<Self> {
        if !args
            .iter()
            .flatten()
            .filter_map(Arg::session)
            .all(|s| s == session)
        {
            return Err(PyValueError::new_err(
                "all arguments must be in the same session",
            ));
        }

        let mut rust_session = session.rust_session()?;
        let args: Vec<_> = args
            .into_iter()
            .map(|arg| {
                match arg {
                    None => rust_session
                        .add_literal(Literal::Null)
                        .map_err(|_| PyRuntimeError::new_err("unable to create null literal")),
                    Some(arg) => {
                        arg.into_ast_dfg_ref(&mut rust_session)
                            // DO NOT SUBMIT: Better error handling.
                            .map_err(|_| PyRuntimeError::new_err("unable to create argument"))
                    }
                }
            })
            .collect::<PyResult<_>>()?;
        let rust_expr = match rust_session.add_expr(&operation, args) {
            Ok(node) => node,
            Err(e) => {
                // DO NOT SUBMIT: Better error handling.
                return Err(PyValueError::new_err(e.to_string()));
            }
        };
        std::mem::drop(rust_session);
        Ok(Self { rust_expr, session })
    }

    /// Return the session this expression is in.
    fn session(&self) -> Session {
        self.session.clone()
    }

    fn execute(&self, options: Option<&PyAny>) -> Result<Execution, Error> {
        let session = self.session.rust_session()?;
        let options = extract_options(options)?;
        let execution = session.execute(&self.rust_expr, options)?;
        Ok(Execution::new(execution))
    }

    /// Return the `pyarrow` type of the resulting expression.
    fn data_type(&self, py: Python<'_>) -> PyResult<Option<PyObject>> {
        match self.rust_expr.data_type() {
            Some(t) => Ok(Some(t.to_pyarrow(py)?)),
            _ => Ok(None),
        }
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
    fn session(&self) -> Option<Session> {
        match self {
            Self::Expr(e) => Some(e.session.clone()),
            _ => None,
        }
    }

    fn into_ast_dfg_ref(
        self,
        session: &mut RustSession,
    ) -> error_stack::Result<RustExpr, sparrow_session::Error> {
        match self {
            Self::Expr(e) => Ok(e.rust_expr.clone()),
            Self::LiteralUInt(n) => session.add_literal(Literal::UInt64(n)),
            Self::LiteralInt(n) => session.add_literal(Literal::Int64(n)),
            Self::LiteralFloat(n) => session.add_literal(Literal::Float64(n)),
            Self::LiteralString(s) => session.add_literal(Literal::String(s)),
        }
    }
}

fn extract_options(options: Option<&PyAny>) -> Result<sparrow_session::ExecutionOptions, Error> {
    match options {
        None => Ok(sparrow_session::ExecutionOptions::default()),
        Some(options) => {
            let py = options.py();
            let row_limit = pyo3::intern!(py, "row_limit");
            let max_batch_size = pyo3::intern!(py, "max_batch_size");
            let materialize = pyo3::intern!(py, "materialize");

            Ok(sparrow_session::ExecutionOptions {
                row_limit: options.getattr(row_limit)?.extract()?,
                max_batch_size: options.getattr(max_batch_size)?.extract()?,
                materialize: options.getattr(materialize)?.extract()?,
            })
        }
    }
}
