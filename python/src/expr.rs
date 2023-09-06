use crate::error::Result;
use crate::execution::Execution;
use crate::session::Session;
use crate::udf::Udf;
use arrow::datatypes::DataType;
use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;

use sparrow_session::{Expr as RustExpr, Literal as RustLiteral, Session as RustSession};

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
    #[staticmethod]
    #[pyo3(signature = (session, operation, args))]
    fn call(session: Session, operation: String, args: Vec<Expr>) -> PyResult<Self> {
        if !args.iter().all(|e| e.session() == session) {
            return Err(PyValueError::new_err(
                "all arguments must be in the same session",
            ));
        }

        let mut rust_session = session.rust_session()?;
        let args: Vec<_> = args.into_iter().map(|e| e.rust_expr).collect();
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

    /// Create a new udf.
    ///
    /// This creates a new expression based on the `udf` and `args` provided.
    #[staticmethod]
    #[pyo3(signature = (session, udf, args))]
    fn call_udf(session: Session, udf: Udf, args: Vec<Expr>) -> PyResult<Self> {
        let sparrow_udf = udf.0;
        if !args.iter().all(|e| e.session() == session) {
            return Err(PyValueError::new_err(
                "all arguments must be in the same session",
            ));
        }

        let mut rust_session = session.rust_session()?;
        let args: Vec<_> = args.into_iter().map(|e| e.rust_expr).collect();

        let rust_expr = match rust_session.add_udf_to_dfg(sparrow_udf.clone(), args) {
            Ok(node) => node,
            Err(e) => {
                // DO NOT SUBMIT: Better error handling.
                return Err(PyValueError::new_err(e.to_string()));
            }
        };
        std::mem::drop(rust_session);

        Ok(Self { rust_expr, session })
    }

    #[staticmethod]
    #[pyo3(signature = (session, value))]
    fn literal(session: Session, value: Option<Literal>) -> PyResult<Self> {
        let mut rust_session = session.rust_session()?;

        let rust_expr = match value {
            None => rust_session
                .add_literal(RustLiteral::Null)
                .map_err(|_| PyRuntimeError::new_err("unable to create null literal"))?,
            Some(arg) => {
                arg.into_ast_dfg_ref(&mut rust_session)
                    // DO NOT SUBMIT: Better error handling.
                    .map_err(|_| PyRuntimeError::new_err("unable to create argument"))?
            }
        };
        std::mem::drop(rust_session);
        Ok(Self { rust_expr, session })
    }

    #[staticmethod]
    #[pyo3(signature = (session, s, ns))]
    fn literal_timedelta(session: Session, s: i64, ns: i64) -> PyResult<Self> {
        let mut rust_session = session.rust_session()?;

        let rust_expr = rust_session
            .add_literal(RustLiteral::Timedelta {
                seconds: s,
                nanos: ns,
            })
            .map_err(|_| PyRuntimeError::new_err("unable to create timedelta"))?;

        std::mem::drop(rust_session);
        Ok(Self { rust_expr, session })
    }

    #[pyo3(signature = (data_type))]
    fn cast(&self, data_type: &PyAny) -> Result<Self> {
        let data_type = DataType::from_pyarrow(data_type)?;

        let mut rust_session = self.session.rust_session()?;
        let rust_expr = rust_session.add_cast(self.rust_expr.clone(), data_type)?;
        std::mem::drop(rust_session);

        let session = self.session.clone();
        Ok(Self { rust_expr, session })
    }

    /// Return the session this expression is in.
    fn session(&self) -> Session {
        self.session.clone()
    }

    fn execute(&self, options: Option<&PyAny>) -> Result<Execution> {
        let mut session = self.session.rust_session()?;
        let options = extract_options(options)?;
        let execution = session.execute(&self.rust_expr, options)?;
        Ok(Execution::new(execution))
    }

    /// Return the `pyarrow` type of the resulting expression.
    fn data_type(&self, py: Python<'_>) -> Result<Option<PyObject>> {
        match self.rust_expr.data_type() {
            Some(t) => Ok(Some(t.to_pyarrow(py)?)),
            _ => Ok(None),
        }
    }

    #[pyo3(signature = ())]
    fn is_continuous(&self) -> bool {
        self.rust_expr.is_continuous()
    }

    fn grouping(&self) -> Option<String> {
        self.rust_expr.grouping()
    }
}

#[derive(FromPyObject)]
enum Literal {
    Bool(bool),
    UInt(u64),
    Int(i64),
    Float(f64),
    String(String),
}

impl Literal {
    fn into_ast_dfg_ref(
        self,
        session: &mut RustSession,
    ) -> error_stack::Result<RustExpr, sparrow_session::Error> {
        match self {
            Self::Bool(b) => session.add_literal(RustLiteral::Bool(b)),
            Self::UInt(n) => session.add_literal(RustLiteral::UInt64(n)),
            Self::Int(n) => session.add_literal(RustLiteral::Int64(n)),
            Self::Float(n) => session.add_literal(RustLiteral::Float64(n)),
            Self::String(s) => session.add_literal(RustLiteral::String(s)),
        }
    }
}

fn extract_options(options: Option<&PyAny>) -> Result<sparrow_session::ExecutionOptions> {
    match options {
        None => Ok(sparrow_session::ExecutionOptions::default()),
        Some(options) => {
            let py = options.py();
            let row_limit = pyo3::intern!(py, "row_limit");
            let max_batch_size = pyo3::intern!(py, "max_batch_size");
            let materialize = pyo3::intern!(py, "materialize");
            let changed_since_time = pyo3::intern!(py, "changed_since");
            let final_at_time = pyo3::intern!(py, "final_at");
            let results = pyo3::intern!(py, "results");

            let results: &str = options.getattr(results)?.extract()?;
            let results = match results {
                "history" => sparrow_session::Results::History,
                "snapshot" => sparrow_session::Results::Snapshot,
                invalid => {
                    return Err(
                        PyValueError::new_err(format!("invalid results '{invalid}'")).into(),
                    )
                }
            };

            Ok(sparrow_session::ExecutionOptions {
                row_limit: options.getattr(row_limit)?.extract()?,
                max_batch_size: options.getattr(max_batch_size)?.extract()?,
                materialize: options.getattr(materialize)?.extract()?,
                results,
                changed_since_time_s: options.getattr(changed_since_time)?.extract()?,
                final_at_time_s: options.getattr(final_at_time)?.extract()?,
            })
        }
    }
}
