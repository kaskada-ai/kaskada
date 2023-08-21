use crate::error::Result;
use crate::execution::Execution;
use crate::session::Session;
use arrow::datatypes::DataType;
use arrow::pyarrow::{FromPyArrow, ToPyArrow};
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
        // TODO: - Support adding a UDF here.
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

    // TODO: FRAZ
    // Could you add a udf() method here that the _timestream calls?
    // I want to be able to call a python function from the _timestream.
    // I want to be able to do like polars:
    // I like their `map` and `apply` method distinction -- `map` works on series,
    // and `apply` works on individual values.
    //
    //
    // TODO: Here is where you need to create your `Arc<PythonUdf>`.
    // That means you need to
    // 1. Infer arg types and result types from python signature
    // 2. Infer FENL TYPES conversions (aka rust/data types kinda)
    // 3. Create a SIGNATURE (Do arg names matter?)
    // 4. Create Python Udf - signature + args?
    //
    // Signature inspection happens in _timestream.
    // So I pass in arg_types and result_type, then construct signature here.

    #[staticmethod]
    #[pyo3(signature = (session, arg_types, result_type, args))]
    fn udf(
        session: Session,
        arg_types: Vec<&str>,
        result_type: &str,
        args: Vec<Expr>,
    ) -> PyResult<Self> {
        if !args.iter().all(|e| e.session() == session) {
            return Err(PyValueError::new_err(
                "all arguments must be in the same session",
            ));
        }

        let mut rust_session = session.rust_session()?;
        let args: Vec<_> = args.into_iter().map(|e| e.rust_expr).collect();
        println!("arg_types: {:?}", arg_types);
        println!("result_type: {:?}", result_type);

        panic!("ok")

        // The python signature
    }

    #[staticmethod]
    #[pyo3(signature = (session, args))]
    fn decorator_udf(
        session: Session,
        // TODO: signature?
        args: Vec<Expr>,
    ) -> PyResult<Self> {
        if !args.iter().all(|e| e.session() == session) {
            return Err(PyValueError::new_err(
                "all arguments must be in the same session",
            ));
        }

        let mut rust_session = session.rust_session()?;
        let args: Vec<_> = args.into_iter().map(|e| e.rust_expr).collect();
        println!("arg_types: {:?}", arg_types);
        println!("result_type: {:?}", result_type);
        panic!("ok")
    }

    #[staticmethod]
    #[pyo3(signature = (session, value))]
    fn literal(session: Session, value: Option<Arg>) -> PyResult<Self> {
        let mut rust_session = session.rust_session()?;

        let rust_expr = match value {
            None => rust_session
                .add_literal(Literal::Null)
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
        let session = self.session.rust_session()?;
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
enum Arg {
    Expr(Expr),
    LiteralBool(bool),
    LiteralUInt(u64),
    LiteralInt(i64),
    LiteralFloat(f64),
    LiteralString(String),
}

impl Arg {
    fn into_ast_dfg_ref(
        self,
        session: &mut RustSession,
    ) -> error_stack::Result<RustExpr, sparrow_session::Error> {
        match self {
            Self::Expr(e) => Ok(e.rust_expr.clone()),
            Self::LiteralBool(b) => session.add_literal(Literal::Bool(b)),
            Self::LiteralUInt(n) => session.add_literal(Literal::UInt64(n)),
            Self::LiteralInt(n) => session.add_literal(Literal::Int64(n)),
            Self::LiteralFloat(n) => session.add_literal(Literal::Float64(n)),
            Self::LiteralString(s) => session.add_literal(Literal::String(s)),
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

            Ok(sparrow_session::ExecutionOptions {
                row_limit: options.getattr(row_limit)?.extract()?,
                max_batch_size: options.getattr(max_batch_size)?.extract()?,
                materialize: options.getattr(materialize)?.extract()?,
            })
        }
    }
}
