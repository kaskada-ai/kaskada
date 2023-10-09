use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::pyarrow::{FromPyArrow, PyArrowType};
use arrow::record_batch::RecordBatch;
use pyo3::prelude::*;
use sparrow_session::Table as RustTable;

use crate::error::Result;
use crate::expr::Expr;
use crate::session::Session;

#[pyclass]
pub(crate) struct Table {
    #[pyo3(get)]
    name: String,
    rust_table: Arc<RustTable>,
    session: Session,
}

#[pymethods]
impl Table {
    /// Create a new table.
    #[new]
    #[pyo3(signature = (session, name, time_column, key_column, schema, queryable, subsort_column, grouping_name, time_unit, source))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        session: Session,
        name: String,
        time_column: &str,
        key_column: &str,
        schema: PyArrowType<Schema>,
        queryable: bool,
        subsort_column: Option<&str>,
        grouping_name: Option<&str>,
        time_unit: Option<&str>,
        source: Option<&str>,
    ) -> Result<Self> {
        let raw_schema = Arc::new(schema.0);

        let rust_table = session.rust_session()?.add_table(
            &name,
            raw_schema,
            time_column,
            queryable,
            subsort_column,
            key_column,
            grouping_name,
            time_unit,
            source,
        )?;

        let table = Table {
            name,
            rust_table: Arc::new(rust_table),
            session,
        };
        Ok(table)
    }

    fn expr(&self) -> Expr {
        let rust_expr = self.rust_table.expr.clone();
        Expr {
            rust_expr,
            session: self.session.clone(),
        }
    }

    /// Add PyArrow data to the given table.
    ///
    /// TODO: Support other kinds of data:
    /// - pyarrow RecordBatchReader
    /// - Parquet file URLs
    /// - Python generators?
    /// TODO: Error handling
    fn add_pyarrow<'py>(&self, data: &'py PyAny, py: Python<'py>) -> Result<&'py PyAny> {
        let data = RecordBatch::from_pyarrow(data)?;

        let rust_table = self.rust_table.clone();
        Ok(pyo3_asyncio::tokio::future_into_py(py, async move {
            let result = rust_table.add_data(data).await;
            Python::with_gil(|py| {
                result.unwrap();
                Ok(py.None())
            })
        })?)
    }

    fn add_parquet<'py>(&mut self, py: Python<'py>, file: String) -> Result<&'py PyAny> {
        let rust_table = self.rust_table.clone();
        Ok(pyo3_asyncio::tokio::future_into_py(py, async move {
            let result = rust_table.add_parquet(&file).await;
            Python::with_gil(|py| {
                result.unwrap();
                Ok(py.None())
            })
        })?)
    }
}
