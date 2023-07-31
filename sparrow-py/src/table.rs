use std::ops::DerefMut;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::pyarrow::{FromPyArrow, PyArrowType};
use arrow::record_batch::RecordBatch;
use pyo3::prelude::*;
use sparrow_session::Table as RustTable;

use crate::error::Result;
use crate::expr::Expr;
use crate::session::Session;

#[pyclass(extends=Expr, subclass)]
pub(crate) struct Table {
    #[pyo3(get)]
    name: String,
    rust_table: RustTable,
    session: Session,
}

#[pymethods]
impl Table {
    /// Create a new table.
    #[new]
    #[pyo3(signature = (session, name, time_column_name, key_column_name, schema, subsort_column_name, grouping_name))]
    fn new(
        session: Session,
        name: String,
        time_column_name: &str,
        key_column_name: &str,
        schema: PyArrowType<Schema>,
        subsort_column_name: Option<&str>,
        grouping_name: Option<&str>,
    ) -> Result<(Self, Expr)> {
        let raw_schema = Arc::new(schema.0);

        let rust_table = session.rust_session()?.add_table(
            &name,
            raw_schema,
            time_column_name,
            subsort_column_name,
            key_column_name,
            grouping_name,
        )?;

        let rust_expr = rust_table.expr.clone();
        let table = Table {
            name,
            rust_table,
            session: session.clone(),
        };
        let expr = Expr { rust_expr, session };
        Ok((table, expr))
    }

    /// Add PyArrow data to the given table.

    ///
    /// TODO: Support other kinds of data:
    /// - pyarrow RecordBatchReader
    /// - Parquet file URLs
    /// - Python generators?
    /// TODO: Error handling
    fn add_pyarrow(&mut self, data: &PyAny) -> Result<()> {
        let mut session = self.session.rust_session()?;
        let data = RecordBatch::from_pyarrow(data)?;
        self.rust_table.add_data(session.deref_mut(), data)?;
        Ok(())
    }
}
