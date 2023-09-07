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
}

#[pymethods]
impl Table {
    /// Create a new table.
    #[new]
    #[pyo3(signature = (session, name, time_column, key_column, schema, retained, subsort_column, grouping_name, time_unit))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        session: Session,
        name: String,
        time_column: &str,
        key_column: &str,
        schema: PyArrowType<Schema>,
        retained: bool,
        subsort_column: Option<&str>,
        grouping_name: Option<&str>,
        time_unit: Option<&str>,
    ) -> Result<(Self, Expr)> {
        let raw_schema = Arc::new(schema.0);

        let rust_table = session.rust_session()?.add_table(
            &name,
            raw_schema,
            time_column,
            retained,
            subsort_column,
            key_column,
            grouping_name,
            time_unit,
        )?;

        let rust_expr = rust_table.expr.clone();
        let table = Table { name, rust_table };
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
        let data = RecordBatch::from_pyarrow(data)?;
        self.rust_table.add_data(data)?;
        Ok(())
    }
}
