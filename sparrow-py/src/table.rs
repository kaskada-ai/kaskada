use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::pyarrow::PyArrowType;
use arrow::record_batch::RecordBatch;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::expr::Expr;
use crate::session::Session;

#[derive(Clone)]
#[pyclass(extends=Expr, subclass)]
pub(crate) struct Table {
    #[pyo3(get)]
    name: String,
    #[pyo3(get)]
    num_data: usize,
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
    ) -> PyResult<(Self, Expr)> {
        let schema = Arc::new(schema.0);

        let node = session
            .session_mut()?
            .query_builder
            .add_table(
                &name,
                schema,
                time_column_name,
                subsort_column_name,
                key_column_name,
                grouping_name,
            )
            .map_err(|_| PyRuntimeError::new_err("failed to create table"))?;

        let table = Table { name, num_data: 0 };
        let expr = Expr { node, session };
        Ok((table, expr))
    }

    /// Add PyArrow data to the given table.
    fn add_pyarrow(&mut self, _data: PyArrowType<RecordBatch>) -> PyResult<()> {
        self.num_data += 1;
        Ok(())
    }
}
