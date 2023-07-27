use std::sync::{Arc, Mutex, MutexGuard};

use arrow::datatypes::Schema;
use arrow::pyarrow::PyArrowType;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use sparrow_compiler::query_builder::QueryBuilder;

use crate::expr::Expr;

/// Kaskada session object.
#[derive(Clone)]
#[pyclass]
pub(crate) struct Session(Arc<Mutex<SessionInfo>>);

impl PartialEq for Session {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for Session {}

#[derive(Default)]
pub(crate) struct SessionInfo {
    pub(crate) query_builder: QueryBuilder,
}

impl Session {
    pub(crate) fn session(&self) -> PyResult<MutexGuard<'_, SessionInfo>> {
        self.0
            .lock()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }
}

#[pymethods]
impl Session {
    #[new]
    fn new() -> Self {
        Self(Arc::new(Mutex::new(SessionInfo::default())))
    }

    /// Adds a table to the session.
    #[pyo3(signature = (name, time_column_name, key_column_name, schema, subsort_column_name, grouping_name))]
    fn add_table(
        &mut self,
        name: &str,
        time_column_name: &str,
        key_column_name: &str,
        schema: PyArrowType<Schema>,
        subsort_column_name: Option<&str>,
        grouping_name: Option<&str>,
    ) -> PyResult<Expr> {
        let schema = Arc::new(schema.0);
        let node = self
            .session()?
            .query_builder
            .add_table(
                name,
                schema,
                time_column_name,
                subsort_column_name,
                key_column_name,
                grouping_name,
            )
            .map_err(|_| PyRuntimeError::new_err("failed to create table"))?;

        Ok(Expr {
            node,
            session: self.clone(),
        })
    }
}
