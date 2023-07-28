use std::sync::{Arc, Mutex, MutexGuard};

use pyo3::prelude::*;
use sparrow_compiler::query_builder::QueryBuilder;

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
    pub(crate) fn session_mut(&self) -> PyResult<MutexGuard<'_, SessionInfo>> {
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
}
