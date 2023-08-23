use std::sync::{Arc, Mutex, MutexGuard};

use pyo3::prelude::*;
use sparrow_session::Session as RustSession;

/// Kaskada session object.
#[derive(Clone)]
#[pyclass]
pub(crate) struct Session(Arc<Mutex<RustSession>>);

impl PartialEq for Session {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for Session {}

impl Session {
    pub(crate) fn rust_session(&self) -> PyResult<MutexGuard<'_, RustSession>> {
        self.0
            .lock()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
    }
}

#[pymethods]
impl Session {
    #[new]
    fn new() -> Self {
        Self(Arc::new(Mutex::new(RustSession::default())))
    }
}
