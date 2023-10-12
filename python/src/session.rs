use std::sync::{Arc, Mutex, MutexGuard};

use arrow::pyarrow::ToPyArrow;
use pyo3::prelude::*;
use sparrow_session::Session as RustSession;
use std::str::FromStr;

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

#[pyfunction]
pub(crate) fn parquet_schema<'py>(
    session: &'py Session,
    url: &str,
    py: Python<'py>,
) -> PyResult<&'py PyAny> {
    let registry = session.rust_session()?.object_store_registry.clone();
    let url = sparrow_runtime::stores::ObjectStoreUrl::from_str(url)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

    Ok(pyo3_asyncio::tokio::future_into_py(py, async move {
        let file = sparrow_runtime::read::ParquetFile::try_new(registry.as_ref(), url, None).await;
        Python::with_gil(|py| {
            let schema = file.map_err(|e| crate::error::Error::from(e))?.schema;
            Ok(schema.to_pyarrow(py)?)
        })
    })?)
}
