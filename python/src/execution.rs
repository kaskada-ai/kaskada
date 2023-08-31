use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::pyarrow::ToPyArrow;
use futures::TryFutureExt;
use pyo3::prelude::*;
use sparrow_session::Execution as RustExecution;
use tokio::sync::{Mutex, MutexGuard, OwnedMutexGuard};

use crate::error::{ErrorContext, IntoError, Result};

/// Kaskada execution object.
#[pyclass]
#[derive(Clone)]
pub(crate) struct Execution {
    execution: Arc<Mutex<Option<RustExecution>>>,
    schema: SchemaRef,
}

impl Execution {
    pub(crate) fn new(execution: RustExecution) -> Self {
        let schema = execution.schema.clone();
        Self {
            execution: Arc::new(Mutex::new(Some(execution))),
            schema,
        }
    }
}

impl Execution {
    /// Return a mutex-locked `RustExecution`.
    ///
    /// Error if the execution has already been completed (consumed).
    fn execution(&self) -> Result<MutexGuard<'_, Option<RustExecution>>> {
        let execution_opt = self.execution.blocking_lock();
        if execution_opt.is_none() {
            return Err(error_stack::report!(ErrorContext::ResultAlreadyCollected).into());
        }
        Ok(execution_opt)
    }

    /// Same as `execution` but returning an `OwnedMutexGuard` which is `Send`.
    async fn owned_execution(&self) -> Result<OwnedMutexGuard<Option<RustExecution>>> {
        let execution_opt = self.execution.clone().lock_owned().await;
        if execution_opt.is_none() {
            return Err(error_stack::report!(ErrorContext::ResultAlreadyCollected).into());
        }
        Ok(execution_opt)
    }

    fn take_execution(&self) -> Result<RustExecution> {
        Ok(self.execution()?.take().unwrap())
    }
}

impl Execution {}

#[pymethods]
impl Execution {
    fn collect_pyarrow(&mut self, py: Python<'_>) -> Result<Vec<PyObject>> {
        let execution = self.take_execution().unwrap();

        // Explicitly allow threads to take the gil during execution, so
        // the udf evaluator can acquire it to execute python.
        let batches = py.allow_threads(move || execution.collect_all_blocking())?;

        let results = batches
            .into_iter()
            .map(|batch| batch.to_pyarrow(py))
            .collect::<PyResult<Vec<_>>>()?;
        Ok(results)
    }

    fn next_pyarrow(&mut self, py: Python<'_>) -> Result<Option<PyObject>> {
        let mut execution = self.execution()?;
        let batch = execution.as_mut().unwrap().next_blocking()?;
        let result = match batch {
            Some(batch) => Some(batch.to_pyarrow(py)?),
            None => None,
        };
        Ok(result)
    }

    fn next_pyarrow_async<'py>(&self, py: Python<'py>) -> PyResult<&'py PyAny> {
        let execution = self.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            // We can't use `?` here because it attempts ot acquire the GIL unexpectedly.
            let next = execution
                .owned_execution()
                .map_err(|e| e.into_error())
                .and_then(|mut execution| async move {
                    execution.as_mut().unwrap().next().await.into_error()
                })
                .await;

            Python::with_gil(|py| {
                if let Some(batch) = next? {
                    Ok(batch.to_pyarrow(py)?)
                } else {
                    Ok(py.None())
                }
            })
        })
    }

    fn schema(&self, py: Python<'_>) -> Result<PyObject> {
        Ok(self.schema.to_pyarrow(py)?)
    }

    fn stop(&mut self) -> Result<()> {
        self.execution()?.as_mut().unwrap().stop();
        Ok(())
    }
}
