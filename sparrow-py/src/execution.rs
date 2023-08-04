use arrow::pyarrow::ToPyArrow;
use pyo3::prelude::*;
use sparrow_session::Execution as RustExecution;

use crate::error::{Error, ErrorContext};

/// Kaskada execution object.
#[pyclass]
pub(crate) struct Execution(Option<RustExecution>);

impl Execution {
    pub(crate) fn new(execution: RustExecution) -> Self {
        Self(Some(execution))
    }
}

#[pymethods]
impl Execution {
    fn collect_pyarrow(&mut self, py: Python<'_>) -> Result<Vec<PyObject>, Error> {
        let execution = self
            .0
            .take()
            .ok_or_else(|| error_stack::report!(ErrorContext::ResultAlreadyCollected))?;
        let batches = execution.collect_all_blocking()?;
        let results = batches
            .into_iter()
            .map(|batch| batch.to_pyarrow(py))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(results)
    }

    fn next_pyarrow(&mut self, py: Python<'_>) -> Result<Option<PyObject>, Error> {
        let execution = self
            .0
            .as_mut()
            .ok_or_else(|| error_stack::report!(ErrorContext::ResultAlreadyCollected))?;
        let batch = execution.next_blocking()?;
        let result = match batch {
            Some(batch) => Some(batch.to_pyarrow(py)?),
            None => None,
        };
        Ok(result)
    }
}
