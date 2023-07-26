use pyo3::prelude::*;

/// Kaskada session object.
#[pyclass]
pub(crate) struct Session {
    tables: Vec<String>,
}

#[pymethods]
impl Session {
    #[new]
    fn new() -> Self {
        Self { tables: vec![] }
    }

    /// Adds a table to the session.
    fn add_table(&mut self, name: &str) -> PyResult<()> {
        self.tables.push(name.to_owned());
        Ok(())
    }

    /// Return the names of registered tables.
    fn table_names(&self) -> PyResult<Vec<String>> {
        Ok(self.tables.clone())
    }
}
