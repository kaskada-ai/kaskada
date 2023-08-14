use error_stack::Report;
use pyo3::exceptions::PyRuntimeError;
use pyo3::PyErr;

#[derive(derive_more::Display, Debug)]
pub enum ErrorContext {
    #[display(fmt = "error in kaskada Rust code")]
    Ffi,
    #[display(fmt = "error in kaskada Pyo3 or Python code")]
    Python,
    #[display(fmt = "result already collected")]
    ResultAlreadyCollected,
}

impl error_stack::Context for ErrorContext {}

pub struct Error(error_stack::Report<ErrorContext>);

pub type Result<T> = std::result::Result<T, Error>;

trait UserErrorInfo: Sync + Send {
    /// If this error is a user facing error, return it.
    ///
    /// When producing the Python error, the outer-most user facing error will
    /// be reported as the message>
    fn user_facing_cause(&self) -> Option<PyErr>;
}

impl std::fmt::Debug for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.0, f)
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.0, f)
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.as_error().source()
    }

    #[cfg(nightly)]
    fn provide<'a>(&'a self, demand: &mut Demand<'a>) {
        self.0.frames().for_each(|frame| frame.provide(demand));
    }
}

impl<T> From<error_stack::Report<T>> for Error {
    fn from(value: error_stack::Report<T>) -> Self {
        Error(value.change_context(ErrorContext::Ffi))
    }
}

impl From<PyErr> for Error {
    fn from(value: PyErr) -> Self {
        Error(Report::from(value).change_context(ErrorContext::Python))
    }
}

impl From<Error> for PyErr {
    fn from(value: Error) -> Self {
        tracing::error!("Reporting error from kaskada FFI:{value:#}");
        value
            .0
            .frames()
            .find_map(|f| {
                f.downcast_ref::<&dyn UserErrorInfo>()
                    .and_then(|info| info.user_facing_cause())
            })
            .unwrap_or_else(|| PyRuntimeError::new_err(format!("{value:#}")))
    }
}

pub(crate) trait IntoError {
    type Result;
    fn into_error(self) -> Self::Result;
}

impl<E: Into<Error>> IntoError for E {
    type Result = Error;
    fn into_error(self) -> Self::Result {
        self.into()
    }
}

impl<T, E: Into<Error>> IntoError for std::result::Result<T, E> {
    type Result = Result<T>;
    fn into_error(self) -> Self::Result {
        self.map_err(|e| e.into())
    }
}
