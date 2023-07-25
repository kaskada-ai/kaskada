use pyo3::prelude::*;

mod expr;
mod session;
mod udf;

/// A Python module implemented in Rust. The name of this function must match
/// the `lib.name` setting in the `Cargo.toml`, else Python will not be able to
/// import the module.
#[pymodule]
fn ffi(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(udf::call_udf, m)?)?;
    m.add_class::<session::Session>()?;
    m.add_class::<expr::Expr>()?;

    Ok(())
}
