use pyo3::prelude::*;

mod error;
mod execution;
mod expr;
mod session;
mod table;
mod udf;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// A Python module implemented in Rust. The name of this function must match
/// the `lib.name` setting in the `Cargo.toml`, else Python will not be able to
/// import the module.
#[pymodule]
#[pyo3(name = "_ffi")]
fn ffi(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<udf::Udf>()?;
    m.add_class::<session::Session>()?;
    m.add_class::<expr::Expr>()?;
    m.add_class::<table::Table>()?;
    m.add_class::<execution::Execution>()?;

    Ok(())
}
