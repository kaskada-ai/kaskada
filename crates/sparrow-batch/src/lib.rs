#![warn(
    rust_2018_idioms,
    nonstandard_style,
    future_incompatible,
    clippy::mod_module_files,
    clippy::print_stdout,
    clippy::print_stderr
)]

mod batch;
mod error;
mod row_time;

pub use batch::*;
pub use error::Error;
pub use row_time::*;

#[cfg(any(test, feature = "testing"))]
pub mod testing;
