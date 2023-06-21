#![warn(
    rust_2018_idioms,
    nonstandard_style,
    future_incompatible,
    clippy::mod_module_files,
    clippy::print_stdout,
    clippy::print_stderr,
    clippy::undocumented_unsafe_blocks
)]

//! Execution of expressions.

mod error;
mod evaluator;
mod evaluators;
mod executor;
mod values;
mod work_area;

pub use error::*;
pub use executor::*;
