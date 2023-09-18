#![warn(
    rust_2018_idioms,
    nonstandard_style,
    future_incompatible,
    clippy::mod_module_files,
    clippy::print_stdout,
    clippy::print_stderr,
    clippy::undocumented_unsafe_blocks
)]

//! Logical execution plans for Kaskada queries.
mod error;
mod expr;
mod grouping;
mod typecheck;

pub use error::*;
pub use expr::*;
pub use grouping::*;
