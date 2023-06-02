#![warn(
    rust_2018_idioms,
    nonstandard_style,
    future_incompatible,
    clippy::mod_module_files,
    clippy::print_stdout,
    clippy::print_stderr,
    clippy::undocumented_unsafe_blocks
)]

//! Physical execution plans for Kaskada queries.

mod expr;
mod plan;
mod step;

pub use expr::*;
pub use plan::*;
pub use step::*;
