#![warn(
    rust_2018_idioms,
    nonstandard_style,
    future_incompatible,
    clippy::mod_module_files,
    clippy::print_stdout,
    clippy::print_stderr
)]

pub mod destination;
mod execution_options;
pub mod expression;
pub mod source;
pub mod types;

pub use execution_options::*;
