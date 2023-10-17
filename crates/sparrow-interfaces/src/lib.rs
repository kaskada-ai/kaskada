#![warn(
    rust_2018_idioms,
    nonstandard_style,
    future_incompatible,
    clippy::mod_module_files,
    clippy::print_stdout,
    clippy::print_stderr
)]

mod execution_options;
mod source;
mod source_error;

pub use execution_options::*;
pub use source::*;
pub use source_error::SourceError;
