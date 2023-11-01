#![warn(
    rust_2018_idioms,
    nonstandard_style,
    future_incompatible,
    clippy::mod_module_files,
    clippy::print_stdout,
    clippy::print_stderr
)]

mod error;
mod execution;
mod expr;
pub mod partitioned;
mod session;
mod table;

pub use error::Error;
pub use execution::Execution;
pub use expr::{Expr, Literal};
pub use session::*;
pub use table::Table;
