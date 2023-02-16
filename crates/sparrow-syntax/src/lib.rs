//! The `sparrow-syntax` crate provides the AST representation
//! and the parser.

#![warn(
    rust_2018_idioms,
    nonstandard_style,
    future_incompatible,
    clippy::mod_module_files,
    clippy::print_stdout,
    clippy::print_stderr
)]

mod parser;
mod syntax;

pub use parser::*;
pub use syntax::*;
