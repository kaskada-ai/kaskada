#![warn(
    rust_2018_idioms,
    nonstandard_style,
    future_incompatible,
    clippy::mod_module_files,
    clippy::print_stdout,
    clippy::print_stderr,
    clippy::undocumented_unsafe_blocks
)]
pub mod attachments;
#[cfg(feature = "avro")]
pub mod avro;
mod concat_take;
pub mod downcast;
pub mod hash;
pub mod hasher;
pub mod scalar_value;
pub mod serde;
pub mod utils;

pub use concat_take::*;
