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

pub mod serde;

pub mod downcast;
pub mod scalar_value;
pub mod utils;
