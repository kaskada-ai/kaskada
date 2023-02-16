#![warn(
    rust_2018_idioms,
    nonstandard_style,
    future_incompatible,
    clippy::mod_module_files,
    clippy::print_stdout,
    clippy::print_stderr,
    clippy::undocumented_unsafe_blocks
)]

/// The Sparrow Query API.
pub mod kaskada {
    // #![allow(clippy::derive_partial_eq_without_eq)] // stop clippy erroring on generated code from tonic (proto)
    // tonic::include_proto!("kaskada.kaskada.v1alpha");

    pub mod v1alpha;
}

/// The (binary) file descriptors corresponding to the Sparrow API.
///
/// Allows creating the reflection service.
pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("sparrow_descriptor");
