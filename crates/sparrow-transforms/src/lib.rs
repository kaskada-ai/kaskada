#![warn(
    rust_2018_idioms,
    nonstandard_style,
    future_incompatible,
    clippy::mod_module_files,
    clippy::print_stdout,
    clippy::print_stderr,
    clippy::undocumented_unsafe_blocks
)]

//! Pipeline for executing 1 or more transform.
//!
//! Transforms are simpler than pipelines -- they apply processing logic to an
//! input batch to produce an output batch. Only the last transform in a pipeline
//! may affect the keys associated with rows -- after that a repartition pipeline
//! must be executed to move data to the appropriate partitions.

mod project;
mod select;
mod transform;
mod transform_pipeline;

pub use transform_pipeline::*;

#[cfg(test)]
mod tests {
    #[test]
    fn test_ensure_expressions_registered() {
        sparrow_expressions::ensure_registered()
    }
}
