#![warn(
    rust_2018_idioms,
    nonstandard_style,
    future_incompatible,
    clippy::mod_module_files,
    clippy::print_stdout,
    clippy::print_stderr,
    clippy::undocumented_unsafe_blocks
)]

pub(crate) mod batch;
mod materialize;
mod prepare;
mod script;
mod serve;
pub mod tracing_setup;

pub use batch::BatchCommand;
pub use materialize::MaterializeCommand;
pub use prepare::PrepareCommand;
pub use serve::*;

#[derive(Debug)]
pub(crate) struct BuildInfo {
    pub sparrow_version: &'static str,
    pub github_ref: &'static str,
    pub github_sha: &'static str,
    pub github_workflow: &'static str,
}

impl Default for BuildInfo {
    fn default() -> Self {
        Self {
            sparrow_version: env!("CARGO_PKG_VERSION"),
            github_ref: option_env!("GITHUB_REF_NAME").unwrap_or("LOCAL"),
            github_sha: option_env!("GITHUB_SHA").unwrap_or("LOCAL"),
            github_workflow: option_env!("GITHUB_WORKFLOW").unwrap_or("LOCAL"),
        }
    }
}

impl BuildInfo {
    pub fn as_resource(&self) -> opentelemetry::sdk::Resource {
        opentelemetry::sdk::Resource::new([
            opentelemetry::KeyValue::new("sparrow_version", self.sparrow_version),
            opentelemetry::KeyValue::new("github_ref", self.github_ref),
            opentelemetry::KeyValue::new("github_sha", self.github_sha),
            opentelemetry::KeyValue::new("github_workflow", self.github_workflow),
        ])
    }
}
