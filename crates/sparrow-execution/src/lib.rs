#![warn(
    rust_2018_idioms,
    nonstandard_style,
    future_incompatible,
    clippy::mod_module_files,
    clippy::print_stdout,
    clippy::print_stderr,
    clippy::undocumented_unsafe_blocks
)]

//! Implementations of the pipelines to be executed.

use error_stack::ResultExt;
use sparrow_scheduler::Scheduler;

#[derive(derive_more::Display, Debug)]
pub enum Error {
    #[display(fmt = "error creating executor")]
    Creating,
    #[display(fmt = "error executing")]
    Executing,
}

impl error_stack::Context for Error {}

/// Execute a physical plan.
pub async fn execute(
    query_id: String,
    plan: &sparrow_physical::Plan,
) -> error_stack::Result<(), Error> {
    let mut scheduler = Scheduler::start(&query_id).change_context(Error::Creating)?;

    for pipeline in &plan.pipelines {
        // The pipelines in the plan correspond solely to sequences of transforms
        // to run. They will all use the `TransformPipeline`.
        let transforms = pipeline.steps.iter().map(|step_id| {
            // TODO
        });
        todo!()
    }
    todo!()
}
