use crate::{Partition, TaskRef};

/// Top level errors reported during partitioned pipeline execution.
#[derive(derive_more::Display, Debug)]
pub enum Error {
    #[display(fmt = "pipeline panicked")]
    PipelinePanic,
    #[display(fmt = "spawning worker")]
    SpawnWorker,
    #[display(
        fmt = "error executing {method} on partition {partition} of pipeline '{name}' ({index})"
    )]
    Pipeline {
        method: &'static str,
        index: usize,
        name: &'static str,
        partition: Partition,
    },
    #[display(fmt = "dropped partition {partition} of pipeline '{name}' ({index})")]
    PipelineDropped {
        index: usize,
        name: &'static str,
        partition: Partition,
    },
    #[display(fmt = "Saw task {_0} after all workers idled")]
    TaskAfterAllIdle(TaskRef),
}

impl error_stack::Context for Error {}
