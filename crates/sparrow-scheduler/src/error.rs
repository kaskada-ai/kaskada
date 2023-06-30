use crate::Partition;

/// Top level errors reported during partitioned pipeline execution.
#[derive(derive_more::Display, Debug)]
pub enum Error {
    #[display(fmt = "worker panicked")]
    WorkerPanicked,
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
}

impl error_stack::Context for Error {}
