#[derive(derive_more::Display, Debug)]
pub enum Error {
    #[display(fmt = "failed to create {_0} transform")]
    CreateTransform(&'static str),
    #[display(fmt = "invalid input index {input} for transform pipeline")]
    InvalidInput { input: usize },
    #[display(fmt = "failed to execute {_0} transform")]
    ExecuteTransform(&'static str),
    #[display(fmt = "illegal state: {_0}")]
    IllegalState(&'static str),
    #[display(fmt = "partition {_0} alreday closed")]
    PartitionClosed(sparrow_scheduler::Partition),
    #[display(fmt = "failed in pipeline sink")]
    Sink,
}

impl error_stack::Context for Error {}
