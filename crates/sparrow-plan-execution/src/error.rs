use uuid::Uuid;

#[derive(derive_more::Display, Debug)]
pub enum Error {
    #[display(fmt = "error creating executor")]
    Creating,
    #[display(fmt = "no source with id: {source_id}")]
    NoSuchSource { source_id: Uuid },
    #[display(fmt = "error receiving input")]
    SourceError,
    #[display(fmt = "panic while running source")]
    SourcePanic,
    #[display(fmt = "error closing input")]
    ClosingInput,
    #[display(fmt = "error starting workers")]
    Starting,
    #[display(fmt = "error stopping workers")]
    Stopping,
}

impl error_stack::Context for Error {}
