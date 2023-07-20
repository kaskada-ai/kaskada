use sparrow_arrow::Batch;

#[derive(derive_more::Display, Debug)]
pub enum Error {
    #[display(fmt = "failed to create {_0} transform")]
    CreateTransform(&'static str),
    #[display(fmt = "failed to create {_0} transform")]
    ExecuteTransform(&'static str),
}

impl error_stack::Context for Error {}

/// Trait implementing a transform, executed as part of a [TransformPipeline].
pub(crate) trait Transform: Send + Sync {
    /// Name of the transform.
    ///
    /// This will default to the name of the struct implementing the transform.
    fn name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }

    /// Apply the transfrom to the given input batch.
    fn apply(&self, batch: Batch) -> error_stack::Result<Batch, Error>;
}
