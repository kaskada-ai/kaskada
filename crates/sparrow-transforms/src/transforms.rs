use sparrow_arrow::Batch;

mod project;

/// Trait implementing a transform, executed as part of a [TransformPipeline].
pub(crate) trait Transform: Send + Sync {
    /// Name of the transform.
    ///
    /// This will default to the name of the struct implementing the transform.
    fn name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }

    /// Apply the transfrom to the given input batch.
    fn apply(&self, batch: Batch) -> error_stack::Result<Batch, crate::Error>;
}
