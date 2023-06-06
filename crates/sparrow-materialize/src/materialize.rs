mod materialization;
mod materialization_control;
pub use materialization::*;
pub use materialization_control::*;

#[derive(derive_more::Display, Debug, Clone, Copy)]
pub enum Error {
    #[display(fmt = "error during materialization process")]
    Materialization,
    #[display(fmt = "failed to create materialization")]
    CreateMaterialization,
    #[display(fmt = "failed to read progress")]
    ReadingProgress,
    #[display(fmt = "internal error")]
    Internal,
}

impl error_stack::Context for Error {}
