use url::Url;

#[derive(derive_more::Display, Debug)]
pub enum Error {
    #[display(fmt = "internal error")]
    Internal,
    #[display(fmt = "failed to create Parquet file reader")]
    CreateReader,
    #[display(fmt = "reading batch")]
    ReadingBatch,
    #[display(fmt = "slicing batch")]
    SlicingBatch,
    #[display(fmt = "non-nullable column '{field}' had {null_count} nulls")]
    NullInNonNullableColumn { field: String, null_count: usize },
    #[display(fmt = "preparing column")]
    PreparingColumn,
    #[display(fmt = "sorting batch")]
    SortingBatch,
    #[display(fmt = "invalid schema provided")]
    ReadSchema,
    #[display(fmt = "failed to write to '{_0}")]
    Write(Url),
    #[display(fmt = "prepare request missing '{_0}'")]
    MissingField(&'static str),
    #[display(
        fmt = "invalid prepare request: slice plan table should match table config, but was '{slice_plan}' and '{table_config}'"
    )]
    IncorrectSlicePlan {
        slice_plan: String,
        table_config: String,
    },
    #[display(fmt = "downloading object to prepare")]
    DownloadingObject,
    #[display(fmt = "invalid url: {_0}")]
    InvalidUrl(String),
}

impl error_stack::Context for Error {}

impl sparrow_core::ErrorCode for Error {
    fn error_code(&self) -> tonic::Code {
        match self {
            Self::MissingField(_) | Self::IncorrectSlicePlan { .. } => tonic::Code::InvalidArgument,
            _ => tonic::Code::Internal,
        }
    }
}
