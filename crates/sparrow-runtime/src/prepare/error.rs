use std::path::PathBuf;

use url::Url;

#[derive(derive_more::Display, Debug)]
pub enum Error {
    #[display(fmt = "unable to open '{path:?}'")]
    OpenFile { path: PathBuf },
    #[display(fmt = "unable to create '{path:?}'")]
    CreateFile { path: PathBuf },
    #[display(fmt = "internal error")]
    Internal,
    #[display(fmt = "failed to create Parquet file reader")]
    CreateParquetReader,
    #[display(fmt = "failed to create CSV file reader")]
    CreateCsvReader,
    #[display(fmt = "failed to create Pulsar reader")]
    CreatePulsarReader,
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
    #[display(fmt = "determine metadata")]
    DetermineMetadata,
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
    #[display(fmt = "unsupported source {_0:?}")]
    UnsupportedOutputPath(&'static str),
    #[display(fmt = "invalid input path")]
    InvalidInputPath,
    #[display(fmt = "failed to read input")]
    ReadInput,
    #[display(fmt = "failed to upload result")]
    UploadResult,
    #[display(fmt = "downloading object {_0} to path {_0}")]
    DownloadingObject(String, String),
    #[display(fmt = "invalid url: {_0}")]
    InvalidUrl(String),
}

impl error_stack::Context for Error {}

impl sparrow_core::ErrorCode for Error {
    fn error_code(&self) -> tonic::Code {
        match self {
            Self::MissingField(_) | Self::IncorrectSlicePlan { .. } | Self::InvalidInputPath => {
                tonic::Code::InvalidArgument
            }
            Self::UnsupportedOutputPath(_) => tonic::Code::Unimplemented,
            _ => tonic::Code::Internal,
        }
    }
}
