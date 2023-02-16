use std::fs::File;
use std::path::Path;

use arrow_array::RecordBatch;
use error_stack::{IntoReport, ResultExt};

use crate::to_record_batch::ToRecordBatch;

/// A temporary data file.
pub struct TempDataFile {
    tempfile: tempfile::NamedTempFile,
}

impl TempDataFile {
    /// The path to the data file.
    pub fn path(&self) -> &Path {
        self.tempfile.path()
    }

    /// The path to the tempfile as as tring.
    pub fn path_str(&self) -> &str {
        self.tempfile
            .path()
            .as_os_str()
            .to_str()
            .expect("invalid path")
    }

    /// Create a new parquet file from the given source data.
    pub fn try_parquet_from_batch(batch: &RecordBatch) -> crate::Result<Self> {
        Self::try_from::<parquet::errors::ParquetError>(batch, ".parquet", |file, batch| {
            let mut parquet_writer =
                parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None)?;
            parquet_writer.write(batch)?;
            parquet_writer.close()?;
            Ok(())
        })
    }

    /// Create a new parquet file from the given data.
    ///
    /// If specified, the `schema` indicates the expected schema for the data (and
    /// thus the Parquet file).
    pub fn try_parquet_from(
        data: impl ToRecordBatch,
        schema: Option<arrow_schema::SchemaRef>,
    ) -> crate::Result<Self> {
        let batch = data.to_record_batch(schema)?;
        Self::try_parquet_from_batch(&batch)
    }

    fn try_from<E>(
        batch: &RecordBatch,
        file_suffix: &str,
        to_file: impl FnOnce(File, &RecordBatch) -> std::result::Result<(), E>,
    ) -> crate::Result<Self>
    where
        error_stack::Report<E>: From<E>,
    {
        let tempfile = tempfile::Builder::new()
            .prefix("test_file")
            .suffix(file_suffix)
            .tempfile()
            .into_report()
            .change_context(crate::Error)?;

        let file = tempfile
            .reopen()
            .into_report()
            .change_context(crate::Error)?;
        to_file(file, batch)
            .into_report()
            .change_context(crate::Error)?;

        Ok(Self { tempfile })
    }
}
