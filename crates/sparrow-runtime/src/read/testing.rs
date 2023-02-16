use std::fs::File;

use arrow::record_batch::RecordBatch;
use parquet::file::properties::{WriterProperties, WriterVersion};
use tempfile::TempPath;

/// Create a parquet file containing the given batch.
///
/// Will the temporary path containing the file.
///
/// The file will be deleted when the `TempPath` is dropped.
pub(crate) fn write_parquet_file(batch: &RecordBatch, props: Option<WriterProperties>) -> TempPath {
    let file = tempfile::Builder::new()
        .prefix("test_file")
        .suffix(".parquet")
        .tempfile()
        .unwrap();

    write_parquet(file.reopen().unwrap(), batch, props);

    file.into_temp_path()
}

/// Write a RecordBatch as Parquet to the given file.
fn write_parquet(file: File, batch: &RecordBatch, props: Option<WriterProperties>) {
    let props = props.unwrap_or_else(|| {
        WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .build()
    });
    let mut parquet_writer =
        parquet::arrow::ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();

    parquet_writer.write(batch).unwrap();
    parquet_writer.close().unwrap();
}
