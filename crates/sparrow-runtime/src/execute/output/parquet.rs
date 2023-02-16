use std::fs::File;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use error_stack::{IntoReport, Result, ResultExt};
use futures::stream::BoxStream;
use futures::StreamExt;
use parquet::arrow::arrow_writer::ArrowWriter;

use crate::execute::output::object_store::Error;
use crate::execute::progress_reporter::ProgressUpdate;

pub(super) async fn write_parquet(
    output_file: &File,
    schema: SchemaRef,
    progress_updates_tx: tokio::sync::mpsc::Sender<ProgressUpdate>,
    mut batches: BoxStream<'static, RecordBatch>,
) -> Result<(), Error> {
    let mut writer = ArrowWriter::try_new(output_file, schema, None)
        .into_report()
        .change_context(Error::LocalWriteFailure)
        .attach_printable_lazy(|| format!("failed to create parquet writer for {output_file:?}"))?;
    while let Some(batch) = batches.next().await {
        writer
            .write(&batch)
            .into_report()
            .change_context(Error::LocalWriteFailure)
            .attach_printable_lazy(|| {
                format!("failed to write parquet batch {batch:?} to {output_file:?}")
            })?;

        progress_updates_tx
            .try_send(ProgressUpdate::Output {
                num_rows: batch.num_rows(),
            })
            .unwrap_or_else(|e| {
                tracing::error!("Failed to send progress update: {e}");
            });
    }

    let metadata = writer
        .close()
        .into_report()
        .change_context(Error::LocalWriteFailure)
        .attach_printable_lazy(|| format!("failed to close parquet writer {output_file:?}"))?;

    // Not fully understood, but https://github.com/rust-lang/rust/issues/63033
    // seems to prevent me from passing a reference to the output path to add
    // to the trace here.
    tracing::info!(
        num_rows = metadata.num_rows,
        "Completed writing to parquet file"
    );

    Ok(())
}
