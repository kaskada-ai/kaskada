use std::fs::File;
use std::io::BufWriter;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use error_stack::{IntoReport, Result, ResultExt};
use futures::stream::BoxStream;
use futures::StreamExt;

use crate::execute::output::object_store::Error;
use crate::execute::progress_reporter::ProgressUpdate;

pub(super) async fn write_csv(
    output_file: &File,
    _schema: SchemaRef,
    progress_updates_tx: tokio::sync::mpsc::Sender<ProgressUpdate>,
    mut batches: BoxStream<'static, RecordBatch>,
) -> Result<(), Error> {
    let output_writer = BufWriter::new(output_file);
    let mut writer = arrow::csv::WriterBuilder::new()
        .has_headers(true)
        .build(output_writer);
    while let Some(batch) = batches.next().await {
        writer
            .write(&batch)
            .into_report()
            .change_context(Error::LocalWriteFailure)
            .attach_printable_lazy(|| {
                format!("failed to write csv batch {batch:?} to {output_file:?}")
            })?;

        progress_updates_tx
            .try_send(ProgressUpdate::Output {
                num_rows: batch.num_rows(),
            })
            .unwrap_or_else(|e| {
                tracing::error!("Failed to send progress update: {e}");
            });
    }

    // Not fully understood, but https://github.com/rust-lang/rust/issues/63033
    // seems to prevent me from passing a reference to the output path to add
    // to the trace here.
    tracing::info!("Closed local CSV File");

    Ok(())
}
