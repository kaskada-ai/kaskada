use std::str::FromStr;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use error_stack::{IntoReport, ResultExt};
use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::ObjectStore;
use parquet::arrow::AsyncArrowWriter;
use sparrow_api::kaskada::v1alpha::destination::Destination;
use sparrow_api::kaskada::v1alpha::{FileType, ObjectStoreDestination};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use uuid::Uuid;

use crate::execute::progress_reporter::ProgressUpdate;
use crate::stores::{ObjectStoreRegistry, ObjectStoreUrl};
use crate::UPLOAD_BUFFER_SIZE_IN_BYTES;

const ROWS_PER_FILE: usize = 1_000_000;

#[derive(derive_more::Display, Debug)]
pub enum Error {
    #[display(fmt = "failed to report output progress")]
    ProgressUpdate,
    #[display(fmt = "invalid destination prefix")]
    InvalidDestination,
    #[display(fmt = "unspecified file type")]
    UnspecifiedFileType,
    #[display(fmt = "internal error")]
    Internal,
    #[display(fmt = "error starting multi-part upload for output")]
    Upload,
    #[display(fmt = "error writing output")]
    Write,
}

impl error_stack::Context for Error {}

#[must_use]
enum WriterState {
    /// Open Parquet writer.
    Parquet {
        url: ObjectStoreUrl,
        writer: AsyncArrowWriter<Box<dyn AsyncWrite + Send + Unpin>>,
    },
    Csv {
        url: ObjectStoreUrl,
        writer: Box<dyn AsyncWrite + Send + Unpin>,
    },
}

impl WriterState {
    async fn open(
        object_store: &dyn ObjectStore,
        output_prefix: &ObjectStoreUrl,
        file_uuid: &Uuid,
        file_index: usize,
        file_type: FileType,
        schema: SchemaRef,
    ) -> error_stack::Result<Self, Error> {
        let suffix = match file_type {
            FileType::Unspecified => error_stack::bail!(Error::UnspecifiedFileType),
            FileType::Parquet => "parquet",
            FileType::Csv => "csv",
        };
        let url = output_prefix
            .join(&format!(
                "{}-part-{file_index}.{suffix}",
                file_uuid.as_hyphenated()
            ))
            .change_context(Error::Internal)?;
        let path = url.path().change_context(Error::Internal)?;
        let (_multipart_id, mut writer) = object_store
            .put_multipart(&path)
            .await
            .into_report()
            .change_context(Error::Upload)?;

        match file_type {
            FileType::Unspecified => error_stack::bail!(Error::UnspecifiedFileType),
            FileType::Parquet => {
                let writer = parquet::arrow::AsyncArrowWriter::try_new(
                    writer,
                    schema,
                    UPLOAD_BUFFER_SIZE_IN_BYTES,
                    None,
                )
                .into_report()
                .change_context(Error::Write)?;
                Ok(Self::Parquet { url, writer })
            }
            FileType::Csv => {
                // Write an empty batch so headers are written.
                let mut buffer = Vec::new();
                let mut csv_writer = arrow::csv::WriterBuilder::new()
                    .has_headers(true)
                    .build(&mut buffer);
                csv_writer
                    .write(&RecordBatch::new_empty(schema))
                    .into_report()
                    .change_context(Error::Write)?;
                std::mem::drop(csv_writer);
                writer
                    .write_all(&buffer)
                    .await
                    .into_report()
                    .change_context(Error::Write)?;

                // Then return the state
                Ok(Self::Csv { url, writer })
            }
        }
    }

    async fn write(&mut self, batch: RecordBatch) -> error_stack::Result<(), Error> {
        match self {
            WriterState::Parquet { writer, .. } => {
                writer
                    .write(&batch)
                    .await
                    .into_report()
                    .change_context(Error::Write)?;
                Ok(())
            }
            WriterState::Csv { writer, .. } => {
                // Re-allocating the buffer on every CSV write is likely inefficient.
                // But, we anticipate CSV output being used with small / example cases so
                // we accept this for now. Ideally, we could have an async CSV writer that
                // allows us to flush to the object store rather thna growing the buffer.
                // In the absence of that, we should support growing the buffer, but also
                // try to slice the batch into small enough chunks that we don't need to
                // and upload in a sequence of "pages".
                let mut buffer = Vec::new();

                // Write the CSV batch to a buffer, for writing to the object store.
                {
                    let mut csv_writer = arrow::csv::WriterBuilder::new()
                        .has_headers(false)
                        .build(&mut buffer);
                    csv_writer
                        .write(&batch)
                        .into_report()
                        .change_context(Error::Write)?;
                }
                writer
                    .write_all(&buffer)
                    .await
                    .into_report()
                    .change_context(Error::Write)?;
                Ok(())
            }
        }
    }

    async fn close(self) -> error_stack::Result<ObjectStoreUrl, Error> {
        match self {
            WriterState::Parquet { url, writer } => {
                writer
                    .close()
                    .await
                    .into_report()
                    .change_context(Error::Write)?;
                Ok(url)
            }
            WriterState::Csv {
                url, mut writer, ..
            } => {
                writer
                    .shutdown()
                    .await
                    .into_report()
                    .change_context(Error::Write)?;
                Ok(url)
            }
        }
    }
}

/// Write `batches` to one or more files in the `destination`.
pub(super) async fn write(
    object_stores: Arc<ObjectStoreRegistry>,
    destination: ObjectStoreDestination,
    schema: SchemaRef,
    progress_updates_tx: tokio::sync::mpsc::Sender<ProgressUpdate>,
    mut batches: BoxStream<'static, RecordBatch>,
) -> error_stack::Result<(), Error> {
    // Inform tracker of destination type
    progress_updates_tx
        .send(ProgressUpdate::Destination {
            destination: Some(Destination::ObjectStore(destination.clone())),
        })
        .await
        .into_report()
        .change_context(Error::ProgressUpdate)?;

    let output_prefix = ObjectStoreUrl::from_str(&destination.output_prefix_uri)
        .change_context(Error::InvalidDestination)?;
    let object_store = object_stores
        .object_store(&output_prefix)
        .change_context(Error::InvalidDestination)?;

    let file_uuid = Uuid::new_v4();
    let mut num_files = 0;
    let mut num_rows_in_file = 0;

    // Always create at least one writer. This ensures that if there are
    // no rows, we still emit an empty file.
    let mut state = Some(
        WriterState::open(
            object_store.as_ref(),
            &output_prefix,
            &file_uuid,
            num_files,
            destination.file_type(),
            schema.clone(),
        )
        .await?,
    );
    num_files += 1;

    // Currently, we pull batches and upload them asynchronously.
    // We could increase concurrency by buffering some batches and performing
    // multiple writes simultaneously.
    while let Some(batch) = batches.next().await {
        if batch.num_rows() == 0 {
            // Don't create empty files. Wait for a non-empty batch before
            // starting a new file.
            continue;
        }

        // Initialize the writer if we need a new one.
        if state.is_none() {
            state = Some(
                WriterState::open(
                    object_store.as_ref(),
                    &output_prefix,
                    &file_uuid,
                    num_files,
                    destination.file_type(),
                    schema.clone(),
                )
                .await?,
            );
            num_files += 1;
        }

        // Write the batch.
        {
            let num_rows = batch.num_rows();
            state.as_mut().expect("opened above").write(batch).await?;
            num_rows_in_file += num_rows;
            progress_updates_tx
                .try_send(ProgressUpdate::Output { num_rows })
                .into_report()
                .change_context(Error::ProgressUpdate)?;
        }

        // If we've written enough rows, rotate the file.
        if num_rows_in_file > ROWS_PER_FILE {
            let url = state.expect("opened above").close().await?;

            tracing::info!("Wrote {num_rows_in_file} rows to file {url}");
            progress_updates_tx
                .try_send(ProgressUpdate::FilesProduced { paths: vec![url] })
                .into_report()
                .change_context(Error::ProgressUpdate)?;

            num_rows_in_file = 0;
            state = None;
        }
    }

    // Close any remaining state.
    if let Some(state) = state {
        let url = state.close().await?;

        tracing::info!("Wrote {num_rows_in_file} to {url}");
        progress_updates_tx
            .try_send(ProgressUpdate::FilesProduced { paths: vec![url] })
            .into_report()
            .change_context(Error::ProgressUpdate)?;
    }

    Ok(())
}
