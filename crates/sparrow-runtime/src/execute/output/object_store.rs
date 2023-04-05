use std::fs::File;
use std::path::PathBuf;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use derive_more::Display;
use error_stack::{IntoReport, IntoReportCompat, Result, ResultExt};
use futures::stream::BoxStream;
use sparrow_api::kaskada::v1alpha::{destination, FileType, ObjectStoreDestination};
use tempfile::NamedTempFile;
use tokio::time::Instant;
use uuid::Uuid;

use crate::{
    execute::progress_reporter::ProgressUpdate,
    s3::{is_s3_path, S3Helper, S3Object},
};

#[derive(Debug, Display)]
pub enum Error {
    MalformedUri,
    ProgressUpdateFailure,
    UnspecifiedFormat,
    LocalWriteFailure,
    UploadFailure,
    UnsupportedUri,
    UnsupportedDestination,
}

impl error_stack::Context for Error {}

pub(super) async fn write(
    object_store: ObjectStoreDestination,
    schema: SchemaRef,
    progress_updates_tx: tokio::sync::mpsc::Sender<ProgressUpdate>,
    batches: BoxStream<'static, RecordBatch>,
) -> Result<(), Error> {
    // Inform tracker of destination type
    progress_updates_tx
        .send(ProgressUpdate::Destination {
            destination: destination::Destination::ObjectStore(object_store.clone()),
        })
        .await
        .into_report()
        .change_context(Error::ProgressUpdateFailure)?;

    let start = Instant::now();
    let format = object_store.file_type();
    let output_prefix = object_store.output_prefix_uri;
    if is_s3_path(&output_prefix) {
        // TODO: This is currently a hacky way of uploading the file to S3. We first
        // write to a local temp file and then we upload. Ideally, we would support
        // uploading multiple files (say in 100MB chunks). This likely requires
        // either (a) having batches come out of query on a channel that is then
        // written to a rotating destination here or (b) having the files be written
        // by the compute executor with just the output paths coming out on a channel.

        // Construct the S3 Client.
        //
        // Note: This currently constructs a client from the local env config.
        // Likely we will accept credentials that will be used to construct
        // the appropriate client here.
        let s3_client = S3Helper::new().await;

        let tmp_output_file_name = output_file_name(format)?;
        let tmp_output_file = create_tempfile(&tmp_output_file_name)?;
        let tmp_output_path = tmp_output_file.path().to_owned();

        // Write the results to the specified format
        write_to_file(
            tmp_output_file.as_file(),
            format,
            schema,
            progress_updates_tx.clone(),
            batches,
        )
        .await?;

        // Upload the temp local file to S3
        let mut remote_output_file = S3Object::try_from_uri(&output_prefix)
            .into_report()
            .change_context(Error::MalformedUri)?;
        remote_output_file.push_delimited(&tmp_output_file_name);
        let output_path = remote_output_file.get_formatted_key();

        s3_client
            .upload_s3(remote_output_file.clone(), &tmp_output_path)
            .await
            .into_report()
            .change_context(Error::UploadFailure)?;

        progress_updates_tx
            .try_send(ProgressUpdate::FilesProduced {
                paths: vec![output_path],
            })
            .into_report()
            .change_context(Error::ProgressUpdateFailure)?
    } else {
        // Assumes local storage otherwise now
        let output_prefix = output_prefix
            .strip_prefix("file://")
            .ok_or(Error::UnsupportedUri)
            .into_report()
            .attach_printable_lazy(|| {
                format!("unsupported output prefix uri {output_prefix}, expected local file (file:///...)")
            })?;

        if !output_prefix.starts_with('/') {
            return Err(
                error_stack::report!(Error::UnsupportedUri).attach_printable(format!(
                    "output prefix uri requires absolute path, got {output_prefix}"
                )),
            );
        }

        let mut output_path = PathBuf::from(&output_prefix);
        if !output_path.exists() {
            std::fs::create_dir_all(&output_path)
                .into_report()
                .change_context(Error::LocalWriteFailure)
                .attach_printable_lazy(|| format!("unable to create path {output_path:?}"))?;
        }

        output_path.push(output_file_name(format)?);
        let output_file = File::create(output_path.clone())
            .into_report()
            .change_context(Error::LocalWriteFailure)
            .attach_printable_lazy(|| format!("unable to create file {output_path:?}"))?;

        // Write the results to the specified format
        write_to_file(
            &output_file,
            format,
            schema,
            progress_updates_tx.clone(),
            batches,
        )
        .await?;

        // Prepend the file:// prefix back
        let mut output_uri = output_path.to_string_lossy().to_string();
        output_uri.insert_str(0, "file://");

        progress_updates_tx
            .try_send(ProgressUpdate::FilesProduced {
                paths: vec![output_uri],
            })
            .into_report()
            .change_context(Error::ProgressUpdateFailure)?
    };
    let elapsed = start.elapsed();
    tracing::debug!("Write took {:?}", elapsed);

    Ok(())
}

/// Writes the stream of result batches to the given local File.
async fn write_to_file(
    local_output_file: &File,
    format: FileType,
    schema: SchemaRef,
    progress_updates_tx: tokio::sync::mpsc::Sender<ProgressUpdate>,
    batches: BoxStream<'static, RecordBatch>,
) -> error_stack::Result<(), Error> {
    match format {
        FileType::Csv => {
            crate::execute::output::csv::write_csv(
                local_output_file,
                schema,
                progress_updates_tx,
                batches,
            )
            .await?
        }
        FileType::Parquet => {
            crate::execute::output::parquet::write_parquet(
                local_output_file,
                schema,
                progress_updates_tx,
                batches,
            )
            .await?;
        }
        FileType::Unspecified => error_stack::bail!(Error::UnspecifiedFormat),
    };
    Ok(())
}

fn output_file_name(format: FileType) -> error_stack::Result<String, Error> {
    // Generate a UUID for the destination.
    let extension = match format {
        FileType::Csv => "csv",
        FileType::Parquet => "parquet",
        FileType::Unspecified => error_stack::bail!(Error::UnspecifiedFormat),
    };
    Ok(format!("{}.{}", Uuid::new_v4().as_hyphenated(), extension))
}

fn create_tempfile(file_name: &str) -> Result<NamedTempFile, Error> {
    tempfile::Builder::new()
        .suffix(&file_name)
        .tempfile()
        .into_report()
        .change_context(Error::LocalWriteFailure)
        .attach_printable("unable to create temp file")
}
