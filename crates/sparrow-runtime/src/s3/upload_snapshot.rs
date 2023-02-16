use std::path::PathBuf;

use anyhow::Context;
use error_stack::{IntoReport, IntoReportCompat, ResultExt};
use sparrow_api::kaskada::v1alpha::execute_request::ComputeSnapshotConfig;
use sparrow_api::kaskada::v1alpha::execute_response::ComputeSnapshot;
use sparrow_instructions::ComputeStore;
use tempfile::TempDir;
use uuid::Uuid;

use super::{S3Helper, S3Object};
use crate::execute::ComputeResult;

#[derive(derive_more::Display, Debug)]
pub enum Error {
    #[display(fmt = "i/o error while uploading snapshot")]
    Io,
    #[display(fmt = "expected path '{source_path:?}' to be in '{source_prefix:?}'")]
    InvalidPrefix {
        source_prefix: PathBuf,
        source_path: PathBuf,
    },
}

impl error_stack::Context for Error {}

/// Uploads a compute snapshot to s3.
///
/// The owned `TempDir` will be dropped on completion of uploading the snapshot.
pub(crate) async fn upload_snapshot(
    s3_helper: S3Helper,
    storage_dir: TempDir,
    config: ComputeSnapshotConfig,
    compute_result: ComputeResult,
) -> error_stack::Result<ComputeSnapshot, Error> {
    // The name is a UUID that is referenced by snapshot metadata.
    let dest_name = Uuid::new_v4().to_string();

    let path = if !crate::s3::is_s3_path(&config.output_prefix) {
        let destination = std::path::Path::new(&config.output_prefix).join(dest_name);

        // If this is a local path, we just need to move the directory to the
        // destination.
        tracing::info!(
            "Moving Rocks DB from {:?} to local output {:?}",
            storage_dir.path(),
            destination
        );

        let storage_dir = storage_dir.into_path();
        tokio::fs::rename(&storage_dir, &destination)
            .await
            .into_report()
            .change_context(Error::Io)?;
        destination.to_string_lossy().into_owned()
    } else {
        let dir = std::fs::read_dir(storage_dir.path())
            .into_report()
            .change_context(Error::Io)?;
        let source_prefix = storage_dir.path();

        // The dest_prefix contains the bucket, so create an s3 object to parse.
        let mut dest_path = S3Object::try_from_uri(&config.output_prefix)
            .into_report()
            .change_context(Error::Io)?;
        dest_path.push_delimited(&dest_name);
        let dest_path = dest_path;

        tracing::info!(
            "Uploading compute snapshot files from '{:?}', to s3 at '{:?}'",
            source_prefix,
            dest_path.get_formatted_key(),
        );

        // Iterate over all files in the storage directory
        for entry in dir {
            let source_path = entry.into_report().change_context(Error::Io)?.path();

            // Split the path to get the destination key
            let source_key = source_path
                .strip_prefix(source_prefix)
                .into_report()
                .change_context_lazy(|| Error::InvalidPrefix {
                    source_prefix: source_prefix.to_owned(),
                    source_path: source_path.clone(),
                })?;

            let dest_path = dest_path.join_delimited(source_key.to_str().ok_or(Error::Io)?);
            tracing::info!(
                "Uploading snapshot to bucket {:?} with key {:?}",
                dest_path.bucket,
                dest_path.key
            );

            s3_helper
                .upload_s3(dest_path, &source_path)
                .await
                .context("Uploading file to s3")
                .into_report()
                .change_context(Error::Io)?;
        }

        // Explicitly close the storage dir so any problems cleaning it up
        // are reported.
        storage_dir
            .close()
            .into_report()
            .change_context(Error::Io)?;

        dest_path.get_formatted_key()
    };

    let snapshot = ComputeSnapshot {
        path,
        max_event_time: Some(compute_result.max_input_timestamp),
        plan_hash: Some(compute_result.plan_hash),
        snapshot_version: ComputeStore::current_version(),
    };

    Ok(snapshot)
}
