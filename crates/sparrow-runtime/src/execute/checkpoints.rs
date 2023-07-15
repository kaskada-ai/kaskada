use std::path::Path;
use std::str::FromStr;

use error_stack::{IntoReport, ResultExt};
use futures::{StreamExt, TryStreamExt};
use itertools::Itertools;
use sparrow_api::kaskada::v1alpha::{ComputeSnapshot, ComputeSnapshotConfig};
use sparrow_instructions::ComputeStore;
use tempfile::TempDir;
use uuid::Uuid;

use crate::execute::ComputeResult;
use crate::stores::{ObjectStoreRegistry, ObjectStoreUrl};

/// Number of concurrent download/upload requests.
const CONCURRENT_LIMIT: usize = 5;

#[derive(derive_more::Display, Debug)]
pub(crate) enum Error {
    #[display(fmt = "error while uploading checkpoint file")]
    UploadIo,
    #[display(fmt = "error while downloading checkpoint file")]
    DownloadIo,
    #[display(fmt = "invalid 'resume_from': '{_0}'")]
    InvalidResumeFrom(String),
    #[display(fmt = "invalid 'output_prefix': '{_0}'")]
    InvalidOutputPrefix(String),
    #[display(fmt = "expected path '{path:?}' to be in '{prefix:?}'")]
    InvalidPrefix { prefix: String, path: String },
    #[display(fmt = "invalid object store")]
    InvalidObjectStore,
    #[display(fmt = "listing files in the checkpoint")]
    ListingFiles,
    #[display(fmt = "invalid path part '{_0}'")]
    InvalidPathPart(String),
}

impl error_stack::Context for Error {}

/// Downloads a compute snapshot from s3 to a local directory.
pub(crate) async fn download(
    resume_from: &str,
    object_stores: &ObjectStoreRegistry,
    storage_path: &Path,
    config: &ComputeSnapshotConfig,
) -> error_stack::Result<(), Error> {
    let output_prefix = ObjectStoreUrl::from_str(&config.output_prefix)
        .change_context_lazy(|| Error::InvalidOutputPrefix(config.output_prefix.clone()))?;
    error_stack::ensure!(
        output_prefix.is_delimited(),
        Error::InvalidOutputPrefix(config.output_prefix.clone())
    );
    let resume_from = output_prefix
        .join(resume_from)
        .change_context_lazy(|| Error::InvalidResumeFrom(resume_from.to_owned()))?;

    let object_store = object_stores
        .object_store(&resume_from)
        .change_context(Error::InvalidObjectStore)?;

    let prefix = resume_from.path().change_context(Error::ListingFiles)?;
    let list_result = object_store
        .list_with_delimiter(Some(&prefix))
        .await
        .into_report()
        .change_context(Error::ListingFiles)?;

    // Do the downloads, reporting progress (and remaining files).
    let count = list_result.objects.len();
    tracing::info!(
        "Downloading {count} files for checkpoint from {resume_from} to {}",
        storage_path.display()
    );

    let resume_from_path = resume_from.path().change_context(Error::DownloadIo)?;
    futures::stream::iter(list_result.objects)
        .map(|object| {
            let Some(relative_path) = object.location.as_ref().strip_prefix(resume_from_path.as_ref()) else {
                error_stack::bail!(Error::InvalidPrefix {
                    prefix: resume_from.to_string(),
                    path: resume_from_path.to_string()
                })
            };
            // Drop the leading `/` (will be left over from the strip prefix above)
            let relative_path = &relative_path[1..];
            let source_url = resume_from.join(relative_path).change_context(Error::DownloadIo)?;
            let destination_path = storage_path.join(relative_path);
            tracing::info!("For object {object:?}, relative path is {relative_path}, downloading {source_url} to {}", destination_path.display());
            Ok((source_url, destination_path))
        })
        .map_ok(|(source_url, destination_path)| async move {
            // This is a bit hacky since we need to go back to a URL to use our
            // existing `download` method and go back through the registry.
            object_stores
                .download(source_url, &destination_path)
                .await
                .change_context(Error::DownloadIo)
        })
        .try_buffer_unordered(CONCURRENT_LIMIT)
        .try_fold(count, |count, ()| {
            let count = count - 1;
            tracing::info!("Downloaded file for checkpoint. {count} remaining.");
            futures::future::ok(count)
        })
        .await?;

    Ok(())
}

/// Uploads a compute snapshot to s3.
///
/// The owned `TempDir` will be dropped on completion of uploading the snapshot.
pub(crate) async fn upload(
    object_stores: &ObjectStoreRegistry,
    storage_dir: TempDir,
    config: ComputeSnapshotConfig,
    compute_result: ComputeResult,
) -> error_stack::Result<ComputeSnapshot, Error> {
    // The name is a UUID that is referenced by snapshot metadata.
    let dest_name = Uuid::new_v4().as_hyphenated().to_string();

    let output_prefix = ObjectStoreUrl::from_str(&config.output_prefix)
        .change_context_lazy(|| Error::InvalidOutputPrefix(config.output_prefix.clone()))?;

    let path = if let Some(local_output_path) = output_prefix.local_path() {
        let destination = local_output_path.join(dest_name);

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
            .change_context(Error::UploadIo)?;

        format!("{}/", destination.display())
    } else {
        let dir = std::fs::read_dir(storage_dir.path())
            .into_report()
            .change_context(Error::UploadIo)?;
        let source_prefix = storage_dir.path();

        let destination = output_prefix
            .join(&format!("{dest_name}/"))
            .change_context_lazy(|| Error::InvalidPathPart(dest_name.clone()))?;

        tracing::info!(
            "Uploading compute snapshot files from '{:?}' to '{}' (output prefix '{}')",
            source_prefix,
            destination,
            output_prefix,
        );

        let entries: Vec<_> = dir
            .map(|entry| -> error_stack::Result<_, Error> {
                let source_path = entry.into_report().change_context(Error::UploadIo)?.path();

                // Figure out the path relative the directory.
                let relative = source_path
                    .strip_prefix(source_prefix)
                    .into_report()
                    .change_context_lazy(|| Error::InvalidPrefix {
                        prefix: source_prefix.display().to_string(),
                        path: source_path.display().to_string(),
                    })?;

                let relative = relative
                    .to_str()
                    .ok_or_else(|| Error::InvalidPathPart(relative.display().to_string()))?;
                let destination = destination
                    .join(relative)
                    .change_context_lazy(|| Error::InvalidPathPart(relative.to_owned()))?;
                Ok((source_path, destination))
            })
            .try_collect()?;

        // Then run the upload futures.
        let count = entries.len();
        futures::stream::iter(entries)
            .map(|(source_path, destination_url)| async move {
                object_stores
                    .upload(&source_path, destination_url)
                    .await
                    .change_context(Error::UploadIo)
            })
            .buffer_unordered(CONCURRENT_LIMIT)
            .try_fold(count, |count, ()| {
                let count = count - 1;
                tracing::info!("Uploaded file for checkpoint. {count} remaining.");
                futures::future::ok(count)
            })
            .await?;

        // Explicitly close the storage dir so any problems cleaning it up
        // are reported.
        storage_dir
            .close()
            .into_report()
            .change_context(Error::UploadIo)?;

        destination.to_string()
    };

    let snapshot = ComputeSnapshot {
        path,
        max_event_time: Some(compute_result.max_input_timestamp),
        plan_hash: Some(compute_result.plan_hash),
        snapshot_version: ComputeStore::current_version(),
    };

    Ok(snapshot)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Test that the async method `download` produces futures that are
    // Send.
    //
    // This test does not need to be executed -- just compiled.
    #[tokio::test]
    async fn require_download_to_be_send() {
        let object_stores = ObjectStoreRegistry::default();
        let storage_path = std::path::Path::new("hello");
        let config = ComputeSnapshotConfig::default();

        fn require_send<T: Send>(_t: T) {}
        require_send(download(
            "resume_from",
            &object_stores,
            storage_path,
            &config,
        ));
    }

    // Test that the async method `upload` produces futures that are
    // Send.
    //
    // This test does not need to be executed -- just compiled.
    #[tokio::test]
    async fn require_upload_to_be_send() {
        let object_stores = ObjectStoreRegistry::default();
        let storage_dir = TempDir::new().unwrap();
        let config = ComputeSnapshotConfig::default();
        let compute_result = ComputeResult::default();

        fn require_send<T: Send>(_t: T) {}
        require_send(upload(&object_stores, storage_dir, config, compute_result));
    }
}
