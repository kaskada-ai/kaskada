use std::path::Path;

use anyhow::Context;
use futures::TryStreamExt;
use itertools::Itertools;
use sparrow_api::kaskada::v1alpha::execute_request::ComputeSnapshotConfig;
use tokio_stream::wrappers::ReadDirStream;
use tracing::{info, info_span, Instrument};

use super::{S3Helper, S3Object};

/// Downloads a compute snapshot from s3 to a local directory.
pub(crate) async fn download_snapshot(
    s3_helper: &S3Helper,
    storage_path: &Path,
    config: &ComputeSnapshotConfig,
) -> anyhow::Result<()> {
    let s3_uri = config
        .resume_from
        .as_ref()
        .context("No resume_from path set")?;

    if !crate::s3::is_s3_path(s3_uri) {
        let source_dir = std::path::Path::new(&config.output_prefix).join(s3_uri);
        copy_contents(&source_dir, storage_path).await?;
        return Ok(());
    }

    let span = info_span!("Downloading snapshot files", ?s3_uri, ?storage_path);
    let _enter = span.enter();
    let snapshot_key_prefix = S3Object::try_from_uri(s3_uri)?.key;

    let snapshot_object = S3Object::try_from_uri(&config.output_prefix)?;
    let s3_objects = s3_helper
        .list_prefix_delimited(&snapshot_object.bucket, &snapshot_key_prefix)
        .in_current_span()
        .await
        .context("Listing s3 objects")?;

    let mut download_futures: futures::stream::FuturesUnordered<_> = s3_objects
        .into_iter()
        .map(|item| -> anyhow::Result<_> {
            // Append key to local directory path
            let file_name = item
                .get_relative_key_path(&snapshot_key_prefix)
                .context("Splitting path by prefix")?;

            let target_path = storage_path.join(file_name);

            Ok(s3_helper.download_s3(item, target_path))
        })
        .try_collect()?;

    let mut count = download_futures.len();
    while let Some(()) = download_futures.try_next().in_current_span().await? {
        count -= 1;
        info!("Downloaded file. {} remaining.", count);
    }

    Ok(())
}

async fn copy_contents(from_dir: &Path, to_dir: &Path) -> anyhow::Result<()> {
    anyhow::ensure!(
        from_dir.is_dir(),
        "Expected source '{:?}' to exist and be a directory",
        from_dir
    );
    anyhow::ensure!(
        to_dir.is_dir(),
        "Expected destination directory '{:?}' to exist and be a directory",
        from_dir
    );

    let read_dir = tokio::fs::read_dir(from_dir).await.context("read dir")?;
    ReadDirStream::new(read_dir)
        .map_err(|e| anyhow::anyhow!("Invalid entry: {:?}", e))
        .try_for_each_concurrent(None, |entry| async move {
            let entry = entry.path();
            anyhow::ensure!(
                entry.is_file(),
                "Expected all entries to be files, but {:?} was not",
                entry
            );
            let destination = to_dir.join(entry.file_name().context("file name")?);
            tokio::fs::copy(entry, destination).await.context("copy")?;
            Ok(())
        })
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    // Test that the async method `download_snapshot` produces futures that are
    // Send.
    //
    // This test does not need to be executed -- just compiled.
    #[tokio::test]
    async fn require_download_snapshots_to_be_send() {
        let s3_helper = S3Helper::new().await;
        let storage_path = std::path::Path::new("hello");
        let config = ComputeSnapshotConfig {
            output_prefix: "foo".to_owned(),
            resume_from: None,
        };

        fn require_send<T: Send>(_t: T) {}
        require_send(download_snapshot(&s3_helper, storage_path, &config));
    }
}
