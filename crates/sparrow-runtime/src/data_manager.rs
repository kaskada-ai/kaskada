use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{anyhow, Context};
use async_once_cell::Lazy;
use chrono::NaiveDateTime;
use futures::FutureExt;
use hashbrown::hash_map::EntryRef;
use hashbrown::HashMap;
use sparrow_api::kaskada::v1alpha::PreparedFile;
use tempfile::NamedTempFile;
use tracing::{debug, error};

use crate::s3::{self, S3Helper, S3Object};

/// Manages the data files on disk and being downloaded.
///
/// It does not make any decisions about *when* to download a file --
/// in a computation with many files, it would be difficult to determine
/// when each file is needed. Similarly, it would be difficult to do that
/// across multiple queries on the same node. Instead, it manages the
/// downloaded files and allows sharing the download across multiple
/// uses of the same file. It also manages the number of parallel downloads
/// and (in the future) the usage of disk space.
///
/// Currently, this uses a strong hash map of the data handles, which means
/// nothing is deleted until the data manager is dropped. We should consider
/// implementing some form of cache eviction and make this shared between
/// queries on the pod.
///
/// # Future: Sharing
///
/// Eventually, we would like to share the DataManager between queries. When
/// we do that, we'll need to create a *different* way for the query to
/// determine the set of all data handles. Currently, it relies on
/// [DataManager::handles].
///
/// # Future: Space Management / Deletion
///
/// The Data Manager currently downloads files to a temp directory,
/// meaning they should be deleted when the pod restarts. In the future,
/// we may use a persistent location, along with intelligent management
/// of the space. Note that we can't just use an in-memory cache, since
/// a major reason for using persistent disk would be to persist across
/// restarts (otherwise we would consider caching data in-memory). So,
/// this needs to be a more intelligent process that examines the free
/// space on the disk, and deletes least recently used files as space
/// runs out.
#[derive(Debug)]
pub struct DataManager {
    s3_helper: S3Helper,
    handles: HashMap<PreparedFile, Arc<DataHandle>>,
}

impl DataManager {
    pub fn new(s3_helper: S3Helper) -> Self {
        Self {
            s3_helper,
            handles: HashMap::default(),
        }
    }

    /// Queues a download of the given path.
    ///
    /// This will return a `DataHandle` which may be used to await the
    /// local path of the downloaded file.
    pub fn queue_download(
        &mut self,
        prepared_file: &PreparedFile,
    ) -> anyhow::Result<Arc<DataHandle>> {
        match self.handles.entry_ref(prepared_file) {
            EntryRef::Occupied(occupied) => Ok(occupied.get().clone()),
            EntryRef::Vacant(vacant) => {
                let handle = Arc::new(DataHandle::try_new(self.s3_helper.clone(), prepared_file)?);
                Ok(vacant.insert(handle).clone())
            }
        }
    }

    pub fn handles(&self) -> impl Iterator<Item = Arc<DataHandle>> + '_ {
        self.handles.values().cloned()
    }
}

/// Error returned when a download failed.
///
/// We can't use `anyhow` because it isn't cloneable.
#[derive(Debug, Clone)]
pub struct DownloadError;

/// A handle on a file that has been requested from the `DataManager`.
#[derive(Debug)]
pub struct DataHandle {
    data_path: DataPath,
    min_event_time: NaiveDateTime,
    max_event_time: NaiveDateTime,
    num_rows: usize,
}

pub enum DataPath {
    Local(PathBuf),
    S3 {
        /// The original (S3 path) for debugging purposes.
        original_path: String,
        /// The (lazy) future downloading the file.
        local_file: Lazy<Result<NamedTempFile, DownloadError>>,
    },
}

impl std::fmt::Debug for DataPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Local(path) => f.debug_tuple("Local").field(path).finish(),
            Self::S3 {
                original_path,
                local_file,
            } => f
                .debug_struct("S3")
                .field("original_path", original_path)
                .field("local_file", &local_file.try_get())
                .finish(),
        }
    }
}

impl DataHandle {
    /// Create a DataHandle.
    ///
    /// Should generally be called via [DataManager]. Visible for testing.
    pub(crate) fn try_new(
        s3_helper: S3Helper,
        prepared_file: &PreparedFile,
    ) -> anyhow::Result<Self> {
        let data_path = if s3::is_s3_path(&prepared_file.path) {
            let s3_object = S3Object::try_from_uri(&prepared_file.path)?;
            DataPath::new_s3(s3_helper, s3_object)
        } else {
            let local_path = PathBuf::from(&prepared_file.path);
            anyhow::ensure!(
                local_path.exists(),
                "Can't create local path from non-existant path {:?}",
                local_path
            );
            anyhow::ensure!(
                local_path.extension() == Some(OsStr::new("parquet")),
                "Expected local file extension to be .parquet, but got {:?}",
                local_path.file_name()
            );
            DataPath::Local(local_path)
        };

        let min_event_time = prepared_file
            .min_event_time
            .as_ref()
            .context("min_event_time")?;
        let min_event_time =
            NaiveDateTime::from_timestamp_opt(min_event_time.seconds, min_event_time.nanos as u32)
                .context("min_event_time to NaiveDateTime")?;
        let max_event_time = prepared_file
            .max_event_time
            .as_ref()
            .context("max_event_time")?;
        let max_event_time =
            NaiveDateTime::from_timestamp_opt(max_event_time.seconds, max_event_time.nanos as u32)
                .context("max_event_time to NaiveDateTime")?;

        Ok(Self {
            data_path,
            min_event_time,
            max_event_time,
            num_rows: prepared_file.num_rows as usize,
        })
    }

    pub fn min_event_time(&self) -> NaiveDateTime {
        self.min_event_time
    }

    pub fn max_event_time(&self) -> NaiveDateTime {
        self.max_event_time
    }

    /// Returns true if the future is already available.
    pub fn is_ready(&self) -> bool {
        match &self.data_path {
            DataPath::Local(_) => true,
            DataPath::S3 { local_file, .. } => local_file.try_get().is_some(),
        }
    }

    /// Compare two [data handles](DataHandle) lexicographically.
    ///
    /// The comparison is performed (in order by the following fields):
    /// - `min_event_time`
    /// - `max_event_time`
    /// - `num_rows`
    /// - `original_path`
    pub fn cmp_time(&self, other: &Self) -> std::cmp::Ordering {
        self.min_event_time
            .cmp(&other.min_event_time)
            .then_with(|| self.max_event_time.cmp(&other.max_event_time))
            .then_with(|| self.num_rows.cmp(&other.num_rows))
    }

    /// Get the local path to the downloaded file.
    ///
    /// If the file is already available, this future will be ready
    /// immediately.
    ///
    /// If the file has not yet been downloaded, this will wait for
    /// it to be downloaded before returning the local path. This may
    /// initiate the download.
    ///
    /// If downloading the file failed, this will return the error.
    ///
    /// Note: Once a file has been downloaded, it will not be deleted
    /// until the corresponding `DataHandles` have been released. This
    /// ensures that a file is not removed between the download and use
    /// of the download.
    pub async fn get_path(&self) -> anyhow::Result<&Path> {
        self.data_path.resolve().await
    }

    /// Ensure the underlying data file is ready.
    ///
    /// Expects an owned [Arc] for the data handle to ensure lifetimes work out.
    ///
    /// Returns the [Arc] for diagnostic purposes.
    pub async fn prefetch(self: Arc<Self>) -> anyhow::Result<Arc<Self>> {
        self.data_path.resolve().await?;
        Ok(self)
    }
}

impl DataPath {
    pub fn new_s3(s3_helper: S3Helper, s3_object: S3Object) -> Self {
        let original_path = s3_object.get_formatted_key();
        // TODO: Ideally, we wouldn't need to clone the s3_object at all.
        let local_file = Lazy::new(
            async move {
                debug!("Downloading {:?}", s3_object);
                let temp_file = tempfile::Builder::new()
                    .suffix(".parquet")
                    .tempfile()
                    .map_err(|err| {
                        error!("Failed to create temp file for download: {}", err);
                        DownloadError
                    })?;
                s3_helper
                    .download_s3(s3_object.clone(), temp_file.path())
                    .await
                    .map_err(|err| {
                        // TODO: Determine if the download should be retried.
                        error!("Failed to download '{:?}' to tempfile: {}", s3_object, err);
                        DownloadError
                    })?;

                Ok(temp_file)
            }
            .boxed(),
        );

        Self::S3 {
            original_path,
            local_file,
        }
    }

    async fn resolve(&self) -> anyhow::Result<&Path> {
        match self {
            DataPath::Local(path) => Ok(path),
            DataPath::S3 {
                original_path,
                local_file,
            } => {
                // This is a bit painful since the underlying error can't be cloned.
                match local_file.get().await {
                    Ok(path) => Ok(path.as_ref()),
                    Err(_) => Err(anyhow!(
                        "Failed to download '{:?}'; see logs",
                        original_path
                    )),
                }
            }
        }
    }
}
