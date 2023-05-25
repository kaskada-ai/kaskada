use std::path::Path;
use std::sync::Arc;

use anyhow::Context;
use error_stack::{IntoReportCompat, ResultExt};
use prost_wkt_types::Timestamp;
use rocksdb::DBPinnableSlice;
use sparrow_api::kaskada::v1alpha::{
    get_materialization_status_response::{
        self,
        materialization_status::{self, State},
        MaterializationStatus,
    },
    PlanHash, ProgressInformation,
};
use tracing::info;

use crate::StoreKey;

#[derive(derive_more::Display, Debug)]
pub enum Error {
    Update(String),
}

impl error_stack::Context for Error {}

/// Storage layer responsible for caching query metadata and results.
///
/// Enables resumeable queries. Currently implemented with RocksDb.
/// Note that RocksDB is automatically closed when it is out of scope.
pub struct ComputeStore {
    /// Persistent RocksDB storage for accumulated and buffered values.
    rocksdb: rocksdb::DBWithThreadMode<rocksdb::SingleThreaded>,
    write_options: rocksdb::WriteOptions,
    /// Whether this was resumed from a populated compute store.
    pub is_resumed: bool,
}

const STORE_VERSION_KEY: &[u8] = b"_store_version";
const STORE_VERSION: i32 = 0;

const PLAN_HASH_KEY: &[u8] = b"_plan_hash";

impl ComputeStore {
    pub fn try_new(
        path: &Path,
        max_allowed_max_event_time: &Timestamp,
        plan_hash: &PlanHash,
    ) -> anyhow::Result<Arc<ComputeStore>> {
        let store = Self::try_new_from_path(path)?;

        if store.is_resumed {
            // Verify the plan hash in the store matches.
            let stored_plan_hash: PlanHash = store
                .get_proto(&PLAN_HASH_KEY)?
                .context("missing plan hash")?;
            anyhow::ensure!(
                &stored_plan_hash == plan_hash,
                "Incompatible compute store -- stored plan hash {}, new plan hash {}",
                stored_plan_hash,
                plan_hash
            );

            // Also verify the max event time in the snapshot is less than (or equal to) the
            // max allowed time. This is computed by the analysis, and represents the
            // maximum event time in the snapshot such that we can correctly
            // produce results given the properties of the query (such as
            // changed since time, final results, etc.).
            let max_event_time: Timestamp = store
                .get_proto(&StoreKey::new_max_event_time())?
                .context("missing max event time")?;

            // `Timestamp` doesn't implement `Ord` so we need to do so manually:
            // https://github.com/fdeantoni/prost-wkt/issues/15
            let cmp = max_event_time
                .seconds
                .cmp(&max_allowed_max_event_time.seconds)
                .then_with(|| max_event_time.nanos.cmp(&max_allowed_max_event_time.nanos));
            anyhow::ensure!(
                cmp.is_le(),
                "Max event time of snapshot ({:?}) was greater than max allowed snapshot time \
                 ({:?})",
                max_event_time,
                max_allowed_max_event_time
            );
        } else {
            // If the store is not resumed, write the plan hash.
            store.put_proto(&PLAN_HASH_KEY, plan_hash)?;
        }
        Ok(Arc::new(store))
    }

    /// Creates a database if it does not exist at the given path.
    ///
    /// Note that attempting to open the same database while already open
    /// results in an error.
    pub fn try_new_from_path(path: &Path) -> anyhow::Result<Self> {
        {
            let mut options = rocksdb::Options::default();
            options.create_if_missing(true);
            options.set_compression_type(rocksdb::DBCompressionType::Lz4);
            // Sets the number of OLD log files we keep around.
            // We may not need any at all, to be honest, but keeping this
            // for now since it shouldn't cause undue harm.
            options.set_keep_log_file_num(3);
            // Set the max log file size to 10MB.
            options.set_max_log_file_size(10_000_000);

            // MMAP and direct seem (on Ben's Mac) to be comparable. We may
            // want to make this an option and configure it to see which is
            // best in-situ.
            //
            options.set_use_direct_io_for_flush_and_compaction(true);
            options.set_use_direct_reads(true);
            // options.set_allow_mmap_reads(true);
            // options.set_allow_mmap_writes(true);

            let mut write_options = rocksdb::WriteOptions::default();
            write_options.set_sync(false);
            write_options.disable_wal(true);

            let rocksdb = rocksdb::DB::open(&options, path).context("Open rocksdb")?;

            // If the STORE_VERSION_KEY already exists within the db, then
            // we know this was restored from existing state.
            let is_resumed = if let Some(bytes) = rocksdb
                .get_pinned(STORE_VERSION_KEY)
                .context("Get stored version")?
            {
                info!("Restoring from existing compute store at path '{:?}'", path);
                let version: i32 =
                    bincode::deserialize(&bytes).context("Deserialize stored version")?;
                anyhow::ensure!(
                    version == STORE_VERSION,
                    "Incompatible stored version {:?}, expected {:?}",
                    version,
                    STORE_VERSION
                );
                true
            } else {
                info!("Created new compute store at path '{:?}'", path);
                rocksdb
                    .put(STORE_VERSION_KEY, bincode::serialize(&STORE_VERSION)?)
                    .context("Write store version")?;
                false
            };

            Ok(Self {
                rocksdb,
                write_options,
                is_resumed,
            })
        }
    }

    /// Returns the current version.
    pub fn current_version() -> i32 {
        STORE_VERSION
    }

    fn get_bytes(&self, key_bytes: &[u8]) -> anyhow::Result<Option<DBPinnableSlice<'_>>> {
        // Note: use `get_pinned` to improve memory usage.
        let bytes = self
            .rocksdb
            .get_pinned(key_bytes)
            .context("Read key from rocksdb")?;

        if bytes.is_none() && self.is_resumed {
            // If state does not exist for a `key`, this indicates a discrepancy
            // between the snapshot and the built plan. For now, we're asserting
            // this because it is easier to relax restrictions than add them.
            //
            // However, this may change if the storage pattern is updated such
            // that rows are removed from storage when their values are null.
            Err(anyhow::anyhow!("Missing state for key '{:?}'", key_bytes))
        } else {
            Ok(bytes)
        }
    }

    /// Retrieve the value stored at the given key.
    ///
    /// This expects the key to be stored if the database has been used,
    /// and will return an error if it doesn't exist.
    pub fn get<T: serde::de::DeserializeOwned>(
        &self,
        key: &impl AsRef<[u8]>,
    ) -> anyhow::Result<Option<T>> {
        if let Some(bytes) = self.get_bytes(key.as_ref())? {
            let value =
                bincode::deserialize(&bytes).context("Deserialize value bytes from rocksdb")?;
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    /// Retrieve the value stored at the given key.
    ///
    /// This expects the key to be stored if the database has been used,
    /// and will return an error if it doesn't exist.
    pub fn get_proto<T: prost::Message + Default>(
        &self,
        key: &impl AsRef<[u8]>,
    ) -> anyhow::Result<Option<T>> {
        if let Some(bytes) = self.get_bytes(key.as_ref())? {
            let value =
                T::decode(bytes.as_ref()).context("Deserialize value bytes from rocksdb")?;
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    /// Retrieve the vector value stored at the given key.
    ///
    /// This expects the key to be stored if the database has been used,
    /// and will return an error if it doesn't exist.
    pub fn get_to_vec<T>(&self, key: &impl AsRef<[u8]>, vec: &mut Vec<T>) -> anyhow::Result<()>
    where
        Vec<T>: serde::de::DeserializeOwned,
    {
        if let Some(new_vec) = self.get(key)? {
            *vec = new_vec;
        } else {
            vec.clear()
        }
        Ok(())
    }

    pub fn put<T: serde::ser::Serialize>(
        &self,
        key: &impl AsRef<[u8]>,
        value: &T,
    ) -> anyhow::Result<()> {
        let bytes = bincode::serialize(&value)?;
        self.rocksdb.put_opt(key, bytes, &self.write_options)?;
        Ok(())
    }

    pub fn put_proto<T: prost::Message>(
        &self,
        key: &impl AsRef<[u8]>,
        value: &T,
    ) -> anyhow::Result<()> {
        // TODO: Consider re-using a per-thread scratch space to avoid
        // allocating when writing protos.
        let bytes = value.encode_to_vec();
        self.rocksdb.put_opt(key, bytes, &self.write_options)?;
        Ok(())
    }

    pub fn get_max_event_time(&self) -> anyhow::Result<Option<Timestamp>> {
        self.get_proto(&StoreKey::new_max_event_time())
    }

    pub fn put_max_event_time(&self, value: &Timestamp) -> anyhow::Result<()> {
        self.put_proto(&StoreKey::new_max_event_time(), value)
    }

    pub fn update_materialization_state(
        &self,
        id: &str,
        state: materialization_status::State,
    ) -> error_stack::Result<(), Error> {
        let key = StoreKey::new_materialization_state(id);
        self.put(&key, &(state as i32))
            .into_report()
            .change_context(Error::Update(format!("failed to update state for {id}")))?;
        Ok(())
    }

    pub fn update_materialization_error(
        &self,
        id: &str,
        error: &str,
    ) -> error_stack::Result<(), Error> {
        let key = StoreKey::new_materialization_error(id);
        self.put(&key, &error)
            .into_report()
            .change_context(Error::Update(format!(
                "failed to update error message for {id}"
            )))?;
        Ok(())
    }

    pub fn update_materialization_progress(
        &self,
        id: &str,
        progress: ProgressInformation,
    ) -> error_stack::Result<(), Error> {
        let key = StoreKey::new_materialization_progress(id);
        self.put_proto(&key, &progress)
            .into_report()
            .change_context(Error::Update(format!("failed to update progress for {id}")))?;
        Ok(())
    }

    pub fn get_materialization_status(
        &self,
        id: &str,
    ) -> error_stack::Result<Option<MaterializationStatus>, Error> {
        let key = StoreKey::new_materialization_status(id);
        let status = self
            .get_proto(&key)
            .into_report()
            .change_context(Error::Update(format!(
                "failed to get status for materialization {id}"
            )))?;
        Ok(status)
    }
}
