use std::str::FromStr;

use anyhow::Context;
use arrow::array::{Array, ArrayRef, AsArray, PrimitiveArray, UInt64Array};
use arrow::datatypes::{DataType, UInt64Type};

use error_stack::{IntoReport, IntoReportCompat, ResultExt};
use futures::TryStreamExt;
use hashbrown::hash_map::Entry;
use hashbrown::HashMap;
use sparrow_arrow::downcast::downcast_primitive_array;
use sparrow_compiler::DataContext;
use sparrow_instructions::GroupId;
use sparrow_instructions::{ComputeStore, StoreKey};

use crate::read::ParquetFile;
use crate::stores::{ObjectStoreRegistry, ObjectStoreUrl};

/// Stores the mapping from key hash u64 to the position in the keys array.
///
/// Used for reverse looking up the entity key hash to original entity key.
/// If the entity key type is null, then all inverse keys are null.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct KeyHashInverse {
    key_hash_to_indices: HashMap<u64, u64>,
    #[serde(with = "sparrow_arrow::serde::array_ref")]
    key: ArrayRef,
}

impl std::fmt::Debug for KeyHashInverse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyHashInverse")
            .field(
                "key_hash_to_indices",
                &format!("{} entries", self.key_hash_to_indices.len()),
            )
            .finish_non_exhaustive()
    }
}

#[derive(derive_more::Display, Debug)]
pub enum Error {
    #[display(fmt = "invalid metadata url '{_0}'")]
    InvalidMetadataUrl(String),
    #[display(fmt = "failed to open metadata")]
    OpeningMetadata,
    #[display(fmt = "failed to read metadata")]
    ReadingMetadata,
    #[display(fmt = "key hashes contained nulls")]
    KeyHashContainedNull,
    #[display(Fmt = "error in Arrow kernel")]
    Arrow,
    #[display(fmt = "key hash not registered")]
    MissingKeyHash,
    #[display(fmt = "key hashes and keys are of different lengths ({keys} != {key_hashes})")]
    MismatchedLengths { keys: usize, key_hashes: usize },
    #[display(fmt = "incompatible key types (expected: {expected:?}, actual: {actual:?})")]
    IncompatibleKeyTypes {
        expected: DataType,
        actual: DataType,
    },
}

impl error_stack::Context for Error {}

impl KeyHashInverse {
    /// Restores the KeyHashInverse from the compute store.
    pub fn restore_from(store: &ComputeStore) -> anyhow::Result<Self> {
        store
            .get(&StoreKey::new_key_hash_inverse())?
            .with_context(|| "unable to get key hash inverses from store")
    }

    /// Stores the KeyHashInverse to the compute store.
    pub fn store_to(&self, compute_store: &ComputeStore) -> anyhow::Result<()> {
        compute_store.put(&StoreKey::new_key_hash_inverse(), &self)?;
        Ok(())
    }

    pub fn key_type(&self) -> &DataType {
        self.key.data_type()
    }

    /// Creates a new key hash inverse from a primary grouping data type.
    pub fn from_data_type(primary_grouping_type: &DataType) -> Self {
        Self {
            key_hash_to_indices: HashMap::new(),
            key: arrow::array::new_empty_array(primary_grouping_type),
        }
    }

    /// Looks up all the tables from the data context matching the primary
    /// grouping to build up the key hash inverse.
    pub async fn add_from_data_context(
        &mut self,
        data_context: &DataContext,
        primary_grouping: GroupId,
        registry: &ObjectStoreRegistry,
    ) -> error_stack::Result<(), Error> {
        let metadata_files = data_context
            .tables_for_grouping(primary_grouping)
            .flat_map(|table| table.metadata_for_files());

        let mut streams = Vec::new();
        for file in metadata_files {
            let url =
                ObjectStoreUrl::from_str(&file).change_context(Error::InvalidMetadataUrl(file))?;
            let file = ParquetFile::try_new(registry, url, None)
                .await
                .change_context(Error::OpeningMetadata)?;
            let stream = file
                .read_stream(None, None)
                .await
                .change_context(Error::OpeningMetadata)?;
            streams.push(stream);
        }

        let mut stream = futures::stream::select_all(streams)
            .map_err(|e| e.change_context(Error::ReadingMetadata));
        while let Some(batch) = stream.try_next().await? {
            let hash_col = batch.column(0);
            let hash_col: &UInt64Array = downcast_primitive_array(hash_col.as_ref())
                .into_report()
                .change_context(Error::ReadingMetadata)?;
            let entity_key_col = batch.column(1);
            self.add(entity_key_col.as_ref(), hash_col)
                .change_context(Error::ReadingMetadata)?;
        }

        // HACKY: Add the in-memory batches to the key hash inverse.
        let in_memory = data_context
            .tables_for_grouping(primary_grouping)
            .flat_map(|table| {
                table
                    .in_memory
                    .as_ref()
                    .and_then(|in_memroy| in_memroy.current())
                    .map(|batch| {
                        let keys = batch
                            .column_by_name(&table.config().group_column_name)
                            .unwrap();
                        let key_hashes = batch.columns()[2].clone();
                        (keys.clone(), key_hashes.clone())
                    })
            });
        for (keys, key_hashes) in in_memory {
            self.add(keys.as_ref(), key_hashes.as_primitive())
                .change_context(Error::ReadingMetadata)
                .unwrap();
        }

        Ok(())
    }

    /// Adds a key array and key hashes to the inverse.
    ///
    /// Assumes that the keys and key_hashes provided are the same length and
    /// values are aligned to map from a key to a hash per index. The
    /// current implementation eagerly adds the keys and hashes to the
    /// inverse but can be optimized to perform the addition lazily.
    fn add(
        &mut self,
        keys: &dyn Array,
        key_hashes: &UInt64Array,
    ) -> error_stack::Result<(), Error> {
        // Since the keys map to the key hashes directly, both arrays need to be the
        // same length
        error_stack::ensure!(key_hashes.null_count() == 0, Error::KeyHashContainedNull);
        error_stack::ensure!(
            keys.data_type() == self.key.data_type(),
            Error::IncompatibleKeyTypes {
                expected: self.key.data_type().clone(),
                actual: keys.data_type().clone(),
            }
        );
        let mut len = self.key_hash_to_indices.len() as u64;
        // Determine the indices that we need to add.
        let indices_from_batch: Vec<u64> = key_hashes
            .values()
            .iter()
            .enumerate()
            .flat_map(|(index, key_hash)| {
                match self.key_hash_to_indices.entry(*key_hash) {
                    Entry::Occupied(_) => {
                        // Key hash is already registered.
                        None
                    }
                    Entry::Vacant(vacancy) => {
                        vacancy.insert(len);
                        len += 1;
                        Some(index as u64)
                    }
                }
            })
            .collect();
        debug_assert_eq!(self.key_hash_to_indices.len(), len as usize);

        if !indices_from_batch.is_empty() {
            let indices_from_batch: PrimitiveArray<UInt64Type> =
                PrimitiveArray::from_iter_values(indices_from_batch);
            let keys = arrow_select::take::take(keys, &indices_from_batch, None)
                .into_report()
                .change_context(Error::Arrow)?;
            let concatenated_keys: Vec<_> = vec![self.key.as_ref(), keys.as_ref()];
            let concatenated_keys = arrow_select::concat::concat(&concatenated_keys)
                .into_report()
                .change_context(Error::Arrow)?;
            self.key = concatenated_keys;
        }
        Ok(())
    }

    /// Checks if any of the keys provided have not been processed
    fn has_new_keys(&self, key_hashes: &UInt64Array) -> bool {
        key_hashes
            .iter()
            .flatten()
            .any(|key_hash| !self.key_hash_to_indices.contains_key(&key_hash))
    }

    /// Inverse lookup from a key hash array to the original entity keys.
    ///
    /// If the entity key type is null, then a null array is returned of same
    /// length.
    pub fn inverse(&self, key_hashes: &UInt64Array) -> error_stack::Result<ArrayRef, Error> {
        let mut key_hash_indices: Vec<u64> = Vec::new();
        for key_hash in key_hashes.values() {
            let key_hash_index = self
                .key_hash_to_indices
                .get(key_hash)
                .ok_or(Error::MissingKeyHash)?;
            key_hash_indices.push(*key_hash_index);
        }
        let key_hash_indices: PrimitiveArray<UInt64Type> =
            PrimitiveArray::from_iter_values(key_hash_indices);
        let result = arrow_select::take::take(&self.key, &key_hash_indices, None)
            .into_report()
            .change_context(Error::Arrow)?;
        Ok(result)
    }
}

/// A thread-safe wrapper around the key hash inverse implemented with tokio
/// read/write locks. The current implementation allows for 128 read locks and
/// is wrapped around the KeyHashInverse.
pub struct ThreadSafeKeyHashInverse {
    key_map: tokio::sync::RwLock<KeyHashInverse>,
    pub key_type: DataType,
}

impl std::fmt::Debug for ThreadSafeKeyHashInverse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.key_map.try_read() {
            Ok(khi) => f
                .debug_struct("ThreadSafeKeyHashInverse")
                .field("key_type", &self.key_type)
                .field("key_map", &khi)
                .finish(),
            Err(_) => f
                .debug_struct("ThreadSafeKeyHashInverse")
                .field("key_type", &self.key_type)
                .field("key_map", &"locked")
                .finish(),
        }
    }
}

impl ThreadSafeKeyHashInverse {
    /// The thread safe key hash inverse is a wrapper around the key hash
    /// inverse that does the book keeping on the concurrent reads and uses
    /// locks to handle single writes.
    pub fn new(key_hash_inverse: KeyHashInverse) -> Self {
        let data_type = key_hash_inverse.key_type().clone();
        Self {
            key_map: tokio::sync::RwLock::with_max_readers(key_hash_inverse, 128),
            key_type: data_type,
        }
    }

    /// Creates a new key hash inverse from a primary grouping data type.
    pub fn from_data_type(primary_grouping_type: &DataType) -> Self {
        Self::new(KeyHashInverse::from_data_type(primary_grouping_type))
    }

    /// Lookup keys from a key hash array.
    ///
    /// This method is thread-safe and acquires the read-lock.
    pub async fn inverse(&self, key_hashes: &UInt64Array) -> error_stack::Result<ArrayRef, Error> {
        let read = self.key_map.read().await;
        read.inverse(key_hashes)
    }

    /// Add a key array and key hashes to the inverse map.
    ///
    /// # Errors
    /// It is an error if key hashes and keys are of different lengths.
    ///
    /// # Thread Safety
    /// This method is thread safe. It acquires the read lock to check if
    /// any of the keys need to be added to the inverse map, and only acquires
    /// the write lock if needed.
    pub async fn add(
        &self,
        keys: &dyn Array,
        key_hashes: &UInt64Array,
    ) -> error_stack::Result<(), Error> {
        error_stack::ensure!(
            keys.len() == key_hashes.len(),
            Error::MismatchedLengths {
                keys: keys.len(),
                key_hashes: key_hashes.len()
            }
        );
        let has_new_keys = {
            let read = self.key_map.read().await;
            read.has_new_keys(key_hashes)
        };

        if has_new_keys {
            let mut write = self.key_map.write().await;
            write.add(keys, key_hashes)
        } else {
            Ok(())
        }
    }

    pub fn blocking_add(
        &self,
        keys: &dyn Array,
        key_hashes: &UInt64Array,
    ) -> error_stack::Result<(), Error> {
        error_stack::ensure!(
            keys.len() == key_hashes.len(),
            Error::MismatchedLengths {
                keys: keys.len(),
                key_hashes: key_hashes.len()
            }
        );
        let has_new_keys = self.key_map.blocking_read().has_new_keys(key_hashes);

        if has_new_keys {
            self.key_map.blocking_write().add(keys, key_hashes)
        } else {
            Ok(())
        }
    }

    /// Stores the KeyHashInverse to the compute store.
    ///
    /// This method is thread-safe and acquires the read-lock.
    pub async fn store_to(&self, compute_store: &ComputeStore) -> anyhow::Result<()> {
        let read = self.key_map.read().await;
        read.store_to(compute_store)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int32Array, StringArray, UInt64Array};
    use arrow::datatypes::DataType;
    use sparrow_instructions::ComputeStore;

    use crate::key_hash_inverse::{KeyHashInverse, ThreadSafeKeyHashInverse};

    #[test]
    fn test_inverse_with_int32() {
        let keys = Arc::new(Int32Array::from(vec![100, 200]));
        let key_hashes = UInt64Array::from(vec![1, 2]);

        let mut key_hash = KeyHashInverse::from_data_type(&DataType::Int32);
        key_hash.add(keys.as_ref(), &key_hashes).unwrap();

        let test_hashes = UInt64Array::from_iter_values([1, 2, 1]);
        let result = key_hash.inverse(&test_hashes).unwrap();
        assert_eq!(result.as_ref(), &Int32Array::from(vec![100, 200, 100]));
    }

    #[test]
    fn test_inverse_with_string() {
        let keys = StringArray::from(vec!["awkward", "tacos"]);
        let key_hashes = UInt64Array::from(vec![1, 2]);

        let mut key_hash = KeyHashInverse::from_data_type(&DataType::Utf8);
        key_hash.add(&keys, &key_hashes).unwrap();

        let test_hashes = UInt64Array::from_iter_values([1, 2, 1]);
        let result = key_hash.inverse(&test_hashes).unwrap();
        assert_eq!(
            result.as_ref(),
            &StringArray::from(vec!["awkward", "tacos", "awkward"])
        );
    }

    #[test]
    fn test_has_new_keys_no_new_keys() {
        let keys = Int32Array::from(vec![100, 200]);
        let key_hashes = UInt64Array::from(vec![1, 2]);
        let mut key_hash = KeyHashInverse::from_data_type(&DataType::Int32);
        key_hash.add(&keys, &key_hashes).unwrap();

        let verify_key_hashes = UInt64Array::from(vec![1, 2]);
        assert!(!key_hash.has_new_keys(&verify_key_hashes));
    }

    #[test]
    fn test_has_new_keys_some_new_keys() {
        let keys = Int32Array::from(vec![100, 200]);
        let key_hashes = UInt64Array::from(vec![1, 2]);
        let mut key_hash = KeyHashInverse::from_data_type(&DataType::Int32);
        key_hash.add(&keys, &key_hashes).unwrap();

        let verify_key_hashes = UInt64Array::from(vec![1, 2, 3]);
        assert!(key_hash.has_new_keys(&verify_key_hashes));
    }

    #[test]
    fn test_has_new_keys_all_new_keys() {
        let keys = Int32Array::from(vec![100, 200]);
        let key_hashes = UInt64Array::from(vec![1, 2]);
        let mut key_hash = KeyHashInverse::from_data_type(&DataType::Int32);
        key_hash.add(&keys, &key_hashes).unwrap();

        let verify_key_hashes = UInt64Array::from(vec![3, 4, 5]);
        assert!(key_hash.has_new_keys(&verify_key_hashes));
    }

    #[tokio::test]
    async fn test_thread_safe_inverse_with_int32() {
        let keys = Int32Array::from(vec![100, 200]);
        let key_hashes = UInt64Array::from(vec![1, 2]);
        let key_hash = KeyHashInverse::from_data_type(&DataType::Int32);

        let key_hash = ThreadSafeKeyHashInverse::new(key_hash);
        key_hash.add(&keys, &key_hashes).await.unwrap();

        let test_hashes = UInt64Array::from_iter_values([1, 2, 1]);
        let result = key_hash.inverse(&test_hashes).await.unwrap();
        assert_eq!(result.as_ref(), &Int32Array::from(vec![100, 200, 100]));
    }

    #[tokio::test]
    async fn test_thread_safe_inverse_with_string() {
        let keys = StringArray::from(vec!["awkward", "tacos"]);
        let key_hashes = UInt64Array::from(vec![1, 2]);
        let key_hash = KeyHashInverse::from_data_type(&DataType::Utf8);

        let key_hash = ThreadSafeKeyHashInverse::new(key_hash);
        key_hash.add(&keys, &key_hashes).await.unwrap();

        let test_hashes = UInt64Array::from_iter_values([1, 2, 1]);
        let result = key_hash.inverse(&test_hashes).await.unwrap();
        assert_eq!(
            result.as_ref(),
            &StringArray::from(vec!["awkward", "tacos", "awkward"])
        );
    }

    #[tokio::test]
    async fn test_inverse_store_to_restore_from_compute_store() {
        // Create a compute store from a temp directory
        let compute_store = compute_store();
        // Create a key hash inverse and populate it with some data
        let key_hash = test_key_hash_inverse().await;
        // Save the key hash inverse to the store
        key_hash.store_to(&compute_store).unwrap();
        // Restore the key hash inverse from the store
        let key_hash = KeyHashInverse::restore_from(&compute_store).unwrap();
        // Verify the previous results are accessible/valid.
        let test_hashes = UInt64Array::from_iter_values([1, 2, 1]);
        let result = key_hash.inverse(&test_hashes).unwrap();
        assert_eq!(
            result.as_ref(),
            &StringArray::from(vec!["awkward", "tacos", "awkward"])
        );
    }

    #[tokio::test]
    async fn test_inverse_restore_from_adds_data() {
        let compute_store = compute_store();
        let key_hash = test_key_hash_inverse().await;
        key_hash.store_to(&compute_store).unwrap();

        let mut key_hash = KeyHashInverse::restore_from(&compute_store).unwrap();
        let keys = StringArray::from(vec!["party", "pizza"]);
        let key_hashes = UInt64Array::from(vec![3, 4]);
        key_hash.add(&keys, &key_hashes).unwrap();
        let test_hashes = UInt64Array::from_iter_values([1, 2, 3, 4]);
        let result = key_hash.inverse(&test_hashes).unwrap();
        assert_eq!(
            result.as_ref(),
            &StringArray::from(vec!["awkward", "tacos", "party", "pizza"])
        );
    }

    async fn test_key_hash_inverse() -> KeyHashInverse {
        let keys = StringArray::from(vec!["awkward", "tacos"]);
        let key_hashes = UInt64Array::from(vec![1, 2]);
        let mut key_hash = KeyHashInverse::from_data_type(&DataType::Utf8);
        key_hash.add(&keys, &key_hashes).unwrap();
        key_hash
    }

    fn compute_store() -> ComputeStore {
        let tempdir = tempfile::Builder::new().tempdir().unwrap();
        ComputeStore::try_new_from_path(tempdir.path()).unwrap()
    }
}
