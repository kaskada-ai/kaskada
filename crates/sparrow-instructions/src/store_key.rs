use smallvec::SmallVec;

/// Information describing the storage key for persistent values.
///
/// Currently used keys:
///
/// - `met` for the max event time in the snapshot.
/// - `oia<operation_index><instruction_id>` for accumulators for a specific
///   instruction.
/// - `ok<operation_index>` for the key-hash to entity-index map for the given
///   operation.
/// - `otk<operation_index>` for the key hash enumeration in the tick operation.
/// - `ots<operation_index>` for the tick state.
/// - `oss<operation_index>` for the shift subsort value.
/// - `osrb<operation_index>` for the shift operation's pending or retained
///   batches.
/// - `<materialization_id>_progress` for the progress of a materialization.
/// - `<materialization_id>_status` for the status of a materialization.
/// - `<materialization_id>_err` for errors (if any) for a materialization.
/// - `<materialization_id>_state` for the state of a materialization.
/// NOTE: No need to reallocate the keys each time, we can make them constants.
pub struct StoreKey {
    /// The RocksDB key (or key prefix) to store values at.
    ///
    /// Should be constructed from the pass and instruction.
    key: SmallVec<[u8; 8]>,
}

impl StoreKey {
    /// Create a `StoreKey` for an instruction.
    pub fn new_accumulator(operation_index: u8, inst_index: u32) -> Self {
        let mut key = SmallVec::with_capacity(8);
        key.extend_from_slice(b"oia"); // 3
        key.push(operation_index); // 1
        key.extend_from_slice(&inst_index.to_be_bytes()); // 4
        Self { key }
    }

    /// Create a `StoreKey` for the key-hash entity-index map.
    ///
    /// Instructions are encoded as `ok<operation_index>`. The operation ID is
    /// a single `u8` .
    pub fn new_key_hash_to_index(operation_index: u8) -> Self {
        let mut key = SmallVec::with_capacity(3);
        // (o)peration, (k)ey hash
        key.extend_from_slice(b"ok"); // 2
        key.push(operation_index); // 1
        Self { key }
    }

    /// Create a `StoreKey` for the sorted key-hashes for ticks.
    ///
    /// Instructions are encoded as `otk<operation_index>`. The operation ID is
    /// a single `u8` .
    pub fn new_key_hash_set(operation_index: u8) -> Self {
        let mut key = SmallVec::with_capacity(4);
        // (o)peration, (t)ick, (k)ey hash
        key.extend_from_slice(b"otk"); // 3
        key.push(operation_index); // 1
        Self { key }
    }

    /// Create a `StoreKey` for the tick state for a tick operation.
    ///
    /// Instructions are encoded as `ots<operation_index>`. The operation ID is
    /// a single `u8`.
    pub fn new_tick_state(operation_index: u8) -> Self {
        let mut key = SmallVec::with_capacity(4);
        // (o)peration, (t)ick, (s)tate
        key.extend_from_slice(b"ots"); // 3
        key.push(operation_index); // 1
        Self { key }
    }

    /// Create a `StoreKey` for an instruction.
    pub fn new_merge_state(operation_index: u8) -> Self {
        let mut key = SmallVec::with_capacity(4);
        key.extend_from_slice(b"oms"); // 3
        key.push(operation_index); // 1
        Self { key }
    }

    /// Create a `StoreKey` for the max event time in the snapshot.
    ///
    /// All new files must have data past the max event time in the snapshot,
    /// otherwise they are considered late data.
    ///
    /// NOTE: We currently do not support snapshotting during a query, so
    /// a source file will always be fully read. This is why we can use the
    /// max event time to determine which files are new or not. In the future,
    /// we'll need to use a `max_time_per_source` to determine which files
    /// to read.
    pub fn new_max_event_time() -> Self {
        let mut key = SmallVec::with_capacity(3);
        key.extend_from_slice(b"met");
        Self { key }
    }

    /// Create a `StoreKey` for the key hash inverse.
    ///
    /// The indices map to values in the key hash inverse array.
    pub fn new_key_hash_inverse() -> Self {
        let mut key = SmallVec::with_capacity(3);
        key.extend_from_slice(b"khi");
        Self { key }
    }

    /// Create a `StoreKey` for a shift's retained batches.
    ///
    /// The array stored is encoded as a `RecordBatch` and written as a Vec<u8>.
    pub fn new_shift_until_retained_batches(operation_index: u8) -> Self {
        let mut key = SmallVec::with_capacity(5);
        key.extend_from_slice(b"osrb"); // 4
        key.push(operation_index); // 1
        Self { key }
    }

    /// Create a `StoreKey` for the shift subsort value.
    pub fn new_shift_to_subsort(operation_index: u8) -> Self {
        let mut key = SmallVec::with_capacity(4);
        key.extend_from_slice(b"oss"); // 3
        key.push(operation_index); // 1
        Self { key }
    }

    /// Creates a `StoreKey` for the state of a specific materialization.
    pub fn new_materialization_state(id: &str) -> Self {
        let progress = b"_state";
        let mut key = SmallVec::with_capacity(id.len() + progress.len());
        id.as_bytes().into_iter().for_each(|b| key.push(*b));
        key.extend_from_slice(progress);
        Self { key }
    }

    /// Creates a `StoreKey` for the error message of a specific materialization.
    pub fn new_materialization_error(id: &str) -> Self {
        let progress = b"_err";
        let mut key = SmallVec::with_capacity(id.len() + progress.len());
        id.as_bytes().into_iter().for_each(|b| key.push(*b));
        key.extend_from_slice(progress);
        Self { key }
    }

    /// Creates a `StoreKey` for the progress for a specific materialization.
    pub fn new_materialization_progress(id: &str) -> Self {
        let progress = b"_progress";
        let mut key = SmallVec::with_capacity(id.len() + progress.len());
        id.as_bytes().into_iter().for_each(|b| key.push(*b));
        key.extend_from_slice(progress);
        Self { key }
    }

    /// Creates a `StoreKey` for the status for a specific materialization.
    pub fn new_materialization_status(id: &str) -> Self {
        let progress = b"_status";
        let mut key = SmallVec::with_capacity(id.len() + progress.len());
        id.as_bytes().into_iter().for_each(|b| key.push(*b));
        key.extend_from_slice(progress);
        Self { key }
    }
}

impl AsRef<[u8]> for StoreKey {
    fn as_ref(&self) -> &[u8] {
        &self.key
    }
}
