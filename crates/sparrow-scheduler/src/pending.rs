use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use hashbrown::HashMap;
use parking_lot::Mutex;

use crate::Partition;

/// Track the number of pending pipeline partitions.
#[derive(Default)]
pub(crate) struct PendingSet {
    /// Map from pending partition to name of the transform.
    ///
    /// Used to report the currently pending partitions. Should not
    /// be used for checking whether a specific partition is pending.
    pending_partitions: Mutex<HashMap<PendingPartitionKey, &'static str>>,
    /// Count of the total pending partitions.
    ///
    /// Keeping this outside the mutex allows for fast checking of the current
    /// count.
    pending_partition_count: AtomicUsize,
}

impl std::fmt::Debug for PendingSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let pending_partitions = self.pending_partitions.lock();
        f.debug_struct("PendingSet")
            .field("pending_partitions", &pending_partitions)
            .field("pending_partition_count", &self.pending_partition_count)
            .finish()
    }
}

pub(crate) struct PendingPartition {
    pending_set: Arc<PendingSet>,
    pub(crate) pipeline_index: usize,
    pub(crate) partition: Partition,
    is_complete: AtomicBool,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
struct PendingPartitionKey {
    pipeline_index: usize,
    partition: Partition,
}

impl PendingSet {
    pub fn add_pending(
        self: &Arc<Self>,
        pipeline_index: usize,
        partition: Partition,
        name: &'static str,
    ) -> PendingPartition {
        let key = PendingPartitionKey {
            pipeline_index,
            partition,
        };
        let pending_partition = PendingPartition {
            pending_set: self.clone(),
            pipeline_index,
            partition,
            is_complete: AtomicBool::new(false),
        };

        let previous = self.pending_partitions.lock().insert(key, name);
        debug_assert_eq!(
            previous, None,
            "Duplicate pipeline partition added to pending set"
        );
        self.pending_partition_count.fetch_add(1, Ordering::Release);

        pending_partition
    }

    pub fn pending_partition_count(&self) -> usize {
        self.pending_partition_count.load(Ordering::Acquire)
    }
}

impl std::fmt::Debug for PendingPartition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingPartition")
            .field("pipeline_index", &self.pipeline_index)
            .field("partition", &self.partition)
            .field("pending", &self.is_complete)
            .finish_non_exhaustive()
    }
}

impl PendingPartition {
    /// Record this pending partition completed.
    pub fn complete(&self) {
        let previous = self.is_complete.fetch_or(true, Ordering::AcqRel);
        debug_assert!(!previous, "Task already completed");

        let previous = self
            .pending_set
            .pending_partitions
            .lock()
            .remove(&PendingPartitionKey {
                pipeline_index: self.pipeline_index,
                partition: self.partition,
            });

        let previous = previous.expect("task should be in pending set");

        let remaining = self
            .pending_set
            .pending_partition_count
            .fetch_sub(1, Ordering::AcqRel);
        tracing::info!(
            "Completed partition {} of pipeline {previous} {}. {remaining} remaining.",
            self.partition,
            self.pipeline_index,
        );
    }

    /// Return true if this partition is completed.
    pub fn is_complete(&self) -> bool {
        self.is_complete.load(Ordering::Acquire)
    }
}
