use std::sync::atomic::{AtomicUsize, Ordering};

use hashbrown::HashSet;
use parking_lot::Mutex;

use crate::Partition;

/// Struct for tracking the pending partitions of each pipeline.
///
/// Used to determine when all pending pipelines are complete.
#[derive(Debug, Default)]
pub(crate) struct Pending {
    /// Partitions that we are still executing.
    pending: Mutex<HashSet<PendingPartition>>,
    /// The number of executing partitions.
    pending_count: AtomicUsize,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
struct PendingPartition {
    pipeline: usize,
    partition: Partition,
}

impl Pending {
    pub fn add_pipeline(&self, pipeline: usize, partitions: usize) {
        let mut pending = self.pending.lock();
        for partition in 0..partitions {
            assert!(
                pending.insert(PendingPartition {
                    pipeline,
                    partition: partition.into(),
                }),
                "Pipeline {pipeline} partition {partition} already added to pending set"
            );
        }
        self.pending_count.fetch_add(partitions, Ordering::SeqCst);
    }

    /// Removes the partition.
    ///
    /// Returns the number of pending processes after removal.
    pub fn remove(&self, pipeline: usize, partition: impl Into<Partition>) -> usize {
        let partition = partition.into();
        let mut pending = self.pending.lock();
        assert!(
            pending.remove(&PendingPartition {
                pipeline,
                partition,
            }),
            "Pipeline {pipeline} partition {partition} already removed from pending set"
        );
        self.pending_count.fetch_sub(1, Ordering::SeqCst) - 1
    }
}
