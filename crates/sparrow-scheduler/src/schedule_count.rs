#[cfg(loom)]
pub(crate) use loom::sync::atomic::{AtomicUsize, Ordering};

#[cfg(not(loom))]
pub(crate) use std::sync::atomic::{AtomicUsize, Ordering};

/// Manages the rescheduling logic for a [Task].
///
/// This is used to ensure that a [Task] is only scheduled once at a time,
/// which is required to prevent multiple threads from working on the same task at once.
///
/// The reason a count is required, rather than just a flag indicating whether the
/// task is scheduled, is because we may have the race condition where:
/// 1. (thread 1) Doing work, sees input queue is empty.
/// 2. (thread 2) Add input to input queue.
/// 3. (thread 2) Set flag.
/// 4. (thread 1) Unset flag.
/// -> Task is not scheduled, even though it should be.
///
/// The count allows us to indicate whether the task was scheduled during execution,
/// and if so, we can re-add it to the queue if necessary.
#[repr(transparent)]
#[derive(Debug, Default)]
pub(crate) struct ScheduleCount(AtomicUsize);

impl ScheduleCount {
    /// Record a request for scheduling.
    ///
    /// Returns true if this task wasn't previously scheduled.
    pub fn schedule(&self) -> bool {
        let count = self.0.fetch_add(1, Ordering::SeqCst);
        tracing::trace!("Schedule count {count}");
        count == 0
    }

    /// Returns a `TaskGuard` which will return the count
    pub fn guard(&self) -> ScheduleGuard<'_> {
        let entry_count = self.0.load(Ordering::SeqCst);
        tracing::trace!("Schedule count on start: {entry_count}");
        debug_assert!(entry_count > 0, "Running task with entry count 0");
        ScheduleGuard {
            count: self,
            entry_count,
        }
    }
}

#[must_use]
pub(crate) struct ScheduleGuard<'a> {
    count: &'a ScheduleCount,
    entry_count: usize,
}

impl<'a> ScheduleGuard<'a> {
    /// Finish executing the task.
    ///
    /// This will reset the entry count. If the count has been increased during execution
    /// this will return `true` to indicate the task should be re-scheduled.
    pub fn finish(self) -> bool {
        let schedule_count = self.count.0.fetch_sub(self.entry_count, Ordering::SeqCst);
        tracing::trace!(
            "Count on entry {}, count on finish {schedule_count}",
            self.entry_count
        );
        schedule_count != self.entry_count
    }
}

#[cfg(test)]
mod tests {

    // Test using `loom` to verify atomic scheduling.
    //
    // To run:
    // `RUSTFLAGS="--cfg loom" cargo test -p sparrow-scheduler schedule_count::*`
    #[cfg(loom)]
    #[test]
    fn test_loom_scheduling() {
        use super::*;

        loom::model(|| {
            let count = loom::sync::Arc::new(ScheduleCount::default());
            assert!(count.schedule());

            let handle = {
                let count = count.clone();
                loom::thread::spawn(move || {
                    let guard = count.guard();
                    assert!(!count.schedule());
                    assert!(guard.finish());

                    let guard = count.guard();
                    assert!(!guard.finish());
                })
            };

            assert_eq!((), handle.join().unwrap());
            assert_eq!(0, count.0.load(Ordering::SeqCst));
        })
    }
}
