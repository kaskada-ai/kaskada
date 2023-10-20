#[cfg(loom)]
pub(crate) use loom::sync::atomic::{AtomicUsize, Ordering};

#[cfg(not(loom))]
pub(crate) use std::sync::atomic::{AtomicUsize, Ordering};

/// Keeps track of the number of times a task has been scheduled.
///
/// This is used to avoid scheduling a task multiple times. Because
/// a task can be bumped from a local to global queue and taken by any worker,
/// we cannot add the same task to a queue if it currently executing (otherwise,
/// we would have multiple workers attempting to run the same task concurrently).
/// Instead, we track the number of times a task has been scheduled, and continue to
/// re-schedule the task after the previous execution of that task is complete.
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
