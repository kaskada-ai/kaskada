use std::sync::Arc;

use parking_lot::{Condvar, Mutex};

/// Track the idle workers.
#[derive(Debug)]
pub(crate) struct IdleWorkers {
    state: Mutex<State>,
    condition: Condvar,
}

#[derive(Debug)]
struct State {
    /// Number of workers.
    num_workers: usize,

    /// Number of (curretly) active workers.
    num_idle: usize,

    /// Whether sources have been completed.
    sources_done: bool,

    // Whether all workers have been idle at once after sources finished.
    all_idle: bool,
}

/// Reason that the thread was woken.
#[must_use]
pub(crate) enum WakeReason {
    /// The thread was woken by an explicit call to `wake_one` or `wake_all`.
    Woken,
    /// The thread was woken when all threads were idle.
    AllIdle,
}

impl IdleWorkers {
    /// Create a new `WorkerIdle` for the given number of workers.
    pub fn new(num_workers: usize) -> Arc<Self> {
        Arc::new(IdleWorkers {
            state: Mutex::new(State {
                num_workers,
                num_idle: 0,
                sources_done: false,
                all_idle: false,
            }),
            condition: Condvar::default(),
        })
    }

    /// Idle the current thread until woken or all threads are idle.
    ///
    /// Returns `true` if all threads were idle
    pub fn idle(&self) -> WakeReason {
        let mut state = self.state.lock();
        state.num_idle += 1;

        if state.sources_done && state.num_idle == state.num_workers {
            tracing::trace!(
                "All {} threads are idle. Waking workers to shutdown.",
                state.num_workers
            );

            state.all_idle = true;
            state.num_idle -= 1;
            std::mem::drop(state);

            self.condition.notify_all();
            WakeReason::AllIdle
        } else {
            let thread_id = std::thread::current().id();
            tracing::trace!(
                "Idling thread {thread_id:?} ({}/{} idle)",
                state.num_idle,
                state.num_workers
            );
            self.condition.wait(&mut state);
            state.num_idle -= 1;
            if state.all_idle {
                tracing::trace!("Thread {thread_id:?} woken to shutdown (all workers idle).");
                WakeReason::AllIdle
            } else {
                tracing::trace!(
                    "Thread {thread_id:?} woken ({}/{} idle).",
                    state.num_idle,
                    state.num_workers
                );
                WakeReason::Woken
            }
        }
    }

    pub fn finish_sources(&self) {
        self.state.lock().sources_done = true;
    }

    pub fn wake_one(&self) {
        tracing::trace!("Waking one worker");
        self.condition.notify_one();
    }
}
