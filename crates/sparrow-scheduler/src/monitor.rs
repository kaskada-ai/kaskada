use std::thread::ThreadId;

use error_stack::{IntoReport, ResultExt};
use hashbrown::HashMap;

use crate::Error;

/// Create a monitor that watches for thread completions.
///
/// Allows spawning "guarded" threads, which report their thread ID upon
/// completion. In turn, this allows joining on the thread results in the order
/// they complete which allows surfacing errors that would otherwise block other
/// threads.
pub(crate) struct Monitor {
    tx: tokio::sync::mpsc::UnboundedSender<ThreadId>,
    rx: tokio::sync::mpsc::UnboundedReceiver<ThreadId>,
    handles: HashMap<ThreadId, std::thread::JoinHandle<error_stack::Result<(), Error>>>,
}

/// Guard used within a thread to watch for thread completion.
pub(crate) struct MonitorGuard {
    tx: tokio::sync::mpsc::UnboundedSender<ThreadId>,
}

impl Drop for MonitorGuard {
    fn drop(&mut self) {
        match self.tx.send(std::thread::current().id()) {
            Ok(_) => (),
            Err(thread_id) => {
                tracing::error!("Failed to send thread completion for {thread_id:?}");
            }
        }
    }
}

impl std::default::Default for Monitor {
    fn default() -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        Self {
            tx,
            rx,
            handles: Default::default(),
        }
    }
}

impl Monitor {
    pub fn spawn_guarded<F: FnOnce() -> error_stack::Result<(), Error> + Send + 'static>(
        &mut self,
        name: String,
        f: F,
    ) -> error_stack::Result<(), Error> {
        let guard = MonitorGuard {
            tx: self.tx.clone(),
        };

        let handle = std::thread::Builder::new()
            .name(name)
            .spawn(move || {
                let result = f();

                std::mem::drop(guard);
                result
            })
            .into_report()
            .change_context(Error::SpawnWorker)?;

        // We can't insert the thread until after we spawn it. While
        // it seems that we could have a race condition if the the
        // thread finishes (and sends out its ID) before we insert the
        // handle, this sholudn't happen. We have `&mut self` here and
        // observing the thread IDs shutting down requires `self`, so
        // this must finish before `wait_all` is called.
        self.handles.insert(handle.thread().id(), handle);

        Ok(())
    }

    pub async fn join_all(self) -> error_stack::Result<(), Error> {
        // Take the `rx` out of self (and drop the `tx` so the stream will end).
        let Self {
            mut rx,
            mut handles,
            tx,
        } = self;
        std::mem::drop(tx);

        while let Some(finished) = rx.recv().await {
            let handle = handles
                .remove(&finished)
                // This should only happen if a spawned thread had a `MonitorGuard`
                // but was not added to `handles`. This should not happen.
                .expect("Finished unregistered handle");

            match handle.join() {
                Ok(worker_result) => worker_result?,
                Err(_) => {
                    error_stack::bail!(Error::PipelinePanic)
                }
            }

            tracing::trace!(
                "Thread {finished:?} completed. Waiting for {} threads.",
                handles.len()
            );
        }

        // This should only happen if a spawned thread was added to the handles
        // but did not register a `MonitorGuard`. This should not happen.
        assert!(
            handles.is_empty(),
            "Not all handles reported completion via monitor"
        );

        Ok(())
    }
}
