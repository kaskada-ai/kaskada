use std::any::Any;
use std::sync::Arc;

use crate::worker::Injector;
use crate::{Error, Partitioned, Pipeline, Task, TaskRef};
use error_stack::{IntoReport, ResultExt};
use itertools::Itertools;

/// Default thread count to use if we aren't able to determine
/// the number of cores.
const DEFAULT_THREAD_COUNT: usize = 8;

/// Number of slots each thread should have in it's local task queue.
const LOCAL_QUEUE_SIZE: u16 = 32;

#[derive(Debug)]
pub struct Scheduler {
    injector: Injector,
    handles: Vec<std::thread::JoinHandle<error_stack::Result<(), Error>>>,
    /// A vector of the pipelines we created.
    ///
    /// Since [`Pipeline`] is not object safe, we can't hold `Arc<dyn Pipeline>`.
    /// This is OK since all interactions with the pipeline should happen via
    /// the `Task` for a specific partition.
    ///
    /// This just needs to hold onto the pipelines during execution so the weak
    /// references each [`Task`] holds can be upgraded as needed.
    pipelines: Vec<Arc<dyn Any + Sync + Send>>,
}

impl Scheduler {
    pub fn start(query_id: &str) -> error_stack::Result<Self, Error> {
        let core_ids = core_affinity::get_core_ids();
        let threads = core_ids
            .as_ref()
            .map(Vec::len)
            .unwrap_or(DEFAULT_THREAD_COUNT);

        let (injector, workers) = Injector::create(threads, LOCAL_QUEUE_SIZE);

        let core_ids = core_ids
            .into_iter()
            .flatten()
            .map(Some)
            .chain(std::iter::repeat(None));
        let handles = workers
            .into_iter()
            .zip(core_ids)
            .enumerate()
            .map(|(index, (worker, core_id))| {
                // Spawn the worker thread.
                let span = tracing::info_span!("compute", query_id, index);
                std::thread::Builder::new()
                    .name(format!("compute-{index}"))
                    .spawn(move || {
                        let _enter = span.enter();

                        // Set the core affinity, if possible, so this thread always
                        // executes on the same core.
                        if let Some(core_id) = core_id {
                            if core_affinity::set_for_current(core_id) {
                                tracing::info!(
                                    "Set core affinity for thread {index} to {core_id:?}"
                                );
                            } else {
                                tracing::info!(
                                    "Failed to set core affinity for thread {index} to {core_id:?}"
                                );
                            }
                        } else {
                            tracing::info!("Setting core affinity not supported");
                        };

                        // Run the worker
                        worker.work_loop()
                    })
                    .into_report()
                    .change_context(Error::SpawnWorker)
            })
            .try_collect()?;

        let scheduler = Self {
            injector,
            handles,
            pipelines: vec![],
        };
        Ok(scheduler)
    }

    /// Return the global injector queue.
    pub fn injector(&self) -> &Injector {
        &self.injector
    }

    pub fn add_pipeline<T, F>(&mut self, partitions: usize, f: F)
    where
        T: Pipeline + 'static,
        F: FnOnce(Partitioned<TaskRef>) -> T,
    {
        let index = self.pipelines.len();
        let pipeline = Arc::new_cyclic(|weak| {
            let tasks = (0..partitions)
                .map(|partition| -> TaskRef {
                    let task = Task::new(index, weak.clone(), partition.into());
                    Arc::new(task)
                })
                .collect();
            f(tasks)
        });
        self.pipelines.push(pipeline)
    }

    pub fn stop(self) -> error_stack::Result<(), Error> {
        for handle in self.handles {
            match handle.join() {
                Ok(worker_result) => worker_result?,
                Err(_) => {
                    error_stack::bail!(Error::WorkerPanicked)
                }
            }
        }

        Ok(())
    }
}
