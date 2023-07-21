use std::sync::Arc;

use crate::worker::Injector;
use crate::{Error, Pipeline, Task, TaskRef};
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
    pipelines: Vec<Arc<dyn Pipeline>>,
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

    /// Adds the pipeline to the scheduler and allocates tasks for executing it.
    ///
    /// `partitions` determines the number of task partitions to allocate.
    pub fn add_pipeline<T>(&mut self, partitions: usize, pipeline: T) -> Arc<dyn Pipeline>
    where
        T: Pipeline + 'static,
    {
        let index = self.pipelines.len();
        let name = std::any::type_name::<T>();

        let pipeline: Arc<T> = Arc::new_cyclic(move |weak| {
            let tasks = (0..partitions)
                .map(|partition| -> TaskRef {
                    let weak: std::sync::Weak<T> = weak.clone();
                    let task = Task::new(index, name, weak, partition.into());
                    Arc::new(task)
                })
                .collect();

            // We can't create the pipeline here because creating it may have produced errors,
            // and `new_cyclic` doesn't support that. So we instead provide the tasks after
            // creation, using the infallible `initialize` method.
            let mut pipeline = pipeline;
            pipeline.initialize(tasks);
            pipeline
        });
        let pipeline: Arc<dyn Pipeline> = pipeline;
        self.pipelines.push(pipeline.clone());
        pipeline
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

#[derive(derive_more::Display, Debug)]
#[display(fmt = "error creating pipeline '{_0}'")]
pub struct CreateError(&'static str);

impl error_stack::Context for CreateError {}
