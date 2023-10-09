use std::sync::Arc;

use crate::pending::PendingSet;
use crate::worker::Injector;
use crate::{Error, Partition, Pipeline, Task, TaskRef, Worker};
use error_stack::{IntoReport, ResultExt};
use itertools::Itertools;

/// Default thread count to use if we aren't able to determine
/// the number of cores.
const DEFAULT_THREAD_COUNT: usize = 8;

/// Number of slots each thread should have in it's local task queue.
const LOCAL_QUEUE_SIZE: u16 = 32;

pub struct WorkerPool {
    query_id: String,
    injector: Injector,
    workers: Vec<Worker>,
    /// A vector of the pipelines we created.
    pipelines: Vec<Arc<dyn Pipeline>>,
    /// Track which pipelines / partitions are still running.
    pending: Arc<PendingSet>,
}

impl WorkerPool {
    /// Create a worker pool.
    ///
    /// Args:
    ///   query_id: The query ID associated with this worker pool. Used as a
    ///     prefix in the tracing spans for each worker thread.
    pub fn new(query_id: String) -> error_stack::Result<Self, Error> {
        let core_ids = core_affinity::get_core_ids();
        let threads = core_ids.as_ref().map(Vec::len).unwrap_or_else(|| {
            tracing::info!(
                "No cores retrieved. Assuming default ({DEFAULT_THREAD_COUNT}) thread count"
            );
            DEFAULT_THREAD_COUNT
        });

        let (injector, workers) = Injector::create(threads, LOCAL_QUEUE_SIZE);

        let scheduler = Self {
            query_id,
            injector,
            workers,
            pipelines: vec![],
            pending: Arc::new(PendingSet::default()),
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

        // `new_cyclic` provides a `Weak` reference to the pipeline before it is
        // created. This allows us to create tasks that reference the pipeline
        // (via weak references) and pass those tasks to the pipeline.
        let pending = self.pending.clone();
        let pipeline: Arc<T> = Arc::new_cyclic(move |weak| {
            let tasks = (0..partitions)
                .map(|partition| -> TaskRef {
                    let pipeline: std::sync::Weak<T> = weak.clone();
                    let partition: Partition = partition.into();

                    let pending = pending.add_pending(index, partition, name);
                    let task = Task::new(pending, name, pipeline);
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

        tracing::info!("Added {partitions} partitions for pipeline {index} {name}");

        pipeline
    }

    /// Start executing the pipelines.
    ///
    /// Returns a `RunningWorkers` used for completing the workers.
    pub fn start(self) -> error_stack::Result<RunningWorkers, Error> {
        let Self {
            pending,
            workers,
            query_id,
            pipelines,
            ..
        } = self;

        let core_ids = core_affinity::get_core_ids();
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
                let pending = pending.clone();
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
                        worker.work_loop(index, pending.clone())
                    })
                    .into_report()
                    .change_context(Error::SpawnWorker)
            })
            .try_collect()?;

        Ok(RunningWorkers {
            query_id,
            _pipelines: pipelines,
            handles,
        })
    }
}

pub struct RunningWorkers {
    query_id: String,
    /// Hold the Arcs for the pipelines so they aren't dropped.
    _pipelines: Vec<Arc<dyn Pipeline>>,
    handles: Vec<std::thread::JoinHandle<error_stack::Result<(), Error>>>,
}

impl RunningWorkers {
    pub fn join(self) -> error_stack::Result<(), Error> {
        tracing::info!(self.query_id, "Waiting for completion of query");
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
