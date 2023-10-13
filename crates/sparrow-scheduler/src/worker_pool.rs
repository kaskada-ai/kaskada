use std::sync::Arc;
use std::thread::ThreadId;

use crate::monitor::Monitor;
use crate::pending::PendingSet;
use crate::worker::Injector;
use crate::{Error, Partition, Pipeline, Task, TaskRef, Worker};
use error_stack::{IntoReport, ResultExt};
use hashbrown::HashMap;
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

        let monitor = Monitor::with_capacity(workers.len());
        let handles = workers
            .into_iter()
            .zip(core_ids)
            .enumerate()
            .map(|(index, (worker, core_id))| {
                // Spawn the worker thread.
                let span = tracing::info_span!("compute", query_id, index);
                let pending = pending.clone();
                let guard = monitor.guard();
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
                        let result = worker.work_loop(index, pending.clone());

                        // Make sure the monitor guard is moved into this thread
                        // and dropped before returning.
                        std::mem::drop(guard);
                        result
                    })
                    .into_report()
                    .change_context(Error::SpawnWorker)
            })
            .map_ok(|handle| (handle.thread().id(), handle))
            .try_collect()?;

        Ok(RunningWorkers {
            query_id,
            _pipelines: pipelines,
            handles,
            finishing_threads: monitor.rx,
        })
    }
}

pub struct RunningWorkers {
    query_id: String,
    /// Hold the Arcs for the pipelines so they aren't dropped.
    _pipelines: Vec<Arc<dyn Pipeline>>,
    handles: HashMap<ThreadId, std::thread::JoinHandle<error_stack::Result<(), Error>>>,
    finishing_threads: tokio::sync::mpsc::Receiver<ThreadId>,
}

impl RunningWorkers {
    pub async fn join(mut self) -> error_stack::Result<(), Error> {
        tracing::info!(self.query_id, "Waiting for completion of query");
        while let Some(finished) = self.finishing_threads.recv().await {
            let handle = self
                .handles
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
        }

        // This should only happen if a spawned thread was added to the handles
        // but did not register a `MonitorGuard`. This should not happen.
        assert!(
            self.handles.is_empty(),
            "Not all handles reported completion via monitor"
        );

        Ok(())
    }
}

#[derive(derive_more::Display, Debug)]
#[display(fmt = "error creating pipeline '{_0}'")]
pub struct CreateError(&'static str);

impl error_stack::Context for CreateError {}

#[cfg(test)]
mod tests {
    use sparrow_batch::{Batch, RowTime};

    use crate::{
        Error, Partition, Partitioned, Pipeline, PipelineError, Scheduler, TaskRef, WorkerPool,
    };

    #[derive(Debug, Default)]
    struct PanicPipeline {
        tasks: Partitioned<crate::TaskRef>,
    }

    impl Pipeline for PanicPipeline {
        fn initialize(&mut self, tasks: Partitioned<TaskRef>) {
            self.tasks = tasks;
        }

        fn add_input(
            &self,
            input_partition: Partition,
            _input: usize,
            _batch: Batch,
            scheduler: &mut dyn Scheduler,
        ) -> error_stack::Result<(), PipelineError> {
            scheduler.schedule(self.tasks[input_partition].clone());
            Ok(())
        }

        fn close_input(
            &self,
            _input_partition: Partition,
            _input: usize,
            _scheduler: &mut dyn Scheduler,
        ) -> error_stack::Result<(), PipelineError> {
            unreachable!("Should panic before closing");
        }

        fn do_work(
            &self,
            _partition: Partition,
            _scheduler: &mut dyn Scheduler,
        ) -> error_stack::Result<(), PipelineError> {
            panic!("PanicPipeline is meant to panic");
        }
    }

    #[tokio::test]
    async fn test_pipeline_panic() {
        sparrow_testing::init_test_logging();

        let mut workers = WorkerPool::new("query".to_owned()).unwrap();
        let pipeline = workers.add_pipeline(1, PanicPipeline::default());
        let mut injector = workers.injector().clone();
        let workers = workers.start().unwrap();

        pipeline
            .add_input(
                0.into(),
                0,
                Batch::new_empty(RowTime::from_timestamp_ns(73)),
                &mut injector,
            )
            .unwrap();
        let result = workers.join().await;
        assert!(result.is_err(), "Expected {result:?} to be an error.");
        let error = result.unwrap_err();
        let error = error.current_context();
        assert!(
            matches!(error, Error::PipelinePanic),
            "Expected {error:?} to be pipeline panic error"
        );
    }
}
