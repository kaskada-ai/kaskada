use std::sync::Arc;

use crate::idle_workers::{IdleWorkers, WakeReason};
use crate::{queue::*, Error, TaskRef};

pub trait Scheduler {
    /// Schedule a task for immediate, local execution.
    ///
    /// For local queues, this will schedule it as the next task, potentially
    /// displacing the other task(s) scheduled at the front.
    ///
    /// If the local queue is full, it will move half of its tasks to the global
    /// queue.
    ///
    /// For the global queue, this will add to the end of the list of tasks.
    fn schedule(&mut self, task: TaskRef);

    /// Schedule a task for eventual, local execution.
    ///
    /// For local and global queues, this will add the task to the end of the tasks.
    ///
    /// For local queues, this can be used to give other tasks a chance to run.
    /// Otherwise, there’s a risk that one task will completely take over a
    /// thread in a push-pop cycle due to the LIFO slot.
    ///
    /// If the local queue is full, it will move half of its tasks to the global
    /// queue.
    fn schedule_yield(&mut self, task: TaskRef);

    /// Schedule a task for eventual execution anywhere.
    ///
    /// For both the local and global queues this adds to the end of the global
    /// queue.
    fn schedule_global(&self, task: TaskRef);
}

/// An injector that allows adding work to the global queue.
#[derive(Debug, Clone)]
pub struct Injector {
    queue: GlobalQueue<TaskRef>,
    pub(crate) idle_workers: Arc<IdleWorkers>,
}

impl Injector {
    pub fn create(workers: usize, local_queue_size: u16) -> (Self, Vec<Worker>) {
        let idle_workers = IdleWorkers::new(workers);
        let queue = GlobalQueue::new(workers, local_queue_size);
        let workers = queue
            .take_local_queues()
            .map(|queue| Worker {
                queue,
                idle_workers: idle_workers.clone(),
            })
            .collect();
        (
            Injector {
                queue,
                idle_workers,
            },
            workers,
        )
    }
}

impl Scheduler for Injector {
    fn schedule_global(&self, task: TaskRef) {
        if task.schedule() {
            tracing::trace!("Added {task:?} to queue");
            self.queue.push(task);
        } else {
            tracing::trace!("{task:?} already executing");
        }
        self.idle_workers.wake_one();
    }

    fn schedule(&mut self, task: TaskRef) {
        self.schedule_global(task);
    }

    fn schedule_yield(&mut self, task: TaskRef) {
        self.schedule_global(task);
    }
}

/// An individual worker that allows adding work to the local or global queue.
pub struct Worker {
    queue: LocalQueue<TaskRef>,
    idle_workers: Arc<IdleWorkers>,
}

impl Worker {
    /// Run the work loop to completion.
    pub(crate) fn work_loop(mut self, index: usize) -> error_stack::Result<(), Error> {
        let thread_id = std::thread::current().id();
        let _span = tracing::info_span!("Worker", ?thread_id, index).entered();
        tracing::info!("Starting work loop");
        loop {
            while let Some(task) = self.queue.pop() {
                if task.do_work(&mut self)? {
                    if task.is_complete() {
                        tracing::warn!("Completed task scheduled during execution.");
                    }

                    // This means that the task was scheduled while we were executing.
                    // As a result, we didn't add it to any queue yet, so we need to
                    // do so now. We use the global queue because generlaly it won't
                    // be processing data just produced.
                    tracing::trace!("Task {task} scheduled during execution. Re-adding.");
                    self.queue.push_yield(task);
                } else {
                    tracing::trace!("Task {task} not scheduled during execution.");
                }
            }

            match self.idle_workers.idle() {
                WakeReason::Woken => continue,
                WakeReason::AllIdle => {
                    if let Some(next) = self.queue.pop() {
                        error_stack::bail!(Error::TaskAfterAllIdle(next));
                    }

                    break;
                }
            }
        }
        Ok(())
    }
}

impl Scheduler for Worker {
    fn schedule(&mut self, task: TaskRef) {
        if task.schedule() {
            tracing::trace!("Added {task:?} to queue");
            self.queue.push(task);
        } else {
            tracing::trace!("{task:?} already executing");
        }
        self.idle_workers.wake_one();
    }

    fn schedule_yield(&mut self, task: TaskRef) {
        if task.schedule() {
            tracing::trace!("Added {task:?} to queue");
            self.queue.push_yield(task);
        } else {
            tracing::trace!("{task:?} already executing");
        }
        self.idle_workers.wake_one();
    }

    fn schedule_global(&self, task: TaskRef) {
        if task.schedule() {
            tracing::trace!("Added {task:?} to queue");
            self.queue.push_global(task);
        } else {
            tracing::trace!("{task:?} already executing");
        }
        self.idle_workers.wake_one();
    }
}
