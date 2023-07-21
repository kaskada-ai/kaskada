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

    /// Schedule n item for eventual execution anywher.
    ///
    /// For both the local and global queues this adds to the end of the global
    /// queue.
    fn schedule_global(&self, task: TaskRef);
}

/// An injector that allows adding work to the global queue.
#[derive(Debug, Clone)]
pub struct Injector {
    queue: GlobalQueue<TaskRef>,
}

impl Injector {
    pub fn create(workers: usize, local_queue_size: u16) -> (Self, Vec<Worker>) {
        let queue = GlobalQueue::new(workers, local_queue_size);
        let workers = queue
            .take_local_queues()
            .map(|queue| Worker { queue })
            .collect();
        (Injector { queue }, workers)
    }
}

impl Scheduler for Injector {
    fn schedule_global(&self, task: TaskRef) {
        if task.schedule() {
            self.queue.push(task)
        }
    }

    fn schedule(&mut self, task: TaskRef) {
        self.schedule_global(task)
    }

    fn schedule_yield(&mut self, task: TaskRef) {
        self.schedule_global(task)
    }
}

/// An individual worker that allows adding work to the local or global queue.
pub struct Worker {
    queue: LocalQueue<TaskRef>,
}

impl Worker {
    /// Run the work loop to completion.
    pub(crate) fn work_loop(mut self) -> error_stack::Result<(), Error> {
        while let Some(task) = self.queue.pop() {
            if task.do_work(&mut self)? {
                // This means that the task was schedule while we were executing.
                // As a result, we didn't add it to any queue yet, so we need to
                // do so now.
                self.queue.push_global(task);
            }
        }
        Ok(())
    }
}

impl Scheduler for Worker {
    fn schedule(&mut self, task: TaskRef) {
        if task.schedule() {
            self.queue.push(task)
        }
    }

    fn schedule_yield(&mut self, task: TaskRef) {
        if task.schedule() {
            self.queue.push_yield(task)
        }
    }

    fn schedule_global(&self, task: TaskRef) {
        if task.schedule() {
            self.queue.push_global(task)
        }
    }
}
