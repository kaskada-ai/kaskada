use crate::{queue::*, Error, TaskRef};

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

impl Queue<TaskRef> for Injector {
    fn push_global(&self, task: TaskRef) {
        if task.schedule() {
            self.queue.push_global(task)
        }
    }

    fn push(&mut self, task: TaskRef) {
        self.push_global(task)
    }

    fn push_yield(&mut self, task: TaskRef) {
        self.push_global(task)
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

impl Queue<TaskRef> for Worker {
    fn push(&mut self, task: TaskRef) {
        if task.schedule() {
            self.queue.push(task)
        }
    }

    fn push_yield(&mut self, task: TaskRef) {
        if task.schedule() {
            self.queue.push_yield(task)
        }
    }

    fn push_global(&self, task: TaskRef) {
        if task.schedule() {
            self.queue.push_global(task)
        }
    }
}
