//! Provide the global and per-thread (local) work queues.
//!
//! These wrap the currently used crate to make it easy to swap in
//! different implementations.

/// A cloneable, global queue for adding elements to any worker.
#[derive(Debug)]
#[repr(transparent)]
pub(crate) struct GlobalQueue<T> {
    queue: work_queue::Queue<T>,
}

/// The local queue for a specific worker.
///
/// Generally, tasks are added (and processed) in FIFO order, but each local
/// queue has a single LIFO slot, allowing recently produced tasks to be
/// immediately executed.
///
/// Also allows adding tasks to the global queue.
#[derive(Debug)]
#[repr(transparent)]
pub(crate) struct LocalQueue<T> {
    queue: work_queue::LocalQueue<T>,
}

// Manually implement Clone since we don't need `T: Clone`.
impl<T> Clone for GlobalQueue<T> {
    fn clone(&self) -> Self {
        Self {
            queue: self.queue.clone(),
        }
    }
}

impl<T: std::fmt::Debug> GlobalQueue<T> {
    pub(crate) fn new(local_queues: usize, local_queue_size: u16) -> Self {
        Self {
            queue: work_queue::Queue::new(local_queues, local_queue_size),
        }
    }

    /// Take the local queues associated with this.
    ///
    /// May only be called once.
    ///
    /// Panics if the local queues have already been taken.
    pub(crate) fn take_local_queues(&self) -> impl Iterator<Item = LocalQueue<T>> + '_ {
        self.queue.local_queues().map(|queue| LocalQueue { queue })
    }

    pub(crate) fn push(&self, item: T) {
        self.queue.push(item)
    }
}

impl<T: std::fmt::Debug> LocalQueue<T> {
    /// Pop an item from the local queue, or steal from the global and sibling queues if it is empty.
    pub fn pop(&mut self) -> Option<T> {
        self.queue.pop()
    }

    pub(crate) fn push(&mut self, item: T) {
        self.queue.push(item)
    }

    pub(crate) fn push_yield(&mut self, item: T) {
        self.queue.push_yield(item)
    }

    pub(crate) fn push_global(&self, item: T) {
        self.queue.global().push(item)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_focus_on_recent_item() {
        let global = GlobalQueue::new(1, 4);
        let mut local = global.take_local_queues().next().unwrap();

        local.push(1);
        local.push(2);
        local.push(3);

        // The fact this pops 3 is important. It is what makes sure the task
        // most recently produced on this CPU (with the data already in the cache)
        // is what is executed next.
        assert_eq!(local.pop(), Some(3));

        // The order of these tasks is somewhat unimportant. Currently, anything
        // other than the "most recently" produced task is LIFO. It may be
        // beneficial to be FIFO in case we have two tasks using "local" data.
        assert_eq!(local.pop(), Some(1));
        assert_eq!(local.pop(), Some(2));
    }

    #[test]
    fn test_take_global_items() {
        let global = GlobalQueue::new(1, 4);
        let mut local = global.take_local_queues().next().unwrap();

        global.push(1);
        global.push(2);

        // The local queue steals work from the global queue, which is LIFO.
        assert_eq!(local.pop(), Some(1));
        assert_eq!(local.pop(), Some(2));
    }
}
