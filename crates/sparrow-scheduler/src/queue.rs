//! Provide the global and per-thread (local) work queues.
//!
//! These wrap the currently used crate to make it easy to swap in
//! different implementations.

pub trait Queue<T> {
    /// Schedule an item for immediate, local execution.
    ///
    /// For local queues, this will schedule it as the next item, potentially
    /// displacing the other item(s) scheduled at the front.
    ///
    /// If the local queue is full, it will move half of its items to the global
    /// queue.
    ///
    /// For the global queue, this will add to the end of the list of items.
    fn schedule(&mut self, item: T);

    /// Schedule an item for eventual, local execution.
    ///
    /// For local and global queues, this will add the item to the end of the queue.
    ///
    /// For local queues, this can be used to give other tasks a chance to run.
    /// Otherwise, there’s a risk that one task will completely take over a
    /// thread in a push-pop cycle due to the LIFO slot.
    ///
    /// If the local queue is full, it will move half of its items to the global
    /// queue.
    fn schedule_yield(&mut self, item: T);

    /// Schedule n item for eventual execution anywher.
    ///
    /// For both the local and global queues this adds to the end of the global
    /// queue.
    fn schedule_global(&self, item: T);
}

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

impl<T> GlobalQueue<T> {
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
}

impl<T> Queue<T> for GlobalQueue<T> {
    fn schedule_global(&self, item: T) {
        self.queue.push(item)
    }

    fn schedule(&mut self, item: T) {
        self.schedule_global(item)
    }

    fn schedule_yield(&mut self, item: T) {
        self.schedule_global(item)
    }
}

impl<T> LocalQueue<T> {
    /// Pop an item from the local queue, or steal from the global and sibling queues if it is empty.
    pub fn pop(&mut self) -> Option<T> {
        self.queue.pop()
    }
}

impl<T> Queue<T> for LocalQueue<T> {
    fn schedule(&mut self, item: T) {
        self.queue.push(item)
    }

    fn schedule_yield(&mut self, item: T) {
        self.queue.push_yield(item)
    }

    fn schedule_global(&self, item: T) {
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

        local.schedule(1);
        local.schedule(2);
        local.schedule(3);

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

        global.schedule_global(1);
        global.schedule_global(2);

        // The local queue steals work from the global queue, which is LIFO.
        assert_eq!(local.pop(), Some(1));
        assert_eq!(local.pop(), Some(2));
    }
}
