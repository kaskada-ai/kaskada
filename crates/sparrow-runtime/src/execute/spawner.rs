use crate::execute;
use futures::stream::FuturesUnordered;
use futures::Future;
use tracing::Instrument;

use crate::util::JoinTask;

pub(super) struct ComputeTaskSpawner {
    /// Tasks that have been spawned.
    futures: FuturesUnordered<JoinTask<()>>,
}

impl ComputeTaskSpawner {
    pub fn new() -> Self {
        Self {
            futures: FuturesUnordered::new(),
        }
    }

    pub fn spawn<F>(&mut self, name: String, span: tracing::Span, future: F)
    where
        F: Future<Output = error_stack::Result<(), execute::Error>> + Send + 'static,
    {
        self.add_handle(JoinTask::new(name, tokio::spawn(future.instrument(span))))
    }

    fn add_handle(&mut self, join_handle: JoinTask<()>) {
        self.futures.push(join_handle)
    }

    pub fn finish(self) -> FuturesUnordered<JoinTask<()>> {
        self.futures
    }
}
