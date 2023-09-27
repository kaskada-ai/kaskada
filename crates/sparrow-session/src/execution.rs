use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use sparrow_api::kaskada::v1alpha::ExecuteResponse;

use crate::Error;

pub struct Execution {
    /// Tokio handle managing this execution.
    handle: tokio::runtime::Handle,
    /// Channel to receive output on.
    output: tokio_stream::wrappers::ReceiverStream<RecordBatch>,
    /// Future which resolves to the first error or None.
    status: Status,
    /// Stop signal. Send `true` to stop execution.
    stop_signal_rx: tokio::sync::watch::Sender<bool>,
    pub schema: SchemaRef,
}

enum Status {
    // Running(BoxFuture<'static, error_stack::Result<(), Error>>),
    Running(BoxStream<'static, error_stack::Result<ExecuteResponse, Error>>),
    Failed,
    Completed,
}

impl Execution {
    pub(super) fn new(
        handle: tokio::runtime::Handle,
        output_rx: tokio::sync::mpsc::Receiver<RecordBatch>,
        progress: BoxStream<'static, error_stack::Result<ExecuteResponse, Error>>,
        stop_signal_rx: tokio::sync::watch::Sender<bool>,
        schema: SchemaRef,
    ) -> Self {
        let output = tokio_stream::wrappers::ReceiverStream::new(output_rx);
        let status = Status::Running(progress);

        Self {
            handle,
            output,
            status,
            stop_signal_rx,
            schema,
        }
    }

    /// Check the status future.
    ///
    /// If it has previously completed (successfully or with error) returns
    /// accordingly. Otherwise, check to see if the future is ready, and update
    /// status (and return) accordingly.
    fn is_done(&mut self) -> error_stack::Result<(), Error> {
        let result = match &mut self.status {
            Status::Running(progress) => {
                // Based on the implementation of `FutureExt::now_or_never`:
                let noop_waker = futures::task::noop_waker();
                let mut cx = std::task::Context::from_waker(&noop_waker);

                match progress.try_poll_next_unpin(&mut cx) {
                    std::task::Poll::Ready(Some(x)) => x,
                    _ => return Ok(()),
                }
            }
            Status::Failed => error_stack::bail!(Error::ExecutionFailed),
            Status::Completed => return Ok(()),
        };

        match result {
            Ok(_) => {
                self.status = Status::Completed;
                Ok(())
            }
            Err(e) => {
                self.status = Status::Failed;
                Err(e)
            }
        }
    }

    /// Send the stop signal.
    ///
    /// This method does *not* wait for all batches to be processed.
    pub fn stop(&mut self) {
        self.stop_signal_rx.send_if_modified(|stop| {
            *stop = true;
            true
        });
    }

    pub async fn next(&mut self) -> error_stack::Result<Option<RecordBatch>, Error> {
        self.is_done()?;
        Ok(self.output.next().await)
    }

    pub fn next_blocking(&mut self) -> error_stack::Result<Option<RecordBatch>, Error> {
        self.is_done()?;
        Ok(self.handle.block_on(self.output.next()))
    }

    pub async fn collect_all(self) -> error_stack::Result<Vec<RecordBatch>, Error> {
        let progress = match self.status {
            Status::Running(progress) => progress,
            Status::Failed => error_stack::bail!(Error::ExecutionFailed),
            Status::Completed => {
                // If the progress channel has completed without error, we know that the output channel
                // hasn't filled up, so we can go ahead and collect the output
                return Ok(self.output.collect().await);
            }
        };

        let mut progress = progress.fuse();
        let mut output = self.output.fuse();

        let mut outputs = Vec::new();
        loop {
            futures::select! {
                progress = progress.select_next_some() => {
                    // Propagate errors
                    let _ = progress?;
                }
                batch = output.select_next_some() => {
                    outputs.push(batch);
                }
                complete => {
                    break
                }
            }
        }
        Ok(outputs)
    }

    pub fn collect_all_blocking(self) -> error_stack::Result<Vec<RecordBatch>, Error> {
        // In order to check the running status, we have to enter the runtime regardless,
        // so there's no reason to check the status prior to entering the runtime
        // here.
        let handle = self.handle.clone();
        handle.block_on(self.collect_all())
    }
}

#[cfg(test)]
mod tests {
    use crate::Execution;

    #[test]
    fn test_send() {
        fn assert_send<T: Send>() {}
        assert_send::<Execution>();
    }
}
