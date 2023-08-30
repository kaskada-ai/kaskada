use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use sparrow_api::kaskada::v1alpha::ExecuteResponse;

use crate::Error;

pub struct Execution {
    /// Tokio runtme managing this execution.
    rt: tokio::runtime::Runtime,
    /// Channel to receive output on.
    output: tokio_stream::wrappers::ReceiverStream<RecordBatch>,
    /// Future which resolves to the first error or  None.
    status: Status,
    /// Stop signal. Send `true` to stop execution.
    stop_signal_rx: tokio::sync::watch::Sender<bool>,
    pub schema: SchemaRef,
}

enum Status {
    Running(BoxFuture<'static, error_stack::Result<(), Error>>),
    Failed,
    Completed,
}

impl Execution {
    pub(super) fn new(
        rt: tokio::runtime::Runtime,
        output_rx: tokio::sync::mpsc::Receiver<RecordBatch>,
        progress: BoxStream<'static, error_stack::Result<ExecuteResponse, Error>>,
        stop_signal_rx: tokio::sync::watch::Sender<bool>,
        schema: SchemaRef,
    ) -> Self {
        let output = tokio_stream::wrappers::ReceiverStream::new(output_rx);
        let status = Status::Running(Box::pin(async move {
            let mut progress = progress;
            while (progress.try_next().await?).is_some() {}
            Ok(())
        }));

        Self {
            rt,
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
            Status::Running(future) => {
                // Based on the implementation of `FutureExt::now_or_never`:
                let noop_waker = futures::task::noop_waker();
                let mut cx = std::task::Context::from_waker(&noop_waker);

                match future.as_mut().poll(&mut cx) {
                    std::task::Poll::Ready(x) => x,
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
        Ok(self.rt.block_on(self.output.next()))
    }

    pub async fn collect_all(self) -> error_stack::Result<Vec<RecordBatch>, Error> {
        // TODO: For large outputs, we likely need to drain the output while waiting for the future.
        match self.status {
            Status::Running(future) => future.await?,
            Status::Failed => error_stack::bail!(Error::ExecutionFailed),
            _ => {}
        };

        Ok(self.output.collect().await)
    }

    pub fn collect_all_blocking(self) -> error_stack::Result<Vec<RecordBatch>, Error> {
        // TODO: For large outputs, we likely need to drain the output while waiting for the future.
        match self.status {
            Status::Running(future) => self.rt.block_on(future)?,
            Status::Failed => error_stack::bail!(Error::ExecutionFailed),
            _ => {}
        };

        Ok(self.rt.block_on(self.output.collect()))
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
