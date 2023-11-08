use std::borrow::BorrowMut;
use std::ops::DerefMut;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use futures::future::BoxFuture;
use futures::StreamExt;
use parking_lot::Mutex;

use crate::Error;

pub struct Execution {
    /// Tokio handle managing this execution.
    handle: tokio::runtime::Handle,
    status: tokio::sync::Mutex<Status>,
    /// Stop signal. Send `true` to stop execution.
    stop_signal_tx: tokio::sync::watch::Sender<bool>,
    pub schema: SchemaRef,
}

enum Status {
    /// This execution is still running and/or there is still unconsumed output.
    Running {
        /// Future that resolves to the first error, if one occurred.
        error: BoxFuture<'static, error_stack::Result<(), Error>>,
        /// Channel to receive output on.
        output: tokio_stream::wrappers::ReceiverStream<RecordBatch>,
    },
    /// This execution has failed.
    Failed,
    /// The output of this execution has been completely consumed and
    /// no failures happened.
    Completed,
}

impl Execution {
    pub(super) fn new(
        handle: tokio::runtime::Handle,
        output_rx: tokio::sync::mpsc::Receiver<RecordBatch>,
        error: BoxFuture<'static, error_stack::Result<(), Error>>,
        stop_signal_tx: tokio::sync::watch::Sender<bool>,
        schema: SchemaRef,
    ) -> Self {
        let output = tokio_stream::wrappers::ReceiverStream::new(output_rx);

        // Constructs a futures that resolves to the first error, if one occurred.
        let status = Status::Running { error, output };
        Self {
            handle,
            status: tokio::sync::Mutex::new(status),
            stop_signal_tx,
            schema,
        }
    }

    /// Returns true if the output of this execution has been completely consumed
    /// and no failures happened.
    ///
    /// If an error has occurred, sets the Status to Failed and returns the error.
    fn is_done(&self) -> error_stack::Result<bool, Error> {
        let mut status = self.status.blocking_lock();
        match *status {
            Status::Running { .. } => {
                // handled below
            }
            Status::Failed => error_stack::bail!(Error::ExecutionFailed),
            Status::Completed => return Ok(true),
        }

        // Replace the status with completed temporarily so we can access the output stream.
        // This is safe to do, as we have locked the status and no other thread can access it.
        let Status::Running {
            mut error,
            mut output,
        } = std::mem::replace(status.deref_mut(), Status::Completed)
        else {
            unreachable!("Status changed unexpectedly")
        };

        // Based on the implementation of `FutureExt::now_or_never`:
        let noop_waker = futures::task::noop_waker();
        let mut cx = std::task::Context::from_waker(&noop_waker);

        match error.as_mut().poll(&mut cx) {
            std::task::Poll::Ready(Err(e)) => {
                *status = Status::Failed;
                Err(e)
            }
            std::task::Poll::Ready(Ok(_)) => {
                // The query has completed without error, so check if all outputs have been consumed.
                if let std::task::Poll::Ready(None) = output.poll_next_unpin(&mut cx) {
                    // The stream has terminated; set the status to complete.
                    *status = Status::Completed;
                    Ok(true)
                } else {
                    // The stream has not terminated; set the status back to running.
                    let _ =
                        std::mem::replace(status.deref_mut(), Status::Running { error, output });
                    Ok(false)
                }
            }
            _ => {
                // Error channel has not resolved, so set the status back to running.
                let _ = std::mem::replace(status.deref_mut(), Status::Running { error, output });
                Ok(false)
            }
        }
    }

    /// Send the stop signal.
    ///
    /// This method does *not* wait for all batches to be processed.
    pub fn stop(&mut self) {
        self.stop_signal_tx.send_if_modified(|stop| {
            *stop = true;
            true
        });
    }

    pub async fn next(&self) -> error_stack::Result<Option<RecordBatch>, Error> {
        self.is_done()?;

        let mut status = self.status.lock().await;
        match *status {
            Status::Failed => error_stack::bail!(Error::ExecutionFailed),
            Status::Completed => {
                // Should this just return None?
                error_stack::bail!(Error::AlreadyConsumed);
            }
            Status::Running { .. } => {
                // handled below
            }
        }

        // Replace the status with completed temporarily so we can access the output stream.
        // This is safe to do, as we have locked the status and no other thread can access it.
        let Status::Running { error, mut output } =
            std::mem::replace(status.deref_mut(), Status::Completed)
        else {
            unreachable!("Status changed unexpectedly")
        };

        let next = output.next().await;

        // Move the original status back
        let _ = std::mem::replace(status.deref_mut(), Status::Running { error, output });

        Ok(next)
    }

    pub fn next_blocking(&self) -> error_stack::Result<Option<RecordBatch>, Error> {
        self.is_done()?;
        let mut status = self.status.blocking_lock();
        match *status {
            Status::Failed => error_stack::bail!(Error::ExecutionFailed),
            Status::Completed => {
                // Should this just return None?
                error_stack::bail!(Error::AlreadyConsumed);
            }
            Status::Running { .. } => {
                // handled below
            }
        }

        // Replace the status with completed temporarily so we can access the output stream.
        // This is safe to do, as we have locked the status and no other thread can access it.
        let Status::Running { error, mut output } =
            std::mem::replace(status.deref_mut(), Status::Completed)
        else {
            unreachable!("Status changed unexpectedly")
        };

        let next = self.handle.block_on(output.next());

        // Move the original status back
        let _ = std::mem::replace(status.deref_mut(), Status::Running { error, output });

        Ok(next)
    }

    pub async fn collect_all(&self) -> error_stack::Result<Vec<RecordBatch>, Error> {
        let mut status = self.status.blocking_lock();

        match *status {
            Status::Failed => error_stack::bail!(Error::ExecutionFailed),
            Status::Completed => {
                error_stack::bail!(Error::AlreadyConsumed);
            }
            Status::Running { .. } => {
                // handled below
            }
        }

        // Replace the status with completed (assuming we won't have an error) for future calls.
        // This is safe to do, as we have locked the status and no other thread can access it.
        let Status::Running { error, output } =
            std::mem::replace(status.deref_mut(), Status::Completed)
        else {
            unreachable!("Status changed unexpectedly")
        };

        let output = output.collect::<Vec<_>>();

        let (first_error, output) = futures::join!(error, output);
        if let Err(e) = first_error {
            *status = Status::Failed;
            Err(e)
        } else {
            Ok(output)
        }
    }

    pub fn collect_all_blocking(&self) -> error_stack::Result<Vec<RecordBatch>, Error> {
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
