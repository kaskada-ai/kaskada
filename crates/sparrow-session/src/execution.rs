use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use futures::future::BoxFuture;
use futures::StreamExt;
use parking_lot::Mutex;

use crate::Error;

pub struct Execution {
    /// Tokio handle managing this execution.
    handle: tokio::runtime::Handle,
    /// Stop signal. Send `true` to stop execution.
    stop_signal_tx: tokio::sync::watch::Sender<bool>,
    /// Handles the mutex-locked execution state.
    state: Mutex<ExecutionState>,
    pub schema: SchemaRef,
}

struct ExecutionState {
    // Future that resolves to the first error, if one occurred.
    status: Status,
    /// Channel to receive output on.
    output: tokio_stream::wrappers::ReceiverStream<RecordBatch>,
}

enum Status {
    Running(BoxFuture<'static, error_stack::Result<(), Error>>),
    Failed,
    Completed,
}

impl Execution {
    pub(super) fn new(
        handle: tokio::runtime::Handle,
        output_rx: tokio::sync::mpsc::Receiver<RecordBatch>,
        future: BoxFuture<'static, error_stack::Result<(), Error>>,
        stop_signal_tx: tokio::sync::watch::Sender<bool>,
        schema: SchemaRef,
    ) -> Self {
        let output = tokio_stream::wrappers::ReceiverStream::new(output_rx);

        // Constructs a futures that resolves to the first error, if one occurred.
        let status = Status::Running(future);
        Self {
            handle,
            state: Mutex::new(ExecutionState { output, status }),
            stop_signal_tx,
            schema,
        }
    }

    /// Check the status future.
    ///
    /// If it has previously completed (successfully or with error) returns
    /// accordingly. Otherwise, check to see if the future is ready, and update
    /// status (and return) accordingly.
    fn is_done(&self) -> error_stack::Result<(), Error> {
        let mut state = self.state.lock();

        let result = match &mut state.status {
            Status::Running(progress) => {
                // Based on the implementation of `FutureExt::now_or_never`:
                let noop_waker = futures::task::noop_waker();
                let mut cx = std::task::Context::from_waker(&noop_waker);

                match progress.as_mut().poll(&mut cx) {
                    std::task::Poll::Ready(x) => x,
                    _ => return Ok(()),
                }
            }
            Status::Failed => error_stack::bail!(Error::ExecutionFailed),
            Status::Completed => return Ok(()),
        };

        match result {
            Ok(_) => {
                state.status = Status::Completed;
                Ok(())
            }
            Err(e) => {
                state.status = Status::Failed;
                Err(e)
            }
        }
    }

    /// Send the stop signal.
    ///
    /// This method does *not* wait for all batches to be processed.
    pub fn stop(&self) {
        println!("rust execution: sending stop signal");
        self.stop_signal_tx.send_if_modified(|stop| {
            *stop = true;
            true
        });
    }

    pub async fn next(&self) -> error_stack::Result<Option<RecordBatch>, Error> {
        println!("Rust execution: called next");
        self.is_done()?;

        println!("Rust execution: awaiting next");
        let mut state = self.state.lock();
        Ok(state.output.next().await)
    }

    pub fn next_blocking(&self) -> error_stack::Result<Option<RecordBatch>, Error> {
        self.is_done()?;
        Ok(self.handle.block_on(self.state.lock().output.next()))
    }

    pub async fn collect_all(self) -> error_stack::Result<Vec<RecordBatch>, Error> {
        todo!()
        // let mut state = self.state.lock();
        // let progress = match &mut state.status {
        //     Status::Running(progress) => progress,
        //     Status::Failed => error_stack::bail!(Error::ExecutionFailed),
        //     Status::Completed => {
        //         // If the progress channel has completed without error, we know that the output channel
        //         // hasn't filled up, so we can go ahead and collect the output
        //         let output = &mut state.output;
        //         return Ok(output.collect().await);
        //     }
        // };

        // let output = &mut state.output;
        // let output = output.collect::<Vec<_>>();

        // let (first_error, output) = futures::join!(progress, output);
        // if let Err(e) = first_error {
        //     Err(e)
        // } else {
        //     Ok(output)
        // }
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
