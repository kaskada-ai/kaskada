use error_stack::ResultExt;
use futures::stream::{BoxStream, FuturesUnordered};
use futures::{StreamExt, TryStreamExt};
use sparrow_batch::Batch;
use sparrow_interfaces::source::SourceError;
use sparrow_scheduler::{Injector, InputHandles};
use tracing::Instrument;
use uuid::Uuid;

use crate::Error;

/// Manages the async streams that must be read for input.
#[derive(Default)]
pub(super) struct SourceTasks {
    tasks: Vec<SourceTask>,
}

struct SourceTask {
    source_uuid: Uuid,
    stream: BoxStream<'static, error_stack::Result<Batch, SourceError>>,
    consumers: InputHandles,
}

impl SourceTasks {
    pub fn add_read(
        &mut self,
        source_uuid: Uuid,
        stream: BoxStream<'static, error_stack::Result<Batch, SourceError>>,
        consumers: InputHandles,
    ) {
        self.tasks.push(SourceTask {
            source_uuid,
            stream,
            consumers,
        })
    }

    /// Execute all of the sources.
    pub async fn run_sources(self, injector: Injector) -> error_stack::Result<(), Error> {
        let mut tasks: FuturesUnordered<tokio::task::JoinHandle<error_stack::Result<(), Error>>> =
            self.tasks
                .into_iter()
                .map(|task| {
                    let SourceTask {
                        mut stream,
                        consumers,
                        source_uuid,
                    } = task;
                    let span = tracing::info_span!("Source Task", source=?source_uuid);
                    let mut injector = injector.clone();
                    tokio::spawn(
                        async move {
                            tracing::info!("Reading batches for source");
                            while let Some(batch) =
                                stream.try_next().await.change_context(Error::SourceError)?
                            {
                                consumers
                                    .add_input(0.into(), batch, &mut injector)
                                    .change_context(Error::SourceError)?;
                            }

                            tracing::info!("Completed stream for source. Closing.");
                            consumers
                                .close_input(0.into(), &mut injector)
                                .change_context(Error::SourceError)?;

                            tracing::info!("Closed source");
                            Ok(())
                        }
                        .instrument(span),
                    )
                })
                .collect();

        while let Some(task_result) = tasks.next().await {
            match task_result {
                Ok(Ok(())) => tracing::trace!("Source task completed. {} remaining.", tasks.len()),
                Ok(Err(e)) => return Err(e),
                Err(e) => {
                    tracing::trace!("Source task panicked: {e}");
                    error_stack::bail!(Error::SourcePanic);
                }
            }
            tracing::info!("Source completed. {} remaining.", tasks.len());
        }
        tracing::info!("All sources completed.");

        Ok(())
    }
}
