use std::fs::File;
use std::sync::Arc;

use enum_map::EnumMap;
use error_stack::{IntoReport, IntoReportCompat, ResultExt};
use futures::stream::{FuturesUnordered, PollNext};
use futures::{FutureExt, Stream, TryFutureExt};
use prost_wkt_types::Timestamp;
use sparrow_api::kaskada::v1alpha::ComputeSnapshot;
use sparrow_api::kaskada::v1alpha::{ExecuteResponse, LateBoundValue, PlanHash};
use sparrow_arrow::scalar_value::ScalarValue;
use sparrow_qfr::io::writer::FlightRecordWriter;
use sparrow_qfr::kaskada::sparrow::v1alpha::FlightRecordHeader;
use sparrow_qfr::FlightRecorderFactory;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_stream::StreamExt;
use tracing::{error, info, info_span, Instrument};

use crate::execute::compute_store_guard::ComputeStoreGuard;
use crate::execute::operation::{OperationContext, OperationExecutor};
use crate::execute::output::Destination;
use crate::execute::progress_reporter::{progress_stream, ProgressUpdate};
use crate::execute::spawner::ComputeTaskSpawner;
use crate::execute::Error;
use crate::execute::Error::Internal;
use crate::stores::ObjectStoreRegistry;
use crate::util::JoinTask;
use crate::{Batch, RuntimeOptions};

pub(crate) struct ComputeExecutor {
    object_stores: Arc<ObjectStoreRegistry>,
    plan_hash: PlanHash,
    futures: FuturesUnordered<JoinTask<()>>,
    progress_updates_rx: tokio::sync::mpsc::Receiver<ProgressUpdate>,
    /// Receiver for the max event timestamp seen by Scan Operations.
    max_event_time_rx: tokio::sync::mpsc::UnboundedReceiver<Timestamp>,
}

/// The final results returned after the compute executor finishes.
#[derive(Default)]
pub struct ComputeResult {
    /// The timestamp of the maximum input event processed by the query.
    pub max_input_timestamp: Timestamp,

    /// The hash of the compute plan that was executed.
    pub plan_hash: PlanHash,
}

impl ComputeExecutor {
    /// Spawns the compute tasks using the new operation based executor.
    #[allow(clippy::too_many_arguments)]
    pub async fn try_spawn(
        mut context: OperationContext,
        plan_hash: PlanHash,
        late_bindings: &EnumMap<LateBoundValue, Option<ScalarValue>>,
        runtime_options: &RuntimeOptions,
        progress_updates_rx: tokio::sync::mpsc::Receiver<ProgressUpdate>,
        destination: Destination,
        stop_signal_rx: Option<tokio::sync::watch::Receiver<bool>>,
    ) -> error_stack::Result<Self, Error> {
        let mut spawner = ComputeTaskSpawner::new();

        // Create the list of consumers for each operation.
        //
        // `consumers[operation_index]` contains the channels that consume (receive)
        // the output of `operation_index`. So, the general logic below for creating
        // operations is:
        // 1. Process operations in reverse order (so that all consumers are
        //    registered before creating the operation).
        // 2. Add each of the consumers for the operation being created
        //    (`consumers[index]`) as consumers of the operation being created.
        // 3. For each input of an operation, create a channel and add the `sender`
        //    to the consumers of the operation that produces the input, and use
        //    the `receiver` to receive input from that producer.
        let mut consumers: Vec<Vec<tokio::sync::mpsc::Sender<Batch>>> =
            vec![vec![]; context.plan.operations.len()];

        // Add a consumer for the output channel.
        let (output_tx, output_rx) = tokio::sync::mpsc::channel(13);
        consumers[context.plan.operations.len() - 1].push(output_tx.clone());

        spawner.spawn(
            "output".to_owned(),
            info_span!("Output Writer", ?destination),
            crate::execute::output::write(
                &context,
                runtime_options.limits.clone(),
                futures::StreamExt::boxed(tokio_stream::wrappers::ReceiverStream::new(output_rx)),
                context.progress_updates_tx.clone(),
                destination,
                runtime_options.max_batch_size,
            )
            .change_context(Internal("error writing output"))?
            .map_err(|e| e.change_context(Internal("error writing output"))),
        );

        // Channel for the max event time seen by a Scan Operation
        //
        // Must be unbounded because we wait until the end of execution to read the
        // messages out. The number of messages should be the number of
        // operations, so we aren't concerned about memory pressure.
        let (max_event_time_tx, max_event_time_rx) = tokio::sync::mpsc::unbounded_channel();

        // Clone the operations for now. Most executors hold a copy.
        // TODO: There is likely a way to avoid this by passing references
        // and only copying the operation proto when needed. Or holding using
        // a lifetime that references the plan.
        let operations = context.plan.operations.clone();

        // Create all the operations.
        for (index, op) in operations.into_iter().enumerate().rev() {
            let operator = op
                .operator()
                .into_report()
                .change_context(Internal("missing operator"))?;
            let inputs: Vec<_> = operator
                .input_ops_iter()
                .flat_map(|input_index| {
                    let input_index = input_index as usize;
                    debug_assert!(input_index < index);

                    // Create a channel and add it to the list of consumers for earlier
                    // operations.
                    //
                    // We'd like to have a `single-publisher, multiple-consumer` channel,
                    // but couldn't find one. So instead, we create separate channels for
                    // each consumer and publish to each of them.
                    let (sender, receiver) = tokio::sync::mpsc::channel(7);
                    consumers[input_index].push(sender);
                    Some(receiver)
                })
                .collect();
            let operation_label = operator.label();

            let mut operation = OperationExecutor::new(op);
            for consumer in consumers[index].drain(0..) {
                operation.add_consumer(consumer);
            }

            spawner.spawn(
                format!("{operation_label}[op={index}]"),
                info_span!("Operation", ?index, operation_label),
                operation
                    .execute(
                        index,
                        &mut context,
                        inputs,
                        max_event_time_tx.clone(),
                        late_bindings,
                        stop_signal_rx.clone(),
                    )
                    .await?,
            );
        }

        Ok(Self {
            object_stores: context.object_stores,
            plan_hash,
            futures: spawner.finish(),
            progress_updates_rx,
            max_event_time_rx,
        })
    }

    /// Execute the computation with a progress stream.
    ///
    /// The `finish` function is called after the final compute result has been
    /// created, but before progress information stops being streamed.
    pub(super) fn execute_with_progress(
        self,
        store: Option<ComputeStoreGuard>,
    ) -> impl Stream<Item = error_stack::Result<ExecuteResponse, Error>> {
        let Self {
            object_stores,
            plan_hash,
            futures,
            progress_updates_rx,
            max_event_time_rx,
        } = self;

        // Final async block that joins on the operation tasks and creates
        // the final execution response as a single element in a stream.
        //
        // Note: This was made to be a stream as it needs to be polled along
        // with the progress reporter, otherwise awaiting on this future
        // would block the progress reporter from pulling progress updates.
        let final_result_fut = async move {
            // Waits for all operations to complete
            let final_update: Result<ProgressUpdate, ProgressUpdate> = {
                let compute_result = join(futures, max_event_time_rx, plan_hash)
                    .await
                    .change_context(Error::Internal("failed to join compute threads"))
                    .map_err(|e| ProgressUpdate::ExecutionFailed { error: e });

                // Return early if join fails
                if let Err(compute_result) = compute_result {
                    return compute_result;
                };
                let compute_result = compute_result.expect("ok");

                let compute_snapshots =
                    upload_compute_snapshots(object_stores.as_ref(), store, compute_result)
                        .instrument(tracing::info_span!("Uploading checkpoint files"))
                        .await
                        .unwrap_or_else(|e| {
                            // Log, but don't fail if we couldn't upload snapshots.
                            // We can still produce valid answers, but won't perform an incremental query.
                            error!("Failed to upload compute snapshot(s):\n{:?}", e);
                            Vec::new()
                        });

                Ok(ProgressUpdate::ExecutionComplete { compute_snapshots })
            };

            final_update.unwrap_or_else(|e| e)
        }
        .boxed();

        use futures::StreamExt;
        let compute_stream = futures::stream::once(final_result_fut).boxed();
        let progress_updates_rx =
            tokio_stream::wrappers::ReceiverStream::new(progress_updates_rx).boxed();

        // Biases to the progress update stream to ensure all updates are received before completion
        let progress_updates = select_biased(progress_updates_rx, compute_stream);

        progress_stream(progress_updates)
    }
}

fn select_biased<T: 'static>(
    preferred: futures::stream::BoxStream<'static, T>,
    other: futures::stream::BoxStream<'static, T>,
) -> futures::stream::BoxStream<'static, T> {
    use futures::StreamExt;

    // Prioritize the left input stream
    fn prio_left(_: &mut ()) -> PollNext {
        PollNext::Left
    }

    futures::stream::select_with_strategy(preferred, other, prio_left).boxed()
}

async fn upload_compute_snapshots(
    object_stores: &ObjectStoreRegistry,
    store: Option<ComputeStoreGuard>,
    compute_result: ComputeResult,
) -> error_stack::Result<Vec<ComputeSnapshot>, Error> {
    let mut snapshots = Vec::new();

    if let Some(store) = store {
        snapshots.push(store.finish(object_stores, compute_result).await?);
    } else {
        tracing::info!("No snapshot config; not uploading compute store.")
    }

    Ok(snapshots)
}

async fn join(
    mut futures: FuturesUnordered<JoinTask<()>>,
    max_event_time_rx: tokio::sync::mpsc::UnboundedReceiver<Timestamp>,
    plan_hash: PlanHash,
) -> error_stack::Result<ComputeResult, Error> {
    tracing::info!("Waiting for {} compute threads", futures.len());
    while (futures::TryStreamExt::try_next(&mut futures).await?).is_some() {
        tracing::info!("Task completed");
    }

    // Collect the maximum input time seen by Scan Operations
    let max_event_time_stream = UnboundedReceiverStream::new(max_event_time_rx);
    let max_input_timestamp = max_event_time_stream
        .fold(
            Timestamp {
                seconds: 0,
                nanos: 0,
            },
            |a, b| match (a, b) {
                (a, b) if a.seconds == b.seconds => {
                    if a.nanos > b.nanos {
                        a
                    } else {
                        b
                    }
                }
                (a, b) if a.seconds > b.seconds => a,
                (_, b) => b,
            },
        )
        .map(|ts| {
            if ts.seconds == 0 && ts.nanos == 0 {
                None
            } else {
                Some(ts)
            }
        })
        .await;

    info!("Compute threads completed");
    // The timestamp *should* always be reported. But it may not be if we ran
    // no rows. Don't fail everything just because of that.
    let max_input_timestamp = max_input_timestamp.unwrap_or(Timestamp {
        seconds: i64::MAX,
        nanos: i32::MAX,
    });

    Ok(ComputeResult {
        max_input_timestamp,
        plan_hash,
    })
}

/// Create a flight recorder if the runtime options indicate.
#[allow(dead_code)]
async fn create_flight_recorder(
    spawner: &mut ComputeTaskSpawner,
    runtime_options: &RuntimeOptions,
    flight_record_header: FlightRecordHeader,
) -> anyhow::Result<FlightRecorderFactory> {
    if let Some(flight_record_path) = &runtime_options.flight_record_path {
        let (sender, mut receiver) = tokio::sync::mpsc::channel(10);
        let flight_record_path = flight_record_path.to_owned();
        spawner.spawn(
            "flight_recorder".to_owned(),
            info_span!("Flight Recorder"),
            async move {
                let flight_record_file = File::create(flight_record_path)
                    .into_report()
                    .change_context(Internal("unable to open file for flight recorder"))?;
                let mut writer =
                    FlightRecordWriter::try_new(flight_record_file, flight_record_header)
                        .into_report()
                        .change_context(Internal("unable to write flight recorder header"))?;

                while let Some(next_record) = receiver.recv().await {
                    writer
                        .write(next_record)
                        .unwrap_or_else(|e| error!("Failed to write flight records: {:?}", e));
                }

                writer
                    .flush()
                    .unwrap_or_else(|e| error!("Failed to flush flight records: {:?}", e));
                Ok(())
            },
        );
        Ok(FlightRecorderFactory::new(sender).await)
    } else {
        Ok(FlightRecorderFactory::new_disabled())
    }
}
