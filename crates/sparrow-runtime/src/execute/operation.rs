//! Operation based execution of Sparrow compute plans.
//!
//! The high-level idea is that each operation produces a stream of
//! `Batch` values by executing on a separate task. Each operation
//! receives input from zero or more channels corresponding to batches
//! produced by earlier operations.
//!
//! To produce batches, each operation first creates the rows and
//! one or more initial columns based on the operation. For example,
//! a scan table will read some rows from the table, and produce
//! columns corresponding to the data read from the table. Then
//! the operation will run its expressions to compute additional
//! columns associated with the same rows. Finally, it will assemble
//! an output batch corresponding to a subset of the columns
//! (original and computed).

mod expression_executor;
mod final_tick;
mod input_batch;
mod lookup_request;
mod lookup_response;
mod merge;
mod scan;
mod select;
mod shift_to;
mod shift_until;
mod single_consumer_helper;
mod sorted_key_hash_map;
mod spread;
mod spread_zip;
#[cfg(test)]
mod testing;
mod tick;
mod tick_producer;
mod with_key;

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::NaiveDateTime;
use enum_map::EnumMap;
use error_stack::{IntoReport, IntoReportCompat, Report, Result, ResultExt};
use futures::Future;
use hashbrown::HashMap;
use prost_wkt_types::Timestamp;
use sparrow_api::kaskada::v1alpha::operation_plan::tick_operation::TickBehavior;
use sparrow_api::kaskada::v1alpha::{operation_plan, ComputePlan, LateBoundValue, OperationPlan};
use sparrow_arrow::scalar_value::ScalarValue;
use sparrow_compiler::DataContext;
use sparrow_instructions::{ComputeStore, Udf};
use tokio::task::JoinHandle;
use tracing::Instrument;
use uuid::Uuid;

use self::final_tick::FinalTickOperation;
use self::input_batch::InputBatch;
use self::lookup_request::LookupRequestOperation;
use self::lookup_response::LookupResponseOperation;
use self::merge::MergeOperation;
use self::scan::ScanOperation;
use self::select::SelectOperation;
use self::tick::TickOperation;
use self::with_key::WithKeyOperation;
use crate::execute::operation::expression_executor::{ExpressionExecutor, InputColumn};
use crate::execute::operation::shift_until::ShiftUntilOperation;
use crate::execute::Error;
use crate::key_hash_inverse::ThreadSafeKeyHashInverse;
use crate::stores::ObjectStoreRegistry;
use crate::Batch;

/// Information used while creating operations.
///
/// This is somewhat hacky, but it works until we can stabilize the
/// new execution plans and see if there are better ways to clean this
/// up.
///
/// If this is mostly needed for creating table scan streams, we may
/// want to just call this something like `InputContext` and move
/// the method to create a table reader on to it.
pub(crate) struct OperationContext {
    pub plan: ComputePlan,
    pub object_stores: Arc<ObjectStoreRegistry>,
    pub data_context: DataContext,
    pub compute_store: Option<Arc<ComputeStore>>,
    /// The key hash inverse to produce the output results, if one exists.
    pub key_hash_inverse: Arc<ThreadSafeKeyHashInverse>,
    /// The max input event time in the restored snapshot, if one exists.
    pub max_event_in_snapshot: Option<NaiveDateTime>,
    /// Channel for sending progress updates.
    pub progress_updates_tx:
        tokio::sync::mpsc::Sender<crate::execute::progress_reporter::ProgressUpdate>,
    /// The time to produce a final tick at.
    ///
    /// If set, the user supplied a specific time to produce values at.
    pub output_at_time: Option<NaiveDateTime>,
    /// The allowed lateness for input data.
    ///
    /// If not set, defaults to the [BOUNDED_LATENESS_NS] const.
    pub bounded_lateness_ns: Option<i64>,
    /// If true, the execution is a materialization.
    ///
    /// It will subscribe to the input stream and continue running as new data
    /// arrives. It won't send final ticks.
    ///
    /// Derived from the ExecutionOptions,
    pub materialize: bool,
    /// Mapping of uuid to user-defined functions.
    pub udfs: HashMap<Uuid, Arc<dyn Udf>>,
}

impl OperationContext {
    pub fn primary_grouping(&self) -> &str {
        &self.plan.primary_grouping
    }
}

/// Trait representing an input stream of batches for an operation.
///
/// Each operation may define how they send the next input batch.
/// This also allows operations to store internal state.
#[async_trait]
trait Operation: Send + Debug {
    fn restore_from(
        &mut self,
        operation_index: u8,
        compute_store: &ComputeStore,
    ) -> anyhow::Result<()>;

    fn store_to(&self, operation_index: u8, compute_store: &ComputeStore) -> anyhow::Result<()>;

    /// Run the operation to completion, returning an error if it occurs.
    ///
    /// Batches of input to operate on should be sent to `send`.
    async fn execute(
        &mut self,
        sender: tokio::sync::mpsc::Sender<InputBatch>,
    ) -> error_stack::Result<(), Error>;
}

type BoxedOperation = Box<dyn Operation + Send>;

/// Builder for an operation executor.
///
/// This is initially created with an `OperationPlan`. Channels are added
/// for consumers of the results of that operation, and then the operation
/// is executed.
pub(super) struct OperationExecutor {
    operation: OperationPlan,
    consumers: Vec<tokio::sync::mpsc::Sender<Batch>>,
}

impl OperationExecutor {
    pub(super) fn new(operation: OperationPlan) -> Self {
        Self {
            operation,
            consumers: Vec::new(),
        }
    }

    /// Add a subscription to this operation.
    pub fn add_consumer(&mut self, channel: tokio::sync::mpsc::Sender<Batch>) {
        self.consumers.push(channel);
    }

    /// Start execution of the operation.
    ///
    /// Inputs are read based on the operation. Expressions are evaluated
    /// against each input. Outputs are produced from the computed columns.
    ///
    /// This function will not return until the operation is complete -- either
    /// because all inputs have been processed or because it has been
    /// interrupted due to an error or reaching a preview-rows limit, etc.
    pub async fn execute(
        self,
        operation_index: usize,
        context: &mut OperationContext,
        input_channels: Vec<tokio::sync::mpsc::Receiver<Batch>>,
        max_event_time_tx: tokio::sync::mpsc::UnboundedSender<Timestamp>,
        late_bindings: &EnumMap<LateBoundValue, Option<ScalarValue>>,
        stop_signal_rx: Option<tokio::sync::watch::Receiver<bool>>,
    ) -> Result<impl Future<Output = Result<(), Error>>, Error> {
        let Self {
            operation,
            consumers,
        } = self;

        let operator = operation
            .operator
            .ok_or_else(|| Error::internal_msg("missing operator"))?;

        let operation_label = operator.label();

        let mut expression_executor = ExpressionExecutor::try_new(
            operation_label,
            operation.expressions,
            late_bindings,
            &context.udfs,
        )
        .into_report()
        .change_context(Error::internal_msg("unable to create executor"))?;

        debug_assert_eq!(operator.input_len(), input_channels.len());

        // `Scan` is unusual in that it creates its own input channels to read from,
        // but depending on the snapshot time, it may need to skip files. Ideally,
        // there's a more intuitive pattern to creating an operation and restoring
        // from a snapshot, but for now we just manually pass in the max event time.
        let compute_store = context.compute_store.clone();
        let key_hash_inverse = context.key_hash_inverse.clone();
        let max_event_in_snapshot: Option<NaiveDateTime> =
            if let Some(compute_store) = &compute_store {
                compute_store
                    .get_max_event_time()
                    .into_report()
                    .change_context(Error::internal_msg("missing max_event_time"))?
                    .and_then(|ts| NaiveDateTime::from_timestamp_opt(ts.seconds, ts.nanos as u32))
            } else {
                None
            };
        context.max_event_in_snapshot = max_event_in_snapshot;

        let mut operation = create_operation(
            context,
            operator,
            input_channels,
            expression_executor.input_columns(),
            stop_signal_rx,
        )
        .await?;

        let mut last_upper_bound = None;

        let (send, mut recv) = tokio::sync::mpsc::channel(1);

        let operation_index = operation_index as u8;

        Ok(async move {
            if let Some(store) = &compute_store {
                let _span = tracing::debug_span!("Restoring state").entered();
                expression_executor
                    .restore(operation_index, store.as_ref())
                    .into_report()
                    .change_context(Error::internal())?;
                operation
                    .restore_from(operation_index, store.as_ref())
                    .into_report()
                    .change_context(Error::internal())?;
            } else {
                tracing::debug!("No state to restore");
            }

            let operation_handle: JoinHandle<error_stack::Result<_, Error>> = tokio::spawn(
                async move {
                    tracing::debug!("Full operation is {:?}", operation);
                    operation.execute(send).await?;
                    Ok(operation)
                }
                .in_current_span(),
            );

            // Ideally, we could run multiple output batches concurrently.
            // But, expressions such as aggregations require sequential processing.
            // We could attempt to determine whether we needed to execute sequentially...
            // but for now it is easier to just have the single path.
            'operation: while let Some(input) = recv.recv().await {
                #[cfg(debug_assertions)]
                input
                    .validate_bounds()
                    .into_report()
                    .change_context(Error::InvalidBounds)
                    .attach_printable(operation_label)?;

                if let Some(last_upper_bound) = last_upper_bound {
                    if input.lower_bound < last_upper_bound {
                        let report = Report::new(Error::InvalidBounds).attach_printable(format!(
                            "Last upper bound {:?}, input lower bound {:?}. In {:?}",
                            last_upper_bound, input.lower_bound, operation_label
                        ));
                        return Err(report);
                    }
                }
                last_upper_bound = Some(input.upper_bound);

                let output = expression_executor
                    .execute(input)
                    .into_report()
                    .change_context(Error::internal())?;

                // For each batch produced by the operation, write it to each channel.
                // We currently do this synchronously in the order channels subscribed.
                // We could attempt to use a select to send this to channels in the
                // order they are able to receive it. That would add complexity / overhead,
                // and is not expected to improve performance.
                //
                // Specifically, if one consumer is "faster" than another, then it is
                // likely to be consistently faster. Thus, it would likely finish the
                // next batch before other consumers even if we don't arrange to give
                // it a "sneak peak". Additionally, this would only let it peek at the
                // next item. If "peeking" is necessary, it would be better to tune
                // the channels to queue up multiple elements and/or have a more general
                // way of "forwarding" elements to multiple downstream channels.
                for consumer in consumers.iter() {
                    if (consumer.send(output.clone()).await).is_err() {
                        tracing::debug!("Downstream receiver closed; breaking");
                        operation_handle.abort();
                        break 'operation;
                    }
                }
            }

            // Send the max input time for each operation
            if let Some(upper_bound) = last_upper_bound {
                // Note that because incremental is only supported with final results,
                // we are guaranteed to get a non-zero final time due to the final
                // tick batch. Once all results are supported, we'll need to rethink
                // what happens if incremental is requested but no new data exists
                // so no batches are processed.
                tracing::debug!(
                    "Operation {:?} sending last upper bound {:?}",
                    operation_index,
                    upper_bound
                );
                let max_event_date =
                    arrow::temporal_conversions::timestamp_ns_to_datetime(upper_bound.time)
                        .expect("unable to represent timestamp");
                let ts = Timestamp::from(max_event_date);
                max_event_time_tx
                    .send(ts)
                    .into_report()
                    .change_context(Error::internal_msg("error sending max_event_time message"))?;
            };

            // Join with the operation
            let operation = match operation_handle.await {
                Err(join_error) if join_error.is_cancelled() => {
                    tracing::warn!("Execution cancelled. Not saving state");
                    return Ok(());
                }
                Err(join_error) => {
                    tracing::error!("Operation Panicked: {join_error:?}");
                    let report = Report::new(Error::internal()).attach_printable(join_error);
                    return Err(report);
                }
                Ok(operation_result) => operation_result.change_context(Error::internal())?,
            };

            if let Some(store) = &compute_store {
                let _span = tracing::debug_span!("Saving state").entered();
                expression_executor
                    .store(operation_index, store.as_ref())
                    .into_report()
                    .change_context(Error::internal())?;
                operation
                    .store_to(operation_index, store.as_ref())
                    .into_report()
                    .change_context(Error::internal())?;
            } else {
                tracing::debug!("No state store; nothing to save");
            }

            let key_hash_handle: JoinHandle<anyhow::Result<_>> = tokio::spawn(
                async move {
                    if let Some(store) = &compute_store {
                        key_hash_inverse.clone().store_to(store).await?;
                    }
                    Ok(())
                }
                .in_current_span(),
            );

            key_hash_handle
                .await
                .into_report()
                .change_context(Error::internal())?
                .into_report()
                .change_context(Error::internal())?;
            Ok(())
        })
    }
}

/// Executes an operation.
///
/// Returns a stream of the resulting record batches.
// TODO: This pattern of passing both the `inputs` and the resulting
// `input_channels` is a bit repetitive. A better strategy may be to
// allow the operation methods to *create* the channel from the `input`
// possibly by interacting with the `OperationContext`. The downside of
// such an approach is that it would "spread out" how channels are
// created. Another option would be to have a struct that combines the
// input and the channel.
async fn create_operation(
    context: &mut OperationContext,
    operator: operation_plan::Operator,
    incoming_channels: Vec<tokio::sync::mpsc::Receiver<Batch>>,
    input_columns: &[InputColumn],
    stop_signal_rx: Option<tokio::sync::watch::Receiver<bool>>,
) -> Result<BoxedOperation, Error> {
    match operator {
        operation_plan::Operator::Scan(scan_operation) => {
            ScanOperation::create(
                context,
                scan_operation,
                incoming_channels,
                input_columns,
                stop_signal_rx,
            )
            .await
        }
        operation_plan::Operator::Merge(merge_operation) => {
            MergeOperation::create(merge_operation, incoming_channels, input_columns)
        }
        operation_plan::Operator::Select(select_operation) => {
            SelectOperation::create(select_operation, incoming_channels, input_columns)
        }
        operation_plan::Operator::WithKey(with_key_operation) => WithKeyOperation::create(
            context,
            with_key_operation,
            incoming_channels,
            input_columns,
        ),
        operation_plan::Operator::Tick(tick_operation) => {
            if matches!(tick_operation.behavior(), TickBehavior::Finished) {
                FinalTickOperation::create(context, incoming_channels, input_columns)
            } else {
                TickOperation::create(tick_operation, incoming_channels, input_columns)
            }
        }
        operation_plan::Operator::LookupRequest(lookup_request) => {
            LookupRequestOperation::create(lookup_request, incoming_channels, input_columns)
        }
        operation_plan::Operator::LookupResponse(lookup_response) => {
            LookupResponseOperation::create(lookup_response, incoming_channels, input_columns)
        }
        operation_plan::Operator::ShiftTo(shift_to) => {
            shift_to::create(shift_to, incoming_channels, input_columns)
        }
        operation_plan::Operator::ShiftUntil(shift_until) => {
            ShiftUntilOperation::create(shift_until, incoming_channels, input_columns)
        }
    }
    .change_context(Error::internal_msg("unable to create operation"))
}
