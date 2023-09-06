use std::sync::Arc;

use chrono::NaiveDateTime;
use enum_map::EnumMap;
use error_stack::{IntoReport, IntoReportCompat, ResultExt};
use futures::Stream;
use hashbrown::HashMap;
use prost_wkt_types::Timestamp;
use sparrow_api::kaskada::v1alpha::execute_request::Limits;
use sparrow_api::kaskada::v1alpha::{
    ComputePlan, ComputeSnapshotConfig, ComputeTable, ExecuteRequest, ExecuteResponse,
    LateBoundValue, PerEntityBehavior, PlanHash,
};
use sparrow_arrow::scalar_value::ScalarValue;
use sparrow_compiler::{hash_compute_plan_proto, DataContext};
use sparrow_instructions::Udf;
use sparrow_qfr::kaskada::sparrow::v1alpha::FlightRecordHeader;
use uuid::Uuid;

use crate::execute::compute_store_guard::ComputeStoreGuard;
use crate::execute::error::Error;
use crate::execute::operation::OperationContext;
use crate::execute::output::Destination;
use crate::key_hash_inverse::{KeyHashInverse, ThreadSafeKeyHashInverse};
use crate::stores::ObjectStoreRegistry;
use crate::RuntimeOptions;

mod checkpoints;
mod compute_executor;
mod compute_store_guard;
pub mod error;
pub(crate) mod operation;
pub mod output;
mod progress_reporter;
mod spawner;
pub use compute_executor::*;

/// The main method for executing a Fenl query.
///
/// The `request` proto contains the execution plan as well as
/// information about the configuration in which to execute.
///
/// The result is a stream of progress reports and the final
/// execute response.
pub async fn execute(
    request: ExecuteRequest,
    bounded_lateness_ns: Option<i64>,
    _flight_record_local_path: Option<std::path::PathBuf>,
    _flight_record_header: FlightRecordHeader,
) -> error_stack::Result<impl Stream<Item = error_stack::Result<ExecuteResponse, Error>>, Error> {
    let plan = request.plan.ok_or(Error::MissingField("plan"))?;

    let destination = request
        .destination
        .ok_or(Error::MissingField("destination"))?;
    let destination =
        Destination::try_from(destination).change_context(Error::InvalidDestination)?;

    let data_context = DataContext::try_from_tables(request.tables.to_vec())
        .into_report()
        .change_context(Error::internal_msg("create data context"))?;

    let options = ExecutionOptions {
        bounded_lateness_ns,
        changed_since_time: request.changed_since.unwrap_or_default(),
        final_at_time: request.final_result_time,
        compute_snapshot_config: request.compute_snapshot_config,
        limits: request.limits,
        ..ExecutionOptions::default()
    };

    execute_new(
        plan,
        destination,
        data_context,
        options,
        None,
        HashMap::new(),
    )
    .await
}

#[derive(Default, Debug)]
pub struct ExecutionOptions {
    pub changed_since_time: Timestamp,
    pub final_at_time: Option<Timestamp>,
    pub bounded_lateness_ns: Option<i64>,
    pub compute_snapshot_config: Option<ComputeSnapshotConfig>,
    pub limits: Option<Limits>,
    pub stop_signal_rx: Option<tokio::sync::watch::Receiver<bool>>,
    /// Maximum rows to emit in a single batch.
    pub max_batch_size: Option<usize>,
    /// If true, the execution is a materialization.
    ///
    /// It will subscribe to the input stream and continue running as new data
    /// arrives. It won't send final ticks.
    pub materialize: bool,
}

impl ExecutionOptions {
    pub fn late_bindings(&self) -> EnumMap<LateBoundValue, Option<ScalarValue>> {
        enum_map::enum_map! {
            LateBoundValue::ChangedSinceTime => Some(ScalarValue::timestamp(
                self.changed_since_time.seconds,
                self.changed_since_time.nanos,
                None,
            )),
            LateBoundValue::FinalAtTime => self.final_at_time.as_ref().map(|t| ScalarValue::timestamp(
                t.seconds,
                t.nanos,
                None,
            )),
            _ => None
        }
    }

    pub fn set_changed_since(&mut self, changed_since: Timestamp) {
        self.changed_since_time = changed_since;
    }

    pub fn set_changed_since_s(&mut self, seconds: i64) {
        self.changed_since_time = Timestamp { seconds, nanos: 0 };
    }

    pub fn set_final_at_time(&mut self, final_at_time: Timestamp) {
        self.final_at_time = Some(final_at_time);
    }

    pub fn set_final_at_time_s(&mut self, seconds: i64) {
        self.final_at_time = Some(Timestamp { seconds, nanos: 0 });
    }

    async fn compute_store(
        &self,
        object_stores: &ObjectStoreRegistry,
        per_entity_behavior: PerEntityBehavior,
        plan_hash: &PlanHash,
    ) -> error_stack::Result<Option<compute_store_guard::ComputeStoreGuard>, Error> {
        // If the snapshot config exists, sparrow should attempt to resume from state,
        // and store new state. Create a new storage path for the local store to
        // exist.
        if let Some(config) = self.compute_snapshot_config.clone() {
            let max_allowed_max_event_time = match per_entity_behavior {
                PerEntityBehavior::Unspecified => {
                    error_stack::bail!(Error::UnspecifiedPerEntityBehavior)
                }
                PerEntityBehavior::All => {
                    // For all results, we need a snapshot with a maximum event time
                    // no larger than the changed_since time, since we need to replay
                    // (and recompute the results for) all events after the changed
                    // since time.
                    self.changed_since_time.clone()
                }
                PerEntityBehavior::Final => {
                    // This is a bit confusing. Right now, the manager is responsible for
                    // choosing a valid snapshot to resume from. Thus, the work of choosing
                    // a valid snapshot with regard to any new input data is already done.
                    // However, the engine does a sanity check here to ensure the snapshot's
                    // max event time is before the allowed max event time the engine supports,
                    // dependent on the entity behavior of the query.
                    //
                    // For FinalResults, the snapshot can have a max event time of "any time",
                    // so we set this to Timestamp::MAX. This is because we just need to be able
                    // to produce results once after all new events have been processed, and
                    // we can already assume a valid snapshot is chosen and the correct input
                    // files are being processed.
                    Timestamp {
                        seconds: i64::MAX,
                        nanos: i32::MAX,
                    }
                }
                PerEntityBehavior::FinalAtTime => {
                    self.final_at_time.clone().expect("final at time")
                }
            };

            let guard = compute_store_guard::ComputeStoreGuard::try_new(
                config,
                object_stores,
                max_allowed_max_event_time,
                plan_hash,
            )
            .await?;

            Ok(Some(guard))
        } else {
            tracing::info!("No snapshot config; not creating compute store.");
            Ok(None)
        }
    }
}

async fn load_key_hash_inverse(
    plan: &ComputePlan,
    data_context: &mut DataContext,
    compute_store: &Option<ComputeStoreGuard>,
    object_stores: &ObjectStoreRegistry,
) -> error_stack::Result<Arc<ThreadSafeKeyHashInverse>, Error> {
    let primary_grouping_key_type = plan
        .primary_grouping_key_type
        .to_owned()
        .ok_or(Error::MissingField("primary_grouping_key_type"))?;
    let primary_grouping_key_type =
        arrow::datatypes::DataType::try_from(&primary_grouping_key_type)
            .into_report()
            .change_context(Error::internal_msg("decode primary_grouping_key_type"))?;

    let primary_group_id = data_context
        .get_or_create_group_id(&plan.primary_grouping, &primary_grouping_key_type)
        .into_report()
        .change_context(Error::internal_msg("get primary grouping ID"))?;

    let mut key_hash_inverse = KeyHashInverse::from_data_type(&primary_grouping_key_type.clone());
    if let Some(compute_store) = &compute_store {
        if let Ok(restored) = KeyHashInverse::restore_from(compute_store.store_ref()) {
            key_hash_inverse = restored
        }
    }

    key_hash_inverse
        .add_from_data_context(data_context, primary_group_id, object_stores)
        .await
        .change_context(Error::internal_msg("initialize key hash inverse"))?;
    let key_hash_inverse = Arc::new(ThreadSafeKeyHashInverse::new(key_hash_inverse));
    Ok(key_hash_inverse)
}

/// Execute a given query based on the options.
///
/// Parameters
/// ----------
/// - key_hash_inverse: If set, specifies the key hash inverses to use. If None, the
///   key hashes will be created.
/// - udfs: contains the mapping of uuid to udf implementation. This is currently used
///   so we can serialize the uuid to the ComputePlan, then look up what implementation to use
///   when creating the evaluator. This works because we are on a single machine, and don't need
///   to pass the plan around. However, we'll eventually need to look into serializing/pickling
///   the callable.
pub async fn execute_new(
    plan: ComputePlan,
    destination: Destination,
    mut data_context: DataContext,
    options: ExecutionOptions,
    key_hash_inverse: Option<Arc<ThreadSafeKeyHashInverse>>,
    udfs: HashMap<Uuid, Arc<dyn Udf>>,
) -> error_stack::Result<impl Stream<Item = error_stack::Result<ExecuteResponse, Error>>, Error> {
    let object_stores = Arc::new(ObjectStoreRegistry::default());

    let plan_hash = hash_compute_plan_proto(&plan);

    let compute_store = options
        .compute_store(
            object_stores.as_ref(),
            plan.per_entity_behavior(),
            &plan_hash,
        )
        .await?;

    let key_hash_inverse = if let Some(key_hash_inverse) = key_hash_inverse {
        key_hash_inverse
    } else {
        load_key_hash_inverse(&plan, &mut data_context, &compute_store, &object_stores)
            .await
            .change_context(Error::internal_msg("load key hash inverse"))?
    };

    // Channel for the output stats.
    let (progress_updates_tx, progress_updates_rx) =
        tokio::sync::mpsc::channel(29.max(plan.operations.len() * 2));

    let output_at_time = options
        .final_at_time
        .as_ref()
        .map(|t| {
            NaiveDateTime::from_timestamp_opt(t.seconds, t.nanos as u32)
                .ok_or_else(|| Error::internal_msg("expected valid timestamp"))
        })
        .transpose()?;

    let context = OperationContext {
        plan,
        object_stores,
        data_context,
        compute_store: compute_store.as_ref().map(|g| g.store()),
        key_hash_inverse,
        max_event_in_snapshot: None,
        progress_updates_tx,
        output_at_time,
        bounded_lateness_ns: options.bounded_lateness_ns,
        materialize: options.materialize,
        udfs,
    };

    // Start executing the query. We pass the response channel to the
    // execution layer so it can periodically report progress.
    tracing::debug!("Starting query execution");

    let late_bindings = options.late_bindings();
    let runtime_options = RuntimeOptions {
        limits: options.limits.unwrap_or_default(),
        flight_record_path: None,
        max_batch_size: options.max_batch_size,
    };

    let compute_executor = ComputeExecutor::try_spawn(
        context,
        plan_hash,
        &late_bindings,
        &runtime_options,
        progress_updates_rx,
        destination,
        options.stop_signal_rx,
    )
    .await
    .change_context(Error::internal_msg("spawn compute executor"))?;

    Ok(compute_executor.execute_with_progress(compute_store))
}

/// The main method for starting a materialization process.
///
/// Similar to the [execute] method, but certain features are not supported
/// in materializations.
pub async fn materialize(
    plan: ComputePlan,
    destination: Destination,
    tables: Vec<ComputeTable>,
    bounded_lateness_ns: Option<i64>,
    stop_signal_rx: tokio::sync::watch::Receiver<bool>,
) -> error_stack::Result<impl Stream<Item = error_stack::Result<ExecuteResponse, Error>>, Error> {
    let options = ExecutionOptions {
        bounded_lateness_ns,
        // TODO: Unimplemented feature - changed_since_time
        changed_since_time: Timestamp {
            seconds: 0,
            nanos: 0,
        },
        // Unsupported: not allowed to materialize at a specific time
        final_at_time: None,
        // TODO: Resuming from state is unimplemented
        compute_snapshot_config: None,
        stop_signal_rx: Some(stop_signal_rx),
        ..ExecutionOptions::default()
    };

    let data_context = DataContext::try_from_tables(tables)
        .into_report()
        .change_context(Error::internal_msg("create data context"))?;

    // TODO: the `execute_with_progress` method contains a lot of additional logic that is theoretically not needed,
    // as the materialization does not exit, and should not need to handle cleanup tasks that regular
    // queries do. We should likely refactor this to use a separate `materialize_with_progress` method.

    // TODO: Unimplemented feature - UDFs
    let udfs = HashMap::new();

    execute_new(plan, destination, data_context, options, None, udfs).await
}
