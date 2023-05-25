use std::sync::Arc;

use chrono::NaiveDateTime;
use error_stack::{IntoReport, IntoReportCompat, ResultExt};
use futures::{Stream, StreamExt, TryStreamExt};
use prost_wkt_types::Timestamp;
use sparrow_api::kaskada::v1alpha::execute_request::Limits;
use sparrow_api::kaskada::v1alpha::get_materialization_status_response::{
    materialization_status, MaterializationStatus,
};
use sparrow_api::kaskada::v1alpha::{
    ExecuteRequest, ExecuteResponse, LateBoundValue, PerEntityBehavior, StartMaterializationRequest,
};
use sparrow_compiler::{hash_compute_plan_proto, DataContext};
use sparrow_core::ScalarValue;
use sparrow_instructions::ComputeStore;
use sparrow_qfr::kaskada::sparrow::v1alpha::FlightRecordHeader;

use crate::data_manager::DataManager;
use crate::execute::key_hash_inverse::{KeyHashInverse, ThreadSafeKeyHashInverse};
use crate::execute::operation::OperationContext;
use crate::s3::S3Helper;
use crate::RuntimeOptions;

mod compute_executor;
mod error;
mod input_prefetch;
pub(crate) mod key_hash_inverse;
pub(crate) mod operation;
pub mod output;
mod progress_reporter;
mod spawner;

pub use compute_executor::*;
pub use error::*;

// The path prefix to the local compute store db.
const STORE_PATH_PREFIX: &str = "compute_snapshot_";

/// The main method for executing a Fenl query.
///
/// The `request` proto contains the execution plan as well as
/// information about the configuration in which to execute.
///
/// The result is a stream of progress reports and the final
/// execute response.
pub async fn execute(
    request: ExecuteRequest,
    s3_helper: S3Helper,
    bounded_lateness_ns: Option<i64>,
    _flight_record_local_path: Option<std::path::PathBuf>,
    _flight_record_header: FlightRecordHeader,
) -> error_stack::Result<impl Stream<Item = error_stack::Result<ExecuteResponse, Error>>, Error> {
    let plan = request.plan.ok_or(Error::MissingField("plan"))?;

    let destination = request
        .destination
        .ok_or(Error::MissingField("destination"))?;

    let changed_since_time = request.changed_since.unwrap_or(Timestamp {
        seconds: 0,
        nanos: 0,
    });

    // Create and populate the late bindings.
    // We don't use the `enum_map::enum_map!(...)` initialization because it would
    // require looping over (and cloning) the scalar value unnecessarily.
    let mut late_bindings = enum_map::enum_map! {
        _ => None
    };
    late_bindings[LateBoundValue::ChangedSinceTime] = Some(ScalarValue::timestamp(
        changed_since_time.seconds,
        changed_since_time.nanos,
        None,
    ));

    let output_at_time = if let Some(output_at_time) = request.final_result_time {
        late_bindings[LateBoundValue::FinalAtTime] = Some(ScalarValue::timestamp(
            output_at_time.seconds,
            output_at_time.nanos,
            None,
        ));
        Some(output_at_time)
    } else {
        late_bindings[LateBoundValue::FinalAtTime] = None;
        None
    };

    let mut data_context = DataContext::try_from_tables(request.tables.to_vec())
        .into_report()
        .change_context(Error::internal_msg("create data context"))?;

    // If the snapshot config exists, sparrow should attempt to resume from state,
    // and store new state. Create a new storage path for the local store to
    // exist.
    let storage_dir = if let Some(config) = &request.compute_snapshot_config {
        let dir = tempfile::Builder::new()
            .prefix(&STORE_PATH_PREFIX)
            .tempdir()
            .into_report()
            .change_context(Error::internal_msg("create snapshot dir"))?;

        // If a `resume_from` path is specified, download the existing state from s3.
        if config.resume_from.is_some() {
            crate::s3::download_snapshot(&s3_helper, dir.path(), config)
                .await
                .into_report()
                .change_context(Error::internal_msg("download snapshot"))?;
        };

        Some(dir)
    } else {
        None
    };

    let plan_hash = hash_compute_plan_proto(&plan);

    let compute_store = if let Some(dir) = &storage_dir {
        let max_allowed_max_event_time = match plan.per_entity_behavior() {
            PerEntityBehavior::Unspecified => {
                error_stack::bail!(Error::UnspecifiedPerEntityBehavior)
            }
            PerEntityBehavior::All => {
                // For all results, we need a snapshot with a maximum event time
                // no larger than the changed_since time, since we need to replay
                // (and recompute the results for) all events after the changed
                // since time.
                changed_since_time.clone()
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
                output_at_time.as_ref().expect("final at time").clone()
            }
        };

        Some(
            ComputeStore::try_new(dir.path(), &max_allowed_max_event_time, &plan_hash)
                .into_report()
                .change_context(Error::internal_msg("loading compute store"))?,
        )
    } else {
        None
    };

    let primary_grouping_key_type = plan
        .primary_grouping_key_type
        .to_owned()
        .ok_or(Error::MissingField("primary_grouping_key_type"))?;
    let primary_grouping_key_type =
        arrow::datatypes::DataType::try_from(&primary_grouping_key_type)
            .into_report()
            .change_context(Error::internal_msg("decode primary_grouping_key_type"))?;
    let mut key_hash_inverse = KeyHashInverse::from_data_type(primary_grouping_key_type.clone());

    if let Some(compute_store) = compute_store.to_owned() {
        if let Ok(restored) = KeyHashInverse::restore_from(&compute_store) {
            key_hash_inverse = restored
        }
    }
    let primary_group_id = data_context
        .get_or_create_group_id(&plan.primary_grouping, &primary_grouping_key_type)
        .into_report()
        .change_context(Error::internal_msg("get primary grouping ID"))?;

    key_hash_inverse
        .add_from_data_context(&data_context, primary_group_id, s3_helper.clone())
        .await
        .into_report()
        .change_context(Error::internal_msg("initialize key hash inverse"))?;
    let key_hash_inverse = Arc::new(ThreadSafeKeyHashInverse::new(key_hash_inverse));

    // Channel for the output stats.
    let (progress_updates_tx, progress_updates_rx) =
        tokio::sync::mpsc::channel(29.max(plan.operations.len() * 2));

    let output_datetime = if let Some(t) = output_at_time {
        Some(
            NaiveDateTime::from_timestamp_opt(t.seconds, t.nanos as u32)
                .ok_or_else(|| Error::internal_msg("expected valid timestamp"))?,
        )
    } else {
        None
    };

    // We use the plan hash for validating the snapshot is as expected.
    // Rather than accepting it as input (which could lead to us getting
    // a correct hash but an incorrect plan) we re-hash the plan.
    let context = OperationContext {
        plan,
        plan_hash,
        data_manager: DataManager::new(s3_helper.clone()),
        data_context,
        compute_store,
        key_hash_inverse,
        max_event_in_snapshot: None,
        progress_updates_tx,
        output_at_time: output_datetime,
        bounded_lateness_ns,
    };

    // Start executing the query. We pass the response channel to the
    // execution layer so it can periodically report progress.
    tracing::debug!("Starting query execution");

    let runtime_options = RuntimeOptions {
        limits: request.limits.unwrap_or_default(),
        flight_record_path: None,
    };

    let compute_executor = ComputeExecutor::try_spawn(
        context,
        &late_bindings,
        &runtime_options,
        progress_updates_rx,
        destination,
    )
    .await
    .change_context(Error::internal_msg("spawn compute executor"))?;

    Ok(compute_executor.execute_with_progress(
        s3_helper,
        storage_dir,
        request.compute_snapshot_config,
    ))
}

pub async fn materialize(
    request: StartMaterializationRequest,
    s3_helper: S3Helper,
    bounded_lateness_ns: Option<i64>,
    compute_store: Arc<ComputeStore>,
) -> error_stack::Result<(), Error> {
    let plan = request.plan.ok_or(Error::MissingField("plan"))?;

    let destination = request
        .destination
        .ok_or(Error::MissingField("destination"))?;

    // TODO: Unimplemented feature - changed_since_time
    let changed_since_time = Timestamp {
        seconds: 0,
        nanos: 0,
    };

    // Create and populate the late bindings.
    // We don't use the `enum_map::enum_map!(...)` initialization because it would
    // require looping over (and cloning) the scalar value unnecessarily.
    let mut late_bindings = enum_map::enum_map! {
        _ => None
    };
    late_bindings[LateBoundValue::ChangedSinceTime] = Some(ScalarValue::timestamp(
        changed_since_time.seconds,
        changed_since_time.nanos,
        None,
    ));

    // Not allowed to materialize at a specific time
    late_bindings[LateBoundValue::FinalAtTime] = None;
    let output_at_time = None;

    let mut data_context = DataContext::try_from_tables(request.tables.to_vec())
        .into_report()
        .change_context(Error::internal_msg("create data context"))?;

    // TODO: Resuming from state is unimplemented
    let storage_dir = None;
    let snapshot_compute_store = None;

    let plan_hash = hash_compute_plan_proto(&plan);

    let primary_grouping_key_type = plan
        .primary_grouping_key_type
        .to_owned()
        .ok_or(Error::MissingField("primary_grouping_key_type"))?;
    let primary_grouping_key_type =
        arrow::datatypes::DataType::try_from(&primary_grouping_key_type)
            .into_report()
            .change_context(Error::internal_msg("decode primary_grouping_key_type"))?;
    let mut key_hash_inverse = KeyHashInverse::from_data_type(primary_grouping_key_type.clone());

    let primary_group_id = data_context
        .get_or_create_group_id(&plan.primary_grouping, &primary_grouping_key_type)
        .into_report()
        .change_context(Error::internal_msg("get primary grouping ID"))?;

    key_hash_inverse
        .add_from_data_context(&data_context, primary_group_id, s3_helper.clone())
        .await
        .into_report()
        .change_context(Error::internal_msg("initialize key hash inverse"))?;
    let key_hash_inverse = Arc::new(ThreadSafeKeyHashInverse::new(key_hash_inverse));

    // Channel for the output stats.
    let (progress_updates_tx, progress_updates_rx) =
        tokio::sync::mpsc::channel(29.max(plan.operations.len() * 2));

    // We use the plan hash for validating the snapshot is as expected.
    // Rather than accepting it as input (which could lead to us getting
    // a correct hash but an incorrect plan) we re-hash the plan.
    let context = OperationContext {
        plan,
        plan_hash,
        data_manager: DataManager::new(s3_helper.clone()),
        data_context,
        compute_store: snapshot_compute_store,
        key_hash_inverse,
        max_event_in_snapshot: None,
        progress_updates_tx,
        output_at_time,
        bounded_lateness_ns,
    };

    // Start executing the query. We pass the response channel to the
    // execution layer so it can periodically report progress.
    tracing::debug!("Starting query execution");

    let runtime_options = RuntimeOptions {
        limits: Limits::default(),
        flight_record_path: None,
    };

    let compute_executor = ComputeExecutor::try_spawn(
        context,
        &late_bindings,
        &runtime_options,
        progress_updates_rx,
        destination,
    )
    .await
    .change_context(Error::internal_msg("spawn compute executor"))?;

    // TODO: the `execute` method contains a lot of additional logic that is theoretically not needed,
    // as the materialization does not exit, and should not need to handle cleanup tasks that regular
    // queries do. We should likely refactor this to use a separate `materialize_with_progress` method.
    let mut progress_stream = compute_executor
        .execute_with_progress(s3_helper, storage_dir, None)
        .boxed();

    // Loop over results in the progress stream and update the compute store
    while let Some(message) = progress_stream.try_next().await? {
        if let Some(progress) = message.progress {
            compute_store
                .update_materialization_progress(&request.materialization_id, progress)
                .change_context(Error::internal_msg("updating progress"))?;
        }
    }

    tracing::error!("unexpectedly exited materialization loop");
    Ok(())
}
