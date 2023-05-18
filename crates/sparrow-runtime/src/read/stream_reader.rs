use std::sync::Arc;

use anyhow::Context;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use chrono::NaiveDateTime;
use error_stack::{IntoReport, IntoReportCompat, ResultExt};
use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use hashbrown::HashSet;
use itertools::Itertools;
use sparrow_api::kaskada::v1alpha::slice_plan::Slice;
use sparrow_api::kaskada::v1alpha::{PulsarSource, PulsarSubscription};
use sparrow_compiler::{DataContext, TableInfo};
use sparrow_core::TableSchema;
use sparrow_qfr::{
    activity, gauge, Activity, FlightRecorder, Gauge, PushRegistration, Registration, Registrations,
};
use tracing::info;

use crate::data_manager::{DataHandle, DataManager};
use crate::execute::operation::OperationContext;
use crate::merge::{homogeneous_merge, GatheredBatches, Gatherer};
use crate::min_heap::{HasPriority, MinHeap};
use crate::read::error::Error;
use crate::read::parquet_stream::{self, new_parquet_stream};
use crate::streams::pulsar::stream;
use crate::{prepare, streams, Batch, RawMetadata};

const READ_STREAM: Activity = activity!("scan.read_stream");

const MIN_BATCH_TIME: Gauge<i64> = gauge!("min_time_in_batch");
const MAX_BATCH_TIME: Gauge<i64> = gauge!("max_time_in_batch");
const NUM_INPUT_ROWS: Gauge<usize> = gauge!("num_input_rows");
const NUM_OUTPUT_ROWS: Gauge<usize> = gauge!("num_output_rows");

static REGISTRATION: Registration = Registration::new(|| {
    let mut r = Registrations::default();
    r.add(READ_STREAM);

    r.add(MIN_BATCH_TIME);
    r.add(MAX_BATCH_TIME);
    r.add(NUM_INPUT_ROWS);
    r.add(NUM_OUTPUT_ROWS);
    r
});

inventory::submit!(&REGISTRATION);

/// The bounded lateness parameter, which configures the delay in watermark time.
///
/// In practical terms, this allows for items in the stream to be within 1 second
/// compared to the max timestamp read.
///
/// This is hard-coded for now, but could easily be made configurable as a parameter
/// to the table. This simple hueristic is a good start, but we can improve on this
/// by statistically modeling event behavior and adapting the watermark accordingly.
const BOUNDED_LATENESS_NS: i64 = 1_000_000_000;

/// Create a stream that continually reads messages from a stream.
pub(crate) async fn stream_reader(
    context: &OperationContext,
    table_info: &TableInfo,
    requested_slice: Option<&Slice>,
    projected_columns: Option<Vec<String>>,
    _flight_recorder: FlightRecorder,
    pulsar_source: &PulsarSource,
) -> error_stack::Result<impl Stream<Item = error_stack::Result<Batch, Error>> + 'static, Error> {
    let pulsar_subscription =
        std::env::var("PULSAR_SUBSCRIPTION").unwrap_or("subscription-default".to_owned());
    let pulsar_config = pulsar_source.config.as_ref().ok_or(Error::Internal)?;
    let pulsar_subscription = PulsarSubscription {
        config: Some(pulsar_config.clone()),
        subscription_id: pulsar_subscription,
        last_publish_time: 0,
    };
    let raw_metadata = RawMetadata::try_from_pulsar(pulsar_config)
        .await
        .change_context(Error::CreateStream)?;

    let consumer =
        streams::pulsar::stream::consumer(&pulsar_subscription, table_info.schema().clone())
            .await
            .change_context(Error::CreateStream)?;
    let stream = streams::pulsar::stream::execution_stream(
        raw_metadata.sparrow_metadata.raw_schema.clone(),
        consumer,
        pulsar_subscription.last_publish_time,
    );

    // TODO: FRAZ - Figure out where you want to do the projected columns work:
    // 1. In prepare_input
    // 2. Here, in the final stream.
    // The parquet_stream() does it implicitly as it reads, because it can decide which columns to read.
    // We can't do that from avro, since it's not columnar based
    let table_config = table_info.config().clone();
    let mut input_stream = prepare::execute_input_stream::prepare_input(
        stream.boxed(),
        table_config,
        raw_metadata.sparrow_metadata,
        0,
        requested_slice,
        context.key_hash_inverse.clone(),
        BOUNDED_LATENESS_NS,
    )
    .await
    .into_report()
    .change_context(Error::CreateStream)?;

    Ok(async_stream::try_stream! {
        loop {
            if let Some(next_input) = input_stream.next().await {
                let next_input = next_input.change_context(Error::ReadNextBatch)?;
                match next_input {
                    None => continue,
                    Some(input) => {
                        yield Batch::try_new_from_batch(input).into_report().change_context(Error::Internal)?
                    }
                }
            } else {
                // Loop indefinitely - it's possible a batch was not produced because the watermark did not advance.
            }
        }
    })
}

/// Compute the projected schema from a base schema and projected columns.
fn projected_schema(
    schema: TableSchema,
    columns: &Option<Vec<String>>,
) -> error_stack::Result<TableSchema, Error> {
    if let Some(columns) = columns {
        let columns: HashSet<&str> = columns.iter().map(|x| x.as_ref()).collect();
        let projected_data_fields: Vec<_> = schema
            .data_fields()
            .iter()
            .filter(|field| columns.contains(field.name().as_str()))
            .cloned()
            .collect();

        debug_assert_eq!(projected_data_fields.len(), columns.len());
        Ok(TableSchema::from_data_fields(projected_data_fields)
            .into_report()
            .change_context(Error::DetermineProjectedSchema)?)
    } else {
        Ok(schema)
    }
}
