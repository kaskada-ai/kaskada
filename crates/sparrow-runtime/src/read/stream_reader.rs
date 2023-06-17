use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use error_stack::{IntoReportCompat, ResultExt};

use futures::{Stream, StreamExt};
use hashbrown::HashSet;
use sparrow_api::kaskada::v1alpha::slice_plan::Slice;
use sparrow_api::kaskada::v1alpha::{PulsarSource, PulsarSubscription};
use sparrow_compiler::TableInfo;
use sparrow_qfr::{
    activity, gauge, Activity, FlightRecorder, Gauge, PushRegistration, Registration, Registrations,
};

use crate::execute::operation::OperationContext;
use crate::read::error::Error;
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
    // TODO: This should be the materialization ID, or configurable by the user.
    // This will be important when restarting a consumer at a specific point.
    let pulsar_subscription =
        std::env::var("PULSAR_SUBSCRIPTION").unwrap_or("subscription-default".to_owned());
    let pulsar_config = pulsar_source.config.as_ref().ok_or(Error::Internal)?;
    let pulsar_subscription = PulsarSubscription {
        config: Some(pulsar_config.clone()),
        subscription_id: pulsar_subscription,
        last_publish_time: 0,
    };
    let pulsar_metadata = RawMetadata::try_from_pulsar(pulsar_config, false)
        .await
        .change_context(Error::CreateStream)?;
    // Verify the provided table schema matches the topic schema
    verify_schema_match(
        pulsar_metadata.user_schema.clone(),
        table_info.schema().clone(),
    )?;

    // The projected schema should come from the table_schema, which includes converted
    // timestamp column, dropped decimal columns, etc.
    // i.e. any changes we make to the raw schema to be able to process rows.
    let projected_schema = if let Some(columns) = &projected_columns {
        projected_schema(pulsar_metadata.sparrow_metadata.table_schema, columns)
            .change_context(Error::CreateStream)?
    } else {
        pulsar_metadata.sparrow_metadata.table_schema
    };

    let consumer = streams::pulsar::stream::consumer(
        &pulsar_subscription,
        pulsar_metadata.user_schema.clone(),
    )
    .await
    .change_context(Error::CreateStream)?;
    let stream = streams::pulsar::stream::execution_stream(
        pulsar_metadata.sparrow_metadata.raw_schema.clone(),
        projected_schema.clone(),
        consumer,
        pulsar_subscription.last_publish_time,
    );

    let table_config = table_info.config().clone();
    let bounded_lateness = if let Some(bounded_lateness) = context.bounded_lateness_ns {
        bounded_lateness
    } else {
        BOUNDED_LATENESS_NS
    };

    let mut input_stream = prepare::execute_input_stream::prepare_input(
        stream.boxed(),
        table_config,
        pulsar_metadata.user_schema.clone(),
        projected_schema,
        0,
        requested_slice,
        context.key_hash_inverse.clone(),
        bounded_lateness,
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
    schema: SchemaRef,
    columns: &[String],
) -> error_stack::Result<SchemaRef, Error> {
    let columns: HashSet<&str> = columns.iter().map(|x| x.as_ref()).collect();
    let projected_data_fields: Vec<_> = schema
        .fields()
        .iter()
        .filter(|field| columns.contains(field.name().as_str()))
        .cloned()
        .collect();

    debug_assert_eq!(projected_data_fields.len(), columns.len());
    Ok(Arc::new(Schema::new(projected_data_fields)))
}

fn verify_schema_match(
    user_schema: SchemaRef,
    table_schema: SchemaRef,
) -> error_stack::Result<(), Error> {
    error_stack::ensure!(
        user_schema.fields().len() == table_schema.fields().len(),
        Error::SchemaMismatch {
            expected_schema: table_schema,
            actual_schema: user_schema,
            context: "stream schema did not match provided table schema".to_owned(),
        }
    );
    for field in user_schema.fields() {
        table_schema
            .field_with_name(field.name())
            .map_err(|_| Error::SchemaMismatch {
                expected_schema: table_schema.clone(),
                actual_schema: user_schema.clone(),
                context: "stream schema did not match provided table schema".to_owned(),
            })?;
    }
    Ok(())
}
