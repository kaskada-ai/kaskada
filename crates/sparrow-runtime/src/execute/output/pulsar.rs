use std::sync::Arc;

use arrow::datatypes::{ArrowPrimitiveType, DataType, Schema, SchemaRef, TimestampMicrosecondType};
use arrow::record_batch::RecordBatch;
use futures::stream::BoxStream;
use futures::StreamExt;
use pulsar::compression::Compression;
use sparrow_api::kaskada::v1alpha::output_to;
use sparrow_api::kaskada::v1alpha::PulsarDestination;

use crate::execute::progress_reporter::ProgressUpdate;
use error_stack::{IntoReport, ResultExt};

use pulsar::{message::proto, producer, Pulsar, TokioExecutor};

#[derive(Debug, derive_more::Display)]
pub enum Error {
    PulsarTopicCreation {
        context: String,
    },
    ProgressUpdate,
    JsonSerialization,
    SendingMessage,
    LocalWrite,
    #[display(fmt = "from: {from}, to: {to}")]
    Cast {
        from: DataType,
        to: DataType,
    },
    #[cfg(not(feature = "avro"))]
    AvroNotEnabled,
    #[cfg(feature = "avro")]
    AvroSchemaConversion,
}

impl error_stack::Context for Error {}

// The "public" tenant and "default" namespace are automatically created by pulsar.
const DEFAULT_PULSAR_TENANT: &str = "public";
const DEFAULT_PULSAR_NAMESPACE: &str = "default";

// WARN: If the batch size exceeds the max message size (5MB), the producer seems
// to panic and continually retry sending the batch, getting stuck in a loop.
// As such, keeping this batch size low to avoid this for now, as well as enabling
// compression at the cost of some cpu overhead.
//
// In the java implementation, there exists a `batchingMaxBytes` configuration
// that sends the batch when the size is reached. Ideally, we can clone that
// implementation over to the rust-native implementation.
const BATCH_SIZE: u32 = 1000;

pub(super) async fn write(
    pulsar: PulsarDestination,
    schema: SchemaRef,
    progress_updates_tx: tokio::sync::mpsc::Sender<ProgressUpdate>,
    mut batches: BoxStream<'static, RecordBatch>,
) -> error_stack::Result<(), Error> {
    let broker_url = if pulsar.broker_service_url.trim().is_empty() {
        error_stack::bail!(Error::PulsarTopicCreation {
            context: "empty broker service url".to_owned()
        })
    } else {
        &pulsar.broker_service_url
    };

    let topic_url = format_topic_url(&pulsar)?;
    let output_schema = get_output_schema(schema)?;
    let formatted_schema = format_schema(output_schema.clone())?;

    tracing::info!("Creating pulsar topic {topic_url} with schema: {formatted_schema}");
    // Note: Pulsar works natively in Avro - move towards serializing
    // results in Avro instead of Json. This is supported (with
    // some type restrictions) in Arrow2.
    let schema = proto::Schema {
        r#type: proto::schema::Type::Json as i32,
        schema_data: formatted_schema.as_bytes().to_vec(),
        ..Default::default()
    };

    // Inform tracker of output type
    progress_updates_tx
        .send(ProgressUpdate::Destination {
            destination: output_to::Destination::Pulsar(pulsar.clone()),
        })
        .await
        .into_report()
        .change_context(Error::ProgressUpdate)?;

    let client = Pulsar::builder(broker_url, TokioExecutor)
        .build()
        .await
        .into_report()
        .change_context(Error::JsonSerialization)?;

    let mut producer = client
        .producer()
        .with_topic(topic_url.clone())
        .with_name("producer")
        .with_options(producer::ProducerOptions {
            schema: Some(schema),
            batch_size: Some(BATCH_SIZE),
            compression: Some(Compression::Lz4(pulsar::compression::CompressionLz4 {
                mode: lz4::block::CompressionMode::DEFAULT,
            })),
            ..Default::default()
        })
        .build()
        .await
        .into_report()
        .change_context(Error::PulsarTopicCreation {
            context: format!("failed to create topic {topic_url}"),
        })?;

    // verify that the broker connections are still valid
    producer
        .check_connection()
        .await
        .into_report()
        .change_context(Error::PulsarTopicCreation {
            context: "connection check failed".to_owned(),
        })?;

    while let Some(batch) = batches.next().await {
        let batch = get_output_batch(output_schema.clone(), batch)?;
        let json_rows = arrow::json::writer::record_batches_to_json_rows(&[batch])
            .into_report()
            .change_context(Error::LocalWrite)?;
        let num_rows = json_rows.len();

        tracing::debug!("Buffering {num_rows} messages to pulsar");
        for row in json_rows {
            let payload = serde_json::to_string(&row)
                .into_report()
                .change_context(Error::JsonSerialization)
                .attach_printable_lazy(|| format!("failed to serialize {row:?} to json"))?;
            producer
                .send(payload.as_bytes())
                .await
                .into_report()
                .change_context(Error::SendingMessage)?;
        }
        tracing::debug!("Success. Buffered {num_rows} messages to pulsar");

        progress_updates_tx
            .send(ProgressUpdate::Output { num_rows })
            .await
            .into_report()
            .change_context(Error::ProgressUpdate)?;
    }

    // Send the buffer in the producer, if one exists.
    //
    // Ideally, this is done automatically by the producer, but in the absense
    // of a `maxPublishDelay` or similar clean-up process that flushes batches,
    // we need to explicitly send the final batch.
    producer
        .send_batch()
        .await
        .into_report()
        .change_context(Error::SendingMessage)?;

    Ok(())
}

// Drops columns to match the given output schema
fn get_output_batch(
    output_schema: SchemaRef,
    batch: RecordBatch,
) -> error_stack::Result<RecordBatch, Error> {
    let mut output_columns = Vec::with_capacity(output_schema.fields().len());

    let timestamp_us_col =
        arrow::compute::kernels::cast(batch.column(0), &TimestampMicrosecondType::DATA_TYPE)
            .into_report()
            .change_context(Error::Cast {
                from: batch.schema().field(0).data_type().clone(),
                to: TimestampMicrosecondType::DATA_TYPE,
            })?;

    // Take the casted _time column
    output_columns.extend_from_slice(&[timestamp_us_col]);

    // Take the _key column and the remaining data columns
    output_columns.extend_from_slice(&batch.columns()[3..]);

    Ok(RecordBatch::try_new(output_schema, output_columns).unwrap())
}

fn get_output_schema(schema: SchemaRef) -> error_stack::Result<SchemaRef, Error> {
    let fields = schema.fields();

    // Avro does not support certain types that we use internally for the implicit columns.
    // For the `_time` (timestamp_ns) column, we cast to timestamp_us, sacrificing nano precision.
    // (Optionally, we could provide a separate column composed of the nanos as a separate i64).
    // The `_subsort` and `_key_hash` columns are dropped.
    let time_us = arrow::datatypes::Field::new("_time", TimestampMicrosecondType::DATA_TYPE, false);
    let mut new_fields = vec![time_us];
    fields
        .iter()
        .skip(3) // Skip the `_time`, `_subsort`, and `_key_hash` fields
        .for_each(|f| new_fields.push(f.clone()));

    Ok(Arc::new(Schema::new(new_fields)))
}

#[cfg(not(feature = "avro"))]
fn format_schema(_schema: SchemaRef) -> error_stack::Result<String, Error> {
    error_stack::bail!(Error::AvroNotEnabled)
}

#[cfg(feature = "avro")]
fn format_schema(schema: SchemaRef) -> error_stack::Result<String, Error> {
    let avro_schema = sparrow_arrow::avro::to_avro_schema(&schema)
        .change_context(Error::AvroSchemaConversion)
        .attach_printable_lazy(|| {
            format!("failed to convert arrow schema to avro schema {schema}")
        })?;
    serde_json::to_string(&avro_schema)
        .into_report()
        .change_context(Error::JsonSerialization)
        .attach_printable_lazy(|| {
            format!("failed to serialize avro schema to json string: {avro_schema:?}")
        })
}

pub fn format_topic_url(pulsar: &PulsarDestination) -> error_stack::Result<String, Error> {
    let tenant = if pulsar.tenant.trim().is_empty() {
        DEFAULT_PULSAR_TENANT
    } else {
        &pulsar.tenant
    };

    let namespace = if pulsar.namespace.trim().is_empty() {
        DEFAULT_PULSAR_NAMESPACE
    } else {
        &pulsar.namespace
    };

    let name = if pulsar.topic_name.is_empty() {
        error_stack::bail!(Error::PulsarTopicCreation {
            context: "missing topic name".to_owned()
        })
    } else {
        &pulsar.topic_name
    };

    Ok(format!("persistent://{tenant}/{namespace}/topic-{name}"))
}
