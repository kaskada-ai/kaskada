use arrow::datatypes::{ArrowPrimitiveType, Schema, SchemaRef, TimestampMicrosecondType};
use arrow::record_batch::RecordBatch;
use futures::stream::BoxStream;
use futures::StreamExt;
use sparrow_api::kaskada::v1alpha::{execute_response::output, PulsarDestination, PulsarOutput};

use crate::execute::progress_reporter::ProgressUpdate;
use error_stack::{IntoReport, ResultExt};

use derive_more::Display;
use pulsar::{message::proto, producer, Pulsar, TokioExecutor};

#[derive(Debug, Display)]
pub enum Error {
    PulsarTopicCreation,
    ProgressUpdate,
    JsonSerialization,
    SendingPulsarPayload,
    LocalWrite,
    AvroSchemaConversion,
}

impl error_stack::Context for Error {}

// Note: Could make these ENV vars
const PULSAR_TENANT: &str = "public";
const PULSAR_NAMESPACE: &str = "namespace";
const PULSAR_ADDR: &str = "pulsar://127.0.0.1:6650";

pub(super) async fn write(
    pulsar: PulsarDestination,
    topic_name: String,
    schema: SchemaRef,
    progress_updates_tx: tokio::sync::mpsc::Sender<ProgressUpdate>,
    mut batches: BoxStream<'static, RecordBatch>,
) -> error_stack::Result<(), Error> {
    let topic_url = format_topic(&pulsar, &topic_name);
    let avro_schema = to_avro_schema(schema)?;
    tracing::info!("Creating pulsar topic {topic_url} with schema: {avro_schema}");

    // Note: Pulsar works natively in Avro - move towards serializing
    // results in Avro instead of Json. This is supported (with
    // some type restrictions) in Arrow2.
    let schema = proto::Schema {
        r#type: proto::schema::Type::Json as i32,
        schema_data: avro_schema.as_bytes().to_vec(),
        ..Default::default()
    };

    let broker_url = if !pulsar.broker_service_url.trim().is_empty() {
        PULSAR_ADDR
    } else {
        &pulsar.broker_service_url
    };

    // Inform tracker of output type
    progress_updates_tx
        .send(ProgressUpdate::OutputType {
            output: output::Output::Pulsar(PulsarOutput {
                topic_url: topic_url.clone(),
                broker_service_url: broker_url.to_owned(),
            }),
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
            ..Default::default()
        })
        .build()
        .await
        .into_report()
        .change_context(Error::PulsarTopicCreation)
        .attach_printable_lazy(|| format!("failed to create topic {topic_url}"))?;

    while let Some(batch) = batches.next().await {
        let json_rows = arrow::json::writer::record_batches_to_json_rows(&[batch])
            .into_report()
            .change_context(Error::LocalWrite)?;
        let num_rows = json_rows.len();
        tracing::debug!("Sending {num_rows} messages to pulsar");

        for row in json_rows {
            let payload = serde_json::to_string(&row)
                .into_report()
                .change_context(Error::JsonSerialization)
                .attach_printable_lazy(|| format!("failed to serialize {row:?} to json"))?;
            producer
                .send(payload.as_bytes())
                .await
                .into_report()
                .change_context(Error::SendingPulsarPayload)?;
        }
        tracing::debug!("Success. Sent {num_rows} messages to pulsar");

        progress_updates_tx
            .send(ProgressUpdate::Output { num_rows })
            .await
            .into_report()
            .change_context(Error::ProgressUpdate)?;
    }

    Ok(())
}

fn to_avro_schema(schema: SchemaRef) -> error_stack::Result<String, Error> {
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

    let new_schema = Schema::new(new_fields);
    let avro_schema = sparrow_arrow::avro::to_avro_schema(&new_schema)
        .change_context(Error::AvroSchemaConversion)
        .attach_printable_lazy(|| {
            format!("failed to convert arrow schema to avro schema {new_schema}")
        })?;
    serde_json::to_string(&avro_schema)
        .into_report()
        .change_context(Error::JsonSerialization)
        .attach_printable_lazy(|| {
            format!("failed to serialize avro schema to json string: {avro_schema:?}")
        })
}

fn format_topic(pulsar: &PulsarDestination, topic_name: &str) -> String {
    let tenant = if pulsar.tenant.trim().is_empty() {
        PULSAR_TENANT
    } else {
        &pulsar.tenant
    };

    let namespace = if pulsar.namespace.trim().is_empty() {
        PULSAR_NAMESPACE
    } else {
        &pulsar.namespace
    };

    format!("persistent://{tenant}/{namespace}/topic-{topic_name}")
}
