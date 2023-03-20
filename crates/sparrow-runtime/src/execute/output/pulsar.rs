use arrow::datatypes::{ArrowPrimitiveType, DataType, SchemaRef, TimestampMicrosecondType};
use arrow::record_batch::RecordBatch;
use futures::stream::BoxStream;
use futures::StreamExt;
use pulsar::compression::Compression;
use pulsar::Authentication;
use sparrow_api::kaskada::v1alpha::output_to;
use sparrow_api::kaskada::v1alpha::PulsarDestination;

use crate::execute::progress_reporter::ProgressUpdate;
use error_stack::{IntoReport, ResultExt};

use crate::execute::pulsar_schema;
use pulsar::{message::proto, producer, Pulsar, TokioExecutor};

#[derive(Debug, derive_more::Display)]
pub enum Error {
    PulsarAuth {
        context: String,
    },
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
    SchemaSerialization,
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
    let output_schema =
        pulsar_schema::get_output_schema(schema).change_context(Error::SchemaSerialization)?;
    let formatted_schema = pulsar_schema::format_schema(output_schema.clone())
        .change_context(Error::SchemaSerialization)?;

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

    let client = build_client(broker_url, &pulsar).await?;
    let mut producer = client
        .producer()
        .with_topic(topic_url.clone())
        .with_name("sparrow-producer")
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

// Builds the pulsar client
async fn build_client(
    broker_url: &str,
    pulsar: &PulsarDestination,
) -> error_stack::Result<Pulsar<TokioExecutor>, Error> {
    let mut client_builder = Pulsar::builder(broker_url, TokioExecutor);

    // Add authorization
    if !pulsar.auth_plugin.is_empty() {
        // Currently, we only support auth with jwt tokens
        // https://pulsar.apache.org/docs/2.4.0/security-token-client/
        error_stack::ensure!(
            pulsar.auth_plugin == "org.apache.pulsar.client.impl.auth.AuthenticationToken",
            Error::PulsarAuth {
                context: format!("unsupported auth plugin: {}", pulsar.auth_plugin)
            }
        );
        // Additionally, only the string format is supported
        let auth_token = if let Some(token) = pulsar.auth_params.strip_prefix("token:") {
            token
        } else {
            error_stack::bail!(Error::PulsarAuth {
                context: format!(
                    "expected \"token:\" style prefix. Saw {}",
                    pulsar.auth_params
                )
            })
        };

        let pulsar_auth = Authentication {
            name: "token".to_owned(),
            data: auth_token.as_bytes().to_vec(),
        };
        client_builder = client_builder.with_auth(pulsar_auth);
    };

    // Add TLS encryption
    if !pulsar.certificate_chain.is_empty() {
        // The default values for the other configs are explicitly show here for
        // clarity. We can allow the user to configure these if requested.
        client_builder = client_builder
            .with_allow_insecure_connection(false)
            .with_tls_hostname_verification_enabled(true)
            .with_certificate_chain(pulsar.certificate_chain.as_bytes().to_vec());
    };

    let client = client_builder
        .build()
        .await
        .into_report()
        .change_context(Error::JsonSerialization)?;

    Ok(client)
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

    Ok(format_topic_url_str(tenant, namespace, name))
}

pub fn format_topic_url_str(tenant: &str, namespace: &str, name: &String) -> String {
    format!("persistent://{tenant}/{namespace}/{name}")
}
