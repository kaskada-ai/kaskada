use crate::prepare::Error;

use arrow::error::ArrowError;
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use avro_rs::types::Value;
use error_stack::{IntoReport, ResultExt};
use futures::executor::block_on;
use pulsar::consumer::InitialPosition;

use pulsar::{
    Authentication, Consumer, ConsumerOptions, DeserializeMessage, Payload, Pulsar, SubType,
    TokioExecutor,
};

use crate::execute::avro_arrow;
use sparrow_api::kaskada::v1alpha::prepare_data_request::PulsarConfig;
use std::io::Cursor;

use std::time::Duration;
use tokio::time::timeout;
use tokio_stream::StreamExt;

pub struct AvroWrapper {
    value: Value,
}

pub struct PulsarReader {
    schema: SchemaRef,
    consumer: Consumer<AvroWrapper, TokioExecutor>,
}

#[derive(Debug)]
struct DeserializeErrorWrapper(error_stack::Report<DeserializeError>);

impl std::fmt::Display for DeserializeErrorWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl std::error::Error for DeserializeErrorWrapper {}

impl From<error_stack::Report<DeserializeError>> for DeserializeErrorWrapper {
    fn from(error: error_stack::Report<DeserializeError>) -> Self {
        DeserializeErrorWrapper(error)
    }
}

#[derive(derive_more::Display, Debug)]
pub enum DeserializeError {
    #[display(fmt = "error reading Avro record")]
    Avro,
    #[display(fmt = "unsupported Avro value")]
    UnsupportedType,
}

impl error_stack::Context for DeserializeError {}

impl DeserializeMessage for AvroWrapper {
    type Output = error_stack::Result<AvroWrapper, DeserializeError>;

    // TODO 1 the "normal" way to serialize binary Avro records is to include the entire
    // schema with each message, which is super inefficient for many common scenarios.
    // We follow the "normal" path here, but should we look at reading and writing
    // raw records using our own copy of the schema instead?
    //
    // TODO 2 for historical reasons, CDC encodes its messages as KeyValue pairs
    // where both key and value are Avro records.  See
    // https://github.com/datastax/astra-streaming-examples/blob/master/java/astra-cdc/javaexamples/consumers/CDCConsumer.java#L45
    // It is not clear to me how to support both normal Avro records, and this kind of
    // KeyValue encoding.  For now, we support the former but not the latter.
    fn deserialize_message(payload: &Payload) -> Self::Output {
        let cursor = Cursor::new(&payload.data);
        let mut reader = avro_rs::Reader::new(cursor)
            .into_report()
            .change_context(DeserializeError::Avro)?;
        let value = reader
            .next()
            .unwrap_or_else(|| {
                let e = avro_rs::Error::DeserializeValue(format!("{:?}", &payload));
                Err(e)
            })
            .into_report()
            .change_context(DeserializeError::Avro)?;
        let mut fields = match value {
            Value::Record(record) => record,
            _ => error_stack::bail!(DeserializeError::UnsupportedType),
        };
        // the Payload data only contains the fields for the user-defined (raw) schema,
        // so we inject the publish time from the metadata
        fields.push((
            "_publish_time".to_string(),
            Value::TimestampMillis(payload.metadata.publish_time as i64),
        ));
        Ok(AvroWrapper {
            value: Value::Record(fields),
        })
    }
}

impl PulsarReader {
    pub fn new(schema: SchemaRef, consumer: Consumer<AvroWrapper, TokioExecutor>) -> Self {
        PulsarReader { schema, consumer }
    }

    async fn next_async(&mut self) -> Option<Result<RecordBatch, ArrowError>> {
        self.next_result_async().await.transpose()
    }

    // using ArrowError is not a great fit but that is what PrepareIter requires
    async fn next_result_async(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        tracing::debug!("reading pulsar messages");
        let max_batch_size = 100000; // TODO make this adaptive based on the size of the messages
        let mut avro_values = Vec::with_capacity(max_batch_size);
        while avro_values.len() < max_batch_size {
            // read the next entry from the pulsar consumer
            // TODO this is fragile since tokio has no idea what is going on inside the consumer,
            // so it's entirely possible to time out while actively reading messages from the broker.
            // experimentally, 1ms is not reliable, but 10ms seems to work.
            let next_result = timeout(Duration::from_millis(1000), self.consumer.try_next()).await;
            let Ok(msg) = next_result else {
                tracing::trace!("timed out reading next message");
                break;
            };
            let msg = msg.map_err(|e| ArrowError::from_external_error(Box::new(e)))?;

            match msg {
                Some(msg) => {
                    self.consumer
                        .ack(&msg)
                        .await
                        .map_err(|e| ArrowError::from_external_error(Box::new(e)))?;
                    let result: error_stack::Result<AvroWrapper, DeserializeError> =
                        msg.deserialize();
                    let aw = match result {
                        Ok(aw) => aw,
                        Err(e) => {
                            let wrapped_error = DeserializeErrorWrapper::from(e);
                            tracing::debug!("error deserializing message: {:#?}", wrapped_error);
                            return Err(ArrowError::from_external_error(Box::new(wrapped_error)));
                        }
                    };
                    match aw.value {
                        Value::Record(fields) => {
                            avro_values.push(fields);
                        }
                        _ => {
                            let e = error_stack::report!(DeserializeError::UnsupportedType)
                                .attach_printable(format!(
                                    "expected a record but got {:?}",
                                    aw.value
                                ));
                            return Err(ArrowError::from_external_error(Box::new(
                                DeserializeErrorWrapper::from(e),
                            )));
                        }
                    }
                }
                None => {
                    // try_next will return None if the stream is closed, which shouldn't
                    // happen in the pulsar scenario.  maybe if the broker shuts down?
                    tracing::debug!("read None from consumer -- not sure how this happens");
                    break;
                }
            }
        }

        tracing::debug!("read {} messages", avro_values.len());
        match avro_values.len() {
            0 => Ok(None),
            _ => {
                let arrow_data = avro_arrow::avro_to_arrow(avro_values)
                    .map_err(|e| ArrowError::from_external_error(Box::new(e)))?;
                RecordBatch::try_new(self.schema.clone(), arrow_data).map(Some)
            }
        }
    }
}

impl Iterator for PulsarReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        block_on(self.next_async())
    }
}

pub(crate) async fn pulsar_consumer(
    pulsar_config: &PulsarConfig,
    schema: SchemaRef,
) -> error_stack::Result<Consumer<AvroWrapper, TokioExecutor>, Error> {
    let ps = pulsar_config.pulsar_source.as_ref().unwrap();
    // specifying persistent:// or non-persistent:// appears to be optional
    let topic_url = format!("{}/{}/{}", ps.tenant, ps.namespace, ps.topic_name);

    let auth_token = crate::execute::pulsar_schema::pulsar_auth_token(ps.auth_params.as_str())
        .change_context(Error::CreatePulsarReader)?;
    let auth = Authentication {
        name: "token".to_string(),
        data: auth_token.as_bytes().to_vec(),
    };
    let client = Pulsar::builder(&ps.broker_service_url, TokioExecutor)
        .with_auth(auth)
        .build()
        .await
        .into_report()
        .change_context(Error::CreatePulsarReader)?;

    let formatted_schema = crate::execute::pulsar_schema::format_schema(schema)
        .change_context(Error::CreatePulsarReader)?;
    let pulsar_schema = pulsar::message::proto::Schema {
        r#type: pulsar::message::proto::schema::Type::Avro as i32,
        schema_data: formatted_schema.as_bytes().to_vec(),
        ..Default::default()
    };

    // create a pulsar client that can read arbitrary avro Values
    let options = ConsumerOptions::default()
        .with_schema(pulsar_schema)
        .with_initial_position(InitialPosition::Earliest);
    let consumer: Consumer<AvroWrapper, TokioExecutor> = client
        .consumer()
        .with_options(options)
        .with_topic(topic_url)
        .with_consumer_name(format!(
            "sparrow consumer for {}",
            &pulsar_config.subscription_id
        ))
        .with_subscription_type(SubType::Exclusive)
        .with_subscription(&pulsar_config.subscription_id)
        .build()
        .await
        .into_report()
        .change_context(Error::CreatePulsarReader)?;

    Ok(consumer)
}
