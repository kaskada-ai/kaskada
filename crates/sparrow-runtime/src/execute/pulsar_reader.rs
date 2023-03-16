use crate::prepare::Error;
use arrow::array::{ArrowPrimitiveType, PrimitiveArray};
use arrow::datatypes::{Float64Type, Int32Type, TimestampMillisecondType};
use arrow::error::ArrowError;
use arrow::{
    array::{Array, ArrayRef},
    datatypes::SchemaRef,
    record_batch::RecordBatch,
};
use avro_rs::types::Value;
use error_stack::{FutureExt, IntoReport, IntoReportCompat, Report, ResultExt};
use fallible_iterator::FallibleIterator;
use futures::executor::block_on;
use pulsar::consumer::InitialPosition;

use pulsar::{
    Consumer, ConsumerOptions, DeserializeMessage, Payload, Pulsar, SubType, TokioExecutor,
};

use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use tokio_stream::StreamExt;
use crate::execute::avro_arrow;

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
    #[display(fmt = "Avro value was not a record")]
    BadRecord,
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
        // let mut decoder = ZlibDecoder::new(&payload.data[..]);
        // let mut decoded = Vec::new();
        // decoder.read_to_end(&mut decoded)?;
        // let cursor = Cursor::new(decoded);

        let cursor = Cursor::new(&payload.data);
        let reader = avro_rs::Reader::new(cursor)
            .into_report()
            .change_context(DeserializeError::Avro)?;
        let mut iter = reader.into_iter();
        let value = iter
            .next()
            .unwrap()
            .into_report()
            .change_context(DeserializeError::Avro)?;
        let mut fields = match value {
            Value::Record(record) => record,
            _ => error_stack::bail!(DeserializeError::BadRecord),
        };
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
            let next_result = timeout(Duration::from_millis(10), self.consumer.try_next()).await;
            if next_result.is_err() {
                tracing::trace!("timed out reading next message");
                break;
            }
            let msg = next_result
                .unwrap()
                .map_err(|e| ArrowError::from_external_error(Box::new(e)))?;
            tracing::trace!("got a message");

            match msg {
                Some(msg) => {
                    self.consumer
                        .ack(&msg)
                        .await
                        .map_err(|e| ArrowError::from_external_error(Box::new(e)))?;
                    tracing::debug!("acked message");
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
                            tracing::debug!("expected a record but got {:?}", aw.value);
                            let e = Report::from(DeserializeError::BadRecord);
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
                }
            }
        }

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
    uri: &String,
    schema: SchemaRef,
) -> error_stack::Result<Consumer<AvroWrapper, TokioExecutor>, Error> {
    let url = url::Url::parse(uri)
        .into_report()
        .change_context(Error::CreatePulsarReader)?;
    let host = url.host_str().unwrap();
    let port = url.port_or_known_default().unwrap();
    let broker_url = format!("pulsar://{}:{}", host, port);

    let mut path_segments = url.path_segments().unwrap();
    let tenant = path_segments.next().unwrap();
    let namespace = path_segments.next().unwrap();
    let topic = path_segments.next().unwrap();
    let _topic_url = format!("persistent://{tenant}/{namespace}/{topic}");

    let client = Pulsar::builder(broker_url, TokioExecutor)
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
        // TODO figure out how to get tenant + namespace in here
        // .with_topic(topic_url)
        .with_topic(topic)
        .with_consumer_name("sparrow-consumer")
        .with_subscription_type(SubType::Exclusive)
        // TODO generate and persist subscription ID
        .with_subscription("sparrow-subscription-8")
        .build()
        .await
        .into_report()
        .change_context(Error::CreatePulsarReader)?;

    Ok(consumer)
}
