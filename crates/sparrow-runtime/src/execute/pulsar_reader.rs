use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;
use arrow::error::ArrowError;
use arrow::{
    array::{Array, ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use arrow::array::{ArrayData, BinaryArray, Float32Array, Int32Array, ListArray, NullArray, StructArray, TimestampMicrosecondArray, TimestampMillisecondArray, UnionArray};
use avro_rs::types::{Record, Value};
use error_stack::{FutureExt, IntoReport, IntoReportCompat, ResultExt};
use fallible_iterator::FallibleIterator;
use futures::executor::block_on;
use pulsar::{Consumer, ConsumerOptions, DeserializeMessage, Payload, Pulsar, SubType, TokioExecutor};
use sha2::digest::generic_array::arr;
use tokio::time::timeout;
use tokio_stream::StreamExt;
use tracing::log::logger;
use crate::prepare::Error;

pub struct AvroWrapper {
    value: Value,
}

pub struct PulsarReader {
    schema: SchemaRef,
    consumer: Consumer<AvroWrapper, TokioExecutor>
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

impl PulsarReader {
    pub fn new(schema: SchemaRef, consumer: Consumer<AvroWrapper, TokioExecutor>) -> Self {
        PulsarReader {
            schema,
            consumer
        }
    }

    // TODO this returns one record batch per message, which is not what we want
    // TODO using ArrowError is not a great fit but that is what PrepareIter requires
    async fn next_async(&mut self) -> Option<Result<RecordBatch, ArrowError>> {
        tracing::debug!("reading pulsar message");
        // // read the next entry from the pulsar consumer
        // let next_result = timeout(Duration::from_micros(1), self.consumer.next())
        //     .await;
        // if next_result.is_err() {
        //     // timed out
        //     return None;
        // }
        // let next = next_result.unwrap();
        let next = self.consumer.next().await;
        tracing::debug!("got a message");
        match next {
            Some(Ok(msg)) => {
                self.consumer.ack(&msg).await.ok()?;
                tracing::debug!("acked message");
                let result: error_stack::Result<AvroWrapper, DeserializeError> = msg.deserialize();
                let aw = match result {
                    Ok(aw) => aw,
                    Err(e) => {
                        // TODO is there a better way to do this?
                        let wrapped_error = DeserializeErrorWrapper::from(e);
                        tracing::debug!("error deserializing message: {:#?}", wrapped_error);
                        return Some(Err(ArrowError::from_external_error(Box::new(wrapped_error))));
                    }
                };
                // Convert the Avro value to Arrow arrays
                match avro_to_arrow(&aw.value) {
                    Ok(arrow_data) => {
                        tracing::debug!("success converting avro to arrow: {:#?}", arrow_data);
                        Some(RecordBatch::try_new(
                            self.schema.clone(),
                            arrow_data))
                    }
                    Err(e) => {
                        tracing::debug!("error converting avro to arrow: {:#?}", e);
                        Some(Err(ArrowError::from_external_error(Box::new(e))))
                    }
                }
            },
            Some(Err(e)) => {
                tracing::debug!("error reading message: {:#?}", e);
                Some(Err(ArrowError::from_external_error(Box::new(e))))
            },
            None => None
        }
    }
}

impl Iterator for PulsarReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        block_on(self.next_async())
    }
}

fn avro_to_arrow_primitive(value: &Value) -> Result<ArrayRef, ArrowError> {
    match value {
        Value::Null => Ok(Arc::new(NullArray::new(1))),
        Value::Boolean(b) => Ok(Arc::new(BooleanArray::from(vec![*b]))),
        Value::Int(i) => Ok(Arc::new(Int32Array::from(vec![*i]))),
        Value::Long(l) => Ok(Arc::new(Int64Array::from(vec![*l]))),
        Value::Float(f) => Ok(Arc::new(Float32Array::from(vec![*f]))),
        Value::Double(d) => Ok(Arc::new(Float64Array::from(vec![*d]))),
        Value::Bytes(b) => Ok(Arc::new(BinaryArray::from(vec![b.as_slice()]))),
        Value::String(s) => Ok(Arc::new(StringArray::from(vec![s.as_str()]))),
        Value::TimestampMillis(t) => Ok(Arc::new(TimestampMillisecondArray::from(vec![*t]))),
        Value::TimestampMicros(t) => Ok(Arc::new(TimestampMicrosecondArray::from(vec![*t]))),
        _ => Err(ArrowError::InvalidArgumentError(format!("{:?} not a primitive type", value)))
    }
}

fn avro_to_arrow(value: &Value) -> Result<Vec<ArrayRef>, ArrowError> {
    match value {
        Value::Null | Value::Boolean(_) | Value::Int(_) | Value::Long(_) | Value::Float(_) | Value::Double(_) | Value::Bytes(_) | Value::String(_) => {
            Ok(vec![avro_to_arrow_primitive(value)?])
        }
        Value::Array(items) => {
            todo!()
        }
        Value::Map(items) => {
            todo!()
        }
        Value::Fixed(_, _) => unimplemented!(),
        Value::Union(items) => {
            todo!()
        }
        Value::Record(r) => {
            // call avro_to_arrow_primitive on each value in the record
            let values = r.iter().try_fold(Vec::new(), |mut acc, (_name, value)| -> Result<Vec<ArrayRef>, ArrowError> {
                let arrow_array = avro_to_arrow_primitive(value)?;
                acc.push(arrow_array);
                Ok(acc)
            })?;
            Ok(values)
        }
        _ => unimplemented!(),
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
        let value = iter.next().unwrap()
            .into_report()
            .change_context(DeserializeError::Avro)?;
        let mut fields = match value {
            Value::Record(record) => record,
            _ => error_stack::bail!(DeserializeError::BadRecord),
        };
        fields.push(("_publish_time".to_string(), Value::TimestampMillis(payload.metadata.publish_time as i64)));
        Ok(AvroWrapper { value: Value::Record(fields) })
    }
}

// todo de-duplicate this from output::pulsar
pub fn format_topic_url_str(tenant: &str, namespace: &str, name: &str) -> String {
    format!("persistent://{tenant}/{namespace}/topic-{name}")
}

pub(crate) async fn pulsar_consumer(uri: &String, schema: SchemaRef) -> error_stack::Result<Consumer<AvroWrapper, TokioExecutor>, Error> {
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
    let topic_url = format_topic_url_str(tenant, namespace, topic);

    let client = Pulsar::builder(broker_url, TokioExecutor)
        .build()
        .await
        .into_report()
        .change_context(Error::CreatePulsarReader)?;

    let formatted_schema = crate::execute::pulsar_schema::format_schema(schema).change_context(Error::CreatePulsarReader)?;
    let pulsar_schema = pulsar::message::proto::Schema {
        r#type: pulsar::message::proto::schema::Type::Avro as i32,
        schema_data: formatted_schema.as_bytes().to_vec(),
        ..Default::default()
    };

    // create a pulsar client that can read arbitrary avro Values
    let consumer: Consumer<AvroWrapper, TokioExecutor> = client
        .consumer()
        .with_options(ConsumerOptions::default().with_schema(pulsar_schema))
        // TODO figure out how to get tenant + namespace in here
        // .with_topic(topic_url)
        .with_topic(topic)
        .with_consumer_name("sparrow-consumer")
        .with_subscription_type(SubType::Exclusive)
        .with_subscription("sparrow-subscription")
        .build()
        .await
        .into_report()
        .change_context(Error::CreatePulsarReader)?;

    Ok(consumer)
}
