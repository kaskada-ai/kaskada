use std::time::SystemTime;

use arrow::{datatypes::SchemaRef, error::ArrowError, record_batch::RecordBatch};
use avro_rs::types::Value;
use avro_rs::Reader;
use avro_rs::Schema;
use error_stack::{IntoReport, ResultExt};
use futures_lite::Stream;
use kafka::consumer::Consumer;
use sparrow_api::kaskada::v1alpha::KafkaSubscription;

use crate::streams::get_columns_to_read;
use crate::streams::DeserializeError;
use crate::streams::DeserializeErrorWrapper;

#[derive(derive_more::Display, Debug)]
pub enum Error {
    #[display(fmt = "missing kafka config")]
    MissingKafkaConfig,
    #[display(fmt = "unable to create consume")]
    CreateKafkaConsumer,
}

impl error_stack::Context for Error {}

pub fn execution_stream(
    kafka_avro_schema: Schema,
    raw_schema: SchemaRef,
    projected_schema: SchemaRef,
    consumer: Consumer,
) -> impl Stream<Item = Result<RecordBatch, ArrowError>> {
    async_stream::try_stream! {
        let mut reader = KafkaReader::new(kafka_avro_schema, raw_schema, projected_schema, consumer);
        loop {
            if let Some(next) = reader.next_result_async().await? {
                yield next
            } else {
                // Keep looping - this may happen if we timed out trying to read from the stream
            }
        }
    }
}

struct KafkaReader {
    kafka_avro_schema: Schema,
    raw_schema: SchemaRef,
    /// The projected schema; includes only columns that are needed by the query.
    projected_schema: SchemaRef,
    /// Kafka consumer client
    consumer: Consumer,
}

impl KafkaReader {
    pub fn new(
        kafka_avro_schema: Schema,
        raw_schema: SchemaRef,
        projected_schema: SchemaRef,
        consumer: Consumer,
    ) -> Self {
        KafkaReader {
            kafka_avro_schema,
            raw_schema,
            projected_schema,
            consumer,
        }
    }

    async fn next_result_async(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        tracing::debug!("reading kafka messages");
        let max_batch_size = 100000;
        let mut avro_values = Vec::with_capacity(max_batch_size);
        let start = SystemTime::now();
        while avro_values.len() < max_batch_size {
            let since_start = SystemTime::now().duration_since(start).unwrap();
            if since_start.as_millis() > 1000 {
                break;
            }
            let next_results = self.consumer.poll();
            let Ok(msg) = next_results else {
                tracing::debug!("unable to poll kafka stream");
                break;
            };
            for ms in msg.iter() {
                for m in ms.messages() {
                    let reader = Reader::with_schema(&self.kafka_avro_schema, m.value).unwrap();
                    for kafka_msg in reader {
                        let kafka_msg = kafka_msg.unwrap();
                        tracing::debug!("read kafka message: {:?}", kafka_msg);
                        match kafka_msg {
                            Value::Record(fields) => avro_values.push(fields),
                            _ => {
                                let e = error_stack::report!(DeserializeError::UnsupportedType)
                                    .attach_printable(format!(
                                        "expected a record but got {:?}",
                                        kafka_msg
                                    ));
                                return Err(ArrowError::from_external_error(Box::new(
                                    DeserializeErrorWrapper::from(e),
                                )));
                            }
                        }
                    }
                }
            }
        }
        tracing::debug!("read {} messages", avro_values.len());
        match avro_values.len() {
            0 => Ok(None),
            _ => {
                let arrow_data = sparrow_arrow::avro::avro_to_arrow(avro_values).map_err(|e| {
                    tracing::error!("avro_to_arrow error: {}", e);
                    ArrowError::from_external_error(Box::new(e))
                })?;
                let batch = RecordBatch::try_new(self.raw_schema.clone(), arrow_data)?;
                tracing::debug!("produced batch: {:?}", batch);
                // Note that the _publish_time is dropped here. This field is added for the purposes of
                // prepare, where the `time` column is automatically set to the `_publish_time`.
                let columns_to_read = get_columns_to_read(&self.raw_schema, &self.projected_schema);
                let columns: Vec<_> = columns_to_read
                    .iter()
                    .map(|index| batch.column(*index).clone())
                    .collect();

                Ok(RecordBatch::try_new(self.projected_schema.clone(), columns).map(Some)?)
            }
        }
    }
}

pub async fn consumer(subscription: &KafkaSubscription) -> error_stack::Result<Consumer, Error> {
    let config = subscription
        .config
        .as_ref()
        .ok_or(Error::MissingKafkaConfig)?;
    let consumer = Consumer::from_hosts(config.hosts.to_owned())
        .with_topic(config.topic.to_owned())
        .create()
        .into_report()
        .change_context(Error::CreateKafkaConsumer)?;
    tracing::info!("created kafka consumer");
    Ok(consumer)
}
