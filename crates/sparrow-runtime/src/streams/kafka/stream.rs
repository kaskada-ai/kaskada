use arrow::{datatypes::SchemaRef, error::ArrowError, record_batch::RecordBatch};
use avro_rs::Reader;
use avro_rs::Schema;
use error_stack::{IntoReport, ResultExt};
use futures_lite::Stream;
use kafka::consumer::Consumer;
use sparrow_api::kaskada::v1alpha::KafkaSubscription;

#[derive(derive_more::Display, Debug)]
pub enum Error {
    #[display(fmt = "missing kafka config")]
    MissingKafkaConfig,
    #[display(fmt = "unable to create consume")]
    CreateKafkaConsumer,
}

impl error_stack::Context for Error {}

pub fn execution_stream(
    raw_schema: Schema,
    projected_schema: SchemaRef,
    consumer: Consumer,
) -> impl Stream<Item = Result<RecordBatch, ArrowError>> {
    async_stream::try_stream! {
        let mut reader = KafkaReader::new(raw_schema, projected_schema, consumer);
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
    /// The raw schema; includes all columns in the stream.
    raw_schema: Schema,
    /// The projected schema; includes only columns that are needed by the query.
    projected_schema: SchemaRef,
    /// Kafka consumer client
    consumer: Consumer,
}

impl KafkaReader {
    pub fn new(raw_schema: Schema, projected_schema: SchemaRef, consumer: Consumer) -> Self {
        KafkaReader {
            raw_schema,
            projected_schema,
            consumer,
        }
    }

    async fn next_result_async(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        tracing::debug!("reading kafka messages");
        let max_batch_size = 100000;
        let mut avro_values = Vec::with_capacity(max_batch_size);
        while avro_values.len() < max_batch_size {
            let next_results = self.consumer.poll();
            let Ok(msg) = next_results else {
                break;
            };
            for ms in msg.iter() {
                for m in ms.messages() {
                    let reader = Reader::with_schema(&self.raw_schema, m.value).unwrap();
                    for value in reader {
                        let value = value.unwrap();
                        tracing::debug!("read kafka message: {:?}", value);
                        avro_values.push(value);
                    }
                }
            }
        }
        tracing::debug!("read {} messages", avro_values.len());
        todo!();
    }
}

pub async fn consumer(subscription: &KafkaSubscription) -> error_stack::Result<Consumer, Error> {
    let config = subscription
        .config
        .as_ref()
        .ok_or(Error::MissingKafkaConfig)?;
    let consumer = Consumer::from_hosts(config.hosts.to_owned())
        .with_topic(config.topic.to_owned())
        // TODO: Set the group
        // .with_group(group)
        .create()
        .into_report()
        .change_context(Error::CreateKafkaConsumer)?;
    tracing::info!("created kafka consumer");
    Ok(consumer)
}
