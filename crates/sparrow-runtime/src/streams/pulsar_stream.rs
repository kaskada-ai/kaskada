use crate::prepare::Error;
use crate::Batch;

use arrow::array::{ArrayRef, TimestampNanosecondArray};
use arrow::compute::SortColumn;
use arrow::error::ArrowError;
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use avro_rs::types::Value;
use error_stack::{IntoReport, IntoReportCompat, ResultExt};
use futures::stream::BoxStream;
use futures::Stream;
use futures_lite::stream::StreamExt;
use itertools::Itertools;
use pulsar::consumer::InitialPosition;

use pulsar::{
    Authentication, Consumer, ConsumerOptions, DeserializeMessage, Payload, Pulsar, SubType,
    TokioExecutor,
};
use sparrow_core::downcast_primitive_array;

use crate::execute::avro_arrow;
use sparrow_api::kaskada::v1alpha::{PulsarSubscription, TableConfig};
use std::collections::BTreeSet;
use std::io::Cursor;

use std::pin::Pin;
use std::time::Duration;
use tokio::time::timeout;

use super::pulsar_schema;

pub struct AvroWrapper {
    value: Value,
}

/// Creates a pulsar stream to be used during execution in a long-lived process.
///
/// This stream should not close naturally. It continually reads messages from the
/// stream, batches them, and passes them to the runtime layer.
pub fn stream_for_execution(
    schema: SchemaRef,
    consumer: Consumer<AvroWrapper, TokioExecutor>,
    last_publish_time: i64,
    config: TableConfig,
) -> impl Stream<Item = error_stack::Result<RecordBatch, Error>> + Send {
    async_stream::try_stream! {
        let mut reader = PulsarReader::new(schema, consumer, last_publish_time, config);
        loop {
        // TODO: This should indefinitely loop. Errors are unexpected.
        // TODO: Implement the input buffer logic here too
        // Another difference is that I need to know the time column. The
        // current code just uses the last publish time and asserts it's ordered.
        // But I need the time column to sort on.
        // The TableConfig in the TableInfo (available in the scan operation)
        // contains the time_column_name, so I can use that to get the time column.
        // Of course, I need it to be the StreamInfo, so I just need to make sure
        // StreamInfo contains the stuff I need.


        // I also need to eventually add slice plans and stuff.
            if let Some(next) = reader.next_result_async_buffer().await? {
                yield next
            } else {
                // Keep looping
            }
        }
    }
}

pub fn stream_for_prepare(
    schema: SchemaRef,
    consumer: Consumer<AvroWrapper, TokioExecutor>,
    last_publish_time: i64,
) -> impl Stream<Item = Result<RecordBatch, ArrowError>> {
    async_stream::try_stream! {
        let config = todo!();
        let mut reader = PulsarReader::new(schema, consumer, last_publish_time, config);
        while let Some(next) = reader.next_result_async().await? {
            yield next
        }
    }
}

struct PulsarReader {
    schema: SchemaRef,
    consumer: Consumer<AvroWrapper, TokioExecutor>,
    last_publish_time: i64,
    // Bounded disorder assumes an allowed amount of lateness in events,
    // which then allows this node to advance its watermark up to event
    // time (t - delta).
    bounded_lateness: i64,
    watermark: i64,
    leftovers: Vec<Vec<(String, Value)>>,
    // TODO: leftovers is a recordbatch
    // then I just uh...hm.
    // I need to append the new batch to the leftovers.
    // But I need to make sure I'm using the correct max event time
    // which should come from the leftovers.
    config: TableConfig,
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
    #[display(fmt = "internal error")]
    InternalError,
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
    pub fn new(
        schema: SchemaRef,
        consumer: Consumer<AvroWrapper, TokioExecutor>,
        last_publish_time: i64,
        config: TableConfig,
    ) -> Self {
        PulsarReader {
            schema,
            consumer,
            last_publish_time,
            bounded_lateness: todo!(),
            watermark: todo!(),
            leftovers: todo!(),
            config,
        }
    }

    async fn next_result_async_buffer(
        &mut self,
    ) -> error_stack::Result<Option<RecordBatch>, Error> {
        // TODO: FRAZ -
        // in the first iteration, maybe you just ignore timeouts as well
        // and you just do self.consumer.try_next().await?
        // and if we get a message and hit max batch size, we produce shit.
        // if we hit the end of the stream, then too bad. This won't demo well...so maybe we do need it though.
        // Eh, it's an iteration.
        tracing::debug!("reading pulsar messages");
        let max_batch_size = 100000; // TODO make this adaptive based on the size of the messages
        let mut avro_values = Vec::with_capacity(max_batch_size);

        // Add any leftovers to the new current batch
        // TODO: make sure this is tested
        // drain should drain everything each time
        self.leftovers.drain(..).map(|i| avro_values.push(i));

        while avro_values.len() < max_batch_size {
            // read the next entry from the pulsar consumer.
            // this is fragile since tokio has no idea what is going on inside the consumer,
            // so it's entirely possible to time out while actively reading messages from the broker.
            // this is no big deal if we've already read some messages (we'll just process the ones
            // we read, then come back for more) but it's a problem if we haven't, because "no messages
            // to read" is how we detect the end of the stream.
            //
            // this problem goes away once we have a long-running sparrow process that can just wait
            // indefinitely to read messages, but in the meantime, we use a larger-than-should-be-necessary
            // timeout to try to avoid this problem.
            //
            // experimentally, 10ms works fine locally, and 1000ms works fine with Astra.
            let next_result = timeout(Duration::from_millis(1000), self.consumer.try_next()).await;
            let Ok(msg) = next_result else {
                tracing::trace!("timed out reading next message");
                break;
            };

            // TODO: Make errors clearer
            let msg = msg.map_err(|e| Error::ReadInput)?;

            match msg {
                Some(msg) => {
                    self.consumer
                        .ack(&msg)
                        .await
                        .map_err(|e| Error::ReadInput)?;
                    let result: error_stack::Result<AvroWrapper, DeserializeError> =
                        msg.deserialize();
                    let aw = match result {
                        Ok(aw) => aw,
                        Err(e) => {
                            // let wrapped_error = DeserializeErrorWrapper::from(e);
                            // tracing::debug!("error deserializing message: {:#?}", &wrapped_error);
                            error_stack::bail!(e.change_context(Error::ReadInput));
                            // // tracing::debug!("error deserializing message: {:#?}", wrapped_error);
                            // return Err(ArrowError::from_external_error(Box::new(wrapped_error)));
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

                            error_stack::bail!(e.change_context(Error::ReadInput));
                        }
                    }
                }
                None => {
                    // try_next will return None if the stream is closed, which shouldn't
                    // happen in the pulsar scenario.  maybe if the broker shuts down?
                    tracing::debug!("read None from consumer -- not sure how this happens. The materialization must be restarted.");
                    break;
                }
            }
        }

        // TODO: Split batch at time max_event_time - bounded_lateness
        tracing::debug!("read {} messages", avro_values.len());
        match avro_values.len() {
            0 => Ok(None),
            _ => {
                let arrow_data =
                    avro_arrow::avro_to_arrow(avro_values).map_err(|e| Error::ReadInput)?;
                let batch = RecordBatch::try_new(self.schema.clone(), arrow_data)
                    .map_err(|e| Error::ReadInput)?;

                // TODO: sort and take
                let time_column = batch
                    .column_by_name(&self.config.time_column_name)
                    .ok_or(Error::ReadInput)?;
                // TODO: Subsort column?

                let key_column = batch
                    .column_by_name(&self.config.group_column_name)
                    .ok_or(Error::ReadInput)?;

                let sort_indices = arrow::compute::lexsort_to_indices(
                    &[
                        SortColumn {
                            values: time_column.clone(),
                            options: None,
                        },
                        SortColumn {
                            values: key_column.clone(),
                            options: None,
                        },
                    ],
                    None,
                )
                .into_report()
                .change_context(Error::ReadInput)?;

                let transform = |column: &ArrayRef| {
                    arrow::compute::take(column.as_ref(), &sort_indices, None)
                        .into_report()
                        .change_context(Error::ReadInput)
                };
                let sorted_columns: Vec<_> = batch
                    .columns()
                    .iter()
                    .map(|column| transform(column))
                    .try_collect()?;

                let sorted_batch = RecordBatch::try_new(self.schema.clone(), sorted_columns)
                    .map_err(|_| Error::ReadInput)?;

                // TODO: Take only up to max_event_time - bounded_lateness
                if sorted_batch.num_rows() == 0 {
                    return Ok(None);
                }

                let time_column = sorted_batch
                    .column_by_name(&self.config.time_column_name)
                    .ok_or(Error::ReadInput)?;
                let time_column: &TimestampNanosecondArray =
                    downcast_primitive_array(time_column.as_ref())
                        .into_report()
                        .change_context(Error::ReadInput)?;
                let min_event_time = time_column.value(0);
                let split_at = time_column.value(time_column.len() - 1) - self.bounded_lateness;

                // If the min event time is greater than the (max_event_time - bounded_lateness),
                // we cannot produce any rows.
                if min_event_time >= split_at {
                    // TODO: Add to leftovers
                    // TODO: Also need to check leftovers when creating new batch here.
                    Ok(None)
                } else {
                    // Split the batch at the max_event_time - bounded_lateness.
                    let times = time_column.values();
                    let split_point = match times.binary_search(&split_at) {
                        Ok(mut found_index) => {
                            // Just do a linear search for the first value less than split time.
                            while found_index > 0 && times[found_index - 1] == split_at {
                                found_index -= 1
                            }
                            found_index
                        }
                        Err(not_found_index) => not_found_index,
                    };

                    let lt = if split_point > 0 {
                        let lt = sorted_batch.slice(0, split_point);
                        Some(
                            Batch::try_new_from_batch(lt)
                                .into_report()
                                .change_context(Error::ReadInput)?,
                        )
                    } else {
                        None
                    };

                    let gte = if split_point < sorted_batch.num_rows() {
                        let gte =
                            sorted_batch.slice(split_point, sorted_batch.num_rows() - split_point);
                        Some(
                            Batch::try_new_from_batch(gte)
                                .into_report()
                                .change_context(Error::ReadInput)?,
                        )
                    } else {
                        None
                    };
                    Ok((lt, gte))
                }
            }
        }
    }

    // using ArrowError is not a great fit but that is what PrepareIter requires
    async fn next_result_async(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        tracing::debug!("reading pulsar messages");
        let max_batch_size = 100000; // TODO make this adaptive based on the size of the messages
        let mut avro_values = Vec::with_capacity(max_batch_size);
        while avro_values.len() < max_batch_size {
            // read the next entry from the pulsar consumer.
            // this is fragile since tokio has no idea what is going on inside the consumer,
            // so it's entirely possible to time out while actively reading messages from the broker.
            // this is no big deal if we've already read some messages (we'll just process the ones
            // we read, then come back for more) but it's a problem if we haven't, because "no messages
            // to read" is how we detect the end of the stream.
            //
            // this problem goes away once we have a long-running sparrow process that can just wait
            // indefinitely to read messages, but in the meantime, we use a larger-than-should-be-necessary
            // timeout to try to avoid this problem.
            //
            // experimentally, 10ms works fine locally, and 1000ms works fine with Astra.
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

        // ensure that _publish_time never goes backwards
        for avro_value in &mut avro_values {
            for (field_name, field_value) in avro_value {
                if field_name == "_publish_time" {
                    match field_value {
                        Value::TimestampMillis(publish_time) => {
                            if *publish_time < self.last_publish_time {
                                *publish_time = self.last_publish_time;
                            } else {
                                self.last_publish_time = *publish_time;
                            }
                        }
                        _ => {
                            let e = error_stack::report!(DeserializeError::InternalError);
                            return Err(ArrowError::from_external_error(Box::new(
                                DeserializeErrorWrapper::from(e),
                            )));
                        }
                    }
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

pub async fn consumer(
    subscription: &PulsarSubscription,
    schema: SchemaRef,
) -> error_stack::Result<Consumer<AvroWrapper, TokioExecutor>, Error> {
    let config = subscription.config.as_ref().ok_or(Error::Internal)?;
    // specifying persistent:// or non-persistent:// appears to be optional
    let topic_url = format!(
        "{}/{}/{}",
        config.tenant, config.namespace, config.topic_name
    );

    let auth_token = pulsar_schema::pulsar_auth_token(config.auth_params.as_str())
        .change_context(Error::CreatePulsarReader)?;
    let auth = Authentication {
        name: "token".to_string(),
        data: auth_token.as_bytes().to_vec(),
    };
    let client = Pulsar::builder(&config.broker_service_url, TokioExecutor)
        .with_auth(auth)
        .build()
        .await
        .into_report()
        .change_context(Error::CreatePulsarReader)?;

    let formatted_schema =
        pulsar_schema::format_schema(schema).change_context(Error::CreatePulsarReader)?;
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
            &subscription.subscription_id
        ))
        .with_subscription_type(SubType::Exclusive)
        .with_subscription(&subscription.subscription_id)
        .build()
        .await
        .into_report()
        .change_context(Error::CreatePulsarReader)?;

    Ok(consumer)
}
