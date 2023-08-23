use std::future::Future;
use std::sync::Arc;

use arrow::array::UInt64Array;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use error_stack::{FutureExt as ESFutureExt, IntoReport, Result, ResultExt};
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt};
use itertools::Itertools;
use sparrow_api::kaskada::v1alpha::execute_request::Limits;
use sparrow_api::kaskada::v1alpha::{data_type, ObjectStoreDestination, PulsarDestination};
use sparrow_arrow::downcast::{downcast_primitive_array, downcast_struct_array};

use crate::execute::operation::OperationContext;
use crate::execute::progress_reporter::ProgressUpdate;
use crate::key_hash_inverse::ThreadSafeKeyHashInverse;
use crate::Batch;

mod object_store;

pub mod pulsar;

#[derive(Debug, derive_more::Display)]
pub enum Error {
    Schema {
        detail: String,
    },
    #[display(fmt = "writing to destination '{_0}'")]
    WritingToDestination(&'static str),
    UnspecifiedDestination,
    #[allow(dead_code)]
    FeatureNotEnabled {
        feature: String,
    },
}

impl error_stack::Context for Error {}

/// The output destination.
///
/// TODO: Replace the protobuf destinations with pure Rust structs.
#[derive(Debug)]
pub enum Destination {
    ObjectStore(ObjectStoreDestination),
    #[cfg(feature = "pulsar")]
    Pulsar(PulsarDestination),
    Channel(tokio::sync::mpsc::Sender<RecordBatch>),
}

impl TryFrom<sparrow_api::kaskada::v1alpha::Destination> for Destination {
    type Error = error_stack::Report<Error>;

    fn try_from(
        value: sparrow_api::kaskada::v1alpha::Destination,
    ) -> std::result::Result<Self, Self::Error> {
        let destination = value.destination.ok_or(Error::UnspecifiedDestination)?;
        match destination {
            sparrow_api::kaskada::v1alpha::destination::Destination::ObjectStore(destination) => {
                Ok(Destination::ObjectStore(destination))
            }
            #[cfg(not(feature = "pulsar"))]
            sparrow_api::kaskada::v1alpha::destination::Destination::Pulsar(_) => {
                error_stack::bail!(Error::FeatureNotEnabled {
                    feature: "pulsar".to_owned()
                })
            }
            #[cfg(feature = "pulsar")]
            sparrow_api::kaskada::v1alpha::destination::Destination::Pulsar(pulsar) => {
                Ok(Destination::Pulsar(pulsar))
            }
        }
    }
}

/// Write the batches to the given output destination.
pub(super) fn write(
    context: &OperationContext,
    limits: Limits,
    batches: BoxStream<'static, Batch>,
    progress_updates_tx: tokio::sync::mpsc::Sender<ProgressUpdate>,
    destination: Destination,
    max_batch_size: Option<usize>,
) -> error_stack::Result<impl Future<Output = Result<(), Error>> + 'static, Error> {
    let sink_schema = determine_output_schema(context)?;

    // Clone things that need to move into the async stream.
    let max_batch_size = max_batch_size.unwrap_or(usize::MAX);
    let sink_schema_clone = sink_schema.clone();
    let key_hash_inverse = context.key_hash_inverse.clone();
    let batches = async_stream::stream! {
        // Move / copy into the stream.
        let sink_schema = sink_schema_clone;
        let key_hash_inverse = key_hash_inverse;

        let limit_rows = limits.preview_rows > 0;
        let mut remaining = limits.preview_rows as usize;
        for await batch in batches {
            let batch = if limit_rows {
                if batch.num_rows() > remaining {
                    let batch = batch.slice(0, remaining);
                    remaining = 0;
                    batch
                } else {
                    remaining -= batch.num_rows();
                    batch
                }
            } else {
                batch
            };


            if batch.num_rows() > max_batch_size {
                for start in (0..batch.num_rows()).step_by(max_batch_size) {
                    let end = (start + max_batch_size).min(batch.num_rows());
                    let length = end - start;
                    let batch = batch.slice(start, length);
                    yield post_process_batch(&sink_schema, batch, &key_hash_inverse).await;
                }
            } else {
                yield post_process_batch(&sink_schema, batch, &key_hash_inverse).await;
            }


            if limit_rows && remaining == 0 {
                break;
            }
        }
    }
    .boxed();

    match destination {
        Destination::ObjectStore(destination) => Ok(object_store::write(
            context.object_stores.clone(),
            destination,
            sink_schema,
            progress_updates_tx,
            batches,
        )
        .change_context(Error::WritingToDestination("object_store"))
        .boxed()),
        Destination::Channel(channel) => {
            Ok(write_to_channel(batches, channel, progress_updates_tx).boxed())
        }
        #[cfg(feature = "pulsar")]
        Destination::Pulsar(pulsar) => {
            Ok(
                pulsar::write(pulsar, sink_schema, progress_updates_tx, batches)
                    .change_context(Error::WritingToDestination("pulsar"))
                    .boxed(),
            )
        }
    }
}

async fn write_to_channel(
    mut batches: BoxStream<'static, RecordBatch>,
    channel: tokio::sync::mpsc::Sender<RecordBatch>,
    _progress_updates_tx: tokio::sync::mpsc::Sender<ProgressUpdate>,
) -> error_stack::Result<(), Error> {
    while let Some(next) = batches.next().await {
        channel
            .send(next)
            .await
            .map_err(|_e| error_stack::report!(Error::WritingToDestination("channel")))?;

        // progress_updates_tx.send(ProgressUpdate::Output { num_rows })
    }

    Ok(())
}

/// Adds additional information to an output batch.
async fn post_process_batch(
    sink_schema: &SchemaRef,
    batch: Batch,
    key_hash_inverse: &Arc<ThreadSafeKeyHashInverse>,
) -> RecordBatch {
    // TODO: Move this into the writer once it's standard.
    // TODO: Support a single output column?
    // Unpack the one struct column into the corresponding fields.
    let mut fields = Vec::with_capacity(4 + sink_schema.fields().len());

    // Add the `_time, _subsort, _key_hash` columns
    fields.extend_from_slice(&batch.columns()[0..3]);

    // Get the original keys using the inverse key hash map
    let key_col = &batch.columns()[2];
    let key_col: &UInt64Array = downcast_primitive_array(key_col.as_ref()).expect("key_col is u64");
    let key_col = key_hash_inverse
        .inverse(key_col)
        .await
        .expect("inverses are defined");
    fields.extend_from_slice(&[key_col]);

    let struct_array =
        downcast_struct_array(batch.columns()[3].as_ref()).expect("value is struct array");
    if batch.columns()[3].null_count() > 0 {
        // The outer struct contains null values, but the corresponding columns inside
        // the struct does not reflect the nulls. Apply the same null flags on top
        // of the column null values.
        let is_null = arrow::compute::is_null(&batch.columns()[3]).expect("is_null never errors");
        for column in struct_array.columns() {
            fields
                .push(arrow::compute::nullif(column.as_ref(), &is_null).expect("null_if succeeds"));
        }
    } else {
        fields.extend_from_slice(struct_array.columns());
    }

    RecordBatch::try_new(sink_schema.clone(), fields).expect("resulting batch is valid")
}

/// Determine the output schema.
///
/// This uses the `plan` to locate the result type.
///
/// This uses the `key_hash_inverse
/// This currently requires knowledge of how we will post-process output batches
/// (by adding the key column back).
fn determine_output_schema(context: &OperationContext) -> Result<SchemaRef, Error> {
    let key_type = context.key_hash_inverse.key_type.clone();

    // There should be cleaner ways to determine the output schema.
    // But -- find the only output expression in the last operation.
    // It should produce a record, which should be the sink schema.
    let last_op = context.plan.operations.last().ok_or(Error::Schema {
        detail: "no operations in plan".to_owned(),
    })?;
    let result_type = last_op
        .expressions
        .iter()
        .filter(|expr| expr.output)
        .exactly_one()
        .map_err(|e| Error::Schema {
            detail: format!("expected one output in last operation, but got {e:?}"),
        })?
        .result_type
        .as_ref()
        .ok_or(Error::Schema {
            detail: "missing result type".to_owned(),
        })?;

    if let Some(data_type::Kind::Struct(data_fields)) = &result_type.kind {
        let mut fields = Vec::with_capacity(4 + data_fields.fields.len());
        // TODO: Share this with `KEY_FIELDS` in expression_evaluator if it stays
        // around.
        fields.extend_from_slice(&[
            Field::new(
                "_time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("_subsort", DataType::UInt64, false),
            Field::new("_key_hash", DataType::UInt64, false),
            Field::new("_key", key_type, true),
        ]);

        for field in data_fields.fields.iter() {
            fields.push(Field::new(
                &field.name,
                field
                    .data_type
                    .as_ref()
                    .ok_or(Error::Schema {
                        detail: format!("missing data_type for field {:?}", &field),
                    })?
                    .try_into()
                    .into_report()
                    .change_context(Error::Schema {
                        detail: "unable to convert Protobuf DataType to Arrow DataType".to_owned(),
                    })?,
                true,
            ));
        }

        Ok(Arc::new(Schema::new(fields)))
    } else {
        error_stack::bail!(Error::Schema {
            detail: format!("unexpected result type for output: {result_type:?}")
        })
    }
}
