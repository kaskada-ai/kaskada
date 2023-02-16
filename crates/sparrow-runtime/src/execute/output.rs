use std::future::Future;
use std::sync::Arc;

use arrow::array::UInt64Array;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use derive_more::Display;
use error_stack::{FutureExt as ESFutureExt, IntoReport, Result, ResultExt};
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt};
use itertools::Itertools;
use sparrow_api::kaskada::v1alpha::data_type;
use sparrow_api::kaskada::v1alpha::execute_request::output_to::Destination;
use sparrow_api::kaskada::v1alpha::execute_request::{Limits, OutputTo};
use sparrow_core::{downcast_primitive_array, downcast_struct_array};

use crate::execute::key_hash_inverse::ThreadSafeKeyHashInverse;
use crate::execute::operation::OperationContext;
use crate::execute::output::Error::{ObjectStoreError, SchemaError};
use crate::execute::progress_reporter::ProgressUpdate;
use crate::Batch;

mod csv;
mod object_store;
mod parquet;
mod redis;

#[derive(Debug, Display)]
pub enum Error {
    SchemaError { detail: String },
    ObjectStoreError,
}

impl error_stack::Context for Error {}

/// Write the batches to the given output destination.
pub(super) fn write(
    context: &OperationContext,
    limits: Limits,
    batches: BoxStream<'static, Batch>,
    output_to: OutputTo,
    progress_updates_tx: tokio::sync::mpsc::Sender<ProgressUpdate>,
) -> Result<impl Future<Output = Result<(), Error>>, Error> {
    let sink_schema = determine_output_schema(context)?;

    // Clone things that need to move into the async stream.
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

            yield post_process_batch(&sink_schema, batch, &key_hash_inverse).await;

            if limit_rows && remaining == 0 {
                break;
            }
        }
    }
    .boxed();

    let destination = output_to.destination.expect("output destination");
    match destination {
        Destination::ObjectStore(store) => {
            Ok(
                object_store::write(store, sink_schema, progress_updates_tx, batches)
                    .change_context(ObjectStoreError)
                    .boxed(),
            )
        }
        Destination::Redis(redis) => {
            Ok(
                redis::write(redis, sink_schema, progress_updates_tx, batches)
                    .change_context(ObjectStoreError)
                    .boxed(),
            )
        }
    }
}

/// Write a batch to the given destination.
async fn post_process_batch(
    sink_schema: &SchemaRef,
    batch: Batch,
    key_hash_inverse: &Arc<ThreadSafeKeyHashInverse>,
) -> RecordBatch {
    // TODO: Move this into the writer once it's standard.
    // TODO: Support a single output column?
    // Unpack the one struct column into the corresponding fields.
    let mut fields = Vec::with_capacity(3 + sink_schema.fields().len());
    fields.extend_from_slice(&batch.columns()[0..3]);
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
    let last_op = context.plan.operations.last().ok_or(SchemaError {
        detail: "no operations in plan".to_owned(),
    })?;
    let result_type = last_op
        .expressions
        .iter()
        .filter(|expr| expr.output)
        .exactly_one()
        .map_err(|e| SchemaError {
            detail: format!("expected one output in last operation, but got {e:?}"),
        })?
        .result_type
        .as_ref()
        .ok_or(SchemaError {
            detail: "missing result type".to_owned(),
        })?;

    if let Some(data_type::Kind::Struct(data_fields)) = &result_type.kind {
        let mut fields = Vec::with_capacity(3 + data_fields.fields.len());
        // TODO: Share this with `KEY_FIELDS` in expression_evaluator if it stays
        // around.
        fields.extend_from_slice(&[
            Field::new(
                "_time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("_subsort", DataType::UInt64, false),
        ]);

        fields.extend_from_slice(&[Field::new("_key_hash", DataType::UInt64, false)]);
        fields.extend_from_slice(&[Field::new("_key", key_type, true)]);

        for field in data_fields.fields.iter() {
            fields.push(Field::new(
                &field.name,
                field
                    .data_type
                    .as_ref()
                    .ok_or(SchemaError {
                        detail: format!("missing data_type for field {:?}", &field),
                    })?
                    .try_into()
                    .into_report()
                    .change_context(SchemaError {
                        detail: "unable to convert Protobuf DataType to Arrow DataType".to_owned(),
                    })?,
                true,
            ));
        }

        Ok(Arc::new(Schema::new(fields)))
    } else {
        error_stack::bail!(SchemaError {
            detail: format!("unexpected result type for output: {result_type:?}")
        })
    }
}
