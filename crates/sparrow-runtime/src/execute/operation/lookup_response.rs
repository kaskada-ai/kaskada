use std::sync::Arc;

use anyhow::Context;
use arrow::array::{Array, AsArray, ListArray, UInt32Array, UInt64Array};
use async_trait::async_trait;
use error_stack::{IntoReport, IntoReportCompat, ResultExt};
use futures::StreamExt;
use itertools::Itertools;
use sparrow_api::kaskada::v1alpha::operation_plan;
use sparrow_arrow::downcast::downcast_primitive_array;
use sparrow_instructions::ComputeStore;
use tokio_stream::wrappers::ReceiverStream;

use super::BoxedOperation;
use crate::execute::error::{invalid_operation, Error};
use crate::execute::operation::expression_executor::InputColumn;
use crate::execute::operation::single_consumer_helper::SingleConsumerHelper;
use crate::execute::operation::{InputBatch, Operation};
use crate::Batch;

#[derive(Debug)]
pub(super) struct LookupResponseOperation {
    requesting_key_hash_column: usize,
    /// The input stream of batches.
    input_stream: ReceiverStream<Batch>,
    helper: SingleConsumerHelper,
}

#[async_trait]
impl Operation for LookupResponseOperation {
    fn restore_from(
        &mut self,
        operation_index: u8,
        compute_store: &ComputeStore,
    ) -> anyhow::Result<()> {
        self.helper.restore_from(operation_index, compute_store)
    }

    fn store_to(&self, operation_index: u8, compute_store: &ComputeStore) -> anyhow::Result<()> {
        self.helper.store_to(operation_index, compute_store)
    }

    async fn execute(
        &mut self,
        sender: tokio::sync::mpsc::Sender<InputBatch>,
    ) -> error_stack::Result<(), Error> {
        while let Some(incoming) = self.input_stream.next().await {
            if let Some(next_input) = self
                .create_input(incoming)
                .into_report()
                .change_context(Error::internal())?
            {
                sender
                    .send(next_input)
                    .await
                    .into_report()
                    .change_context(Error::internal())?;
            }

            // TODO: Batch bound refactoring
            // Need to send an empty batch with a monotically increasing time
            // bound to indicate progress to consumers.
        }

        Ok(())
    }
}

impl LookupResponseOperation {
    /// Create the input stream for lookup responses.
    ///
    /// This operation converts from a domain containing keys associated
    /// with a "foreign" grouping to a domain containing keys associated
    /// with a "primary" grouping. The primary key and subsort are
    /// provided as arguments received from a corresponding lookup request.
    pub(super) fn create(
        operation: operation_plan::LookupResponseOperation,
        input_channels: Vec<tokio::sync::mpsc::Receiver<Batch>>,
        input_columns: &[InputColumn],
    ) -> error_stack::Result<BoxedOperation, super::Error> {
        let input_channel = input_channels
            .into_iter()
            .exactly_one()
            .into_report()
            .change_context(Error::internal_msg("expected one channel"))?;

        // Lookup responses don't perform any interpolation of inputs, so they are
        // effectively `Null`. We should make sure that the subsort, key_hash,
        // and any other accessed input columns are `Interpolation::Null`.

        let requesting_key_hash_column = operation
            .requesting_key_hash
            .ok_or_else(|| invalid_operation!("missing requesting_key_hash"))?
            .input_column as usize;

        Ok(Box::new(Self {
            requesting_key_hash_column,
            input_stream: ReceiverStream::new(input_channel),
            helper: SingleConsumerHelper::try_new(operation.foreign_operation, input_columns)
                .into_report()
                .change_context(Error::internal_msg("error creating single consumer helper"))?,
        }))
    }

    fn create_input(&mut self, foreign_batch: Batch) -> anyhow::Result<Option<InputBatch>> {
        // For each request, we may have 1 or more "requesting key hashes" to send the
        // response to.
        let requesting_key_hash_list = foreign_batch.column(self.requesting_key_hash_column);

        if requesting_key_hash_list.null_count() == foreign_batch.num_rows() {
            return Ok(None);
        }

        let requesting_key_hash_list: &ListArray = requesting_key_hash_list.as_ref().as_list();
        let requesting_key_hashes = requesting_key_hash_list.values();
        let requesting_key_hashes: &UInt64Array =
            downcast_primitive_array(requesting_key_hashes.as_ref())?;
        let requesting_key_hashes = requesting_key_hashes.values();
        let requesting_key_hash_offsets = requesting_key_hash_list.value_offsets();

        // We use the requesting key hashes length, which should be the number of values
        // in all the lists.
        let mut take_indices = UInt32Array::builder(requesting_key_hashes.len());
        let mut key_hash = UInt64Array::builder(requesting_key_hash_list.len());
        for i in 0..requesting_key_hash_list.len() {
            let value_start = requesting_key_hash_offsets[i] as usize;
            let value_end = requesting_key_hash_offsets[i + 1] as usize;
            let value_length = value_end - value_start;
            if value_length == 0 || requesting_key_hash_list.is_null(i) {
                // If the value at index `i` has length 0 (or is null), we
                // continue. We do this because there are *no* requests to
                // respond to, thus there should be *no* rows added to the
                // `LookupRequest` operation.
                continue;
            }

            // Append `value_length` copies of the current indices. For the
            // time/subsort/values this used with `take` will fix things.
            //
            // SAFETY: The repeat/take iterator has a trusted length.
            unsafe {
                take_indices.append_trusted_len_iter(std::iter::repeat(i as u32).take(value_length))
            };

            // Append the requesting key hashes from the list.
            key_hash.append_slice(&requesting_key_hashes[value_start..value_end]);
        }

        let take_indices = take_indices.finish();
        let key_hash = Arc::new(key_hash.finish());

        let time = arrow::compute::take(foreign_batch.column(0), &take_indices, None)?;
        let subsort = arrow::compute::take(foreign_batch.column(1), &take_indices, None)?;

        // We need to sort the `(time, subsort, key_hash)` tuples, because the original
        // keys may not be in the same order as the foreign keys.
        //
        // TODO: Lookup optimization
        // We could keep track in the previous loop on whether or not this was
        // necessary, but always sorting keeps it simple for now.
        let sort_indices = crate::sort_in_time::sort_in_time_indices_dyn(
            time.as_ref(),
            subsort.as_ref(),
            key_hash.as_ref(),
        )?;
        let key_hash = arrow::compute::take(key_hash.as_ref(), &sort_indices, None)?;

        let take_indices = arrow::compute::take(&take_indices, &sort_indices, None)?;
        let take_indices: &UInt32Array = downcast_primitive_array(take_indices.as_ref())?;

        self.helper.new_input_batch_with_keys(
            &foreign_batch,
            time,
            subsort,
            key_hash,
            |unfiltered_column| {
                arrow::compute::take(unfiltered_column, take_indices, None)
                    .context("take for lookup response operation")
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field};
    use sparrow_api::kaskada::v1alpha::operation_plan::LookupResponseOperation;
    use sparrow_api::kaskada::v1alpha::{self, data_type};
    use sparrow_api::kaskada::v1alpha::{
        expression_plan, operation_input_ref, operation_plan, ExpressionPlan, OperationInputRef,
        OperationPlan,
    };

    use crate::execute::operation::testing::{batch_from_json, run_operation_json};

    #[tokio::test]
    async fn test_lookup_response_same_times_ordered() {
        let input = r#"
            {"_time": 2000, "_subsort": 0, "_key_hash": 1, "e0": [1],    "e1": 1,  "e2": 2.7}
            {"_time": 3000, "_subsort": 1, "_key_hash": 1, "e0": [5],    "e1": 2,  "e2": 3.8}
            {"_time": 3000, "_subsort": 2, "_key_hash": 1, "e0": null,   "e1": 42, "e2": 4.0}
            {"_time": 3000, "_subsort": 3, "_key_hash": 1, "e0": [],     "e1": 42, "e2": null}
            {"_time": 4000, "_subsort": 0, "_key_hash": 1, "e0": [3, 4], "e1": 3,  "e2": 7}
            "#;

        insta::assert_snapshot!(run_test(input).await, @r###"
        {"_key_hash":1,"_subsort":0,"_time":"1970-01-01T00:00:00.000002","e0":1,"e1":2.7}
        {"_key_hash":5,"_subsort":1,"_time":"1970-01-01T00:00:00.000003","e0":2,"e1":3.8}
        {"_key_hash":3,"_subsort":0,"_time":"1970-01-01T00:00:00.000004","e0":3,"e1":7.0}
        {"_key_hash":4,"_subsort":0,"_time":"1970-01-01T00:00:00.000004","e0":3,"e1":7.0}
        "###)
    }

    #[tokio::test]
    async fn test_lookup_response_singletons() {
        let input = r#"
        {"_time": 2000, "_subsort": 0, "_key_hash": 1, "e0": [1],    "e1": 1,  "e2": 2.7}
        {"_time": 3000, "_subsort": 1, "_key_hash": 1, "e0": [5],    "e1": 2,  "e2": 3.8}
        {"_time": 3000, "_subsort": 2, "_key_hash": 1, "e0": null,   "e1": 42, "e2": 4.0}
        {"_time": 3000, "_subsort": 3, "_key_hash": 1, "e0": [],     "e1": 42, "e2": null}
        {"_time": 4000, "_subsort": 0, "_key_hash": 1, "e0": [3],    "e1": 3,  "e2": 7}
        "#;

        insta::assert_snapshot!(run_test(input).await, @r###"
        {"_key_hash":1,"_subsort":0,"_time":"1970-01-01T00:00:00.000002","e0":1,"e1":2.7}
        {"_key_hash":5,"_subsort":1,"_time":"1970-01-01T00:00:00.000003","e0":2,"e1":3.8}
        {"_key_hash":3,"_subsort":0,"_time":"1970-01-01T00:00:00.000004","e0":3,"e1":7.0}
        "###);
    }

    #[tokio::test]
    async fn test_lookup_response_unordered() {
        // This tests the case of where the foreign keys are out of order
        // compared to the primary keys that requested them.
        let input = r#"
            {"_time": 4000, "_subsort": 0, "_key_hash": 1, "e0": [2], "e1": 3, "e2": 7}
            {"_time": 4000, "_subsort": 0, "_key_hash": 2, "e0": [1], "e1": 4, "e2": 8}
            "#;

        insta::assert_snapshot!(run_test(input).await, @r###"
        {"_key_hash":1,"_subsort":0,"_time":"1970-01-01T00:00:00.000004","e0":4,"e1":8.0}
        {"_key_hash":2,"_subsort":0,"_time":"1970-01-01T00:00:00.000004","e0":3,"e1":7.0}
        "###)
    }

    /// Runs a test of the lookup request operation.
    ///
    /// The input should be a JSON string containing the key columns and two
    /// values `e0` (a list of the requesting subsorts). `e1` and `e2` are the
    /// foreign values to be returned (i64 and f64 respectively). The
    /// expectation is that foreign values would be interpolated to the rows
    /// with non-null `e0`.
    async fn run_test(input_json: &str) -> String {
        let plan = OperationPlan {
            expressions: vec![
                ExpressionPlan {
                    arguments: vec![],
                    result_type: Some(v1alpha::DataType {
                        kind: Some(data_type::Kind::Primitive(
                            data_type::PrimitiveType::I64 as i32,
                        )),
                    }),
                    output: true,
                    operator: Some(expression_plan::Operator::Input(OperationInputRef {
                        producing_operation: 0,
                        column: None,
                        input_column: 4,
                        interpolation: operation_input_ref::Interpolation::Null as i32,
                    })),
                },
                ExpressionPlan {
                    arguments: vec![],
                    result_type: Some(v1alpha::DataType {
                        kind: Some(data_type::Kind::Primitive(
                            data_type::PrimitiveType::F64 as i32,
                        )),
                    }),
                    output: true,
                    operator: Some(expression_plan::Operator::Input(OperationInputRef {
                        producing_operation: 0,
                        column: None,
                        input_column: 5,
                        interpolation: operation_input_ref::Interpolation::Null as i32,
                    })),
                },
            ],
            operator: Some(operation_plan::Operator::LookupResponse(
                LookupResponseOperation {
                    foreign_operation: 0,
                    requesting_key_hash: Some(OperationInputRef {
                        producing_operation: 0,
                        column: None,
                        input_column: 3,
                        interpolation: operation_input_ref::Interpolation::Null as i32,
                    }),
                },
            )),
        };

        let input = batch_from_json(
            input_json,
            vec![
                DataType::List(Arc::new(Field::new("item", DataType::UInt64, true))),
                DataType::Int64,
                DataType::Float64,
            ],
        )
        .unwrap();

        run_operation_json(vec![input], plan).await.unwrap()
    }
}
