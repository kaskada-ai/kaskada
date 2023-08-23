use std::sync::Arc;

use super::BoxedOperation;
use crate::execute::error::{invalid_operation, Error};
use crate::execute::operation::expression_executor::InputColumn;
use crate::execute::operation::single_consumer_helper::SingleConsumerHelper;
use crate::execute::operation::{InputBatch, Operation, OperationContext};
use crate::key_hash_inverse::ThreadSafeKeyHashInverse;
use crate::Batch;
use anyhow::Context;
use async_trait::async_trait;
use error_stack::{IntoReport, IntoReportCompat, ResultExt};
use futures::StreamExt;
use itertools::Itertools;
use sparrow_api::kaskada::v1alpha::operation_plan;
use sparrow_arrow::downcast::downcast_primitive_array;
use sparrow_instructions::ComputeStore;
use tokio_stream::wrappers::ReceiverStream;

#[derive(Debug)]
pub(super) struct WithKeyOperation {
    new_key_input_index: usize,
    /// The input stream of batches.
    input_stream: ReceiverStream<Batch>,
    helper: SingleConsumerHelper,
    key_hash_inverse: Arc<ThreadSafeKeyHashInverse>,
    is_primary_grouping: bool,
}

#[async_trait]
impl Operation for WithKeyOperation {
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
            if let Some(input) = self
                .create_input(incoming)
                .await
                .into_report()
                .change_context(Error::internal())?
            {
                sender
                    .send(input)
                    .await
                    .into_report()
                    .change_context(Error::internal())?;
            }
        }
        Ok(())
    }
}

impl WithKeyOperation {
    /// Create the stream of input batches for a with_key operation.
    ///
    /// `with_key` converts the domain of the input by re-keying the batch
    /// to the requested key and sorting the new re-keyed batch.
    pub(super) fn create(
        context: &mut OperationContext,
        operation: operation_plan::WithKeyOperation,
        input_channels: Vec<tokio::sync::mpsc::Receiver<Batch>>,
        input_columns: &[InputColumn],
    ) -> error_stack::Result<BoxedOperation, super::Error> {
        let input_channel = input_channels
            .into_iter()
            .exactly_one()
            .into_report()
            .change_context(Error::internal_msg("expected one channel"))?;

        let new_key_input_index = operation
            .new_key
            .ok_or_else(|| invalid_operation!("missing new key"))?
            .input_column as usize;
        let is_primary_grouping = context.primary_grouping() == operation.grouping;

        Ok(Box::new(Self {
            new_key_input_index,
            input_stream: ReceiverStream::new(input_channel),
            helper: SingleConsumerHelper::try_new(operation.input, input_columns)
                .into_report()
                .change_context(Error::internal_msg("error creating single consumer helper"))?,
            key_hash_inverse: context.key_hash_inverse.clone(),
            is_primary_grouping,
        }))
    }

    async fn create_input(&mut self, input: Batch) -> anyhow::Result<Option<InputBatch>> {
        debug_assert_eq!(input.schema().field(0).name(), "_time");
        debug_assert_eq!(input.schema().field(1).name(), "_subsort");
        debug_assert_eq!(input.schema().field(2).name(), "_key_hash");

        // Hash the new key column
        let new_keys = input.column(self.new_key_input_index);
        let new_key_hashes = sparrow_arrow::hash::hash(new_keys).map_err(|e| e.into_error())?;
        let time = input.column(0);
        let subsort = input.column(1);

        let time = downcast_primitive_array(time)?;
        let subsort = downcast_primitive_array(subsort)?;

        // The key hash inverse only needs to know about keys/hashes related to the
        // primary grouping to produce the key hash inverse for output.
        if self.is_primary_grouping {
            self.key_hash_inverse
                .add(new_keys.as_ref(), &new_key_hashes)
                .await
                .map_err(|e| e.into_error())?;
        }

        // Get the take indices, which will allow us to get the requested columns from
        // the original batch.
        let take_indices =
            crate::read::sort_in_time::sort_in_time_indices(time, subsort, &new_key_hashes)?;

        // Take the new key triple columns
        let time = arrow::compute::take(time, &take_indices, None)?;
        let subsort = arrow::compute::take(subsort, &take_indices, None)?;
        let key_hash = arrow::compute::take(&new_key_hashes, &take_indices, None)?;

        self.helper
            .new_input_batch_with_keys(&input, time, subsort, key_hash, |column| {
                arrow::compute::take(column, &take_indices, None).context("take for with_key")
            })
    }
}

#[cfg(test)]
mod tests {
    use sparrow_api::kaskada::v1alpha::operation_plan::WithKeyOperation;
    use sparrow_api::kaskada::v1alpha::{self, data_type};
    use sparrow_api::kaskada::v1alpha::{
        expression_plan, operation_input_ref, operation_plan, ExpressionPlan, OperationInputRef,
        OperationPlan,
    };

    use crate::execute::operation::testing::{batch_from_csv, run_operation};

    #[tokio::test]
    async fn test_with_key() {
        let plan = OperationPlan {
            expressions: vec![
                ExpressionPlan {
                    arguments: vec![],
                    result_type: Some(v1alpha::DataType {
                        kind: Some(data_type::Kind::Primitive(
                            data_type::PrimitiveType::I64 as i32,
                        )),
                    }),
                    output: false,
                    operator: Some(expression_plan::Operator::Input(OperationInputRef {
                        producing_operation: 0,
                        column: None,
                        input_column: 3,
                        interpolation: operation_input_ref::Interpolation::Null as i32,
                    })),
                },
                ExpressionPlan {
                    arguments: vec![0],
                    result_type: Some(v1alpha::DataType {
                        kind: Some(data_type::Kind::Primitive(
                            data_type::PrimitiveType::F64 as i32,
                        )),
                    }),
                    output: false,
                    operator: Some(expression_plan::Operator::Instruction("cast".to_owned())),
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
                ExpressionPlan {
                    arguments: vec![1, 2],
                    result_type: Some(v1alpha::DataType {
                        kind: Some(data_type::Kind::Primitive(
                            data_type::PrimitiveType::F64 as i32,
                        )),
                    }),
                    output: true,
                    operator: Some(expression_plan::Operator::Instruction("add".to_owned())),
                },
            ],
            operator: Some(operation_plan::Operator::WithKey(WithKeyOperation {
                input: 0,
                new_key: Some(OperationInputRef {
                    producing_operation: 0,
                    column: None,
                    input_column: 4,
                    interpolation: operation_input_ref::Interpolation::Null as i32,
                }),
                grouping: "grouping".to_owned(),
            })),
        };

        // The (hypothetical) producing operation this with_key receives from has 5
        // expressions and outputs 3 of them.
        //
        // - e0: produces an i64 that is output
        // - e1: some random expression
        // - e2: some random expression
        // - e3: generate a boolean (use an inequality on e0...e2) that is output
        // - e4: generate an f64 (perhaps casting something from e0...e3) that is output
        let input = batch_from_csv(
            "
        _time,_subsort,_key_hash,e0,e3,e4
        1970-01-01T00:00:00.000002000,0,1,1,joe,0.2
        1970-01-01T00:00:00.000003000,0,1,2,ally,2.0
        1970-01-01T00:00:00.000004000,0,2,3,joe,3.2
        1970-01-01T00:00:00.000005000,0,5,4,joe,2.1",
            None,
        )
        .unwrap();
        insta::assert_snapshot!(run_operation(vec![input], plan).await.unwrap(), @r###"
        _time,_subsort,_key_hash,e2,e3
        1970-01-01T00:00:00.000002000,0,11333881584776451256,0.2,1.2
        1970-01-01T00:00:00.000003000,0,4285267486210181199,2.0,4.0
        1970-01-01T00:00:00.000004000,0,11333881584776451256,3.2,6.2
        1970-01-01T00:00:00.000005000,0,11333881584776451256,2.1,6.1
        "###);
    }
}
