use anyhow::Context;
use arrow::array::BooleanArray;
use async_trait::async_trait;
use error_stack::{IntoReport, IntoReportCompat, ResultExt};
use futures::StreamExt;
use itertools::Itertools;
use sparrow_api::kaskada::v1alpha::operation_plan;
use sparrow_arrow::downcast::downcast_boolean_array;
use sparrow_instructions::ComputeStore;
use tokio_stream::wrappers::ReceiverStream;

use super::BoxedOperation;
use crate::execute::error::{invalid_operation, Error};
use crate::execute::operation::expression_executor::InputColumn;
use crate::execute::operation::single_consumer_helper::SingleConsumerHelper;
use crate::execute::operation::{InputBatch, Operation};
use crate::Batch;

#[derive(Debug)]
pub(super) struct SelectOperation {
    condition_input_column: usize,
    /// The input stream of batches.
    input_stream: ReceiverStream<Batch>,
    helper: SingleConsumerHelper,
}

#[async_trait]
impl Operation for SelectOperation {
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
            let input = self
                .create_input(incoming)
                .into_report()
                .change_context(Error::internal())?;
            sender
                .send(input)
                .await
                .into_report()
                .change_context(Error::internal())?;
        }
        Ok(())
    }
}

impl SelectOperation {
    /// Create the stream of input batches for a select operation.
    pub(super) fn create(
        operation: operation_plan::SelectOperation,
        input_channels: Vec<tokio::sync::mpsc::Receiver<Batch>>,
        input_columns: &[InputColumn],
    ) -> error_stack::Result<BoxedOperation, super::Error> {
        let input_channel = input_channels
            .into_iter()
            .exactly_one()
            .into_report()
            .change_context(Error::internal_msg("expected one channel"))?;

        let condition_input_column = operation
            .condition
            .ok_or_else(|| invalid_operation!("missing condition"))?
            .input_column as usize;

        Ok(Box::new(Self {
            condition_input_column,
            input_stream: ReceiverStream::new(input_channel),
            helper: SingleConsumerHelper::try_new(operation.input, input_columns)
                .into_report()
                .change_context(Error::internal_msg("error creating single consumer helper"))?,
        }))
    }

    fn create_input(&mut self, unfiltered_input: Batch) -> anyhow::Result<InputBatch> {
        // Create a filter from the boolean condition column.
        let condition = unfiltered_input.column(self.condition_input_column);
        let condition: &BooleanArray = downcast_boolean_array(condition.as_ref())?;
        let filter = arrow::compute::FilterBuilder::new(condition)
            .optimize()
            .build();

        let output = self
            .helper
            .new_input_batch(unfiltered_input, |unfiltered_column| {
                filter
                    .filter(unfiltered_column.as_ref())
                    .context("filter for select operation")
            })?;
        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use sparrow_api::kaskada::v1alpha::operation_plan::SelectOperation;
    use sparrow_api::kaskada::v1alpha::{self, data_type};
    use sparrow_api::kaskada::v1alpha::{
        expression_plan, operation_input_ref, operation_plan, ExpressionPlan, OperationInputRef,
        OperationPlan,
    };

    use crate::execute::operation::testing::{batch_from_csv, run_operation};

    #[tokio::test]
    async fn test_select() {
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
            operator: Some(operation_plan::Operator::Select(SelectOperation {
                input: 0,
                condition: Some(OperationInputRef {
                    producing_operation: 0,
                    column: None,
                    input_column: 4,
                    interpolation: operation_input_ref::Interpolation::Null as i32,
                }),
            })),
        };

        // The (hypothetical) producing operation this select receives from has 5
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
        1970-01-01T00:00:00.000002000,0,1,1,true,0.2
        1970-01-01T00:00:00.000003000,0,1,2,false,2.0
        1970-01-01T00:00:00.000004000,0,2,3,true,3.2",
            None,
        )
        .unwrap();
        insta::assert_snapshot!(run_operation(vec![input], plan).await.unwrap(), @r###"
        _time,_subsort,_key_hash,e2,e3
        1970-01-01T00:00:00.000002000,0,1,0.2,1.2
        1970-01-01T00:00:00.000004000,0,2,3.2,6.2
        "###)
    }
}
