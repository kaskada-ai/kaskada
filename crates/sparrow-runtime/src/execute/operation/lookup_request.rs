use std::sync::Arc;

use arrow::array::{
    Array, ArrayBuilder, ArrayData, ArrayRef, Int32BufferBuilder, ListArray,
    TimestampNanosecondArray, UInt32Array, UInt64Array,
};
use arrow::buffer::Buffer;
use arrow::datatypes::Field;
use async_trait::async_trait;
use error_stack::{IntoReport, IntoReportCompat, ResultExt};
use futures::StreamExt;
use itertools::Itertools;
use sparrow_api::kaskada::v1alpha::operation_plan;
use sparrow_arrow::downcast::downcast_primitive_array;
use sparrow_instructions::{ComputeStore, GroupingIndices};
use tokio_stream::wrappers::ReceiverStream;

use super::BoxedOperation;
use crate::execute::error::{invalid_operation, Error};
use crate::execute::operation::expression_executor::InputColumn;
use crate::execute::operation::single_consumer_helper::SingleConsumerHelper;
use crate::execute::operation::{InputBatch, Operation};
use crate::Batch;

#[derive(Debug)]
pub(super) struct LookupRequestOperation {
    foreign_key_column: usize,

    /// The input stream of batches.
    input_stream: ReceiverStream<Batch>,
    helper: SingleConsumerHelper,
}

#[async_trait]
impl Operation for LookupRequestOperation {
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

impl LookupRequestOperation {
    /// Create the input stream for lookup requests.
    ///
    /// This operation converts from a domain containing keys associated
    /// with a "primary" grouping to a domain containing keys associated
    /// with a "foreign" grouping. Note the primary and foreign grouping
    /// may be the same, differing only in keys. This is very similar to
    /// the behavior of `with_key`, but intended to implement lookup
    /// in combination with `lookup_response`.
    pub(super) fn create(
        operation: operation_plan::LookupRequestOperation,
        input_channels: Vec<tokio::sync::mpsc::Receiver<Batch>>,
        input_columns: &[InputColumn],
    ) -> error_stack::Result<BoxedOperation, super::Error> {
        let input_channel = input_channels
            .into_iter()
            .exactly_one()
            .into_report()
            .change_context(Error::internal_msg("expected one channel"))?;

        error_stack::ensure!(
            input_columns.len() == 1,
            crate::execute::error::invalid_operation!("lookup should have one input column")
        );
        error_stack::ensure!(
            // Column 0 = time, 1 = subsort, 2 = key hash, which is the only
            // column lookup requests should input from.
            input_columns[0].input_ref.input_column == 2,
            crate::execute::error::invalid_operation!(format!(
                "first input to lookup request should be for key hashes but was {}",
                input_columns[0].input_ref.input_column
            ))
        );

        let foreign_key_column = operation
            .foreign_key_hash
            .ok_or_else(|| invalid_operation!("missing foreign_key_hash"))?
            .input_column as usize;

        Ok(Box::new(Self {
            foreign_key_column,
            input_stream: ReceiverStream::new(input_channel),
            helper: SingleConsumerHelper::try_new(operation.primary_operation, input_columns)
                .into_report()
                .change_context(Error::internal_msg("error creating single consumer helper"))?,
        }))
    }

    fn create_input(&mut self, input: Batch) -> anyhow::Result<Option<InputBatch>> {
        if input.num_rows() == 0 {
            let input_columns = self
                .helper
                .incoming_columns
                .iter()
                .map(|index| make_empty_request_list(input.column(*index)))
                .try_collect()?;
            let res = InputBatch {
                time: input.column(0).clone(),
                subsort: input.column(1).clone(),
                key_hash: input.column(2).clone(),
                grouping: GroupingIndices::new_empty(),
                input_columns,
                lower_bound: input.lower_bound,
                upper_bound: input.upper_bound,
            };
            return Ok(Some(res));
        }

        // Re-key to the foreign key and hash it.
        let foreign_key_hash = input.column(self.foreign_key_column).clone();
        let foreign_key_hash =
            sparrow_arrow::hash::hash(&foreign_key_hash).map_err(|e| e.into_error())?;

        // Now, we need to determine how many actual foreign rows there are and collect
        // the requesting primary keys.
        //
        // We do this by producing two arrays:
        // 1. An array of offsets for use in a list array. This indicates the number
        //    of primary keys that mapped to a given foreign key hash.
        // 2. An array of take indices that re-order the rows by `(time, subsort,
        // foreign_key_hash)`.
        //
        // There are likely more efficient algorithms for doing this by operating
        // on chunks of input rows with equal times and subsorts. But for now, we take
        // the relatively naive approach of just sorting.

        let value_indices = crate::sort_in_time::sort_in_time_indices_dyn(
            input.column(0).as_ref(),
            input.column(1).as_ref(),
            &foreign_key_hash,
        )?;

        // Create the offset array.
        // This should be the first index of each triple within the sorted foreign
        // batch. We do this at the same time as creating the foreign `time,
        // subsort, key_hash` columns.
        let time: &TimestampNanosecondArray = downcast_primitive_array(input.column(0).as_ref())?;
        let subsort: &UInt64Array = downcast_primitive_array(input.column(1).as_ref())?;

        // TODO: Lookup Optimization
        // Rather than eagerly applying `take` on the foreign key hash which
        // allocates, we could instead just "read through" the indirection within
        // the loop
        let foreign_key_hash = arrow::compute::take(&foreign_key_hash, &value_indices, None)?;
        let foreign_key_hash: &UInt64Array = downcast_primitive_array(foreign_key_hash.as_ref())?;

        let mut offset_builder = Int32BufferBuilder::new(512);
        let mut foreign_time_builder = TimestampNanosecondArray::builder(512);
        let mut foreign_subsort_builder = UInt64Array::builder(512);
        let mut foreign_key_hash_builder = UInt64Array::builder(512);

        let mut prev_time = time.value(0);
        let mut prev_subsort = subsort.value(0);
        let mut prev_key_hash = foreign_key_hash.value(0);

        offset_builder.append(0);
        foreign_time_builder.append_value(prev_time);
        foreign_subsort_builder.append_value(prev_subsort);
        foreign_key_hash_builder.append_value(prev_key_hash);

        // TODO: Lookup optimization
        // Instead of building the foreign columns as we go, we could instead
        // keep track of whether everything was sorted. We have some cases:
        //
        // 1. If everything is not sorted, we need to apply `take`
        //    to reorder things.
        // 2. We can create the "list" by using the offsets we created.
        // 3. We can find the "keys" by using the offset indices into
        //    the properly ordered array.
        for index in 1..time.len() {
            let time = time.value(index);
            let subsort = subsort.value(index);
            let key_hash = foreign_key_hash.value(index);

            if prev_time != time || prev_subsort != subsort || prev_key_hash != key_hash {
                offset_builder.append(foreign_time_builder.len() as i32);

                foreign_time_builder.append_value(time);
                foreign_subsort_builder.append_value(subsort);
                foreign_key_hash_builder.append_value(key_hash);

                prev_time = time;
                prev_subsort = subsort;
                prev_key_hash = key_hash;
            }
        }
        offset_builder.append(time.len() as i32);

        let offset_buffer = offset_builder.finish();
        let foreign_time = Arc::new(foreign_time_builder.finish());
        let foreign_subsort = Arc::new(foreign_subsort_builder.finish());
        let foreign_key_hash = Arc::new(foreign_key_hash_builder.finish());

        let len = foreign_time.len();
        let result = self.helper.new_input_batch_with_keys(
            &input,
            foreign_time,
            foreign_subsort,
            foreign_key_hash,
            |column| make_request_list(len, &offset_buffer, &value_indices, column),
        )?;

        Ok(result)
    }
}

/// Creates a list array using underlying Arrow operations.
///
/// We specifically do it this way since we have computed the offsets already,
/// and we just want to create a list array from the given offsets and values.
fn make_request_list(
    len: usize,
    offset_buffer: &Buffer,
    value_indices: &UInt32Array,
    values: &ArrayRef,
) -> anyhow::Result<ArrayRef> {
    let values = arrow::compute::take(values.as_ref(), value_indices, None)?;
    let values_data = values.into_data();

    let field = Arc::new(Field::new(
        "item",
        values_data.data_type().clone(),
        true, // TODO: find a consistent way of getting this
    ));
    let data_type = ListArray::DATA_TYPE_CONSTRUCTOR(field);
    let array_data = ArrayData::builder(data_type)
        .len(len)
        .add_buffer(offset_buffer.clone())
        .add_child_data(values_data)
        .build()?;

    let result = Arc::new(ListArray::from(array_data));
    Ok(result)
}

fn make_empty_request_list(col: &ArrayRef) -> anyhow::Result<ArrayRef> {
    let field = Arc::new(Field::new("item", col.data_type().clone(), true));
    let data_type = ListArray::DATA_TYPE_CONSTRUCTOR(field);
    Ok(Arc::new(ListArray::from(ArrayData::new_empty(&data_type))))
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;
    use sparrow_api::kaskada::v1alpha::operation_plan::LookupRequestOperation;
    use sparrow_api::kaskada::v1alpha::{self, data_type};
    use sparrow_api::kaskada::v1alpha::{
        expression_plan, operation_input_ref, operation_plan, ExpressionPlan, OperationInputRef,
        OperationPlan,
    };

    use crate::execute::operation::testing::{batch_from_json, run_operation_json};

    #[tokio::test]
    async fn test_lookup_request_same_times() {
        let input_json = r#"
            {"_time": 2000, "_subsort": 0, "_key_hash": 1, "e0": 1}
            {"_time": 3000, "_subsort": 1, "_key_hash": 1, "e0": 5}
            {"_time": 4000, "_subsort": 0, "_key_hash": 1, "e0": 3}
            {"_time": 4000, "_subsort": 0, "_key_hash": 2, "e0": 3}
            "#;

        insta::assert_snapshot!(run_test(input_json).await, @r###"
        {"_key_hash":18433805721903975440,"_subsort":0,"_time":"1970-01-01T00:00:00.000002","e0":[1]}
        {"_key_hash":16461383214845928621,"_subsort":1,"_time":"1970-01-01T00:00:00.000003","e0":[1]}
        {"_key_hash":5496774745203840792,"_subsort":0,"_time":"1970-01-01T00:00:00.000004","e0":[1,2]}
        "###)
    }

    #[tokio::test]
    async fn test_lookup_request_unique_times_ordered() {
        let input_json = r#"
            {"_time": 2000, "_subsort": 0, "_key_hash": 1, "e0": 1}
            {"_time": 3000, "_subsort": 1, "_key_hash": 1, "e0": 5}
            {"_time": 4000, "_subsort": 0, "_key_hash": 1, "e0": 3}
            {"_time": 4000, "_subsort": 0, "_key_hash": 2, "e0": 2}
            "#;

        // This tests that multiple rows at the same time subort (but different key
        // hash) map to unique foreign rows, but in the same order.
        insta::assert_snapshot!(run_test(input_json).await, @r###"
        {"_key_hash":18433805721903975440,"_subsort":0,"_time":"1970-01-01T00:00:00.000002","e0":[1]}
        {"_key_hash":16461383214845928621,"_subsort":1,"_time":"1970-01-01T00:00:00.000003","e0":[1]}
        {"_key_hash":2694864431690786590,"_subsort":0,"_time":"1970-01-01T00:00:00.000004","e0":[2]}
        {"_key_hash":5496774745203840792,"_subsort":0,"_time":"1970-01-01T00:00:00.000004","e0":[1]}
        "###)
    }

    #[tokio::test]
    async fn test_lookup_request_unique_times_unordered() {
        let input_json = r#"
            {"_time": 2000, "_subsort": 0, "_key_hash": 1, "e0": 1}
            {"_time": 3000, "_subsort": 1, "_key_hash": 1, "e0": 5}
            {"_time": 4000, "_subsort": 0, "_key_hash": 1, "e0": 2}
            {"_time": 4000, "_subsort": 0, "_key_hash": 2, "e0": 3}
            "#;

        // This tests that multiple rows at the same time subort (but different key
        // hash) map to unique foreign rows, but in a different order.
        insta::assert_snapshot!(run_test(input_json).await, @r###"
        {"_key_hash":18433805721903975440,"_subsort":0,"_time":"1970-01-01T00:00:00.000002","e0":[1]}
        {"_key_hash":16461383214845928621,"_subsort":1,"_time":"1970-01-01T00:00:00.000003","e0":[1]}
        {"_key_hash":2694864431690786590,"_subsort":0,"_time":"1970-01-01T00:00:00.000004","e0":[1]}
        {"_key_hash":5496774745203840792,"_subsort":0,"_time":"1970-01-01T00:00:00.000004","e0":[2]}
        "###)
    }

    /// Runs a test of the lookup request operation.
    ///
    /// The input should be a JSON string, containing the key columns and an
    /// `e0` column which will be used as the foreign key being looked up.
    ///
    /// The result should be keyed by the requested (foreign) keys, and has a
    /// single column `e0` containing the original (primary) keys.
    async fn run_test(input_json: &str) -> String {
        let plan = OperationPlan {
            expressions: vec![ExpressionPlan {
                arguments: vec![],
                result_type: Some(v1alpha::DataType {
                    kind: Some(data_type::Kind::List(Box::new(data_type::List {
                        name: "item".to_owned(),
                        item_type: Some(Box::new(v1alpha::DataType {
                            kind: Some(data_type::Kind::Primitive(
                                data_type::PrimitiveType::U64 as i32,
                            )),
                        })),
                        nullable: true,
                    }))),
                }),
                output: true,
                operator: Some(expression_plan::Operator::Input(OperationInputRef {
                    producing_operation: 0,
                    column: None,
                    input_column: 2,
                    interpolation: operation_input_ref::Interpolation::Null as i32,
                })),
            }],
            operator: Some(operation_plan::Operator::LookupRequest(
                LookupRequestOperation {
                    primary_operation: 0,
                    foreign_key_hash: Some(OperationInputRef {
                        producing_operation: 0,
                        column: None,
                        input_column: 3,
                        interpolation: operation_input_ref::Interpolation::Null as i32,
                    }),
                },
            )),
        };

        let input = batch_from_json(input_json, vec![DataType::UInt64]).unwrap();

        // The output (e0) should be the list of original requesting keys.
        // The key_hash should be the hash of e0 from the input.
        run_operation_json(vec![input], plan).await.unwrap()
    }
}
