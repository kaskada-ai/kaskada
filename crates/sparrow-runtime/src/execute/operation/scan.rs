use std::pin::Pin;
use std::sync::Arc;

use arrow::array::StructArray;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use error_stack::{IntoReport, IntoReportCompat, ResultExt};
use futures::{Stream, StreamExt, TryStreamExt};
use itertools::Itertools;
use sparrow_api::kaskada::v1alpha::{operation_input_ref, operation_plan};
use sparrow_core::downcast_primitive_array;
use sparrow_instructions::ComputeStore;
use sparrow_plan::TableId;
use sparrow_qfr::FlightRecorder;

use super::BoxedOperation;
use crate::execute::operation::expression_executor::InputColumn;
use crate::execute::operation::{InputBatch, Operation, OperationContext};
use crate::execute::progress_reporter::ProgressUpdate;
use crate::execute::Error;
use crate::key_hash_index::KeyHashIndex;
use crate::{table_reader, Batch};

pub(super) struct ScanOperation {
    /// The schema of the scanned inputs.
    projected_schema: SchemaRef,
    /// The input stream of batches.
    input_stream: Pin<Box<dyn Stream<Item = error_stack::Result<Batch, Error>> + Send>>,
    key_hash_index: KeyHashIndex,
    progress_updates_tx: tokio::sync::mpsc::Sender<ProgressUpdate>,
}

impl std::fmt::Debug for ScanOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScanOperation")
            .field("projected_schema", &self.projected_schema)
            .field("key_hash_index", &self.key_hash_index)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl Operation for ScanOperation {
    fn restore_from(
        &mut self,
        operation_index: u8,
        compute_store: &ComputeStore,
    ) -> anyhow::Result<()> {
        self.key_hash_index
            .restore_from(operation_index, compute_store)
    }

    fn store_to(&self, operation_index: u8, compute_store: &ComputeStore) -> anyhow::Result<()> {
        self.key_hash_index.store_to(operation_index, compute_store)
    }

    async fn execute(
        &mut self,
        sender: tokio::sync::mpsc::Sender<InputBatch>,
    ) -> error_stack::Result<(), Error> {
        while let Some(incoming) = self
            .input_stream
            .try_next()
            .await
            .change_context(Error::GetNextInput)?
        {
            let input = self
                .create_input(incoming)
                .into_report()
                .change_context(Error::PreprocessNextInput)?;

            // Send progress update
            if self
                .progress_updates_tx
                .send(ProgressUpdate::Input {
                    num_rows: input.len(),
                })
                .await
                .is_err()
            {
                tracing::info!("Progress stream closed. Ending scan");
                break;
            };

            // Send batch.
            sender
                .send(input)
                .await
                .into_report()
                .change_context(Error::internal_msg("send next output"))?;
        }

        Ok(())
    }
}

impl ScanOperation {
    /// Create the stream of input batches for a scan operation.
    pub(super) fn create(
        context: &mut OperationContext,
        scan_operation: operation_plan::ScanOperation,
        input_channels: Vec<tokio::sync::mpsc::Receiver<Batch>>,
        input_columns: &[InputColumn],
    ) -> error_stack::Result<BoxedOperation, Error> {
        error_stack::ensure!(
            input_channels.is_empty(),
            crate::execute::error::invalid_operation!("scan operations should take no input")
        );

        let input_column = input_columns.iter().exactly_one().map_err(|e| {
            crate::execute::error::invalid_operation!(
                "scan operation should have one input column, but was {}",
                e.len()
            )
        })?;

        error_stack::ensure!(
            input_column.input_ref.column == Some(operation_input_ref::Column::ScanRecord(())),
            crate::execute::error::invalid_operation!(
                "input to scan operation should be ScanRecord, but was {input_column:?}"
            )
        );

        let table_id = TableId::new(
            scan_operation
                .table_id
                .ok_or_else(|| {
                    crate::execute::error::invalid_operation!("scan operation missing table id")
                })?
                .into(),
        );

        let table_info = context.data_context.table_info(table_id).ok_or_else(|| {
            crate::execute::error::invalid_operation!(
                "scan operation with invalid table id {table_id:?}"
            )
        })?;

        let requested_slice = scan_operation
            .slice_plan
            .ok_or_else(|| {
                crate::execute::error::invalid_operation!("scan operation missing slice plan")
            })?
            .slice;

        let projected_schema: SchemaRef = Arc::new(
            scan_operation
                .schema
                .as_ref()
                .ok_or_else(|| {
                    crate::execute::error::invalid_operation!("scan operation missing schema")
                })?
                .as_arrow_schema()
                .into_report()
                .change_context(crate::execute::error::invalid_operation!(
                    "scan operation failed to convert schema to Arrow schema"
                ))?,
        );

        // Figure out the projected columns from the table schema.
        //
        // TODO: Can we clean anything up by changing the table reader API
        // to accept the desired table schema?
        let projected_columns = Some(
            scan_operation
                .schema
                .expect("already reported missing schema")
                .fields
                .into_iter()
                .map(|field| field.name)
                .collect(),
        );

        // Send initial progress information.
        let total_num_rows = table_info
            .prepared_files_for_slice(&requested_slice)
            .into_report()
            .change_context(crate::execute::error::invalid_operation!(
                "scan operation references undefined slice {requested_slice:?} for table '{}'",
                table_info.name()
            ))?
            .iter()
            .map(|file| file.num_rows as usize)
            .sum();

        context
            .progress_updates_tx
            .try_send(ProgressUpdate::InputMetadata { total_num_rows })
            .into_report()
            .change_context(Error::internal())?;

        let input_stream = table_reader(
            &mut context.data_manager,
            table_info,
            &requested_slice,
            projected_columns,
            // TODO: Fix flight recorder
            FlightRecorder::disabled(),
            context.max_event_in_snapshot,
            context.output_at_time,
        )
        .change_context(Error::internal_msg("failed to create table reader"))?
        .map_err(|e| e.change_context(Error::internal_msg("failed to read batch")))
        .boxed();

        Ok(Box::new(Self {
            projected_schema,
            input_stream,
            key_hash_index: KeyHashIndex::default(),
            progress_updates_tx: context.progress_updates_tx.clone(),
        }))
    }

    fn create_input(&mut self, batch: Batch) -> anyhow::Result<InputBatch> {
        debug_assert_eq!(batch.schema().field(0).name(), "_time");
        debug_assert_eq!(batch.schema().field(1).name(), "_subsort");
        debug_assert_eq!(batch.schema().field(2).name(), "_key_hash");

        let time = batch.column(0).clone();
        let subsort = batch.column(1).clone();
        let key_hash = batch.column(2).clone();

        // HACKY: There are probably cleaner ways to handle the "key columns".
        // But this works for now.
        // Output batch only includes the requested columns.
        // This generally means dropping the key columns.
        let input = Arc::new(StructArray::from(
            self.projected_schema
                .fields()
                .iter()
                .zip(batch.columns()[3..].iter())
                .map(|(field, array)| (field.clone(), array.clone()))
                .collect::<Vec<_>>(),
        ));

        let grouping = self
            .key_hash_index
            .get_or_update_indices(downcast_primitive_array(key_hash.as_ref())?)?;

        Ok(InputBatch {
            time,
            subsort,
            key_hash,
            grouping,
            input_columns: vec![input],
            lower_bound: batch.lower_bound,
            upper_bound: batch.upper_bound,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::default::Default;
    use std::sync::Arc;

    use arrow::array::{StringArray, TimestampNanosecondArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use futures::StreamExt;
    use itertools::Itertools;
    use sparrow_api::kaskada::v1alpha::compute_table::FileSet;
    use sparrow_api::kaskada::v1alpha::operation_input_ref::{self, Column};
    use sparrow_api::kaskada::v1alpha::operation_plan::ScanOperation;
    use sparrow_api::kaskada::v1alpha::{self, data_type};
    use sparrow_api::kaskada::v1alpha::{
        expression_plan, literal, operation_plan, ComputePlan, ComputeTable, ExpressionPlan,
        Literal, OperationInputRef, OperationPlan, PlanHash, PreparedFile, SlicePlan, TableConfig,
        TableMetadata,
    };
    use sparrow_compiler::DataContext;
    use sparrow_core::downcast_primitive_array;
    use uuid::Uuid;

    use crate::data_manager::DataManager;
    use crate::execute::key_hash_inverse::{KeyHashInverse, ThreadSafeKeyHashInverse};
    use crate::execute::operation::testing::batches_to_csv;
    use crate::execute::operation::{OperationContext, OperationExecutor};
    use crate::read::testing::write_parquet_file;
    use crate::s3::S3Helper;

    #[tokio::test]
    async fn test_scan_execution() {
        let mut data_context = DataContext::default();
        let table_id = Uuid::new_v4();

        let (temp_file, prepared_file) = mk_file(&[
            (0, 1, 1, "a", "b"),
            (0, 2, 0, "c", "d"),
            (1, 0, 1, "e", "f"),
        ]);

        let table_schema = Schema::new(vec![
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            Field::new("key", DataType::UInt64, true),
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Utf8, true),
        ]);
        let table_schema = v1alpha::Schema::try_from(&table_schema).unwrap();

        data_context
            .add_table(ComputeTable {
                config: Some(TableConfig {
                    name: "table".to_owned(),
                    uuid: table_id.to_string(),
                    time_column_name: "time".to_owned(),
                    subsort_column_name: None,
                    group_column_name: "key".to_owned(),
                    grouping: "grouping".to_owned(),
                }),
                metadata: Some(TableMetadata {
                    schema: Some(table_schema),
                    file_count: 1,
                }),
                file_sets: vec![FileSet {
                    slice_plan: Some(SlicePlan {
                        table_name: "table".to_owned(),
                        slice: None,
                    }),
                    prepared_files: vec![prepared_file],
                }],
            })
            .unwrap();

        let scan_schema = Schema::new(vec![Field::new("b", DataType::Utf8, true)]);
        let scan_schema = v1alpha::Schema::try_from(&scan_schema).unwrap();
        let scan_record_schema = v1alpha::DataType {
            kind: Some(data_type::Kind::Struct(scan_schema.clone())),
        };
        let plan = OperationPlan {
            expressions: vec![
                ExpressionPlan {
                    arguments: vec![],
                    result_type: Some(scan_record_schema),
                    output: false,
                    operator: Some(expression_plan::Operator::Input(OperationInputRef {
                        producing_operation: 0,
                        column: Some(Column::ScanRecord(())),
                        input_column: 0,
                        interpolation: operation_input_ref::Interpolation::Null as i32,
                    })),
                },
                ExpressionPlan {
                    arguments: vec![],
                    result_type: Some(v1alpha::DataType {
                        kind: Some(data_type::Kind::Primitive(
                            data_type::PrimitiveType::String as i32,
                        )),
                    }),
                    output: false,
                    operator: Some(expression_plan::Operator::Literal(Literal {
                        literal: Some(literal::Literal::Utf8("b".to_owned())),
                    })),
                },
                ExpressionPlan {
                    arguments: vec![0, 1],
                    result_type: Some(v1alpha::DataType {
                        kind: Some(data_type::Kind::Primitive(
                            data_type::PrimitiveType::String as i32,
                        )),
                    }),
                    output: true,
                    operator: Some(expression_plan::Operator::Instruction(
                        "field_ref".to_owned(),
                    )),
                },
            ],
            operator: Some(operation_plan::Operator::Scan(ScanOperation {
                table_id: Some(table_id.into()),
                schema: Some(scan_schema),
                slice_plan: Some(SlicePlan {
                    table_name: "table".to_owned(),
                    slice: None,
                }),
            })),
        };

        let key_hash_inverse = KeyHashInverse::from_data_type(DataType::Utf8);
        let key_hash_inverse = Arc::new(ThreadSafeKeyHashInverse::new(key_hash_inverse));

        let (max_event_tx, mut max_event_rx) = tokio::sync::mpsc::unbounded_channel();
        let (sender, receiver) = tokio::sync::mpsc::channel(10);
        let mut executor = OperationExecutor::new(plan.clone());
        executor.add_consumer(sender);

        // Channel for the output stats.
        let (progress_updates_tx, mut progress_updates_rx) = tokio::sync::mpsc::channel(29);
        let s3_helper = S3Helper::new().await;
        let mut context = OperationContext {
            plan: ComputePlan {
                operations: vec![plan],
                ..ComputePlan::default()
            },
            plan_hash: PlanHash::default(),
            data_manager: DataManager::new(s3_helper),
            data_context,
            compute_store: None,
            key_hash_inverse,
            max_event_in_snapshot: None,
            progress_updates_tx,
            output_at_time: None,
        };

        executor
            .execute(0, &mut context, vec![], max_event_tx, &Default::default())
            .unwrap()
            .await
            .unwrap();

        max_event_rx.close();

        let csv_string = batches_to_csv(receiver).await.unwrap();

        progress_updates_rx.close();
        let progress_updates: Vec<_> =
            tokio_stream::wrappers::ReceiverStream::new(progress_updates_rx)
                .collect()
                .await;

        insta::assert_snapshot!(csv_string, @r###"
        _time,_subsort,_key_hash,e2
        1970-01-01T00:00:00.000000000,1,1,b
        1970-01-01T00:00:00.000000000,2,0,d
        1970-01-01T00:00:00.000000001,0,1,f
        "###);

        insta::assert_debug_snapshot!(progress_updates, @r###"
        [
            InputMetadata {
                total_num_rows: 3,
            },
            Input {
                num_rows: 2,
            },
            Input {
                num_rows: 1,
            },
        ]
        "###);
        temp_file.close().unwrap();
    }

    // TODO: This testing helper is copied from `table_reader.rs`
    // Should try to refactor into a shareable place.
    fn mk_file(
        rows: &[(i64, u64, u64, &'static str, &'static str)],
    ) -> (tempfile::TempPath, PreparedFile) {
        let (batch, metadata) = mk_batch(rows);

        let times: &TimestampNanosecondArray =
            downcast_primitive_array(batch.column(0).as_ref()).unwrap();
        let min_event_time = times.value_as_datetime(0).unwrap();
        let max_event_time = times.value_as_datetime(times.len() - 1).unwrap();
        let num_rows = batch.num_rows() as i64;

        let parquet_file = write_parquet_file(&batch, None);
        let metadata_parquet_file = write_parquet_file(&metadata, None);

        let prepared = PreparedFile {
            path: parquet_file.to_string_lossy().to_string(),
            min_event_time: Some(min_event_time.into()),
            max_event_time: Some(max_event_time.into()),
            num_rows,
            metadata_path: metadata_parquet_file.to_string_lossy().to_string(),
        };

        (parquet_file, prepared)
    }

    fn mk_batch(
        rows: &[(i64, u64, u64, &'static str, &'static str)],
    ) -> (RecordBatch, RecordBatch) {
        let times = Arc::new(TimestampNanosecondArray::from_iter_values(
            rows.iter().map(|tuple| tuple.0),
        ));
        let subsort = Arc::new(UInt64Array::from_iter_values(
            rows.iter().map(|tuple| tuple.1),
        ));
        let key_hash = Arc::new(UInt64Array::from_iter_values(
            rows.iter().map(|tuple| tuple.2),
        ));
        let metadata_key_hash = Arc::new(UInt64Array::from_iter_values(
            rows.iter().map(|tuple| tuple.2),
        ));
        let time = times.clone();
        let key = key_hash.clone();
        let a = Arc::new(StringArray::from_iter_values(
            rows.iter().map(|tuple| tuple.3),
        ));
        let b = Arc::new(StringArray::from_iter_values(
            rows.iter().map(|tuple| tuple.4),
        ));

        let keys_str = Arc::new(StringArray::from(
            rows.iter().map(|tuple| tuple.2.to_string()).collect_vec(),
        ));

        (
            RecordBatch::try_new(
                PREPARED_SCHEMA.clone(),
                vec![times, subsort, key_hash, time, key, a, b],
            )
            .unwrap(),
            RecordBatch::try_new(METADATA_SCHEMA.clone(), vec![metadata_key_hash, keys_str])
                .unwrap(),
        )
    }

    #[static_init::dynamic]
    static PREPARED_SCHEMA: SchemaRef = {
        Arc::new(Schema::new(vec![
            Field::new(
                "_time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("_subsort", DataType::UInt64, false),
            Field::new("_key_hash", DataType::UInt64, false),
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            Field::new("key", DataType::UInt64, true),
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Utf8, true),
        ]))
    };

    #[static_init::dynamic]
    static METADATA_SCHEMA: SchemaRef = {
        Arc::new(Schema::new(vec![
            Field::new("_key_hash", DataType::UInt64, false),
            Field::new("_entity_key", DataType::Utf8, false),
        ]))
    };
}
