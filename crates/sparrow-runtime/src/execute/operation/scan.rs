use std::pin::Pin;
use std::sync::Arc;

use arrow::array::StructArray;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use error_stack::{IntoReport, IntoReportCompat, ResultExt};
use futures::{Stream, StreamExt, TryStreamExt};
use itertools::Itertools;
use sparrow_api::kaskada::v1alpha::{self, operation_input_ref, operation_plan};
use sparrow_arrow::downcast::downcast_primitive_array;
use sparrow_instructions::ComputeStore;
use sparrow_instructions::TableId;
use sparrow_qfr::FlightRecorder;

use super::BoxedOperation;
use crate::execute::operation::expression_executor::InputColumn;
use crate::execute::operation::{InputBatch, Operation, OperationContext};
use crate::execute::progress_reporter::ProgressUpdate;
use crate::execute::{error, Error};
use crate::key_hash_index::KeyHashIndex;
use crate::stream_reader::stream_reader;
use crate::table_reader::table_reader;
use crate::Batch;

pub(super) struct ScanOperation {
    /// The schema of the scanned inputs.
    projected_schema: SchemaRef,
    /// The input stream of batches.
    ///
    /// Takes until the stop signal is received or the stream is empty.
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
        while let Some(batch) = self.input_stream.next().await {
            let input = self
                .create_input(batch?)
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
    pub(super) async fn create(
        context: &mut OperationContext,
        scan_operation: operation_plan::ScanOperation,
        input_channels: Vec<tokio::sync::mpsc::Receiver<Batch>>,
        input_columns: &[InputColumn],
        stop_signal_rx: Option<tokio::sync::watch::Receiver<bool>>,
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

        if let Some(in_memory) = &table_info.in_memory {
            // Hacky. When doing the Python-builder, use the in-memory batch.
            // Ideally, this would be merged with the contents of the file.
            // Bonus points if it deduplicates. That would allow us to use the
            // in-memory batch as the "hot-store" for history+stream hybrid
            // queries.
            assert!(requested_slice.is_none());

            // TODO: Consider stoppable batch scans (and queries).
            let input_stream = if context.materialize {
                in_memory
                    .subscribe()
                    .map_err(|e| e.change_context(Error::internal_msg("invalid input")))
                    .and_then(|batch| async move {
                        Batch::try_new_from_batch(batch)
                            .into_report()
                            .change_context(Error::internal_msg("invalid input"))
                    })
                    // TODO: Share this code / unify it with other scans.
                    .take_until(async move {
                        let mut stop_signal_rx =
                            stop_signal_rx.expect("stop signal for use with materialization");
                        while !*stop_signal_rx.borrow() {
                            match stop_signal_rx.changed().await {
                                Ok(_) => (),
                                Err(e) => {
                                    tracing::error!(
                                        "stop signal receiver dropped unexpectedly: {:?}",
                                        e
                                    );
                                    break;
                                }
                            }
                        }
                    })
                    .boxed()
            } else if let Some(batch) = in_memory.current() {
                futures::stream::once(async move {
                    Batch::try_new_from_batch(batch)
                        .into_report()
                        .change_context(Error::internal_msg("invalid input"))
                })
                .boxed()
            } else {
                futures::stream::empty().boxed()
            };
            return Ok(Box::new(Self {
                projected_schema,
                input_stream,
                key_hash_index: KeyHashIndex::default(),
                progress_updates_tx: context.progress_updates_tx.clone(),
            }));
        }

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

        // Scans can read from tables (files) or streams.
        let backing_source = match table_info.config().source.as_ref() {
            Some(v1alpha::Source { source }) => source.as_ref().expect("source"),
            _ => error_stack::bail!(Error::Internal("expected source")),
        };

        let input_stream = match backing_source {
            v1alpha::source::Source::Kaskada(_) => {
                // Send initial progress information.
                let total_num_rows = table_info
                    .prepared_files_for_slice(&requested_slice)
                    .into_report()
                    .change_context(error::invalid_operation!("scan operation references undefined slice {requested_slice:?} for table '{}'", table_info.name()))?
                    .iter()
                    .map(|file| file.num_rows as usize)
                    .sum();

                context
                    .progress_updates_tx
                    .try_send(ProgressUpdate::InputMetadata { total_num_rows })
                    .into_report()
                    .change_context(Error::internal())?;

                let input_stream = table_reader(
                    &context.object_stores,
                    table_info,
                    &requested_slice,
                    projected_columns,
                    // TODO: Fix flight recorder
                    FlightRecorder::disabled(),
                    context.max_event_in_snapshot,
                    context.output_at_time,
                )
                .await
                .change_context(Error::internal_msg("failed to create table reader"))?
                .map_err(|e| e.change_context(Error::internal_msg("failed to read batch")))
                .boxed();
                input_stream
            }
            v1alpha::source::Source::Pulsar(p) => {
                let input_stream = stream_reader(
                    context,
                    table_info,
                    requested_slice.as_ref(),
                    projected_columns,
                    // TODO: Fix flight recorder
                    FlightRecorder::disabled(),
                    p,
                )
                .await
                .change_context(Error::internal_msg("failed to create stream reader"))?
                .map_err(|e| e.change_context(Error::internal_msg("failed to read batch")))
                .boxed();

                input_stream
            }
            v1alpha::source::Source::Kafka(_) => todo!(),
        };

        // Currently configures the stream for the following cases:
        // 1) Streams until the stop signal is received or source is done producing (currently only used for materializations)
        // 2) Streams until the source is done producing
        let input_stream = if let Some(mut stop_signal_rx) = stop_signal_rx {
            input_stream
                .take_until(async move {
                    while !*stop_signal_rx.borrow() {
                        match stop_signal_rx.changed().await {
                            Ok(_) => (),
                            Err(e) => {
                                tracing::error!(
                                    "stop signal receiver dropped unexpectedly: {:?}",
                                    e
                                );
                                break;
                            }
                        }
                    }
                })
                .boxed()
        } else {
            input_stream.boxed()
        };

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
    use hashbrown::HashMap;
    use itertools::Itertools;
    use sparrow_api::kaskada::v1alpha::compute_table::FileSet;
    use sparrow_api::kaskada::v1alpha::operation_input_ref::{self, Column};
    use sparrow_api::kaskada::v1alpha::operation_plan::ScanOperation;
    use sparrow_api::kaskada::v1alpha::KaskadaSource;
    use sparrow_api::kaskada::v1alpha::{self, data_type};
    use sparrow_api::kaskada::v1alpha::{
        expression_plan, literal, operation_plan, ComputePlan, ComputeTable, ExpressionPlan,
        Literal, OperationInputRef, OperationPlan, PreparedFile, SlicePlan, TableConfig,
        TableMetadata,
    };
    use sparrow_arrow::downcast::downcast_primitive_array;
    use sparrow_compiler::DataContext;
    use uuid::Uuid;

    use crate::execute::operation::testing::batches_to_csv;
    use crate::execute::operation::{OperationContext, OperationExecutor};
    use crate::key_hash_inverse::ThreadSafeKeyHashInverse;
    use crate::read::testing::write_parquet_file;
    use crate::stores::ObjectStoreRegistry;

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
        let source = v1alpha::Source {
            source: Some(v1alpha::source::Source::Kaskada(KaskadaSource {})),
        };

        data_context
            .add_table(ComputeTable {
                config: Some(TableConfig {
                    name: "table".to_owned(),
                    uuid: table_id.to_string(),
                    time_column_name: "time".to_owned(),
                    subsort_column_name: None,
                    group_column_name: "key".to_owned(),
                    grouping: "grouping".to_owned(),
                    source: Some(source),
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

        let key_hash_inverse = Arc::new(ThreadSafeKeyHashInverse::from_data_type(&DataType::Utf8));

        let (max_event_tx, mut max_event_rx) = tokio::sync::mpsc::unbounded_channel();
        let (sender, receiver) = tokio::sync::mpsc::channel(10);
        let mut executor = OperationExecutor::new(plan.clone());
        executor.add_consumer(sender);

        // Channel for the output stats.
        let (progress_updates_tx, mut progress_updates_rx) = tokio::sync::mpsc::channel(29);
        let mut context = OperationContext {
            plan: ComputePlan {
                operations: vec![plan],
                ..ComputePlan::default()
            },
            object_stores: Arc::new(ObjectStoreRegistry::default()),
            data_context,
            compute_store: None,
            key_hash_inverse,
            max_event_in_snapshot: None,
            progress_updates_tx,
            output_at_time: None,
            bounded_lateness_ns: None,
            materialize: false,
            udfs: HashMap::new(),
        };

        executor
            .execute(
                0,
                &mut context,
                vec![],
                max_event_tx,
                &Default::default(),
                None,
            )
            .await
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
            path: format!("file://{}", parquet_file.display()),
            min_event_time: Some(min_event_time.into()),
            max_event_time: Some(max_event_time.into()),
            num_rows,
            metadata_path: format!("file://{}", metadata_parquet_file.display()),
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
