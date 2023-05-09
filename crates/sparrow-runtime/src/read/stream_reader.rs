use std::sync::Arc;

use anyhow::Context;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use chrono::NaiveDateTime;
use error_stack::{IntoReport, IntoReportCompat, ResultExt};
use futures::stream::BoxStream;
use futures::Stream;
use hashbrown::HashSet;
use itertools::Itertools;
use sparrow_api::kaskada::v1alpha::slice_plan::Slice;
use sparrow_api::kaskada::v1alpha::{PulsarSource, PulsarSubscription};
use sparrow_compiler::{DataContext, TableInfo};
use sparrow_core::TableSchema;
use sparrow_qfr::{
    activity, gauge, Activity, FlightRecorder, Gauge, PushRegistration, Registration, Registrations,
};
use tokio_stream::StreamExt;
use tracing::info;

use crate::data_manager::{DataHandle, DataManager};
use crate::execute::operation::OperationContext;
use crate::merge::{homogeneous_merge, GatheredBatches, Gatherer};
use crate::min_heap::{HasPriority, MinHeap};
use crate::prepare::execute_iter::ExecuteIter;
use crate::read::parquet_stream::{self, new_parquet_stream};
use crate::streams::pulsar_stream;
use crate::{Batch, RawMetadata};

const READ_STREAM: Activity = activity!("scan.read_stream");

const MIN_BATCH_TIME: Gauge<i64> = gauge!("min_time_in_batch");
const MAX_BATCH_TIME: Gauge<i64> = gauge!("max_time_in_batch");
const NUM_INPUT_ROWS: Gauge<usize> = gauge!("num_input_rows");
const NUM_OUTPUT_ROWS: Gauge<usize> = gauge!("num_output_rows");

static REGISTRATION: Registration = Registration::new(|| {
    let mut r = Registrations::default();
    r.add(READ_STREAM);

    r.add(MIN_BATCH_TIME);
    r.add(MAX_BATCH_TIME);
    r.add(NUM_INPUT_ROWS);
    r.add(NUM_OUTPUT_ROWS);
    r
});

inventory::submit!(&REGISTRATION);

/// Create a stream that continually reads messages from a stream.
pub(crate) fn stream_reader(
    context: &OperationContext,
    table_info: &TableInfo,
    requested_slice: Option<&Slice>,
    projected_columns: Option<Vec<String>>,
    flight_recorder: FlightRecorder,
    pulsar_source: &PulsarSource,
) -> error_stack::Result<impl Stream<Item = error_stack::Result<Batch, Error>> + 'static, Error> {
    // This schema came from Wren, therefore it should be the user facing schema.
    // Therefore it should *not* already have the key columns.
    // This adds the key columns to the schema.
    let schema = TableSchema::try_from_data_schema(table_info.schema().as_ref())
        .into_report()
        .change_context(Error::LoadTableSchema)?;

    // Create an owned version of the table info so the stream can be `'static`.
    let table_name = table_info.name().to_owned();

    let pulsar_subscription =
        std::env::var("PULSAR_SUBSCRIPTION").unwrap_or("subscription-default".to_owned());
    let pulsar_config = pulsar_source.config.as_ref().ok_or(Error::Internal)?;
    let pulsar_subscription = PulsarSubscription {
        config: Some(pulsar_config.clone()),
        subscription_id: pulsar_subscription,
        last_publish_time: 0,
    };

    let consumer = futures::executor::block_on(pulsar_stream::consumer(
        &pulsar_subscription,
        table_info.schema().clone(),
    ))
    .change_context(Error::CreateStream)?;
    let stream = pulsar_stream::stream_for_prepare(
        table_info.schema().clone(),
        consumer,
        pulsar_subscription.last_publish_time,
    );

    let raw_metadata = RawMetadata::from_raw_schema(table_info.schema().clone());

    // TODO: FRAZ prepare hash and  slice?
    let mut iter = ExecuteIter::try_new(
        stream,
        table_info.config().clone(),
        raw_metadata,
        0,
        requested_slice,
        context.key_hash_inverse.clone(),
        0,
    )
    .into_report()
    .change_context(Error::CreateStream)?;

    Ok(async_stream::try_stream! {
        while let Some(next_input) = iter.next().await {
            let next_input = next_input.into_report().change_context(Error::Internal).attach_printable("expected input")?;
            match next_input {
                None => break,
                Some(input) => {
                    let input = input;
                    let batch = Batch::try_new_from_batch(input).into_report().change_context(Error::Internal)?;
                    yield batch
                }
            }
        }

        tracing::error!("unexpected - never should get None from underlying stream");
    })

    //     Ok(async_stream::try_stream! {
    //         // Project the columns from the schema.
    //         // TODO: Cleanup this duplication.
    //         let projected_schema = projected_schema(schema, &projected_columns)?;
    //         let projected_schema_ref = projected_schema.schema_ref();

    //            let next_batch = READ_STREAM.instrument::<error_stack::Result<_, Error>, _>(&flight_recorder, |metrics| {
    //                 // Weird syntax because we can't easily say "move metrics but not projected schema".
    //                 // This may get easier with async closures https://github.com/rust-lang/rust/issues/62290.
    //                 let input = next_input.next_batch(&projected_schema, upper_bound_opt);
    //                 async move {
    //                     let input = input.await?;

    //                     // if let Some(input) = &input {
    //                     //     metrics.report_metric(MIN_BATCH_TIME, input.lower_bound.time);
    //                     //     metrics.report_metric(MAX_BATCH_TIME, input.upper_bound.time);
    //                     //     metrics.report_metric(NUM_INPUT_ROWS, input.num_rows());
    //                     // }

    //                     Ok(input)
    //                 }
    //             }).await?;

    //             if next_batch.is_some() {
    //                 // If we got a batch, the input is still active.
    //                 active.push(next_input);
    //             }

    //             let active_len = active.len();
    //             let next_output = gather_next_output(
    //                 &flight_recorder,
    //                 &mut gatherer,
    //                 index,
    //                 next_batch,
    //                 active_len
    //             )?;

    //             if let Some(next_output) = next_output {
    //                 let batch = merge_next_output(&table_name, &flight_recorder, projected_schema_ref, next_output)
    //                      .into_report()
    //                      .change_context(Error::Internal)?;
    //                 if batch.num_rows() > 0 {
    //                     yield Batch::try_new_from_batch(batch).into_report().change_context(Error::Internal)?;
    //                 }
    //             }
    //     })
}

#[derive(derive_more::Display, Debug)]
pub enum Error {
    #[display(fmt = "failed to create input stream")]
    CreateStream,
    #[display(fmt = "failed to read next batch")]
    ReadNextBatch,
    #[display(fmt = "unsupported: {_0}")]
    Unsupported(&'static str),
    #[display(fmt = "internal error: ")]
    Internal,
    #[display(
        fmt = "internal error: min next time ({min_next_time}) must be <= next lower bound {lower_bound}"
    )]
    MinNextGreaterThanNextLowerBound {
        min_next_time: i64,
        lower_bound: i64,
    },
    #[display(
        fmt = "internal error: max event time ({max_event_time}) must be > next upper bound {upper_bound}"
    )]
    MaxEventTimeLessThanNextUpperBound {
        max_event_time: i64,
        upper_bound: i64,
    },
    #[display(fmt = "failed to select necessary prepared files")]
    SelectPreparedFiles,
    #[display(fmt = "failed to queue download of necessary prepared files")]
    QueueFileDownloads,
    #[display(
        fmt = "unexpected file '{file:?}' in table '{table_name}' with data partially before the snapshot time {snapshot_time:?}"
    )]
    PartialOverlap {
        file: Arc<DataHandle>,
        table_name: String,
        snapshot_time: Option<NaiveDateTime>,
    },
    #[display(fmt = "failed to skip to minimum event")]
    SkippingToMinEvent,
    #[display(fmt = "failed to load table schema")]
    LoadTableSchema,
    #[display(fmt = "failed to determine projected schema")]
    DetermineProjectedSchema,
}

impl error_stack::Context for Error {}

/// Select the necessary prepared files for the given silce.
fn select_prepared_files(
    data_manager: &mut DataManager,
    table_info: &TableInfo,
    requested_slice: &Option<Slice>,
    max_event_in_snapshot: Option<NaiveDateTime>,
) -> error_stack::Result<Vec<Arc<DataHandle>>, Error> {
    let prepared_files = table_info
        .prepared_files_for_slice(requested_slice)
        .into_report()
        .change_context(Error::SelectPreparedFiles)?;
    let mut selected_files = Vec::with_capacity(prepared_files.len());
    for prepared_file in prepared_files {
        let min_event_time = prepared_file
            .min_event_time()
            .change_context(Error::SelectPreparedFiles)?;
        let max_event_time = prepared_file
            .max_event_time()
            .change_context(Error::SelectPreparedFiles)?;

        if prepared_file.num_rows == 0 {
            info!(
                "Skipping empty file '{}' for table '{}'",
                prepared_file.path,
                table_info.name(),
            );
            continue;
        } else if matches!(max_event_in_snapshot, Some(t) if max_event_time <= t) {
            // NOTE: This use of matches! is the implementation of the unstable
            // `max_event_in_snapshot.is_some_with(|&t| max_event_time <= t)`. We may want
            // to replace it once that is supported.

            info!(
                "Skipping '{:?}' for table '{}' -- fully before max event in snapshot {:?}",
                prepared_file,
                table_info.name(),
                max_event_in_snapshot
            );

            // All data has already been persisted, nothing more to process.
            continue;
        }

        // Don't queue the download until we know we want the file.
        let data_handle = data_manager
            .queue_download(prepared_file)
            .into_report()
            .change_context(Error::QueueFileDownloads)?;
        debug_assert_eq!(min_event_time, data_handle.min_event_time());
        debug_assert_eq!(max_event_time, data_handle.max_event_time());

        // Currently, incremental relies on finding a snapshot completely before the
        // new data. Thus, any given source file should either be
        // (a) completely old (min_time <= max_time < max_time_processed)
        // (b) completely new (max_time_processed < min_time <= max_time).
        //
        // We know since we didn't `continue` above, that `max_time >
        // max_time_processed`, which means we're not in case `a`. Thus, we
        // need to ensure we're in case `b` by checking the `min_time >
        // max_time_processed`.
        error_stack::ensure!(
            max_event_in_snapshot.iter().all(|t| &min_event_time > t),
            Error::PartialOverlap {
                file: data_handle,
                table_name: table_info.name().to_owned(),
                snapshot_time: max_event_in_snapshot
            }
        );

        selected_files.push(data_handle);
    }

    selected_files.sort_by(|a, b| a.cmp_time(b));
    Ok(selected_files)
}

/// Compute the projected schema from a base schema and projected columns.
fn projected_schema(
    schema: TableSchema,
    columns: &Option<Vec<String>>,
) -> error_stack::Result<TableSchema, Error> {
    if let Some(columns) = columns {
        let columns: HashSet<&str> = columns.iter().map(|x| x.as_ref()).collect();
        let projected_data_fields: Vec<_> = schema
            .data_fields()
            .iter()
            .filter(|field| columns.contains(field.name().as_str()))
            .cloned()
            .collect();

        debug_assert_eq!(projected_data_fields.len(), columns.len());
        Ok(TableSchema::from_data_fields(projected_data_fields)
            .into_report()
            .change_context(Error::DetermineProjectedSchema)?)
    } else {
        Ok(schema)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{StringArray, TimestampNanosecondArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use chrono::NaiveDateTime;
    use futures::TryStreamExt;
    use prost_wkt_types::Timestamp;
    use sparrow_api::kaskada::v1alpha::{
        compute_table, ComputeTable, PreparedFile, TableConfig, TableMetadata,
    };
    use sparrow_compiler::DataContext;
    use sparrow_core::downcast_primitive_array;
    use sparrow_qfr::FlightRecorder;
    use static_init::dynamic;
    use uuid::Uuid;

    use super::*;
    use crate::data_manager::DataManager;
    use crate::read::testing::write_parquet_file;
    use crate::s3::S3Helper;

    #[tokio::test]
    async fn test_single_parquet_file_ordered() {
        let (_file1, prepared1) = mk_file(&[
            (0, 1, 1, "a", "b"),
            (0, 2, 0, "c", "d"),
            (1, 0, 1, "e", "f"),
        ]);

        check_read_table(
            vec![prepared1],
            mk_batch(&[
                (0, 1, 1, "a", "b"),
                (0, 2, 0, "c", "d"),
                (1, 0, 1, "e", "f"),
            ]),
            None,
            None,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_multi_parquet_file_no_overlap() {
        let (_file1, prepared1) = mk_file(&[(0, 1, 1, "a", "b"), (0, 2, 0, "c", "d")]);
        let (_file2, prepared2) = mk_file(&[(1, 0, 0, "g", "h"), (1, 0, 1, "e", "f")]);

        check_read_table(
            vec![prepared1, prepared2],
            mk_batch(&[
                (0, 1, 1, "a", "b"),
                (0, 2, 0, "c", "d"),
                (1, 0, 0, "g", "h"),
                (1, 0, 1, "e", "f"),
            ]),
            None,
            None,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_multi_parquet_file_overlap() {
        let (_file1, prepared1) = mk_file(&[
            (0, 2, 0, "c", "d"),
            (1, 0, 1, "e", "f"),
            (2, 0, 0, "x", "y"),
        ]);

        let (_file2, prepared2) = mk_file(&[(0, 1, 1, "a", "b"), (1, 0, 0, "g", "h")]);

        check_read_table(
            vec![prepared1, prepared2],
            mk_batch(&[
                (0, 1, 1, "a", "b"),
                (0, 2, 0, "c", "d"),
                (1, 0, 0, "g", "h"),
                (1, 0, 1, "e", "f"),
                (2, 0, 0, "x", "y"),
            ]),
            None,
            None,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_multi_file_resume() {
        // Batch 1 contains times from [0, 2]
        let (_file1, prepared1) = mk_file(&[
            (0, 2, 0, "c", "d"),
            (1, 0, 1, "e", "f"),
            (2, 0, 0, "x", "y"),
        ]);

        // Batch 2 contains times from [3, 5]
        let (_file2, prepared2) = mk_file(&[(3, 1, 1, "a", "b"), (5, 0, 0, "g", "h")]);

        check_read_table(
            vec![prepared1, prepared2],
            mk_batch(&[(3, 1, 1, "a", "b"), (5, 0, 0, "g", "h")]),
            // Snapshot contains data from time 2, so it expects all events up to
            // and including that time to be included in the snapshot. Only events
            // after time 2 should be processed, which means only batch 2.
            Some(NaiveDateTime::from_timestamp_opt(0, 2).unwrap()),
            None,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    #[ignore = "FinalAtTime unsupported"]
    async fn test_max_event_timestamp_lte_middle() {
        // Batch 1 contains times from [0, 2]
        let (_file1, prepared1) = mk_file(&[
            (0, 2, 0, "c", "d"),
            (1, 0, 1, "e", "f"),
            (2, 0, 0, "x", "y"),
        ]);

        check_read_table(
            vec![prepared1],
            mk_batch(&[(0, 2, 0, "c", "d"), (1, 0, 1, "e", "f")]),
            None,
            Some(Timestamp {
                seconds: 0,
                nanos: 1,
            }),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    #[ignore = "FinalAtTime unsupported"]
    async fn test_max_event_timestamp_no_results() {
        // Batch 1 contains times from [0, 2]
        let (_file1, prepared1) = mk_file(&[
            (10, 2, 0, "c", "d"),
            (11, 0, 1, "e", "f"),
            (12, 0, 0, "x", "y"),
        ]);

        check_read_table(
            vec![prepared1],
            mk_batch(&[]),
            None,
            Some(Timestamp {
                seconds: 0,
                nanos: 5,
            }),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    #[ignore = "FinalAtTime unsupported"]
    async fn test_max_event_timestamp_all_results() {
        // Batch 1 contains times from [0, 2]
        let (_file1, prepared1) = mk_file(&[
            (0, 2, 0, "c", "d"),
            (1, 0, 1, "e", "f"),
            (2, 0, 0, "x", "y"),
        ]);

        check_read_table(
            vec![prepared1],
            mk_batch(&[
                (0, 2, 0, "c", "d"),
                (1, 0, 1, "e", "f"),
                (2, 0, 0, "x", "y"),
            ]),
            None,
            Some(Timestamp {
                seconds: 0,
                nanos: 5,
            }),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_multi_file_resume_partial() {
        // Batch 1 contains times from [0, 2]
        let (_file1, prepared1) = mk_file(&[
            (0, 2, 0, "c", "d"),
            (1, 0, 1, "e", "f"),
            (2, 0, 0, "x", "y"),
        ]);

        // Batch 2 contains times from [1, 5]
        let (_file2, prepared2) = mk_file(&[(1, 1, 1, "a", "b"), (5, 0, 0, "g", "h")]);

        let err = check_read_table(
            vec![prepared1, prepared2],
            mk_batch(&[(1, 1, 1, "a", "b"), (5, 0, 0, "g", "h")]),
            // Snapshot exists at time 2, so only events > 2 should be processed.
            Some(NaiveDateTime::from_timestamp_opt(0, 2).unwrap()),
            None,
        )
        .await
        .err()
        .unwrap()
        .to_string();

        // For now, this is an error. Eventually, we should be able to handle
        // the partial file. It would just mean that we read only those rows
        // of the file after the given time.
        assert!(
            err.contains("data partially before the snapshot time"),
            "Was: {err}"
        );
    }

    #[tokio::test]
    async fn test_select_prepared_files_skips_unnecessary_ones() {
        sparrow_testing::init_test_logging();

        let max_event_in_snapshot = NaiveDateTime::from_timestamp_opt(0, 10).unwrap();
        let (_file1, prepared1) = mk_file(&[(0, 2, 0, "c", "d"), (5, 0, 0, "x", "y")]);
        let (_file2, prepared2) = mk_file(&[(11, 2, 0, "c", "d"), (12, 0, 0, "x", "y")]);
        let (_file3, prepared3) = mk_file(&[(0, 2, 0, "c", "d"), (10, 0, 0, "x", "y")]);
        let (_file4, prepared4) = mk_file(&[(0, 2, 0, "c", "d"), (4, 0, 0, "x", "y")]);
        let (_file5, prepared5) = mk_file(&[(11, 2, 0, "c", "d"), (15, 0, 0, "x", "y")]);
        let prepared_files = vec![prepared1, prepared2, prepared3, prepared4, prepared5];

        let mut data_context = DataContext::default();
        let schema =
            sparrow_api::kaskada::v1alpha::Schema::try_from(TABLE_SCHEMA.as_ref()).unwrap();
        let table_id = data_context
            .add_table(ComputeTable {
                config: Some(CONFIG.clone()),
                metadata: Some(TableMetadata {
                    file_count: prepared_files.len() as i64,
                    schema: Some(schema),
                }),
                file_sets: vec![compute_table::FileSet {
                    slice_plan: None,
                    prepared_files,
                }],
            })
            .unwrap();
        let table_info = data_context.table_info(table_id).unwrap();

        let mut data_manager = DataManager::new(S3Helper::new().await);
        let data_handles = select_prepared_files(
            &mut data_manager,
            table_info,
            &None,
            Some(max_event_in_snapshot),
        )
        .unwrap();
        assert_eq!(data_handles.len(), 2);
        for d in data_handles {
            assert!(d.max_event_time() > max_event_in_snapshot)
        }
    }

    #[test]
    #[ignore = "Multiple files with different schemas unsupported - see https://github.com/apache/arrow-rs/issues/782"]
    fn test_multi_parquet_file_diff_field_order() {
        todo!()
    }

    #[test]
    #[ignore = "Multiple files with different schemas unsupported - see https://github.com/apache/arrow-rs/issues/782"]
    fn test_multi_parquet_file_same_schema_projection() {
        todo!()
    }

    #[test]
    #[ignore = "Multiple files with different schemas unsupported - see https://github.com/apache/arrow-rs/issues/782"]
    fn test_multi_parquet_file_diff_schema_projection() {
        todo!()
    }

    #[test]
    #[ignore = "Multiple files with different schemas unsupported - see https://github.com/apache/arrow-rs/issues/782"]
    fn test_multi_parquet_file_diff_field_order_projection() {
        todo!()
    }

    async fn check_read_table(
        prepared_files: Vec<PreparedFile>,
        expected: RecordBatch,
        max_time_processed: Option<NaiveDateTime>,
        max_event_time: Option<Timestamp>,
    ) -> error_stack::Result<(), Error> {
        sparrow_testing::init_test_logging();

        let mut data_context = DataContext::default();
        let schema =
            sparrow_api::kaskada::v1alpha::Schema::try_from(TABLE_SCHEMA.as_ref()).unwrap();
        let table_id = data_context
            .add_table(ComputeTable {
                config: Some(CONFIG.clone()),
                metadata: Some(TableMetadata {
                    file_count: prepared_files.len() as i64,
                    schema: Some(schema),
                }),
                file_sets: vec![compute_table::FileSet {
                    slice_plan: None,
                    prepared_files,
                }],
            })
            .unwrap();
        let table_info = data_context.table_info(table_id).unwrap();

        let upper_bound_opt = if let Some(ts) = max_event_time {
            NaiveDateTime::from_timestamp_opt(ts.seconds, ts.nanos as u32)
        } else {
            None
        };

        let mut data_manager = DataManager::new(S3Helper::new().await);
        let actual: Vec<_> = table_reader(
            &mut data_manager,
            table_info,
            &None,
            None,
            FlightRecorder::disabled(),
            max_time_processed,
            upper_bound_opt,
        )?
        .try_collect()
        .await?;

        assert_eq!(expected.schema(), actual[0].schema());
        let actual: Vec<_> = actual.into_iter().map(|b| b.data).collect();
        let actual = arrow::compute::concat_batches(&expected.schema(), &actual).unwrap();

        assert_eq!(actual, expected);
        Ok(())
    }

    #[dynamic]
    static CONFIG: TableConfig = TableConfig::new(
        "TestTable",
        &Uuid::new_v4(),
        "time",
        Some("subsort"),
        "key",
        "grouping",
    );

    #[dynamic]
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

    #[dynamic]
    static TABLE_SCHEMA: SchemaRef = {
        Arc::new(Schema::new(vec![
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

    fn mk_batch_metadata(
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

    fn mk_batch(rows: &[(i64, u64, u64, &'static str, &'static str)]) -> RecordBatch {
        let (batch, _) = mk_batch_metadata(rows);
        batch
    }

    fn mk_file(
        rows: &[(i64, u64, u64, &'static str, &'static str)],
    ) -> (tempfile::TempPath, PreparedFile) {
        let (batch, metadata) = mk_batch_metadata(rows);

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
}
