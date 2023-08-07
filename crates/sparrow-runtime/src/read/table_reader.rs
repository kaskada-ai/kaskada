use anyhow::Context;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use chrono::NaiveDateTime;
use error_stack::{IntoReportCompat, ResultExt};
use futures::stream::BoxStream;
use futures::Stream;
use hashbrown::HashSet;
use itertools::Itertools;
use sparrow_api::kaskada::v1alpha::slice_plan::Slice;
use sparrow_api::kaskada::v1alpha::PreparedFile;
use sparrow_compiler::TableInfo;
use sparrow_core::TableSchema;
use sparrow_qfr::{
    activity, gauge, Activity, FlightRecorder, Gauge, PushRegistration, Registration, Registrations,
};
use tokio_stream::StreamExt;
use tracing::info;

use crate::min_heap::{HasPriority, MinHeap};
use crate::read::error::Error;
use crate::read::parquet_stream::{self, new_parquet_stream};
use crate::stores::ObjectStoreRegistry;
use crate::Batch;
use sparrow_merge::old::{homogeneous_merge, GatheredBatches, Gatherer};

const READ_TABLE: Activity = activity!("scan.read_file");
const GATHER_TABLE_BATCHES: Activity = activity!("scan.gather");
const MERGE_TABLE_BATCHES: Activity = activity!("scan.merge");

const MIN_BATCH_TIME: Gauge<i64> = gauge!("min_time_in_batch");
const MAX_BATCH_TIME: Gauge<i64> = gauge!("max_time_in_batch");
const NUM_INPUT_ROWS: Gauge<usize> = gauge!("num_input_rows");
const NUM_OUTPUT_ROWS: Gauge<usize> = gauge!("num_output_rows");
const ACTIVE_SOURCES: Gauge<usize> = gauge!("active_files");
const REMAINING_FILES: Gauge<usize> = gauge!("remaining_files");

static REGISTRATION: Registration = Registration::new(|| {
    let mut r = Registrations::default();
    r.add(READ_TABLE);
    r.add(GATHER_TABLE_BATCHES);
    r.add(MERGE_TABLE_BATCHES);

    r.add(MIN_BATCH_TIME);
    r.add(MAX_BATCH_TIME);
    r.add(NUM_INPUT_ROWS);
    r.add(NUM_OUTPUT_ROWS);
    r.add(ACTIVE_SOURCES);
    r.add(REMAINING_FILES);
    r
});

inventory::submit!(&REGISTRATION);

/// Create a stream that reads the contents of the given table.
pub async fn table_reader(
    object_stores: &ObjectStoreRegistry,
    table_info: &TableInfo,
    requested_slice: &Option<Slice>,
    projected_columns: Option<Vec<String>>,
    flight_recorder: FlightRecorder,
    max_event_in_snapshot: Option<NaiveDateTime>,
    upper_bound_opt: Option<NaiveDateTime>,
) -> error_stack::Result<impl Stream<Item = error_stack::Result<Batch, Error>> + 'static, Error> {
    let data_handles = select_prepared_files(table_info, requested_slice, max_event_in_snapshot)?;

    let mut gatherer = Gatherer::new(data_handles.len(), None);
    let mut active = Vec::with_capacity(data_handles.len());

    // This schema came from Wren, therefore it should be the user facing schema.
    // Therefore it should *not* already have the key columns.
    // This adds the key columns to the schema.
    let schema = TableSchema::try_from_data_schema(table_info.schema().as_ref())
        .into_report()
        .change_context(Error::LoadTableSchema)?;
    // Project the columns from the schema.
    // TODO: Cleanup this duplication.
    let projected_schema = projected_schema(schema, &projected_columns)?;

    for (index, prepared_file) in data_handles.into_iter().enumerate() {
        // The file contains no data less than the first row in the file.
        //
        // Inform the gatherer that this input is empty up to that point so it doesn't
        // "wait" for a batch from this input to create "complete" batches at times less
        // than the given time.
        //
        // TODO: This would be cleaner if we could instead *add* the file to the
        // gatherer once it has data. This would require the gatherer have a bit
        // more logic for dynamically growing the set of managed files.
        let min_event_time = prepared_file
            .min_event_time()
            .change_context(Error::Internal)?
            .timestamp_nanos();
        let max_event_time = prepared_file
            .max_event_time()
            .change_context(Error::Internal)?
            .timestamp_nanos();

        info!(
            "Skipping to time {} for data file {:?} for index {}",
            min_event_time, prepared_file, index
        );
        gatherer
            .skip_to(index, min_event_time)
            .into_report()
            .change_context(Error::SkippingToMinEvent)?;

        let stream = new_parquet_stream(object_stores, &prepared_file.path, &projected_schema)
            .await
            .change_context(Error::CreateStream)?;
        active.push(ActiveInput {
            min_next_time: min_event_time,
            max_event_time,
            index,
            stream,
        });
    }

    let mut active = MinHeap::from(active);

    // Create an owned version of the table info so the stream can be `'static`.
    let table_name = table_info.name().to_owned();

    Ok(async_stream::try_stream! {
        let projected_schema_ref = projected_schema.schema_ref();

        while let Some(mut next_input) = active.pop() {
            let index = next_input.index;

            let next_batch = READ_TABLE.instrument::<error_stack::Result<_, Error>, _>(&flight_recorder, |metrics| {
                // Weird syntax because we can't easily say "move metrics but not projected schema".
                // This may get easier with async closures https://github.com/rust-lang/rust/issues/62290.
                let input = next_input.next_batch(upper_bound_opt);
                async move {
                    let input = input.await?;

                    if let Some(input) = &input {
                        metrics.report_metric(MIN_BATCH_TIME, input.lower_bound.time);
                        metrics.report_metric(MAX_BATCH_TIME, input.upper_bound.time);
                        metrics.report_metric(NUM_INPUT_ROWS, input.num_rows());
                    }

                    Ok(input)
                }
            }).await?;

            if next_batch.is_some() {
                // If we got a batch, the input is still active.
                active.push(next_input);
            }

            let active_len = active.len();
            let next_output = gather_next_output(
                &flight_recorder,
                &mut gatherer,
                index,
                next_batch,
                active_len
            )?;

            if let Some(next_output) = next_output {
                let batch = merge_next_output(&table_name, &flight_recorder, projected_schema_ref, next_output)
                     .into_report()
                     .change_context(Error::Internal)?;
                if batch.num_rows() > 0 {
                    yield Batch::try_new_from_batch(batch).into_report().change_context(Error::Internal)?;
                }
            }
        }
    })
}

fn gather_next_output(
    flight_recorder: &FlightRecorder,
    gatherer: &mut Gatherer<Batch>,
    index: usize,
    next_batch: Option<Batch>,
    active_len: usize,
) -> error_stack::Result<Option<GatheredBatches<Batch>>, Error> {
    let mut activation = GATHER_TABLE_BATCHES.start(flight_recorder);
    let next_output = gatherer
        .add_batch(index, next_batch)
        .into_report()
        .change_context(Error::Internal)?;
    if let Some(next_output) = &next_output {
        activation.report_metric(MIN_BATCH_TIME, next_output.min_time_inclusive);
        activation.report_metric(MAX_BATCH_TIME, next_output.max_time_inclusive);
    }
    activation.report_metric(ACTIVE_SOURCES, active_len);
    activation.report_metric(REMAINING_FILES, gatherer.remaining_sources());
    Ok(next_output)
}

/// Select the necessary prepared files for the given silce.
fn select_prepared_files(
    table_info: &TableInfo,
    requested_slice: &Option<Slice>,
    max_event_in_snapshot: Option<NaiveDateTime>,
) -> error_stack::Result<Vec<PreparedFile>, Error> {
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
                file: prepared_file.path.clone(),
                table_name: table_info.name().to_owned(),
                snapshot_time: max_event_in_snapshot
            }
        );

        selected_files.push(prepared_file.clone());
    }

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

fn merge_next_output(
    table_name: &str,
    flight_recorder: &FlightRecorder,
    projected_schema: &SchemaRef,
    gathered: GatheredBatches<Batch>,
) -> anyhow::Result<RecordBatch> {
    let mut activation = MERGE_TABLE_BATCHES.start(flight_recorder);

    // NOTE: The gathered batches are already totally ordered
    // (verified by debug assertions in `Batch`).
    let inputs: Vec<_> = gathered
        .batches
        .into_iter()
        .map(|input_batches| -> anyhow::Result<RecordBatch> {
            // Each `input_batches` is a set of batches from a single input file.
            // Collect and concatenate the non-empty batches.
            let input_batches: Vec<_> = input_batches.into_iter().map(|input| input.data).collect();
            let input_batch = arrow::compute::concat_batches(projected_schema, &input_batches)?;
            Ok(input_batch)
        })
        .try_collect()?;

    let num_input_rows = inputs.iter().map(|input| input.num_rows()).sum();
    let merged_batch = homogeneous_merge(projected_schema, inputs)
        .with_context(|| format!("merging batches for '{table_name}'"))?;

    // Ideally, the number of input rows would be reported per-source. But this
    // would require reporting a vector, which is not currently supported by metrics.
    // So we just report the total number of input rows across all inputs.
    activation.report_metric(NUM_INPUT_ROWS, num_input_rows);
    activation.report_metric(NUM_OUTPUT_ROWS, merged_batch.num_rows());
    Ok(merged_batch)
}

struct ActiveInput {
    /// The minimum possible time of the first row in the next batch from this
    /// input.
    ///
    /// Should not change while the item is in the active heap since it is used
    /// as the key.
    /// This is initially the minimum timestamp in a file. After that, it is the
    /// maximum timestamp of the previous batch.
    min_next_time: i64,
    /// Maximum event time from this input.
    max_event_time: i64,
    /// Index of this input within the gatherer.
    index: usize,
    stream: BoxStream<'static, error_stack::Result<Batch, parquet_stream::Error>>,
}

impl ActiveInput {
    /// Return the next batch (if any) from this input.
    ///
    /// If no reader has been created for the file, a reader is created.
    ///
    /// Note: Once this returns `None`, it will always return `None`. It
    /// should be removed from the active inputs and not consulted again.
    async fn next_batch(
        &mut self,
        upper_bound_opt: Option<NaiveDateTime>,
    ) -> error_stack::Result<Option<Batch>, Error> {
        if let Some(next) = self
            .stream
            .try_next()
            .await
            .change_context(Error::ReadNextBatch)?
        {
            error_stack::ensure!(
                self.min_next_time <= next.lower_bound.time,
                Error::MinNextGreaterThanNextLowerBound {
                    min_next_time: self.min_next_time,
                    lower_bound: next.lower_bound.time
                }
            );

            error_stack::ensure!(
                self.max_event_time >= next.upper_bound.time,
                Error::MaxEventTimeLessThanNextUpperBound {
                    max_event_time: self.max_event_time,
                    upper_bound: next.upper_bound.time
                }
            );

            self.min_next_time = next.upper_bound.time;

            // Filter out all events after a timestamp, if provided.
            //
            // This allows users to supply a specific timestamp to produce outputs at.
            // While the query should filter out rows past this timestamp, sparrow can
            // pre-emptively stop reading at this time to avoid doing unnecessary work.
            match upper_bound_opt {
                Some(upper_bound) => {
                    let times = next.times().into_report().change_context(Error::Internal)?;
                    let ts_nanos = upper_bound.timestamp_nanos();

                    // Slice the data such that all data is less than or equal to the upper bound
                    let length = match times.binary_search(&ts_nanos) {
                        Ok(mut length) => {
                            while times.get(length) == Some(&ts_nanos) {
                                length += 1
                            }
                            length
                        }
                        Err(length) => length,
                    };

                    if length == 0 {
                        Ok(None)
                    } else {
                        let slice = next.data().slice(0, length);
                        let batch = Batch::try_new_from_batch(slice)
                            .into_report()
                            .change_context(Error::Internal)?;
                        self.min_next_time = batch.upper_bound.time;
                        Ok(Some(batch))
                    }
                }
                None => {
                    error_stack::ensure!(
                        self.max_event_time >= next.upper_bound.time,
                        Error::MaxEventTimeLessThanNextUpperBound {
                            max_event_time: self.max_event_time,
                            upper_bound: next.upper_bound.time
                        }
                    );

                    self.min_next_time = next.upper_bound.time;
                    Ok(Some(next))
                }
            }
        } else {
            Ok(None)
        }
    }
}

/// [ActiveInput] occurs at the minimum possible timestamp of the next row.
///
/// When used with the [MinHeap] this ensures we read batches in order
/// to maximize the ability of the Gatherer to create complete batches.
impl HasPriority for ActiveInput {
    type Priority = i64;

    fn priority(&self) -> i64 {
        self.min_next_time
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
    use sparrow_arrow::downcast::downcast_primitive_array;
    use sparrow_compiler::DataContext;
    use sparrow_qfr::FlightRecorder;
    use static_init::dynamic;
    use uuid::Uuid;

    use super::*;
    use crate::read::testing::write_parquet_file;

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
        let table_info = data_context
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

        let prepared_files =
            select_prepared_files(table_info, &None, Some(max_event_in_snapshot)).unwrap();
        assert_eq!(prepared_files.len(), 2);
        for f in prepared_files {
            assert!(f.max_event_time().unwrap() > max_event_in_snapshot)
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
        let table_info = data_context
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

        let upper_bound_opt = if let Some(ts) = max_event_time {
            NaiveDateTime::from_timestamp_opt(ts.seconds, ts.nanos as u32)
        } else {
            None
        };

        let actual: Vec<_> = table_reader(
            &ObjectStoreRegistry::default(),
            table_info,
            &None,
            None,
            FlightRecorder::disabled(),
            max_time_processed,
            upper_bound_opt,
        )
        .await?
        .try_collect()
        .await?;

        if expected.num_rows() > 0 {
            assert_eq!(expected.schema(), actual[0].schema());
            let actual: Vec<_> = actual.into_iter().map(|b| b.data).collect();
            let actual = arrow::compute::concat_batches(&expected.schema(), &actual).unwrap();
            assert_eq!(actual, expected);
        } else {
            assert_eq!(actual.len(), 0);
        };
        Ok(())
    }

    #[dynamic]
    static CONFIG: TableConfig = TableConfig::new_with_table_source(
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
            path: format!("file://{}", parquet_file.display()),
            min_event_time: Some(min_event_time.into()),
            max_event_time: Some(max_event_time.into()),
            num_rows,
            metadata_path: format!("file://{}", metadata_parquet_file.display()),
        };

        (parquet_file, prepared)
    }
}
