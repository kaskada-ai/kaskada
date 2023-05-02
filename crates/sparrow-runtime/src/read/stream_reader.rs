use futures::stream::BoxStream;
use std::sync::Arc;

use anyhow::Context;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use chrono::NaiveDateTime;
use error_stack::{IntoReportCompat, ResultExt};
use futures::Stream;
use hashbrown::HashSet;
use itertools::Itertools;
use sparrow_api::kaskada::v1alpha::slice_plan::Slice;
use sparrow_api::kaskada::v1alpha::PulsarSubscription;
use sparrow_compiler::TableInfo;
use sparrow_core::TableSchema;
use sparrow_qfr::{
    activity, gauge, Activity, FlightRecorder, Gauge, PushRegistration, Registration, Registrations,
};
use tokio_stream::StreamExt;
use tracing::info;

use crate::data_manager::{DataHandle, DataManager};
use crate::merge::{homogeneous_merge, GatheredBatches, Gatherer};
use crate::min_heap::{HasPriority, MinHeap};
use crate::read::parquet_stream::{self, new_parquet_stream};
use crate::streams::pulsar_stream;
use crate::Batch;

// const READ_TABLE: Activity = activity!("scan.read_file");
// const GATHER_TABLE_BATCHES: Activity = activity!("scan.gather");
// const MERGE_TABLE_BATCHES: Activity = activity!("scan.merge");

// const MIN_BATCH_TIME: Gauge<i64> = gauge!("min_time_in_batch");
// const MAX_BATCH_TIME: Gauge<i64> = gauge!("max_time_in_batch");
// const NUM_INPUT_ROWS: Gauge<usize> = gauge!("num_input_rows");
// const NUM_OUTPUT_ROWS: Gauge<usize> = gauge!("num_output_rows");
// const ACTIVE_SOURCES: Gauge<usize> = gauge!("active_files");
// const REMAINING_FILES: Gauge<usize> = gauge!("remaining_files");

// static REGISTRATION: Registration = Registration::new(|| {
//     let mut r = Registrations::default();
//     r.add(READ_TABLE);
//     r.add(GATHER_TABLE_BATCHES);
//     r.add(MERGE_TABLE_BATCHES);

//     r.add(MIN_BATCH_TIME);
//     r.add(MAX_BATCH_TIME);
//     r.add(NUM_INPUT_ROWS);
//     r.add(NUM_OUTPUT_ROWS);
//     r.add(ACTIVE_SOURCES);
//     r.add(REMAINING_FILES);
//     r
// });

// inventory::submit!(&REGISTRATION);

/// Create a stream that reads the contents of the given stream
pub fn stream_reader(
    data_manager: &mut DataManager,
    // TODO: FRAZ - I need the PulsarConfig here in order to connect to the correct pulsar topic
    // with auth, and whatnot.
    table_info: &TableInfo,
    projected_columns: Option<Vec<String>>,
    flight_recorder: FlightRecorder,
    max_event_in_snapshot: Option<NaiveDateTime>,
    upper_bound_opt: Option<NaiveDateTime>,
) -> error_stack::Result<impl Stream<Item = error_stack::Result<Batch, Error>> + 'static, Error> {
    // This schema came from Wren, therefore it should be the user facing schema.
    // Therefore it should *not* already have the key columns.
    // This adds the key columns to the schema.
    let schema = TableSchema::try_from_data_schema(table_info.schema().as_ref())
        .into_report()
        .change_context(Error::LoadTableSchema)?;

    // Create an owned version of the table info so the stream can be `'static`.
    let table_name = table_info.name().to_owned();

    // TODO: This for now just assumes pulsar. We can maybe make this stream_reader generic to handle
    // multiple types of streaming platforms.
    let subscription = PulsarSubscription::default();

    // TODO: cloning table info is necessary since I use it to create the pulsar stream, meaning the reference must outlive the static lifetime of the stream.
    // Is there a better option?
    let table_info = table_info.clone();
    let config = todo!(); // TODO: FRAZ
    let stream = async_stream::try_stream! {
            let consumer = pulsar_stream::consumer(&subscription, table_info.schema().clone())
                .await
                .change_context(Error::CreateStream)?;
            let mut stream = Box::pin(pulsar_stream::stream_for_execution(
                table_info.schema().clone(),
                consumer,
                subscription.last_publish_time,
                config,
            ));

    // TODO: This should indefinitely loop. Errors are unexpected.
            while let Some(next) = stream.try_next().await.unwrap() {
                let batch = Batch::try_new_from_batch(next)
                    .into_report()
                    .change_context(Error::ReadNextBatch).unwrap();
                yield batch
            }
        };

    Ok(stream)
}

// TODO: FRAZ - move this to error.rs file in read package
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
}
