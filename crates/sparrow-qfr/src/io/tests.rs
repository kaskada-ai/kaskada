use fallible_iterator::FallibleIterator;
use tempfile::NamedTempFile;

use crate::kaskada::sparrow::v1alpha::flight_record::execute_pass_metrics::Cause;
use crate::kaskada::sparrow::v1alpha::flight_record::{EventType, ExecutePassMetrics, Metrics};
use crate::kaskada::sparrow::v1alpha::flight_record_header::BuildInfo;
use crate::kaskada::sparrow::v1alpha::thread_id::ThreadKind;
use crate::kaskada::sparrow::v1alpha::{FlightRecord, FlightRecordHeader, ThreadId};
use crate::{FlightRecordReader, FlightRecordWriter};

#[test]
fn round_trip_small() {
    let file = NamedTempFile::new().unwrap();

    let header = FlightRecordHeader {
        version: 1,
        request_id: "request_id".to_owned(),
        sparrow_build_info: Some(BuildInfo {
            sparrow_version: "sparrow_version".to_owned(),
            github_ref: "ref".to_owned(),
            github_sha: "sha".to_owned(),
            github_workflow: "workflow".to_owned(),
        }),
    };

    let record1 = FlightRecord {
        thread_id: Some(ThreadId {
            pass_id: 5000,
            kind: ThreadKind::Merge as i32,
            thread_id: 0,
        }),
        event_type: EventType::ExecutePass as i32,
        index: 5,
        wall_timestamp_us: 573,
        wall_duration_us: 200,
        cpu_duration_us: 180,
        metrics: None,
    };

    let record2 = FlightRecord {
        thread_id: Some(ThreadId {
            pass_id: 5010,
            kind: ThreadKind::Merge as i32,
            thread_id: 0,
        }),
        event_type: EventType::MergeReceiveInput as i32,
        index: 8,
        wall_timestamp_us: 878,
        wall_duration_us: 240,
        cpu_duration_us: 1880,
        metrics: Some(Metrics::ExecutePass(ExecutePassMetrics {
            num_rows: 5000,
            num_cumulative_entities: 8000,
            cause: Cause::Inputs as i32,
        })),
    };

    let record3 = FlightRecord {
        thread_id: Some(ThreadId {
            pass_id: 5000,
            kind: ThreadKind::SinkProcessor as i32,
            thread_id: 10,
        }),
        event_type: EventType::ExecutePassInstruction as i32,
        index: 13,
        wall_timestamp_us: 588,
        wall_duration_us: 240,
        cpu_duration_us: 1880,
        metrics: None,
    };

    // Write some records.
    let mut writer = FlightRecordWriter::try_new(file.reopen().unwrap(), header.clone()).unwrap();
    writer
        .write_batch(vec![record1.clone(), record2.clone()])
        .unwrap();

    writer.write_batch(vec![record3.clone()]).unwrap();
    writer.flush().unwrap();

    let reader = FlightRecordReader::try_new(file.path()).unwrap();
    assert_eq!(reader.header(), &header);

    let records = reader.events().unwrap();
    let records: Vec<_> = records.collect().unwrap();
    assert_eq!(records, vec![record1, record2, record3]);

    file.close().unwrap();
}
