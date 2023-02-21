use fallible_iterator::FallibleIterator;
use tempfile::NamedTempFile;

use super::{FlightRecordReader, FlightRecordWriter};
use crate::kaskada::sparrow::v1alpha::flight_record::{RegisterActivity, ReportActivity};
use crate::kaskada::sparrow::v1alpha::flight_record_header::BuildInfo;
use crate::kaskada::sparrow::v1alpha::metric_value::Value;
use crate::kaskada::sparrow::v1alpha::{FlightRecord, FlightRecordHeader, MetricValue};

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
        record: Some(
            crate::kaskada::sparrow::v1alpha::flight_record::Record::RegisterActivity(
                RegisterActivity {
                    activity_id: 57,
                    label: "hello".to_owned(),
                    parent_activity_id: None,
                },
            ),
        ),
    };

    let record2 = FlightRecord {
        record: Some(
            crate::kaskada::sparrow::v1alpha::flight_record::Record::RegisterActivity(
                RegisterActivity {
                    activity_id: 58,
                    label: "world".to_owned(),
                    parent_activity_id: Some(57),
                },
            ),
        ),
    };

    let record3 = FlightRecord {
        record: Some(
            crate::kaskada::sparrow::v1alpha::flight_record::Record::ReportActivity(
                ReportActivity {
                    activity_id: 58,
                    thread_id: 18,
                    wall_timestamp_us: 1000,
                    wall_duration_us: 8710,
                    cpu_duration_us: 879_791_878,
                    metrics: vec![MetricValue {
                        metric_id: 987,
                        value: Some(Value::I64Value(18)),
                    }],
                },
            ),
        ),
    };

    // Write some records.
    let mut writer = FlightRecordWriter::try_new(file.reopen().unwrap(), header.clone()).unwrap();
    writer.write(record1.clone()).unwrap();
    writer.write(record2.clone()).unwrap();
    writer.write(record3.clone()).unwrap();
    writer.flush().unwrap();

    let reader = FlightRecordReader::try_new(file.path()).unwrap();
    assert_eq!(reader.header(), &header);

    let records: Vec<_> = reader.records().unwrap().collect().unwrap();
    assert_eq!(records, vec![record1, record2, record3]);

    file.close().unwrap();
}
