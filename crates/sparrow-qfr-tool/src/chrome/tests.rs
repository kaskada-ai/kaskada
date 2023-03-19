use sparrow_qfr::kaskada::sparrow::v1alpha::flight_record::{
    Record, RegisterThread, ReportActivity, ReportMetrics,
};
use sparrow_qfr::kaskada::sparrow::v1alpha::flight_record_header::register_metric::MetricKind;
use sparrow_qfr::kaskada::sparrow::v1alpha::flight_record_header::{
    BuildInfo, RegisterActivity, RegisterMetric,
};
use sparrow_qfr::kaskada::sparrow::v1alpha::{
    metric_value, FlightRecord, FlightRecordHeader, MetricValue,
};

use crate::chrome::conversion::Conversion;
use crate::chrome_tracing::Trace;

fn run_test_ok(
    activities: Vec<RegisterActivity>,
    metrics: Vec<RegisterMetric>,
    records: Vec<Record>,
) -> Trace {
    let header = FlightRecordHeader {
        version: sparrow_qfr::QFR_VERSION,
        request_id: "request_id".to_owned(),
        sparrow_build_info: Some(BuildInfo {
            sparrow_version: "test_version".to_owned(),
            github_ref: "".to_owned(),
            github_sha: "".to_owned(),
            github_workflow: "".to_owned(),
        }),
        activities,
        metrics,
    };

    let conversion = Conversion::new(&header);
    let records =
        fallible_iterator::convert::<_, error_stack::Report<sparrow_qfr::io::reader::Error>, _>(
            records
                .into_iter()
                .map(|record| FlightRecord {
                    record: Some(record),
                })
                .map(Ok),
        );
    conversion
        .convert_all(records)
        .expect("successful conversion")
}

#[test]
fn test_conversion_empty_trace() {
    let activities = vec![];
    let metrics = vec![];
    let records = vec![];
    insta::assert_json_snapshot!(run_test_ok(activities, metrics, records));
}

#[test]
fn test_conversion_some_activities() {
    let activities = vec![
        RegisterActivity {
            activity_id: 17,
            label: "child1".to_owned(),
            parent_activity_id: Some(18),
        },
        RegisterActivity {
            activity_id: 18,
            label: "root".to_owned(),
            parent_activity_id: None,
        },
        RegisterActivity {
            activity_id: 19,
            label: "child2".to_owned(),
            parent_activity_id: Some(18),
        },
    ];
    let metrics = vec![];

    let thread_id = 11;
    let records = vec![
        Record::RegisterThread(RegisterThread {
            thread_id,
            label: "operation 0".to_owned(),
        }),
        Record::ReportActivity(ReportActivity {
            activity_id: 19,
            thread_id,
            wall_timestamp_us: 1_000_000,
            wall_duration_us: 2_573,
            cpu_duration_us: 1_172,
            metrics: vec![],
        }),
    ];
    insta::assert_json_snapshot!(run_test_ok(activities, metrics, records));
}

#[test]
fn test_conversion_activity_metrics() {
    let activities = vec![RegisterActivity {
        activity_id: 18,
        label: "root".to_owned(),
        parent_activity_id: None,
    }];
    let metrics = vec![RegisterMetric {
        metric_id: 87,
        label: "gauge_metric".to_owned(),
        kind: MetricKind::U64Gauge as i32,
    }];

    let thread_id = 11;
    let records = vec![
        Record::RegisterThread(RegisterThread {
            thread_id,
            label: "operation 0".to_owned(),
        }),
        Record::ReportActivity(ReportActivity {
            activity_id: 18,
            thread_id,
            wall_timestamp_us: 1_000_000,
            wall_duration_us: 2_573,
            cpu_duration_us: 1_172,
            metrics: vec![MetricValue {
                metric_id: 87,
                value: Some(metric_value::Value::U64Value(18)),
            }],
        }),
    ];
    insta::assert_json_snapshot!(run_test_ok(activities, metrics, records));
}

#[test]
fn test_conversion_root_metrics() {
    let activities = vec![RegisterActivity {
        activity_id: 18,
        label: "root".to_owned(),
        parent_activity_id: None,
    }];
    let metrics = vec![RegisterMetric {
        metric_id: 87,
        label: "gauge_metric".to_owned(),
        kind: MetricKind::U64Gauge as i32,
    }];

    let thread_id = 11;
    let records = vec![
        Record::RegisterThread(RegisterThread {
            thread_id,
            label: "operation 0".to_owned(),
        }),
        Record::ReportMetrics(ReportMetrics {
            thread_id,
            wall_timestamp_us: 1_000_000,
            metrics: vec![MetricValue {
                metric_id: 87,
                value: Some(metric_value::Value::U64Value(18)),
            }],
        }),
    ];
    insta::assert_json_snapshot!(run_test_ok(activities, metrics, records));
}
