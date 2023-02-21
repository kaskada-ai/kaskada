use crate::kaskada::sparrow::v1alpha::flight_record::register_metric::MetricKind;
use crate::kaskada::sparrow::v1alpha::flight_record::{Record, RegisterActivity, RegisterMetric};
use crate::kaskada::sparrow::v1alpha::FlightRecord;
use crate::Activity;

pub enum Registration {
    Activity(Activity),
    Metric {
        metric_id: u32,
        label: &'static str,
        kind: MetricKind,
    },
}

impl Registration {
    pub(super) fn to_flight_record(&self) -> FlightRecord {
        match self {
            Self::Activity(activity) => FlightRecord {
                record: Some(Record::RegisterActivity(RegisterActivity {
                    activity_id: activity.activity_id,
                    label: activity.label.to_owned(),
                    parent_activity_id: activity.parent_activity_id,
                })),
            },
            Self::Metric {
                metric_id,
                label,
                kind,
            } => FlightRecord {
                record: Some(Record::RegisterMetric(RegisterMetric {
                    metric_id: *metric_id,
                    kind: *kind as i32,
                    label: (*label).to_owned(),
                })),
            },
        }
    }
}

inventory::collect!(Registration);
