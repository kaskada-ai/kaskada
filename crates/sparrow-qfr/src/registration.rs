use crate::kaskada::sparrow::v1alpha::flight_record_header::{
    self, RegisterActivity, RegisterMetric,
};
use crate::kaskada::sparrow::v1alpha::FlightRecordHeader;
use crate::{Activity, IntoMetricValue, Metric, MetricKindTrait};

/// Trait for things that can be registered with the flight recorder.
pub trait PushRegistration<T> {
    fn add(&mut self, item: T);
}

#[derive(Default)]
pub struct Registrations {
    pub(super) activities: Vec<RegisterActivity>,
    pub(super) metrics: Vec<RegisterMetric>,
}

impl PushRegistration<Activity> for Registrations {
    fn add(&mut self, item: Activity) {
        self.activities.push(RegisterActivity {
            activity_id: item.activity_id,
            label: item.label.to_owned(),
            parent_activity_id: item.parent_activity_id,
        })
    }
}

impl<T, K> PushRegistration<Metric<T, K>> for Registrations
where
    T: IntoMetricValue,
    K: MetricKindTrait<T>,
{
    fn add(&mut self, item: Metric<T, K>) {
        self.metrics.push(RegisterMetric {
            metric_id: item.metric_id,
            kind: K::KIND as i32,
            label: item.label.to_owned(),
        })
    }
}

pub struct Registration(once_cell::sync::Lazy<Registrations>);

impl Registration {
    pub const fn new(f: fn() -> Registrations) -> Self {
        Self(once_cell::sync::Lazy::new(f))
    }
}

impl Registrations {
    fn from_inventory() -> Self {
        let mut registrations = Self::default();
        for registration in inventory::iter::<&'static Registration> {
            registrations.extend(&registration.0)
        }
        registrations
    }

    fn extend(&mut self, other: &Self) {
        self.activities.extend_from_slice(&other.activities);
        self.metrics.extend_from_slice(&other.metrics);
    }
}

inventory::collect!(&'static Registration);

impl FlightRecordHeader {
    pub fn with_registrations(
        request_id: String,
        build_info: flight_record_header::BuildInfo,
    ) -> Self {
        let Registrations {
            activities,
            metrics,
        } = Registrations::from_inventory();

        Self {
            version: crate::QFR_VERSION,
            request_id,
            sparrow_build_info: Some(build_info),
            activities,
            metrics,
        }
    }
}
