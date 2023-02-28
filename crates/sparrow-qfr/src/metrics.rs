use std::marker::PhantomData;

use crate::kaskada::sparrow::v1alpha::flight_record_header::register_metric::MetricKind;
use crate::kaskada::sparrow::v1alpha::{metric_value, MetricValue};

/// Trait for values that can be encoded as metric values.
pub trait IntoMetricValue {
    fn into_metric_value(self) -> metric_value::Value;
}

impl IntoMetricValue for u64 {
    fn into_metric_value(self) -> metric_value::Value {
        metric_value::Value::U64Value(self)
    }
}
impl IntoMetricValue for usize {
    fn into_metric_value(self) -> metric_value::Value {
        metric_value::Value::U64Value(self as u64)
    }
}
impl IntoMetricValue for u32 {
    fn into_metric_value(self) -> metric_value::Value {
        metric_value::Value::U64Value(self as u64)
    }
}

impl IntoMetricValue for i64 {
    fn into_metric_value(self) -> metric_value::Value {
        metric_value::Value::I64Value(self)
    }
}
impl IntoMetricValue for i32 {
    fn into_metric_value(self) -> metric_value::Value {
        metric_value::Value::I64Value(self as i64)
    }
}

impl IntoMetricValue for f64 {
    fn into_metric_value(self) -> metric_value::Value {
        metric_value::Value::F64Value(self)
    }
}

pub trait MetricKindTrait<T>
where
    T: IntoMetricValue,
{
    const KIND: MetricKind;
}

pub struct GaugeKind;
impl MetricKindTrait<u64> for GaugeKind {
    const KIND: MetricKind = MetricKind::U64Gauge;
}
impl MetricKindTrait<usize> for GaugeKind {
    const KIND: MetricKind = MetricKind::U64Gauge;
}
impl MetricKindTrait<u32> for GaugeKind {
    const KIND: MetricKind = MetricKind::U64Gauge;
}
impl MetricKindTrait<i64> for GaugeKind {
    const KIND: MetricKind = MetricKind::I64Gauge;
}
impl MetricKindTrait<i32> for GaugeKind {
    const KIND: MetricKind = MetricKind::I64Gauge;
}
impl MetricKindTrait<f64> for GaugeKind {
    const KIND: MetricKind = MetricKind::F64Gauge;
}

pub struct CounterKind;
impl MetricKindTrait<u64> for CounterKind {
    const KIND: MetricKind = MetricKind::U64Counter;
}
impl MetricKindTrait<i64> for CounterKind {
    const KIND: MetricKind = MetricKind::I64Counter;
}
impl MetricKindTrait<f64> for CounterKind {
    const KIND: MetricKind = MetricKind::F64Counter;
}

pub struct Metric<T, K>
where
    T: IntoMetricValue,
    K: MetricKindTrait<T>,
{
    pub label: &'static str,
    pub metric_id: u32,
    _phantom: PhantomData<fn(T, K) -> K>,
}

impl<T, K> Metric<T, K>
where
    T: IntoMetricValue,
    K: MetricKindTrait<T>,
{
    pub const fn new(label: &'static str, metric_id: u32) -> Self {
        Self {
            label,
            metric_id,
            _phantom: PhantomData,
        }
    }

    pub(super) fn value(&self, value: T) -> MetricValue {
        MetricValue {
            metric_id: self.metric_id,
            value: Some(value.into_metric_value()),
        }
    }
}

pub type Gauge<T> = Metric<T, GaugeKind>;
pub type Counter<T> = Metric<T, CounterKind>;
