use cpu_time::ThreadTime;
use tokio::time::Instant;

use crate::kaskada::sparrow::v1alpha::MetricValue;
use crate::{Activity, FlightRecorder, IntoMetricValue, Metric, MetricKindTrait};

/// Struct for tracking a batch activation of an activity.
pub struct Activation<'a> {
    activity_id: u32,
    recorder: &'a FlightRecorder,
    wall_start: Instant,
    cpu_start: ThreadTime,
    metrics: Vec<MetricValue>,
}

impl<'a> Activation<'a> {
    pub(super) fn new(activity: &Activity, recorder: &'a FlightRecorder) -> Self {
        Self {
            activity_id: activity.activity_id,
            recorder,
            wall_start: Instant::now(),
            cpu_start: ThreadTime::now(),
            metrics: vec![],
        }
    }

    pub fn report_metric<T, K>(&mut self, metric: Metric<T, K>, value: T)
    where
        T: IntoMetricValue,
        K: MetricKindTrait<T>,
    {
        self.metrics.push(metric.value(value));
    }

    pub fn finish(self) {
        // Nothing to do -- calling `finish` will drop the activation, which reports the time.
    }
}

impl<'a> Drop for Activation<'a> {
    fn drop(&mut self) {
        let cpu_elapsed = self.cpu_start.elapsed();
        let wall_elapsed = self.wall_start.elapsed();

        let metrics = std::mem::take(&mut self.metrics);
        self.recorder.report_activity(
            self.activity_id,
            self.wall_start,
            wall_elapsed,
            cpu_elapsed,
            metrics,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Since `Activation` uses CPU time it should not be sent between threads.
    // This is (currently) true because it stores a `ThreadTime`, but we add
    // a static assertion to make sure it continues to be true.
    static_assertions::assert_not_impl_any!(Activation<'static>: Send);
}
