use std::ops::DerefMut;
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::Duration;

use cpu_time::ThreadTime;
use futures::future::FusedFuture;
use futures::Future;
use pin_project::pin_project;
use tokio::time::Instant;

use crate::kaskada::sparrow::v1alpha::MetricValue;
use crate::{FlightRecorder, IntoMetricValue, Metric, MetricKindTrait};

#[derive(Default, Clone, Debug)]
pub struct Metrics(Arc<Mutex<Vec<MetricValue>>>);

impl Metrics {
    fn take_metrics(self) -> Vec<MetricValue> {
        std::mem::take(self.0.lock().expect("get lock").deref_mut())
    }

    pub fn report_metric<T, K>(&self, metric: Metric<T, K>, value: T)
    where
        T: IntoMetricValue,
        K: MetricKindTrait<T>,
    {
        let metric = metric.value(value);
        self.0.lock().expect("get lock").push(metric);
    }
}

#[pin_project(project = TimedTaskProj, project_replace = TimedTaskProjReplace)]
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub enum TimedTask<'a, Fut> {
    Incomplete {
        recorder: &'a FlightRecorder,
        activity_id: u32,
        wall_start: Instant,
        cpu_elapsed: Duration,
        metrics: Metrics,
        #[pin]
        fut: Fut,
    },
    Complete,
}

impl<'a, Fut: Future> TimedTask<'a, Fut> {
    pub(super) fn new(
        recorder: &'a FlightRecorder,
        activity_id: u32,
        fut: impl FnOnce(Metrics) -> Fut,
    ) -> Self {
        let metrics = Metrics::default();
        let fut = fut(metrics.clone());
        TimedTask::Incomplete {
            recorder,
            activity_id,
            wall_start: Instant::now(),
            cpu_elapsed: Duration::ZERO,
            metrics,
            fut,
        }
    }
}

impl<'a, Fut: Future> FusedFuture for TimedTask<'a, Fut> {
    fn is_terminated(&self) -> bool {
        matches!(self, Self::Complete)
    }
}

impl<'a, Fut: Future> Future for TimedTask<'a, Fut> {
    type Output = Fut::Output;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match self.as_mut().project() {
            TimedTaskProj::Incomplete {
                cpu_elapsed, fut, ..
            } => {
                let poll_cpu_start = ThreadTime::now();
                let poll_result = fut.poll(cx);
                let poll_cpu_elapsed = poll_cpu_start.elapsed();

                // Update the elapsed CPU time.
                *cpu_elapsed += poll_cpu_elapsed;

                // Then, propagate pending (if needed)
                let value = futures::ready!(poll_result);

                // If we get here, the underyling future is ready.
                // We replace our state with `Complete`.
                match self.project_replace(Self::Complete) {
                    TimedTaskProjReplace::Incomplete {
                        recorder,
                        activity_id,
                        wall_start,
                        cpu_elapsed,
                        metrics,
                        ..
                    } => {
                        let wall_elapsed = wall_start.elapsed();
                        let metrics = metrics.take_metrics();
                        recorder.report_activity(
                            activity_id,
                            wall_start,
                            wall_elapsed,
                            cpu_elapsed,
                            metrics,
                        );
                        Poll::Ready(value)
                    }
                    TimedTaskProjReplace::Complete => unreachable!(),
                }
            }
            TimedTaskProj::Complete => {
                panic!("TimedTask should not be polled after it returned `Poll::Ready`")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    static_assertions::assert_impl_all!(Metrics: Send, Sync);
}
