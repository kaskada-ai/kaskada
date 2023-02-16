use std::task::Poll;
use std::time::Duration;

use cpu_time::ThreadTime;
use futures::future::FusedFuture;
use futures::Future;
use pin_project::pin_project;
use tokio::time::Instant;

use crate::kaskada::sparrow::v1alpha::flight_record::{EventType, Metrics};
use crate::FlightRecorder;

#[derive(Debug)]
pub struct Times {
    pub wall_start: Instant,
    pub wall_elapsed: Duration,
    pub cpu_elapsed: Duration,
}

impl Times {
    /// Creates the `Times` for an instantenous event.
    pub(crate) fn timestamp() -> Self {
        Self {
            wall_start: Instant::now(),
            wall_elapsed: Duration::ZERO,
            cpu_elapsed: Duration::ZERO,
        }
    }

    pub fn relative_wall_timestamp_us(&self, trace_start: Instant) -> u64 {
        duration_to_usec(self.wall_start.duration_since(trace_start))
    }

    pub fn wall_elapsed_us(&self) -> u64 {
        duration_to_usec(self.wall_elapsed)
    }

    pub fn cpu_elapsed_us(&self) -> u64 {
        duration_to_usec(self.cpu_elapsed)
    }
}

fn duration_to_usec(duration: Duration) -> u64 {
    duration.as_micros() as u64
}

/// Wrapper containing a value and timing information.
#[derive(Debug)]
#[must_use = "timed values should be reported"]
pub struct Timed<T> {
    value: T,
    wall_start: Instant,
    cpu_elapsed: Duration,
    metrics: Option<Metrics>,
}

impl Default for Timed<()> {
    fn default() -> Self {
        Self::new(())
    }
}

impl<T> Timed<T> {
    pub fn new(value: T) -> Self {
        Self {
            value,
            wall_start: Instant::now(),
            cpu_elapsed: Duration::ZERO,
            metrics: None,
        }
    }

    pub fn and_then<T2>(self, f: impl FnOnce(T) -> T2) -> Timed<T2> {
        let cpu_start = ThreadTime::now();

        let value = f(self.value);

        let cpu_elapsed = cpu_start.elapsed() + self.cpu_elapsed;

        Timed {
            value,
            wall_start: self.wall_start,
            cpu_elapsed,
            metrics: self.metrics,
        }
    }

    pub fn and_then_async<Fut: Future>(self, f: impl FnOnce(T) -> Fut) -> TimedTask<Fut> {
        let fut = self.and_then(f);

        TimedTask::Incomplete {
            wall_start: fut.wall_start,
            cpu_elapsed: fut.cpu_elapsed,
            metrics: fut.metrics,
            fut: fut.value,
        }
    }

    /// Time a synchronous function.
    pub fn sync(f: impl FnOnce() -> T) -> Timed<T> {
        Timed::default().and_then(|_| f())
    }

    /// Time an asynchronous future.
    ///
    /// Wall time is measured from creation (call to this method) until the
    /// future is polled and reports ready. Elapsed CPU time is the sum of
    /// CPU time spent on each call to poll.
    ///
    /// The `Output = T` bound is not strictly necessary, but it aids the type
    /// inference determine the (eventual) result type of the `Timed<T>`.
    pub fn future<Fut: Future<Output = T>>(fut: Fut) -> TimedTask<Fut> {
        Timed::default().and_then_async(|_| fut)
    }

    pub fn with_metrics(self, metrics: Metrics) -> Self {
        Self {
            metrics: Some(metrics),
            ..self
        }
    }

    pub fn with_computed_metrics(self, f: impl FnOnce(&T) -> Option<Metrics>) -> Self {
        let metrics = f(&self.value);
        Self { metrics, ..self }
    }

    /// Record a duration event with the given recorder.
    ///
    /// Arguments:
    /// - The `recorder` to report to.
    /// - The `event_type` indicates the event being recorded.
    /// - The `index` depends on the `EventType` and is documented there. It
    ///   generally indicates which part of the overall thread the event relates
    ///   to.
    pub fn record(self, recorder: &mut FlightRecorder, event_type: EventType, index: usize) -> T {
        recorder.record_processing(
            event_type,
            index,
            Times {
                wall_start: self.wall_start,
                wall_elapsed: self.wall_start.elapsed(),
                cpu_elapsed: self.cpu_elapsed,
            },
            self.metrics,
        );
        self.value
    }
}

impl<T> Timed<Option<T>> {
    pub fn with_computed_metrics_opt(self, f: impl FnOnce(Option<&T>) -> Option<Metrics>) -> Self {
        let metrics = f(self.value.as_ref());
        Self { metrics, ..self }
    }
}

impl<T, E> Timed<Result<T, E>> {
    pub fn with_computed_metrics_ok(self, f: impl FnOnce(&T) -> Option<Metrics>) -> Self {
        let metrics = self.value.as_ref().ok().and_then(f);
        Self { metrics, ..self }
    }

    /// Transform the result value (if Ok) and extract metrics.
    pub fn transform_metrics_ok<T2>(
        self,
        f: impl FnOnce(T) -> (T2, Option<Metrics>),
    ) -> Timed<Result<T2, E>> {
        let (value, metrics) = match self.value {
            Ok(value) => {
                let (value, metrics) = f(value);
                (Ok(value), metrics)
            }
            Err(e) => (Err(e), self.metrics),
        };

        Timed {
            value,
            wall_start: self.wall_start,
            cpu_elapsed: self.cpu_elapsed,
            metrics,
        }
    }
}

#[pin_project(project = TimedProj, project_replace = TimedProjReplace)]
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub enum TimedTask<Fut> {
    Incomplete {
        wall_start: Instant,
        cpu_elapsed: Duration,
        metrics: Option<Metrics>,
        #[pin]
        fut: Fut,
    },
    Complete,
}

impl<Fut: Future> FusedFuture for TimedTask<Fut> {
    fn is_terminated(&self) -> bool {
        matches!(self, Self::Complete)
    }
}

impl<Fut: Future> Future for TimedTask<Fut> {
    type Output = Timed<Fut::Output>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match self.as_mut().project() {
            TimedProj::Incomplete {
                cpu_elapsed, fut, ..
            } => {
                let poll_cpu_start = ThreadTime::now();
                let poll_result = fut.poll(cx);
                let poll_cpu_elapsed = poll_cpu_start.elapsed();

                // Update the elapsed CPU time.
                *cpu_elapsed += poll_cpu_elapsed;

                // Then, propagate pending (if needed)
                let value = futures::ready!(poll_result);

                match self.project_replace(Self::Complete) {
                    TimedProjReplace::Incomplete {
                        wall_start,
                        cpu_elapsed,
                        metrics,
                        ..
                    } => Poll::Ready(Timed {
                        value,
                        wall_start,
                        cpu_elapsed,
                        metrics,
                    }),
                    TimedProjReplace::Complete => unreachable!(),
                }
            }
            TimedProj::Complete => {
                panic!("TimedTask should not be polled after it returned `Poll::Ready`")
            }
        }
    }
}
