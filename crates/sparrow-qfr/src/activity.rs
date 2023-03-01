use futures::Future;

use crate::activation::Activation;
use crate::{FlightRecorder, Metrics, TimedTask};

/// An activity represents something that one or more threads do.
///
/// When reported, it will include information about how long the
/// activity was performed as well as any metrics reported
/// specifically against the activity.
pub struct Activity {
    pub(super) label: &'static str,
    pub activity_id: u32,
    pub(super) parent_activity_id: Option<u32>,
}

impl Activity {
    pub const fn new(
        label: &'static str,
        activity_id: u32,
        parent_activity_id: Option<u32>,
    ) -> Self {
        Self {
            label,
            activity_id,
            parent_activity_id,
        }
    }

    pub fn start<'a>(&'static self, recorder: &'a FlightRecorder) -> Activation<'a> {
        Activation::new(self, recorder)
    }

    pub fn instrument<'a, T, Fut: Future<Output = T>>(
        &'static self,
        recorder: &'a FlightRecorder,
        fut: impl FnOnce(Metrics) -> Fut,
    ) -> TimedTask<'a, Fut> {
        TimedTask::new(recorder, self.activity_id, fut)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::Instant;

    use crate::kaskada::sparrow::v1alpha::flight_record::{Record, ReportActivity};
    use crate::kaskada::sparrow::v1alpha::FlightRecord;
    use crate::{gauge, Activity, FlightRecorder, Gauge};

    const ACTIVITY: Activity = Activity::new("activity", 0, None);
    const GAUGE: Gauge<u64> = gauge!("value");

    #[tokio::test(start_paused = true)]
    async fn test_sync_activity_drop() {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let recorder = FlightRecorder::Active {
            thread_id: 57,
            trace_start: Instant::now(),
            tx,
        };

        tokio::time::advance(Duration::from_micros(500_000)).await;
        {
            let _activation = ACTIVITY.start(&recorder);
            tokio::time::advance(Duration::from_micros(1_000_000)).await;
        }
        tokio::time::advance(Duration::from_micros(2_000_000)).await;

        let activity = get_activity_record(rx).await;
        assert_eq!(activity.activity_id, ACTIVITY.activity_id);
        assert_eq!(activity.thread_id, 57);

        assert_eq!(activity.wall_duration_us, 1_000_000);
        assert_eq!(activity.wall_timestamp_us, 500_000);

        assert_eq!(activity.metrics, vec![]);
    }

    #[tokio::test(start_paused = true)]
    async fn test_sync_activity_finish() {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let recorder = FlightRecorder::Active {
            thread_id: 57,
            trace_start: Instant::now(),
            tx,
        };

        tokio::time::advance(Duration::from_micros(500_000)).await;
        let activation = ACTIVITY.start(&recorder);
        tokio::time::advance(Duration::from_micros(1_000_000)).await;
        activation.finish();
        tokio::time::advance(Duration::from_micros(2_000_000)).await;

        let activity = get_activity_record(rx).await;
        assert_eq!(activity.activity_id, ACTIVITY.activity_id);
        assert_eq!(activity.thread_id, 57);

        assert_eq!(activity.wall_duration_us, 1_000_000);
        assert_eq!(activity.wall_timestamp_us, 500_000);

        assert_eq!(activity.metrics, vec![]);
    }

    #[tokio::test(start_paused = true)]
    async fn test_sync_activity_metrics() {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let recorder = FlightRecorder::Active {
            thread_id: 57,
            trace_start: Instant::now(),
            tx,
        };

        tokio::time::advance(Duration::from_micros(500_000)).await;
        let mut activation = ACTIVITY.start(&recorder);
        tokio::time::advance(Duration::from_micros(1_000_000)).await;
        let metric = GAUGE.value(57);
        activation.report_metric(GAUGE, 57);
        activation.finish();

        let activity = get_activity_record(rx).await;
        assert_eq!(activity.activity_id, ACTIVITY.activity_id);
        assert_eq!(activity.thread_id, 57);

        assert_eq!(activity.wall_duration_us, 1_000_000);
        assert_eq!(activity.wall_timestamp_us, 500_000);

        assert_eq!(activity.metrics, vec![metric]);
    }

    #[tokio::test(start_paused = true)]
    async fn test_async_activity_block() {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let recorder = FlightRecorder::Active {
            thread_id: 57,
            trace_start: Instant::now(),
            tx,
        };

        tokio::time::advance(Duration::from_micros(500_000)).await;
        let result = ACTIVITY
            .instrument(&recorder, |_| async {
                tokio::time::advance(Duration::from_micros(1_000_000)).await;
                59
            })
            .await;
        assert_eq!(result, 59);
        tokio::time::advance(Duration::from_micros(1_000_000)).await;

        let activity = get_activity_record(rx).await;
        assert_eq!(activity.activity_id, ACTIVITY.activity_id);
        assert_eq!(activity.thread_id, 57);

        assert_eq!(activity.wall_duration_us, 1_000_000);
        assert_eq!(activity.wall_timestamp_us, 500_000);

        assert_eq!(activity.metrics, vec![]);
    }

    #[tokio::test(start_paused = true)]
    async fn test_async_activity_metrics() {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let recorder = FlightRecorder::Active {
            thread_id: 57,
            trace_start: Instant::now(),
            tx,
        };

        tokio::time::advance(Duration::from_micros(500_000)).await;
        let result = ACTIVITY
            .instrument(&recorder, |metrics| async move {
                tokio::time::advance(Duration::from_micros(1_000_000)).await;
                metrics.report_metric(GAUGE, 523);
                59
            })
            .await;
        assert_eq!(result, 59);
        tokio::time::advance(Duration::from_micros(1_000_000)).await;

        let activity = get_activity_record(rx).await;
        assert_eq!(activity.activity_id, ACTIVITY.activity_id);
        assert_eq!(activity.thread_id, 57);

        assert_eq!(activity.wall_duration_us, 1_000_000);
        assert_eq!(activity.wall_timestamp_us, 500_000);

        assert_eq!(activity.metrics, vec![GAUGE.value(523)]);
    }

    async fn get_activity_record(
        mut rx: tokio::sync::mpsc::Receiver<FlightRecord>,
    ) -> ReportActivity {
        rx.close();

        let record = rx.recv().await.expect("exactly one record");
        assert_eq!(rx.recv().await, None, "exactly one record");

        match record.record {
            Some(Record::ReportActivity(activity)) => activity,
            unexpected => panic!("Expected ReportActivity, but was {unexpected:?}"),
        }
    }
}
