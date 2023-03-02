use std::time::Duration;

use tokio::time::Instant;

use crate::kaskada::sparrow::v1alpha::flight_record::{Record, ReportActivity};
use crate::kaskada::sparrow::v1alpha::{FlightRecord, MetricValue};

/// The flight recorder for a specific thread.
#[derive(Clone, Debug)]
pub enum FlightRecorder {
    Active {
        thread_id: u32,
        trace_start: Instant,
        tx: tokio::sync::mpsc::Sender<FlightRecord>,
    },
    Disabled,
}

impl FlightRecorder {
    /// Create a new disabled flight recorder.
    pub fn disabled() -> Self {
        FlightRecorder::Disabled
    }

    pub(super) fn report_activity(
        &self,
        activity_id: u32,
        wall_start: Instant,
        wall_elapsed: Duration,
        cpu_elapsed: Duration,
        metrics: Vec<MetricValue>,
    ) {
        match self {
            FlightRecorder::Active {
                thread_id,
                trace_start,
                tx,
            } => {
                let wall_timestamp = wall_start.duration_since(*trace_start);
                let record = ReportActivity {
                    activity_id,
                    thread_id: *thread_id,
                    wall_timestamp_us: duration_to_usec(wall_timestamp),
                    wall_duration_us: duration_to_usec(wall_elapsed),
                    cpu_duration_us: duration_to_usec(cpu_elapsed),
                    metrics,
                };
                let record = FlightRecord {
                    record: Some(Record::ReportActivity(record)),
                };

                match tx.try_send(record) {
                    Ok(()) => {
                        // Sending the flight record succeeded.
                    }
                    Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                        tracing::warn!("Failed to send flight record -- channel closed. Flight records will be truncated.");
                    }
                    Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                        tracing::warn!("Failed to send flight record -- channel full. Flight records will be missing.");
                    }
                }
            }
            FlightRecorder::Disabled => {
                // nothing to do -- flight recorder is disabled
            }
        }
    }
}

fn duration_to_usec(duration: Duration) -> u64 {
    duration.as_micros() as u64
}

// #[cfg(test)]
// mod tests {
//     use std::time::Duration;

//     use futures::StreamExt;
//     use tokio::time::Instant;

//     use crate::kaskada::sparrow::v1alpha::flight_record::EventType;
//     use crate::kaskada::sparrow::v1alpha::thread_id::ThreadKind;
//     use crate::kaskada::sparrow::v1alpha::{FlightRecord, ThreadId};
//     use crate::{FlightRecorderFactory, Times};

//     #[tokio::test(start_paused = true)]
//     async fn test_flight_recorder() {
//         let (sender, receiver) = tokio::sync::mpsc::channel(10);
//         let factory = FlightRecorderFactory::new(sender);
//         let thread1_id = ThreadId {
//             pass_id: 0,
//             kind: ThreadKind::SourceReader as i32,
//             thread_id: 1,
//         };
//         let mut thread1 = factory.create_for_thread(thread1_id.clone());

//         tokio::time::advance(Duration::from_secs(1)).await;
//         let thread2_id = ThreadId {
//             pass_id: 0,
//             kind: ThreadKind::Execute as i32,
//             thread_id: 0,
//         };
//         let mut thread2 = factory.create_for_thread(thread2_id.clone());

//         tokio::time::advance(Duration::from_secs(5)).await;
//         thread1.record_processing(
//             EventType::ExecutePassInstruction,
//             1,
//             Times {
//                 wall_start: Instant::now(),
//                 wall_elapsed: Duration::from_secs(8),
//                 cpu_elapsed: Duration::from_secs(3),
//             },
//             None,
//         );

//         tokio::time::advance(Duration::from_secs(3)).await;
//         thread2.record_processing(
//             EventType::ExecutePassSink,
//             2,
//             Times {
//                 wall_start: Instant::now(),
//                 wall_elapsed: Duration::from_secs(2),
//                 cpu_elapsed: Duration::from_secs(1),
//             },
//             None,
//         );

//         std::mem::drop(thread1);
//         std::mem::drop(thread2);
//         std::mem::drop(factory);

//         let receiver = tokio_stream::wrappers::ReceiverStream::new(receiver);
//         let records: Vec<_> = receiver.concat().await;

//         assert_eq!(
//             records,
//             vec![
//                 // Thread1 recorder is dropped first, so it flushes first.
//                 FlightRecord {
//                     thread_id: Some(thread1_id.clone()),
//                     event_type: EventType::Start as i32,
//                     index: 0,
//                     wall_timestamp_us: 0,
//                     wall_duration_us: 0,
//                     cpu_duration_us: 0,
//                     metrics: None
//                 },
//                 FlightRecord {
//                     thread_id: Some(thread1_id.clone()),
//                     event_type: EventType::ExecutePassInstruction as i32,
//                     index: 1,
//                     wall_timestamp_us: 6_000_000,
//                     wall_duration_us: 8_000_000,
//                     cpu_duration_us: 3_000_000,
//                     metrics: None
//                 },
//                 FlightRecord {
//                     thread_id: Some(thread1_id),
//                     event_type: EventType::Finish as i32,
//                     index: 0,
//                     wall_timestamp_us: 9_000_000,
//                     wall_duration_us: 0,
//                     cpu_duration_us: 0,
//                     metrics: None
//                 },
//                 // Thread2 recorder is dropped second, so it flushes second.
//                 FlightRecord {
//                     thread_id: Some(thread2_id.clone()),
//                     event_type: EventType::Start as i32,
//                     index: 0,
//                     wall_timestamp_us: 1_000_000,
//                     wall_duration_us: 0,
//                     cpu_duration_us: 0,
//                     metrics: None
//                 },
//                 FlightRecord {
//                     thread_id: Some(thread2_id.clone()),
//                     event_type: EventType::ExecutePassSink as i32,
//                     index: 2,
//                     wall_timestamp_us: 9_000_000,
//                     wall_duration_us: 2_000_000,
//                     cpu_duration_us: 1_000_000,
//                     metrics: None
//                 },
//                 FlightRecord {
//                     thread_id: Some(thread2_id),
//                     event_type: EventType::Finish as i32,
//                     index: 0,
//                     wall_timestamp_us: 9_000_000,
//                     wall_duration_us: 0,
//                     cpu_duration_us: 0,
//                     metrics: None
//                 }
//             ]
//         );
//     }
// }
