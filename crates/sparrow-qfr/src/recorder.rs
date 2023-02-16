use tokio::sync::mpsc::error::TrySendError;
use tokio::time::Instant;
use tracing::{error, info, warn};

use crate::kaskada::sparrow::v1alpha::flight_record::{EventType, Metrics};
use crate::kaskada::sparrow::v1alpha::{FlightRecord, ThreadId};
use crate::Times;

#[derive(Clone)]
pub struct FlightRecorderFactory {
    /// The time at which the trace started.
    ///
    /// All instants reported relative this.
    trace_start: Instant,
    state: State,
}

impl FlightRecorderFactory {
    pub fn new(sender: tokio::sync::mpsc::Sender<Vec<FlightRecord>>) -> Self {
        Self {
            trace_start: Instant::now(),
            state: State::Active(sender),
        }
    }

    pub fn new_disabled() -> Self {
        Self {
            trace_start: Instant::now(),
            state: State::Disabled,
        }
    }

    pub fn create_for_thread(&self, thread_id: ThreadId) -> FlightRecorder {
        let mut recorder = FlightRecorder {
            trace_start: self.trace_start,
            buffer: Vec::with_capacity(BUFFER_SIZE),
            state: self.state.clone(),
            thread_id,
        };

        // Record the start event.
        recorder.record_event(EventType::Start, 0, None);

        recorder
    }
}

/// Sends batches of flight records to the FlightRecordWriter.
///
/// Buffering some events locally before sending a batch minimizes
/// the channel interactions.
pub struct FlightRecorder {
    /// The time at which the trace started.
    ///
    /// All instants reported relative this.
    trace_start: Instant,
    buffer: Vec<FlightRecord>,
    state: State,
    thread_id: ThreadId,
}

#[derive(Clone)]
enum State {
    Disabled,
    Active(tokio::sync::mpsc::Sender<Vec<FlightRecord>>),
    Finished,
}

/// The size of the (local thread) buffer for flight records.
///
/// Once this buffer is full, the events are sent on the channel to be written
/// and a new buffer of this size is allocated.
const BUFFER_SIZE: usize = 128;

impl FlightRecorder {
    /// Create a disabled flight recorder.
    pub fn disabled_for_test() -> Self {
        Self {
            trace_start: Instant::now(),
            buffer: Vec::new(),
            state: State::Disabled,
            thread_id: ThreadId::default(),
        }
    }

    /// Record an instantaneous event associated with this thread.
    ///
    /// Arguments:
    /// - The `event_type` indicates the event being recorded.
    /// - The `index` depends on the `EventType` and is documented there. It
    ///   generally indicates which part of the overall thread the event relates
    ///   to.
    /// - The `metrics` are optional metrics to include in the event.
    pub fn record_event(&mut self, event_type: EventType, index: usize, metrics: Option<Metrics>) {
        self.record_processing(event_type, index, Times::timestamp(), metrics);
    }

    pub(crate) fn record_processing(
        &mut self,
        event_type: EventType,
        index: usize,
        times: Times,
        metrics: Option<Metrics>,
    ) {
        match self.state {
            State::Disabled => (),
            State::Active(_) => {
                self.buffer.push(FlightRecord {
                    thread_id: Some(self.thread_id.clone()),
                    event_type: event_type as i32,
                    index: index as u32,
                    wall_timestamp_us: times.relative_wall_timestamp_us(self.trace_start),
                    wall_duration_us: times.wall_elapsed_us(),
                    cpu_duration_us: times.cpu_elapsed_us(),
                    metrics,
                });

                if self.buffer.len() >= BUFFER_SIZE {
                    self.flush()
                }
            }
            State::Finished => {
                error!("Recording flight record after finished: {:?}", event_type)
            }
        }
    }

    fn flush(&mut self) {
        let to_send = std::mem::replace(&mut self.buffer, Vec::with_capacity(BUFFER_SIZE));

        let sender = match &mut self.state {
            State::Disabled => return,
            State::Active(sender) => sender,
            State::Finished => {
                error!("Flushing finished flight recorder");
                return;
            }
        };

        // We intentionally don't panic on invalid flight records.
        match sender.try_send(to_send) {
            Ok(()) => {
                // Successfully sent the flight record.
            }
            Err(TrySendError::Closed(_)) => {
                warn!("Flight Record Receiver disconnected. Flight records will be truncated.");

                // Mark the sender as disabled so we stop buffering/sending.
                self.state = State::Disabled;
            }
            Err(TrySendError::Full(buffer)) => {
                // We generally try to avoid this happening. Since we're buffering to a vector,
                // we "handle" this by allowing the local vector to grow, hoping that there will
                // be space made in the receiver. However, the extra replacing that occurs may
                // lead to slowness. If this is common, we should either increase local
                // buffering or improve the rate at which the flight records are
                // written.
                warn!("Flight Record Receiver is full. Flight records will be delayed.");
                self.buffer = buffer;
            }
        }
    }
}

impl Drop for FlightRecorder {
    fn drop(&mut self) {
        if matches!(self.state, State::Active(_)) {
            info!("Dropping Active Flight Recorder. Marking finished.");
            self.record_event(EventType::Finish, 0, None);
            self.flush();
            self.state = State::Finished;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::StreamExt;
    use tokio::time::Instant;

    use crate::kaskada::sparrow::v1alpha::flight_record::EventType;
    use crate::kaskada::sparrow::v1alpha::thread_id::ThreadKind;
    use crate::kaskada::sparrow::v1alpha::{FlightRecord, ThreadId};
    use crate::{FlightRecorderFactory, Times};

    #[tokio::test(start_paused = true)]
    async fn test_flight_recorder() {
        let (sender, receiver) = tokio::sync::mpsc::channel(10);
        let factory = FlightRecorderFactory::new(sender);
        let thread1_id = ThreadId {
            pass_id: 0,
            kind: ThreadKind::SourceReader as i32,
            thread_id: 1,
        };
        let mut thread1 = factory.create_for_thread(thread1_id.clone());

        tokio::time::advance(Duration::from_secs(1)).await;
        let thread2_id = ThreadId {
            pass_id: 0,
            kind: ThreadKind::Execute as i32,
            thread_id: 0,
        };
        let mut thread2 = factory.create_for_thread(thread2_id.clone());

        tokio::time::advance(Duration::from_secs(5)).await;
        thread1.record_processing(
            EventType::ExecutePassInstruction,
            1,
            Times {
                wall_start: Instant::now(),
                wall_elapsed: Duration::from_secs(8),
                cpu_elapsed: Duration::from_secs(3),
            },
            None,
        );

        tokio::time::advance(Duration::from_secs(3)).await;
        thread2.record_processing(
            EventType::ExecutePassSink,
            2,
            Times {
                wall_start: Instant::now(),
                wall_elapsed: Duration::from_secs(2),
                cpu_elapsed: Duration::from_secs(1),
            },
            None,
        );

        std::mem::drop(thread1);
        std::mem::drop(thread2);
        std::mem::drop(factory);

        let receiver = tokio_stream::wrappers::ReceiverStream::new(receiver);
        let records: Vec<_> = receiver.concat().await;

        assert_eq!(
            records,
            vec![
                // Thread1 recorder is dropped first, so it flushes first.
                FlightRecord {
                    thread_id: Some(thread1_id.clone()),
                    event_type: EventType::Start as i32,
                    index: 0,
                    wall_timestamp_us: 0,
                    wall_duration_us: 0,
                    cpu_duration_us: 0,
                    metrics: None
                },
                FlightRecord {
                    thread_id: Some(thread1_id.clone()),
                    event_type: EventType::ExecutePassInstruction as i32,
                    index: 1,
                    wall_timestamp_us: 6_000_000,
                    wall_duration_us: 8_000_000,
                    cpu_duration_us: 3_000_000,
                    metrics: None
                },
                FlightRecord {
                    thread_id: Some(thread1_id),
                    event_type: EventType::Finish as i32,
                    index: 0,
                    wall_timestamp_us: 9_000_000,
                    wall_duration_us: 0,
                    cpu_duration_us: 0,
                    metrics: None
                },
                // Thread2 recorder is dropped second, so it flushes second.
                FlightRecord {
                    thread_id: Some(thread2_id.clone()),
                    event_type: EventType::Start as i32,
                    index: 0,
                    wall_timestamp_us: 1_000_000,
                    wall_duration_us: 0,
                    cpu_duration_us: 0,
                    metrics: None
                },
                FlightRecord {
                    thread_id: Some(thread2_id.clone()),
                    event_type: EventType::ExecutePassSink as i32,
                    index: 2,
                    wall_timestamp_us: 9_000_000,
                    wall_duration_us: 2_000_000,
                    cpu_duration_us: 1_000_000,
                    metrics: None
                },
                FlightRecord {
                    thread_id: Some(thread2_id),
                    event_type: EventType::Finish as i32,
                    index: 0,
                    wall_timestamp_us: 9_000_000,
                    wall_duration_us: 0,
                    cpu_duration_us: 0,
                    metrics: None
                }
            ]
        );
    }
}
