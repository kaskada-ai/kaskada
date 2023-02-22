use std::borrow::Cow;
use std::time::Instant;

use crate::kaskada::sparrow::v1alpha::{FlightRecord, ThreadId};
use crate::{FlightRecorder, State};

#[derive(Clone)]
pub struct FlightRecorderFactory {
    /// The time at which the trace started.
    ///
    /// All instants reported relative this.
    trace_start: Instant,
    state: State,
    /// Number of threads already created.
    threads: u32,
}

impl FlightRecorderFactory {
    pub fn new(sender: tokio::sync::mpsc::Sender<Vec<FlightRecord>>) -> Self {
        Self {
            trace_start: Instant::now(),
            state: State::Active(sender),
            threads: 0,
        }
    }

    pub fn new_disabled() -> Self {
        Self {
            trace_start: Instant::now(),
            state: State::Disabled,
            threads: 0,
        }
    }

    pub fn create_for_thread(&self, thread_id: ThreadId) -> FlightRecorder {
        let mut recorder = FlightRecorder::new(self.tracte_start, self.state.clone(), thread_id)

        // Record the start event.
        recorder.record_event(EventType::Start, 0, None);

        recorder
    }
}
