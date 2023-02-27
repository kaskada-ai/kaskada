use tokio::time::Instant;

use crate::kaskada::sparrow::v1alpha::flight_record::RegisterThread;
use crate::kaskada::sparrow::v1alpha::FlightRecord;
use crate::registration::Registration;
use crate::FlightRecorder;

#[derive(Clone)]
pub enum FlightRecorderFactory {
    Active {
        next_thread_id: u32,
        trace_start: Instant,
        tx: tokio::sync::mpsc::Sender<FlightRecord>,
    },
    Disabled,
}

impl FlightRecorderFactory {
    /// Create a new `FlightRecorderFactory` for the given sender.
    ///
    /// Registers all of the metrics and activities that have been added to the `inventory`.
    pub async fn new(tx: tokio::sync::mpsc::Sender<FlightRecord>) -> Self {
        Self::new_with_registrations(tx, inventory::iter::<&'static Registration>).await
    }

    pub(crate) async fn new_with_registrations(
        tx: tokio::sync::mpsc::Sender<FlightRecord>,
        registrations: impl IntoIterator<Item = &&'static Registration>,
    ) -> Self {
        let registrations: Vec<_> = registrations
            .into_iter()
            .flat_map(|r| r.records())
            .cloned()
            .collect();

        for record in registrations {
            match tx.send(record).await {
                Ok(()) => (),
                Err(e) => {
                    tracing::warn!(
                        "Failed to register thread with flight recorder: {e}; Disabling."
                    );
                    return Self::Disabled;
                }
            }
        }

        Self::Active {
            next_thread_id: 0,
            trace_start: Instant::now(),
            tx,
        }
    }

    /// Create a flight recorder factory that creates disabled flight recorders.
    pub fn new_disabled() -> Self {
        Self::Disabled
    }

    pub async fn create_recorder(&mut self, label: String) -> FlightRecorder {
        match self {
            FlightRecorderFactory::Active {
                next_thread_id,
                trace_start,
                tx,
            } => {
                let thread_id = *next_thread_id;
                *next_thread_id += 1;

                let tx = tx.clone();

                let record = FlightRecord {
                    record: Some(
                        crate::kaskada::sparrow::v1alpha::flight_record::Record::RegisterThread(
                            RegisterThread { thread_id, label },
                        ),
                    ),
                };

                match tx.send(record).await {
                    Ok(()) => FlightRecorder::Active {
                        thread_id,
                        trace_start: *trace_start,
                        tx,
                    },
                    Err(e) => {
                        tracing::warn!("Failed to register thread with flight recorder: {e}");
                        FlightRecorder::Disabled
                    }
                }
            }
            FlightRecorderFactory::Disabled => FlightRecorder::Disabled,
        }
    }
}
