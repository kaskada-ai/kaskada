use std::time::Duration;

use futures::Stream;
use sparrow_api::kaskada::v1alpha::execute_response::ComputeSnapshot;
use sparrow_api::kaskada::v1alpha::execute_response::ProgressInformation;
use sparrow_api::kaskada::v1alpha::object_store_destination::ResultPaths;
use sparrow_api::kaskada::v1alpha::output_to;
use sparrow_api::kaskada::v1alpha::ObjectStoreDestination;
use sparrow_api::kaskada::v1alpha::OutputTo;
use sparrow_api::kaskada::v1alpha::{ExecuteResponse, LongQueryState};
use tokio_stream::StreamExt;

#[cfg(feature = "pulsar")]
use super::output::pulsar::format_topic_url;
#[cfg(feature = "pulsar")]
use error_stack::ResultExt;
#[cfg(feature = "pulsar")]
use sparrow_api::kaskada::v1alpha::PulsarDestination;

use super::Error;

/// Report progress every 10 seconds.
const PROGRESS_PERIOD: Duration = Duration::from_secs(10);

struct ProgressTracker {
    /// Number of batches that have been output since the last progress report.
    ///
    /// When this is `0` we haven't produced any output since the last progress
    /// report.
    output_batches_since_progress: usize,
    /// Whether we have logged a message indicating lack of progress.
    logged_no_batches: bool,
    /// The progress inforamation to include in the streaming RPC response.
    progress: ProgressInformation,
    /// The paths to the output files produced so far.
    ///
    /// If the output is not configured to write to files, this will be empty.
    output_paths: Vec<String>,
    /// Information on where the outputs are materialized to.
    destination: Option<output_to::Destination>,
}

#[derive(Debug)]
pub(crate) enum ProgressUpdate {
    /// Informs the progress tracker of the output destination.
    Destination { destination: output_to::Destination },
    /// Progress update reported for each table indicating total size.
    InputMetadata { total_num_rows: usize },
    /// Progress update indicating the given number of rows have been read.
    Input { num_rows: usize },
    /// Progress update indicating the given number of rows have been output.
    Output { num_rows: usize },
    /// Progress update reporting the output files produced.
    FilesProduced { paths: Vec<String> },
    /// Sent to indicate all operations have completed.
    ///
    /// For now, contains the compute snapshots, as we only snapshot
    /// once on completion of a query.
    ExecutionComplete {
        compute_snapshots: Vec<ComputeSnapshot>,
    },
    /// Message sent to indicate the execution failed.
    /// Contains details of failure.
    ExecutionFailed { error: error_stack::Report<Error> },
}

impl ProgressTracker {
    fn new() -> Self {
        Self {
            // We start this as `1` so the first progress period doesn't
            // log the "no progress" message. This gives time for things to "warm up".
            output_batches_since_progress: 1,
            logged_no_batches: false,
            progress: ProgressInformation {
                total_input_rows: 0,
                processed_input_rows: 0,
                buffered_rows: 0,
                processed_buffered_rows: 0,
                min_event_time: 0,
                max_event_time: 0,
                output_time: 0,
                produced_output_rows: 0,
            },
            output_paths: vec![],
            destination: None,
        }
    }

    fn process_update(&mut self, stats: ProgressUpdate) {
        match stats {
            ProgressUpdate::Destination { destination } => {
                self.destination = Some(destination);
            }
            ProgressUpdate::InputMetadata { total_num_rows } => {
                self.progress.total_input_rows += total_num_rows as i64;
            }
            ProgressUpdate::Input { num_rows } => {
                self.progress.processed_input_rows += num_rows as i64;
            }
            ProgressUpdate::Output { num_rows } => {
                self.output_batches_since_progress += 1;
                self.progress.produced_output_rows += num_rows as i64;
            }
            ProgressUpdate::FilesProduced { mut paths } => {
                self.output_paths.append(&mut paths);
            }
            ProgressUpdate::ExecutionComplete { .. } | ProgressUpdate::ExecutionFailed { .. } => {
                panic!("Shouldn't update process on final message")
            }
        }
    }

    fn progress_message(&mut self) -> error_stack::Result<ExecuteResponse, Error> {
        // This currently reports after the first period with no batches.
        // If this is spammy, we could require there be N periods with no batches.
        if self.output_batches_since_progress == 0 && !self.logged_no_batches {
            tracing::info!(
                "No batches produced for {:?}. This may indicate hung processing or may be \
                 expected (shift_to, shift_until, final results).",
                PROGRESS_PERIOD
            );
            self.logged_no_batches = true;
        }

        self.output_batches_since_progress = 0;

        let destination = self.destination_to_output()?;
        Ok(ExecuteResponse {
            state: LongQueryState::Running as i32,
            is_query_done: false,
            progress: Some(self.progress.clone()),
            flight_record_path: None,
            plan_yaml_path: None,
            compute_snapshots: Vec::new(),
            output_to: Some(destination),
        })
    }

    fn destination_to_output(&mut self) -> error_stack::Result<OutputTo, Error> {
        // Clone the output paths in for object store destinations
        let destination = self
            .destination
            .as_ref()
            .ok_or(Error::Internal("expected destination"))?;
        match destination {
            output_to::Destination::ObjectStore(store) => Ok(OutputTo {
                destination: Some(output_to::Destination::ObjectStore(
                    ObjectStoreDestination {
                        file_type: store.file_type,
                        output_prefix_uri: store.output_prefix_uri.clone(),
                        output_paths: Some(ResultPaths {
                            paths: self.output_paths.clone(),
                        }),
                    },
                )),
            }),
            #[cfg(not(feature = "pulsar"))]
            output_to::Destination::Pulsar(pulsar) => {
                error_stack::bail!(Error::FeatureNotEnabled { feature: "pulsar" })
            }
            #[cfg(feature = "pulsar")]
            output_to::Destination::Pulsar(pulsar) => Ok(OutputTo {
                destination: Some(output_to::Destination::Pulsar(PulsarDestination {
                    broker_service_url: pulsar.broker_service_url.clone(),
                    tenant: pulsar.tenant.clone(),
                    namespace: pulsar.namespace.clone(),
                    topic_name: pulsar.topic_name.clone(),
                    topic_url: format_topic_url(pulsar)
                        .change_context(Error::MissingField("missing topic name"))?,
                    auth_plugin: pulsar.auth_plugin.clone(),
                    auth_params: pulsar.auth_params.clone(),
                })),
            }),
            output_to::Destination::Redis(_) => {
                error_stack::bail!(Error::UnsupportedOutput { output: "redis" })
            }
        }
    }
}

pub(super) fn progress_stream(
    mut progress_updates_rx: futures::stream::BoxStream<'static, ProgressUpdate>,
) -> impl Stream<Item = error_stack::Result<ExecuteResponse, Error>> {
    // Create a stream of ticks starting at `now + PROGRESS_PERIOD`, ticking every
    // `PROGRESS_PERIOD`.
    let mut ticks = tokio::time::interval_at(
        tokio::time::Instant::now() + PROGRESS_PERIOD,
        PROGRESS_PERIOD,
    );
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    // Ideally, we'd use `try_stream!` instead of `stream` so we could use the `?`
    // to produce errors. However, we can't do that due to https://github.com/tokio-rs/async-stream/issues/63.
    async_stream::stream! {
        let mut tracker = ProgressTracker::new();

        loop {
            tokio::select! {
                // Poll futures in the order listed.
                biased;

                _ = ticks.tick() => {
                    yield tracker.progress_message();
                },
                progress_update = progress_updates_rx.next() => {
                    if let Some(update) = progress_update {
                        match update {
                            ProgressUpdate::ExecutionComplete { compute_snapshots } => {
                                // Loop to ensure all progress updates are received before completion
                                loop {
                                    tokio::select! {
                                        biased;
                                        _ = ticks.tick() => {
                                            yield tracker.progress_message();
                                        }
                                        stats = progress_updates_rx.next() => {
                                            match stats {
                                                None => {
                                                    // All stats messages have been incorporated.
                                                    break;
                                                }
                                                Some(stats) => {
                                                    tracker.process_update(stats);
                                                }
                                            }
                                        }
                                    }
                                }

                                let output = match tracker.destination_to_output() {
                                    Ok(output) => output,
                                    Err(e) => {
                                        yield Err(e);
                                        continue;
                                    }
                                };

                                let final_result = Ok(ExecuteResponse {
                                    state: LongQueryState::Running as i32,
                                    is_query_done: true,
                                    progress: Some(tracker.progress),
                                    flight_record_path: None,
                                    plan_yaml_path: None,
                                    compute_snapshots,
                                    output_to: Some(output),
                                });
                                yield final_result;
                                break
                            },
                            ProgressUpdate::ExecutionFailed { error } => {
                                yield Err(error);
                                break
                            },
                            _ => tracker.process_update(update),
                        }
                    }
                },
            }
        }
    }
}
