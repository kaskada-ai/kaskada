use fallible_iterator::FallibleIterator;
use hashbrown::HashMap;
use smallvec::smallvec;
use sparrow_plan::{ComputePlan, CrossPass, SinkKind, SourceKind};
use sparrow_qfr::kaskada::sparrow::v1alpha::flight_record::execute_pass_metrics::Cause;
use sparrow_qfr::kaskada::sparrow::v1alpha::flight_record::{EventType, Metrics};
use sparrow_qfr::kaskada::sparrow::v1alpha::thread_id::ThreadKind;
use sparrow_qfr::kaskada::sparrow::v1alpha::{FlightRecord, FlightRecordHeader, ThreadId};
use sparrow_qfr::{FlightRecordReader, QFR_VERSION};

use super::chrome_tracing::EventScope;
use crate::chrome::chrome_tracing::{
    Args, DisplayTimeUnit, Event, StackFrame, StackFrameId, Trace,
};

// Struct managing state for the performing the conversion.
struct Conversion {
    /// Stack frames. Map from specific thread/index/event_types to
    /// the ID of the corresponding stack frame.
    stack_frames: HashMap<StackFrameKey, StackFrameId>,

    /// Names created for counters.
    counter_names: HashMap<CounterKey, &'static str>,

    /// The trace being produced.
    trace: Trace,
}

// TODO: Rather than looking up the static string, could we pass the
// counter key as the name of the counter? Possibly using the Display impl?
#[derive(Hash, PartialEq, Eq, Debug, Clone)]
enum CounterKey {
    /// Name of the counter for the number of active sources in each gatherer.
    ///
    /// Since counters are scoped to the process (pass) we need to use different
    /// names for each table gatherer in a pass, as well as the merge gatherer.
    GatherSources(StackFrameId),
    /// Name of the counter for rows buffered at the given sink.
    SinkBufferedRows(StackFrameId),
    /// Name of the counter for the sink channel (capacity and length).
    SinkChannel(StackFrameId),
}

#[derive(Hash, PartialEq, Eq, Debug, Clone)]
struct StackFrameKey {
    thread_id: ThreadId,
    event_type: EventType,
    index: u32,
}

#[allow(clippy::print_stdout)]
pub(super) fn qfr_to_chrome(
    plan: &ComputePlan,
    reader: FlightRecordReader,
) -> anyhow::Result<Trace> {
    if reader.header().version != QFR_VERSION {
        println!(
            "Flight Record Version ({}) may be incompatible with this tool version ({})",
            reader.header().version,
            QFR_VERSION
        );
    }
    let mut builder = Conversion::new();

    builder.register_header(reader.header());
    builder.register_plan(plan);

    let mut records = reader.events()?;
    while let Some(record) = records.next()? {
        builder.add_record(record)
    }

    Ok(builder.trace)
}

impl Conversion {
    fn new() -> Self {
        Self {
            stack_frames: HashMap::new(),
            counter_names: HashMap::new(),
            trace: Trace::new(DisplayTimeUnit::Millisecond),
        }
    }

    fn register_header(&mut self, header: &FlightRecordHeader) {
        let metadata = self.trace.metadata_mut();

        metadata.push("request_id", header.request_id.clone());

        let build_info = header
            .sparrow_build_info
            .as_ref()
            .expect("Record missing build info");
        let mut build_info_args = Args::new();
        build_info_args.push("sparrow_version", build_info.sparrow_version.clone());
        build_info_args.push("compiler_version", build_info.compiler_version.clone());
        build_info_args.push("build_timestamp", build_info.build_timestamp.clone());
        build_info_args.push("build_profile", build_info.build_profile.clone());
        build_info_args.push("version_control", build_info.version_control.clone());
        metadata.push("build_info", build_info_args);
    }

    fn register_process(&mut self, pass_id: u32, name: &'static str) {
        self.trace.add_process_name(pass_id, name);
    }

    fn register_thread(&mut self, thread_id: &ThreadId, name: &'static str) {
        let tid = self.get_tid(thread_id);
        self.trace.add_thread_name(thread_id.pass_id, tid, name);

        // Register start/stop frames for each thread.
        self.register_stack_frame(thread_id.clone(), EventType::Start, 0, "Start", "", None);
        self.register_stack_frame(thread_id.clone(), EventType::Finish, 0, "Finish", "", None);
    }

    fn register_stack_frame(
        &mut self,
        thread_id: ThreadId,
        event_type: EventType,
        index: u32,
        name: &'static str,
        category: &'static str,
        parent: Option<StackFrameId>,
    ) -> StackFrameId {
        let stack_frame_id = self.trace.add_stack_frame(StackFrame {
            category,
            name,
            parent,
        });

        self.stack_frames.insert(
            StackFrameKey {
                thread_id,
                event_type,
                index,
            },
            stack_frame_id,
        );

        stack_frame_id
    }

    fn get_tid(&self, thread_id: &ThreadId) -> u32 {
        // `0`            - The merger thread.
        // `1`            - The executor thread.
        // `2 + n`        - The Nth source thread.
        // `u32::MAX - 1` - The Mth sink thread.
        // `u32::MAX`     - Unknown.

        match thread_id.kind() {
            ThreadKind::Merge => 0,
            ThreadKind::Execute => 1,
            ThreadKind::SourceReader => 2 + thread_id.thread_id,
            ThreadKind::SinkProcessor => u32::MAX - 1 - thread_id.thread_id,
            ThreadKind::Unknown => u32::MAX,
        }
    }

    /// Based on the plan, make sure everything is registered as needed.
    fn register_plan(&mut self, plan: &ComputePlan) {
        for pass in &plan.passes {
            let pass_id = pass.pass_id as u32;
            self.register_process(pass_id, intern(format!("Pass {}", pass.pass_id)));

            for (index, source) in pass.sources.iter().enumerate() {
                let source_thread = ThreadId {
                    pass_id,
                    kind: ThreadKind::SourceReader as i32,
                    thread_id: index as u32,
                };

                match &source.kind {
                    SourceKind::ScanTable { table_id, .. } => {
                        // TODO: Register the name of the table and stack frames
                        // for each source file.
                        self.register_thread(
                            &source_thread,
                            intern(format!("Scan Table {}", table_id)),
                        );
                        // TODO: Make the table plans a HashMap so this can be a lookup.
                        // let table = &plan.table_plans[table_id];
                        let table = plan
                            .table_plans
                            .iter()
                            .find(|t| t.id == *table_id)
                            .expect("table plan for ID");
                        let table_stack_frame = self.trace.add_stack_frame(StackFrame {
                            category: "INPUT",
                            name: intern(format!("Scan Table '{}'", table.name)),
                            parent: None,
                        });

                        let read = self.register_stack_frame(
                            source_thread.clone(),
                            EventType::InputReadTableFile,
                            0,
                            "Read Source File",
                            "INPUT",
                            Some(table_stack_frame),
                        );

                        self.counter_names.insert(
                            CounterKey::GatherSources(read),
                            intern(format!("Files for '{}'", table.name)),
                        );

                        self.register_stack_frame(
                            source_thread.clone(),
                            EventType::InputMergeTableBatches,
                            0,
                            "Merge Batches",
                            "INPUT",
                            Some(table_stack_frame),
                        );
                    }
                    SourceKind::CrossPass(_) => {
                        // Cross pass sources don't require any handling
                    }
                }
            }

            // Register the merger and the related operations.
            let merger_thread = ThreadId {
                pass_id,
                kind: ThreadKind::Merge as i32,
                thread_id: 0,
            };
            self.register_thread(&merger_thread, "Merger");

            // The merger is initiated by receiving an input.
            let merger_frame = self.register_stack_frame(
                merger_thread.clone(),
                EventType::MergeReceiveInput,
                0,
                "Receive Input",
                "MERGE",
                None,
            );
            let gather = self.register_stack_frame(
                merger_thread.clone(),
                EventType::MergeGatherInputs,
                0,
                "Gather Batches",
                "MERGE",
                Some(merger_frame),
            );

            self.counter_names
                .insert(CounterKey::GatherSources(gather), "Sources for pass");

            self.register_stack_frame(
                merger_thread.clone(),
                EventType::MergeMergeInputs,
                0,
                "Merge Inputs",
                "MERGE",
                Some(merger_frame),
            );

            // Register the executor thread and the related operations.
            let executor_thread = ThreadId {
                pass_id,
                kind: ThreadKind::Execute as i32,
                thread_id: 0,
            };
            self.register_thread(&executor_thread, "Executor");
            let executor_frame = self.register_stack_frame(
                executor_thread.clone(),
                EventType::ExecutePass,
                0,
                "Executor",
                "EXEC",
                None,
            );

            for (index, inst) in pass.insts.iter().enumerate() {
                self.register_stack_frame(
                    executor_thread.clone(),
                    EventType::ExecutePassInstruction,
                    index as u32,
                    // TODO: We could have an enum map of these names to avoid interning.
                    intern(format!("{} inst", inst.kind)),
                    "INST",
                    Some(executor_frame),
                );
            }

            for (index, sink) in pass.sinks.iter().enumerate() {
                // Create the frame for running the sink within the pass.
                let sink_id = self.register_stack_frame(
                    executor_thread.clone(),
                    EventType::ExecutePassSink,
                    index as u32,
                    // TODO: We could have an enum map of these names to avoid interning.
                    intern(format!("{} sink", sink.kind)),
                    "SINK",
                    Some(executor_frame),
                );

                // Also register the thread for processing the batches written to the sink if
                // there is one.

                let sink_thread = match &sink.kind {
                    SinkKind::Output => Some(("Write Output", "IO", EventType::SinkWriteResults)),
                    SinkKind::CrossPass(CrossPass::Buffer) => None,
                    SinkKind::CrossPass(CrossPass::LookupRequest) => Some((
                        "Lookup Request Adapter",
                        "SINK",
                        EventType::SinkLookupRequest,
                    )),
                    SinkKind::CrossPass(CrossPass::LookupResponse(_)) => Some((
                        "Lookup Response Adapter",
                        "SINK",
                        EventType::SinkLookupResponse,
                    )),
                    SinkKind::CrossPass(CrossPass::ShiftTo) => None,
                    SinkKind::CrossPass(CrossPass::ShiftUntil) => None,
                    SinkKind::CrossPass(CrossPass::WithKey) => None,
                };

                if let Some((label, category, event_type)) = sink_thread {
                    let sink_thread = ThreadId {
                        pass_id,
                        kind: ThreadKind::SinkProcessor as i32,
                        thread_id: index as u32,
                    };

                    self.register_thread(&sink_thread, label);

                    self.register_stack_frame(sink_thread, event_type, 0, label, category, None);
                }

                self.counter_names.insert(
                    CounterKey::SinkBufferedRows(sink_id),
                    intern(format!(
                        "Buffered rows for sink {:03} ({})",
                        index, sink.kind
                    )),
                );

                self.counter_names.insert(
                    CounterKey::SinkChannel(sink_id),
                    intern(format!(
                        "Output channel for sink {:03} ({})",
                        index, sink.kind
                    )),
                );
            }
        }
    }

    fn get_counter_name(&self, counter_key: CounterKey) -> &'static str {
        self.counter_names
            .get(&counter_key)
            .unwrap_or_else(|| panic!("Missing counter name for key {:?}", counter_key))
    }

    fn add_record(&mut self, record: FlightRecord) {
        // First, add the event for the record.

        let thread_id = record.thread_id.as_ref().expect("missing thread id");

        let process_id = thread_id.pass_id;

        // Handle the metrics first, since we may want to use those to compute the
        // arguments.
        let mut args = Args::new();

        let index = if record.event_type() == EventType::InputReadTableFile {
            // Use a single stack frame for all input read table file events.
            // Instead put the file index in the arguments.
            args.push("index", record.index);

            0
        } else {
            record.index
        };

        let key = StackFrameKey {
            thread_id: thread_id.clone(),
            event_type: record.event_type(),
            index,
        };
        let stack_frame_id = *self
            .stack_frames
            .get(&key)
            .unwrap_or_else(|| panic!("No stack frame registered for {:?}", key));
        let thread_id = self.get_tid(thread_id);

        let wall_timestamp_us = record.wall_timestamp_us;
        let name = self.trace.get_stack_frame(stack_frame_id).name;

        // Then, record any metrics related to the event.
        match &record.metrics {
            None => (),
            Some(Metrics::ExecutePass(metrics)) => {
                self.trace.add_event(
                    "input_rows",
                    wall_timestamp_us,
                    Event::Counter {
                        args: smallvec![("num_rows", metrics.num_rows as i64)].into(),
                    },
                    process_id,
                    thread_id,
                );

                self.trace.add_event(
                    "cumulative_entity_count",
                    wall_timestamp_us,
                    Event::Counter {
                        args: smallvec![("entities", metrics.num_cumulative_entities as i64)]
                            .into(),
                    },
                    process_id,
                    thread_id,
                );

                args.push("num_input_rows", metrics.num_rows);
                args.push("cumulative_entities", metrics.num_cumulative_entities);
                args.push(
                    "cause",
                    match metrics.cause() {
                        Cause::Unknown => "unknown",
                        Cause::Inputs => "input",
                        Cause::Ticks => "ticks",
                        Cause::Finish => "finish",
                    },
                );
            }
            Some(Metrics::ExecuteSink(metrics)) => {
                // Show a counter for the number of buffered rows in this sink.
                // TODO: Consider showing this as a stacked-bar across all sinks in the pass.
                self.trace.add_event(
                    self.get_counter_name(CounterKey::SinkBufferedRows(stack_frame_id)),
                    wall_timestamp_us,
                    Event::Counter {
                        args: smallvec![("num_buffered_rows", metrics.num_buffered_rows as i64)]
                            .into(),
                    },
                    process_id,
                    thread_id,
                );

                // Show a counter for the output channel (if there is one).
                if let Some(channel_stats) = &metrics.channel_stats {
                    let args = if let Some(capacity) = channel_stats.capacity {
                        smallvec![
                            ("used", channel_stats.len as i64),
                            ("free", (capacity - channel_stats.len) as i64)
                        ]
                        .into()
                    } else {
                        smallvec![("used", channel_stats.len as i64)].into()
                    };
                    self.trace.add_event(
                        self.get_counter_name(CounterKey::SinkChannel(stack_frame_id)),
                        wall_timestamp_us,
                        Event::Counter { args },
                        process_id,
                        thread_id,
                    );
                }

                args.push("num_buffered_rows", metrics.num_buffered_rows);
                args.push("num_output_rows", metrics.num_output_rows);
            }
            Some(Metrics::ReceiveBatch(metrics)) => {
                args.push("min_time", metrics.min_time);
                args.push("max_time", metrics.max_time);
                args.push("num_rows", metrics.num_rows);
            }
            Some(Metrics::GatherBatches(metrics)) => {
                // Show a stacked bar counter.
                // Height is total remaining sources, active/inactive are the two categories.
                self.trace.add_event(
                    self.get_counter_name(CounterKey::GatherSources(stack_frame_id)),
                    wall_timestamp_us,
                    Event::Counter {
                        args: smallvec![
                            ("active", metrics.active_sources as i64),
                            (
                                "inactive",
                                (metrics.remaining_sources - metrics.active_sources) as i64
                            )
                        ]
                        .into(),
                    },
                    process_id,
                    thread_id,
                );

                args.push("min_time", metrics.min_time);
                args.push("max_time", metrics.max_time);
                args.push("remaining_sources", metrics.remaining_sources);
                args.push("active_sources", metrics.active_sources)
            }
            Some(Metrics::MergeBatches(metrics)) => {
                // TODO: Show a stacked bar.
                // Height is total input rows. Each source is a separate category.
                let input_rows: u32 = metrics.num_input_rows.iter().copied().sum();
                args.push("input_rows", input_rows);
                args.push("output_rows", metrics.num_rows);
            }
            Some(unhandled) => {
                todo!("Handle or ignore metrics: {:?}", unhandled)
            }
        }

        // Then, report the actual event.
        let event = match record.event_type() {
            EventType::Start | EventType::Finish => Event::Instant {
                scope: EventScope::Thread,
                args,
            },
            _ => Event::Complete {
                wall_duration_us: record.wall_duration_us,
                thread_duration_us: record.cpu_duration_us,
                stack_frame: Some(stack_frame_id),
                end_stack_frame: None,
                args,
            },
        };
        self.trace
            .add_event(name, wall_timestamp_us, event, process_id, thread_id);
    }
}

/// Intern a string.
///
/// For simplicity, the Chrome Trace format structs operate on static strings.
/// This makes it easy to avoid copying strings excessively.
///
/// We take an easy approach -- just leak the string to make it `static`.
/// Since the conversion program is run on a single input and then terminates,
/// we aren't concerned about the potential memory leak.
fn intern(string: String) -> &'static str {
    Box::leak(string.into_boxed_str())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use sparrow_api::kaskada::v1alpha::PerEntityBehavior;
    use sparrow_core::ScalarValue;
    use sparrow_plan::{ComputePlan, Inst, InstKind, InstOp, PassPlan, ValueRef};
    use sparrow_qfr::kaskada::sparrow::v1alpha::flight_record::execute_pass_metrics::Cause;
    use sparrow_qfr::kaskada::sparrow::v1alpha::flight_record::{
        EventType, ExecutePassMetrics, Metrics,
    };
    use sparrow_qfr::kaskada::sparrow::v1alpha::flight_record_header::BuildInfo;
    use sparrow_qfr::kaskada::sparrow::v1alpha::thread_id::ThreadKind;
    use sparrow_qfr::kaskada::sparrow::v1alpha::{FlightRecord, ThreadId};

    use super::*;

    fn run_conversion(
        header: FlightRecordHeader,
        plan: &ComputePlan,
        records: Vec<FlightRecord>,
    ) -> Trace {
        let mut conversion = Conversion::new();
        conversion.register_header(&header);
        conversion.register_plan(plan);
        for record in records {
            conversion.add_record(record)
        }
        conversion.trace
    }

    #[test]
    fn test_conversion_empty_trace() {
        let header = FlightRecordHeader {
            version: 1,
            request_id: "request_id".to_owned(),
            sparrow_build_info: Some(BuildInfo {
                sparrow_version: "sparrow_version".to_owned(),
                compiler_version: "compiler_version".to_owned(),
                build_timestamp: "build_timestamp".to_owned(),
                build_profile: "build_profile".to_owned(),
                version_control: "version_control".to_owned(),
            }),
        };
        let plan = ComputePlan {
            passes: vec![],
            table_plans: vec![],
            per_entity_behavior: PerEntityBehavior::Unspecified,
        };
        let trace = run_conversion(header, &plan, vec![]);
        insta::assert_json_snapshot!(trace);
    }

    #[test]
    fn test_conversion_instruction_execution() {
        let header = FlightRecordHeader {
            version: 1,
            request_id: "request_id".to_owned(),
            sparrow_build_info: Some(BuildInfo {
                sparrow_version: "sparrow_version".to_owned(),
                compiler_version: "compiler_version".to_owned(),
                build_timestamp: "build_timestamp".to_owned(),
                build_profile: "build_profile".to_owned(),
                version_control: "version_control".to_owned(),
            }),
        };
        let plan = ComputePlan {
            passes: vec![Arc::new(PassPlan {
                grouping: "".to_owned(),
                pass_id: 0,
                sources: vec![],
                ticks: vec![],
                insts: vec![
                    Inst {
                        kind: InstKind::Simple(InstOp::Sum),
                        args: vec![
                            ValueRef::Literal(ScalarValue::Int64(Some(1))),
                            ValueRef::Literal(ScalarValue::Boolean(None)),
                            ValueRef::Literal(ScalarValue::Int64(None)),
                        ],
                    },
                    Inst {
                        kind: InstKind::Simple(InstOp::First),
                        args: vec![
                            ValueRef::Literal(ScalarValue::Int64(Some(1))),
                            ValueRef::Literal(ScalarValue::Boolean(None)),
                            ValueRef::Literal(ScalarValue::Int64(None)),
                        ],
                    },
                ],
                sinks: vec![],
            })],
            table_plans: vec![],
            per_entity_behavior: PerEntityBehavior::Unspecified,
        };
        let flight_records = vec![
            FlightRecord {
                thread_id: Some(ThreadId {
                    pass_id: 0,
                    kind: ThreadKind::Execute as i32,
                    thread_id: 0,
                }),
                event_type: EventType::ExecutePassInstruction as i32,
                index: 0,
                wall_timestamp_us: 1100,
                wall_duration_us: 700,
                cpu_duration_us: 650,
                metrics: None,
            },
            FlightRecord {
                thread_id: Some(ThreadId {
                    pass_id: 0,
                    kind: ThreadKind::Execute as i32,
                    thread_id: 0,
                }),
                event_type: EventType::ExecutePassInstruction as i32,
                index: 1,
                wall_timestamp_us: 1810,
                wall_duration_us: 680,
                cpu_duration_us: 600,
                metrics: None,
            },
            FlightRecord {
                thread_id: Some(ThreadId {
                    pass_id: 0,
                    kind: ThreadKind::Execute as i32,
                    thread_id: 0,
                }),
                event_type: EventType::ExecutePass as i32,
                index: 0,
                wall_timestamp_us: 1000,
                wall_duration_us: 1500,
                cpu_duration_us: 1400,
                metrics: Some(Metrics::ExecutePass(ExecutePassMetrics {
                    num_rows: 5000,
                    num_cumulative_entities: 4000,
                    cause: Cause::Inputs as i32,
                })),
            },
        ];
        let trace = run_conversion(header, &plan, flight_records);
        // We explicitly convert to a json string rather than using insta due
        // to their recent upgrade to them dropping serde as a dependency.
        // See https://github.com/mitsuhiko/insta/pull/265
        let json = serde_json::to_string(&trace).unwrap();
        let expected = r#"{"traceEvents":[{"name":"process_name","ph":"M","args":{"name":"Pass 0"},"ts":0,"pid":0,"tid":0},{"name":"thread_name","ph":"M","args":{"name":"Merger"},"ts":0,"pid":0,"tid":0},{"name":"thread_name","ph":"M","args":{"name":"Executor"},"ts":0,"pid":0,"tid":1},{"name":"sum inst","ph":"X","dur":700,"tdur":650,"sf":9,"ts":1100,"pid":0,"tid":1},{"name":"first inst","ph":"X","dur":680,"tdur":600,"sf":10,"ts":1810,"pid":0,"tid":1},{"name":"input_rows","ph":"C","args":{"num_rows":5000},"ts":1000,"pid":0,"tid":1},{"name":"cumulative_entity_count","ph":"C","args":{"entities":4000},"ts":1000,"pid":0,"tid":1},{"name":"Executor","ph":"X","dur":1500,"tdur":1400,"sf":8,"args":{"num_input_rows":5000,"cumulative_entities":4000,"cause":"input"},"ts":1000,"pid":0,"tid":1}],"stackFrames":{"1":{"category":"","name":"Start"},"2":{"category":"","name":"Finish"},"3":{"category":"MERGE","name":"Receive Input"},"4":{"category":"MERGE","name":"Gather Batches","parent":3},"5":{"category":"MERGE","name":"Merge Inputs","parent":3},"6":{"category":"","name":"Start"},"7":{"category":"","name":"Finish"},"8":{"category":"EXEC","name":"Executor"},"9":{"category":"INST","name":"sum inst","parent":8},"10":{"category":"INST","name":"first inst","parent":8}},"metadata":{"request_id":"request_id","build_info":{"sparrow_version":"sparrow_version","compiler_version":"compiler_version","build_timestamp":"build_timestamp","build_profile":"build_profile","version_control":"version_control"}}}"#;
        assert_eq!(json, expected);
    }
}
