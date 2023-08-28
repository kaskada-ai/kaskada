use crate::chrome_tracing::{Arg, Args, DisplayTimeUnit, Event, StackFrame, StackFrameId, Trace};
use crate::error::Error;
use error_stack::ResultExt;
use fallible_iterator::FallibleIterator;
use hashbrown::HashMap;
use sparrow_qfr::kaskada::sparrow::v1alpha::flight_record::{
    Record, ReportActivity, ReportMetrics,
};
use sparrow_qfr::kaskada::sparrow::v1alpha::flight_record_header::{
    RegisterActivity, RegisterMetric,
};
use sparrow_qfr::kaskada::sparrow::v1alpha::{FlightRecord, FlightRecordHeader};

pub(super) struct Conversion {
    trace: Trace,
    activity_names: HashMap<StackFrameId, &'static str>,
    metric_names: HashMap<u32, &'static str>,
}

impl Conversion {
    #[allow(clippy::print_stdout)]
    pub fn new(header: &FlightRecordHeader) -> Self {
        if header.version != sparrow_qfr::QFR_VERSION {
            println!(
                "Flight Record Version ({}) may be incompatible with this tool version ({})",
                header.version,
                sparrow_qfr::QFR_VERSION
            );
        }

        let mut trace = Trace::new(DisplayTimeUnit::Millisecond);
        populate_metadata(&mut trace, header);
        let activity_names = populate_activities(&mut trace, &header.activities);
        let metric_names = populate_metrics(&mut trace, &header.metrics);

        Self {
            trace,
            activity_names,
            metric_names,
        }
    }

    fn convert(&mut self, record: FlightRecord) -> error_stack::Result<(), Error> {
        match record.record {
            Some(Record::RegisterThread(thread)) => {
                // We use processes instead of threads, since they are more feature-rich
                // as top-level containers in chrome traces (specifically, a process can
                // have separate metrics, while a thread cannot).
                //
                // TODO: Revisit?
                self.trace
                    .add_process_name(thread.thread_id, intern(thread.label));
                Ok(())
            }
            Some(Record::ReportActivity(activity)) => self.report_activity(activity),
            Some(Record::ReportMetrics(metrics)) => self.report_metrics(metrics),
            unsupported => error_stack::bail!(Error::UnsupportedRecord(unsupported)),
        }
    }

    fn report_activity(&mut self, activity: ReportActivity) -> error_stack::Result<(), Error> {
        let stack_frame_id = StackFrameId(activity.activity_id);
        let Some(name) = self.activity_names.get(&stack_frame_id) else {
            error_stack::bail!(Error::UndefinedActivityId)
        };

        let wall_timestamp_us = activity.wall_timestamp_us;

        let process_id = activity.thread_id;
        let thread_id = 0;

        let mut args = Args::new();
        for metric in activity.metrics {
            let Some(name) = self.metric_names.get(&metric.metric_id) else {
                error_stack::bail!(Error::UndefinedMetricId);
            };
            let value = match metric.value {
                Some(n) => Arg::from(n),
                None => error_stack::bail!(Error::MissingMetricValue),
            };

            args.push(name, value)
        }

        let event = Event::Complete {
            wall_duration_us: activity.wall_duration_us,
            thread_duration_us: activity.cpu_duration_us,
            stack_frame: Some(stack_frame_id),
            end_stack_frame: None,
            args,
        };
        self.trace
            .add_event(name, wall_timestamp_us, event, process_id, thread_id);

        Ok(())
    }

    fn report_metrics(&mut self, metrics: ReportMetrics) -> error_stack::Result<(), Error> {
        let wall_timestamp_us = metrics.wall_timestamp_us;
        let process_id = metrics.thread_id;
        let thread_id = 0;

        // Allow reporting "stacked" bar chart metrics.
        for metric in metrics.metrics {
            let Some(name) = self.metric_names.get(&metric.metric_id) else {
                error_stack::bail!(Error::UndefinedMetricId);
            };
            let value = match metric.value {
                Some(n) => Arg::from(n),
                None => error_stack::bail!(Error::MissingMetricValue),
            };
            self.trace.add_event(
                name,
                wall_timestamp_us,
                Event::Counter {
                    args: smallvec::smallvec![(*name, value)].into(),
                },
                process_id,
                thread_id,
            );
        }
        todo!()
    }

    /// Apply the conversion to all flight records in the given reader.
    ///
    /// Returns the final `Trace`.
    ///
    /// TODO: We should be able to do the JSON conversion in a streaming manner
    /// so that we don't need to create a single in-memory trace containing all
    /// the data.
    pub fn convert_all(
        mut self,
        mut records: impl FallibleIterator<
            Item = FlightRecord,
            Error = error_stack::Report<sparrow_qfr::io::reader::Error>,
        >,
    ) -> error_stack::Result<Trace, Error> {
        while let Some(record) = records.next().change_context(Error::Internal)? {
            self.convert(record)?;
        }

        Ok(self.trace)
    }
}

/// Add metadata to the trace.
fn populate_metadata(trace: &mut Trace, header: &FlightRecordHeader) {
    let metadata = trace.metadata_mut();

    metadata.push("request_id", header.request_id.clone());

    let build_info = header
        .sparrow_build_info
        .as_ref()
        .expect("Record missing build info");
    let mut build_info_args = Args::new();
    build_info_args.push("sparrow_version", build_info.sparrow_version.clone());
    build_info_args.push("github_ref", build_info.github_ref.clone());
    build_info_args.push("github_sha", build_info.github_sha.clone());
    build_info_args.push("github_workflow", build_info.github_workflow.clone());
    metadata.push("build_info", build_info_args);
}

fn populate_activities(
    trace: &mut Trace,
    activities: &[RegisterActivity],
) -> HashMap<StackFrameId, &'static str> {
    let mut names = HashMap::with_capacity(activities.len());
    for activity in activities {
        let id: StackFrameId = StackFrameId(activity.activity_id);
        let name: &'static str = intern(activity.label.clone());
        names.insert(id, name);

        trace.add_stack_frame(StackFrame {
            id,
            category: "",
            name,
            parent: activity.parent_activity_id.map(StackFrameId),
        });
    }
    names
}

fn populate_metrics(_trace: &mut Trace, metrics: &[RegisterMetric]) -> HashMap<u32, &'static str> {
    let mut names = HashMap::with_capacity(metrics.len());
    for metric in metrics {
        names.insert(metric.metric_id, intern(metric.label.clone()));
    }
    names
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
