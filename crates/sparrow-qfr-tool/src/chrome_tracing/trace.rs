use std::fs::File;
use std::io::{BufWriter, Write};

use crate::chrome_tracing::args::{Arg, Args};
use crate::chrome_tracing::categories::Categories;
use crate::chrome_tracing::stack_frame::StackFrame;
use crate::chrome_tracing::trace_event::{Event, SnapshotArgs};
use crate::error::Error;

use super::trace_event::TraceEvent;
use error_stack::{IntoReport, ResultExt};
use hashbrown::HashMap;
use serde::ser::SerializeMap;
use serde::Serialize;
use smallvec::smallvec;

/// The outermost container for Chrome Tracing.
#[derive(Serialize, PartialEq, Debug)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Trace {
    trace_events: Vec<TraceEvent>,
    #[serde(default, skip_serializing_if = "DisplayTimeUnit::is_default")]
    display_time_unit: DisplayTimeUnit,
    #[serde(
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "serialize_stack_frames"
    )]
    stack_frames: Vec<StackFrame>,
    #[serde(flatten)]
    other_data: HashMap<&'static str, &'static str>,
    #[serde(skip_serializing_if = "Args::is_empty")]
    metadata: Args,
}

fn serialize_stack_frames<S>(frames: &[StackFrame], serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let mut map = serializer.serialize_map(Some(frames.len()))?;
    for frame in frames.iter() {
        map.serialize_entry(&frame.id, frame)?;
    }
    map.end()
}

impl Trace {
    /// Create a new Trace with the given time unit.
    pub fn new(display_time_unit: DisplayTimeUnit) -> Self {
        Self {
            trace_events: Vec::new(),
            display_time_unit,
            stack_frames: Vec::new(),
            other_data: HashMap::new(),
            metadata: Args::new(),
        }
    }

    pub fn metadata_mut(&mut self) -> &mut Args {
        &mut self.metadata
    }

    /// Add a stack frame to the trace.
    pub fn add_stack_frame(&mut self, frame: StackFrame) {
        self.stack_frames.push(frame);
    }

    /// Add a process name for the process
    pub fn add_process_name(&mut self, process_id: u32, name: &'static str) {
        self.trace_events.push(TraceEvent {
            name: "process_name",
            categories: Categories::default(),
            event: Event::Metadata {
                args: Args(smallvec!(("name", Arg::StaticString(name)))),
            },
            wall_timestamp_us: 0,
            process_id,
            thread_id: 0,
        })
    }

    #[allow(dead_code)]
    pub fn add_thread_name(&mut self, process_id: u32, thread_id: u32, name: &'static str) {
        self.trace_events.push(TraceEvent {
            name: "thread_name",
            categories: Categories::default(),
            event: Event::Metadata {
                args: Args(smallvec!(("name", Arg::StaticString(name)))),
            },
            wall_timestamp_us: 0,
            process_id,
            thread_id,
        })
    }

    pub fn add_event(
        &mut self,
        name: &'static str,
        wall_timestamp_us: u64,
        event: Event,
        process_id: u32,
        thread_id: u32,
    ) {
        self.trace_events.push(TraceEvent {
            name,
            categories: Categories::default(),
            event,
            wall_timestamp_us,
            process_id,
            thread_id,
        })
    }

    #[allow(dead_code)]
    pub fn add_object(
        &mut self,
        process_id: u32,
        name: &'static str,
        id: &'static str,
        args: Args,
    ) {
        self.trace_events.push(TraceEvent {
            name,
            categories: Categories::default(),
            event: Event::NewObject { id },
            wall_timestamp_us: 0,
            process_id,
            thread_id: 0,
        });

        self.trace_events.push(TraceEvent {
            name,
            categories: Categories::default(),
            event: Event::SnapshotObject {
                id,
                args: SnapshotArgs { snapshot: args },
            },
            wall_timestamp_us: 0,
            process_id,
            thread_id: 0,
        });
    }

    pub fn write_to(&self, path: &std::path::Path) -> error_stack::Result<(), Error> {
        let output = File::create(path)
            .into_report()
            .change_context(Error::Internal)?;
        let mut buffer = BufWriter::new(output);
        serde_json::to_writer(&mut buffer, self)
            .into_report()
            .change_context(Error::Internal)?;
        buffer
            .flush()
            .into_report()
            .change_context(Error::Internal)?;

        Ok(())
    }
}

/// Time unit to display on traces.
#[derive(Serialize, Eq, PartialEq, Debug)]
pub(crate) enum DisplayTimeUnit {
    #[serde(rename = "ms")]
    Millisecond,
    #[serde(rename = "ns")]
    #[allow(dead_code)]
    Nanosecond,
}

impl Default for DisplayTimeUnit {
    fn default() -> Self {
        Self::Millisecond
    }
}

impl DisplayTimeUnit {
    fn is_default(&self) -> bool {
        self == &Self::default()
    }
}
