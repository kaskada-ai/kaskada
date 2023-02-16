//! Contains structs and serializing corresponding to Chrome trace events.
//!
//! <https://aras-p.info/blog/2017/01/23/Chrome-Tracing-as-Profiler-Frontend/>
//!
//! <https://docs.google.com/document/d/1CvAClvFfyA5R-PhYUmn5OOQtYMH4h6I0nSsKchNAySU>

#![allow(unused)]

use std::marker::PhantomData;
use std::num::{NonZeroU32, NonZeroUsize};

use hashbrown::HashMap;
use itertools::Itertools;
use serde::de::Visitor;
use serde::ser::{SerializeMap, SerializeStruct};
use serde::Serialize;
use smallvec::{smallvec, Array, SmallVec};
use sparrow_qfr::Times;

/// The outermost container for Chrome Tracing.
#[derive(Serialize, Eq, PartialEq, Debug)]
#[serde(rename_all = "camelCase")]
pub(super) struct Trace {
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
    metadata: Args<Arg>,
}

fn serialize_stack_frames<S>(frames: &[StackFrame], serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let mut map = serializer.serialize_map(Some(frames.len()))?;
    for (index, frame) in frames.iter().enumerate() {
        map.serialize_entry(&(index + 1), frame)?;
    }
    map.end()
}

impl Trace {
    /// Create a new Trace with the given time unit.
    pub(super) fn new(display_time_unit: DisplayTimeUnit) -> Self {
        Self {
            trace_events: Vec::new(),
            display_time_unit,
            stack_frames: Vec::new(),
            other_data: HashMap::new(),
            metadata: Args::new(),
        }
    }

    pub fn metadata_mut(&mut self) -> &mut Args<Arg> {
        &mut self.metadata
    }

    /// Add a stack frame to the trace.
    pub fn add_stack_frame(&mut self, frame: StackFrame) -> StackFrameId {
        self.stack_frames.push(frame);
        let id = self.stack_frames.len() as u32;
        let id = NonZeroU32::new(id).expect("len should be non zero");
        StackFrameId(id)
    }

    pub fn get_stack_frame(&mut self, id: StackFrameId) -> &StackFrame {
        let index = id.0.get() - 1;
        &self.stack_frames[index as usize]
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

    pub fn add_object(
        &mut self,
        process_id: u32,
        name: &'static str,
        id: &'static str,
        args: Args<Arg>,
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
}

#[derive(Serialize, Eq, PartialEq, Debug)]
#[repr(transparent)]
pub(super) struct Category(&'static str);

impl From<&'static str> for Category {
    fn from(string: &'static str) -> Self {
        Category(string)
    }
}

#[derive(Eq, PartialEq, Debug, Default)]
#[repr(transparent)]
pub(super) struct Categories(SmallVec<[Category; 2]>);

impl Categories {
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl<T> From<T> for Categories
where
    T: IntoIterator<Item = &'static str>,
{
    fn from(items: T) -> Self {
        Categories(items.into_iter().map(Category).collect())
    }
}

impl Serialize for Categories {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_str(&self.0.iter().map(|cat| cat.0).format(","))
    }
}

#[derive(Eq, PartialEq, Debug)]
#[repr(transparent)]
pub(super) struct Args<T>(SmallVec<[(&'static str, T); 4]>);

impl<T> Args<T> {
    pub(super) fn new() -> Self {
        Args(SmallVec::new())
    }

    pub(super) fn with_capacity(n: usize) -> Self {
        Args(SmallVec::with_capacity(n))
    }

    pub(super) fn push(&mut self, name: &'static str, value: impl Into<T>) {
        self.0.push((name, value.into()))
    }

    pub(super) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

#[derive(PartialEq, Eq, Debug)]
pub(super) enum Arg {
    Unsigned(u64),
    Signed(i64),
    StaticString(&'static str),
    String(String),
    ObjectRef(&'static str),
    Nested(Box<Args<Arg>>),
}

impl Serialize for Arg {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Arg::Unsigned(n) => serializer.serialize_u64(*n),
            Arg::Signed(n) => serializer.serialize_i64(*n),
            Arg::StaticString(str) => serializer.serialize_str(str),
            Arg::String(str) => serializer.serialize_str(str),
            Arg::ObjectRef(object_id) => {
                let mut object_ref = serializer.serialize_struct("Reference", 1)?;
                object_ref.serialize_field("id_ref", &object_id)?;
                object_ref.end()
            }
            Arg::Nested(nested) => nested.serialize(serializer),
        }
    }
}

impl<T> From<SmallVec<[(&'static str, T); 4]>> for Args<T> {
    fn from(args: SmallVec<[(&'static str, T); 4]>) -> Self {
        Args(args)
    }
}

impl<T: Serialize> Serialize for Args<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_map(self.0.iter().map(|(k, v)| (k, v)))
    }
}

#[derive(Serialize, Eq, PartialEq, Debug)]
struct TraceEvent {
    /// The name of the event, as displayed in Trace Viewer.
    name: &'static str,
    /// The event categories.
    ///
    /// This is a comma separated list of categories for the event.
    /// The categories can be used to hide events in the Trace Viewer UI.
    #[serde(rename = "cat", skip_serializing_if = "Categories::is_empty")]
    categories: Categories,
    /// The event type.
    ///
    /// This is a single character which changes depending
    /// on the type of event.
    #[serde(flatten)]
    event: Event,
    /// The tracing clock timestamp of the event.
    ///
    /// This should correspond to the elapsed time.
    ///
    /// The timestamps are provided at microsecond granularity.
    #[serde(rename = "ts")]
    wall_timestamp_us: u64,
    /// The process ID of the process that output this event.
    #[serde(rename = "pid")]
    process_id: u32,
    /// The thread ID of the process that output this event.
    #[serde(rename = "tid")]
    thread_id: u32,
}

/// Stack Frame ID.
#[repr(transparent)]
#[derive(Serialize, Eq, PartialEq, Debug, Hash, Clone, Copy)]
#[serde(transparent)]
pub(super) struct StackFrameId(NonZeroU32);

#[derive(Serialize, Eq, PartialEq, Debug)]
#[serde(tag = "ph")]
pub(super) enum Event {
    /// Indicates a complete event (start and end). Can reduce size of stack
    /// frame.
    #[serde(rename = "X")]
    Complete {
        /// Duration of the complete event, in microseconds.
        ///
        /// This should measure elapsed time.
        #[serde(rename = "dur")]
        wall_duration_us: u64,
        /// Thread clock duration of the complete event, in microseconds.
        ///
        /// This should measure CPU time.
        #[serde(rename = "tdur")]
        thread_duration_us: u64,
        #[serde(rename = "sf", skip_serializing_if = "Option::is_none")]
        stack_frame: Option<StackFrameId>,
        #[serde(rename = "esf", skip_serializing_if = "Option::is_none")]
        end_stack_frame: Option<StackFrameId>,
        #[serde(skip_serializing_if = "Args::is_empty")]
        args: Args<Arg>,
    },
    /// Indicates an instant event with no associated duration.
    #[serde(rename = "i")]
    Instant {
        /// The scope of the instant event.
        #[serde(rename = "s", skip_serializing_if = "EventScope::is_default")]
        scope: EventScope,
        #[serde(skip_serializing_if = "Args::is_empty")]
        args: Args<Arg>,
    },
    /// Assign metadata to a specific process or thread.
    ///
    /// These events require using special names for the event.
    ///
    /// If the name is `process_name`, then the argument `name` assigns the
    /// process name.
    ///
    /// If the name is `thread_name`, then the argument `name` assigns the
    /// thread name.
    #[serde(rename = "M")]
    Metadata { args: Args<Arg> },
    /// Adds a counter event.
    ///
    /// The names are scoped to the process. The thread ID is ignored.
    ///
    /// Each argument is a value for the metric.
    /// Multiple values are stacked at the point in time they occur.
    #[serde(rename = "C")]
    Counter { args: Args<i64> },

    /// Register the creation of an object with the given ID.
    #[serde(rename = "N")]
    NewObject { id: &'static str },
    /// Register the deletion of an object with the given ID.
    #[serde(rename = "D")]
    DestroyObject { id: &'static str },

    /// Register a snapshot of the given object.
    #[serde(rename = "O")]
    SnapshotObject {
        id: &'static str,
        args: SnapshotArgs,
    },

    /// Register the given object ID as the context for the current thread.
    #[serde(rename = "(")]
    ContextEnter { id: &'static str },

    /// Leave the given context for the current thread.
    #[serde(rename = ")")]
    ContextLeave { id: &'static str },
    // TODO: Flow events to associate information between threads?
}

#[derive(Serialize, PartialEq, Eq, Debug)]
pub(super) struct SnapshotArgs {
    snapshot: Args<Arg>,
}

/// The scope of an instant event.
///
/// Controls how tall the the event will be.
#[derive(Serialize, Eq, PartialEq, Debug)]
pub(super) enum EventScope {
    /// The event will be drawn from the top to bottom of the timeline.
    #[serde(rename = "g")]
    Global,
    /// The event will be drawn through all threads of a given process.
    #[serde(rename = "p")]
    Process,
    /// The event will be the height of a single thread.
    #[serde(rename = "t")]
    Thread,
}

impl Default for EventScope {
    fn default() -> Self {
        EventScope::Thread
    }
}

impl EventScope {
    fn is_default(&self) -> bool {
        self == &Self::default()
    }
}

#[derive(Serialize, Eq, PartialEq, Debug)]
pub(super) enum DisplayTimeUnit {
    #[serde(rename = "ms")]
    Millisecond,
    #[serde(rename = "ns")]
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

#[derive(Serialize, Eq, PartialEq, Debug)]
pub(super) struct StackFrame {
    pub(super) category: &'static str,
    pub(super) name: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) parent: Option<StackFrameId>,
}

impl From<&'static str> for Arg {
    fn from(s: &'static str) -> Self {
        Self::StaticString(s)
    }
}

impl From<String> for Arg {
    fn from(s: String) -> Self {
        Self::String(s)
    }
}

impl From<u32> for Arg {
    fn from(n: u32) -> Self {
        Self::Unsigned(n as u64)
    }
}

impl From<i32> for Arg {
    fn from(n: i32) -> Self {
        Self::Signed(n as i64)
    }
}

impl From<u64> for Arg {
    fn from(n: u64) -> Self {
        Self::Unsigned(n)
    }
}

impl From<i64> for Arg {
    fn from(n: i64) -> Self {
        Self::Signed(n)
    }
}

impl From<Args<Arg>> for Arg {
    fn from(nested: Args<Arg>) -> Self {
        Self::Nested(Box::new(nested))
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;

    use smallvec::smallvec;

    use super::*;

    #[test]
    fn test_write_complete_event() {
        insta::assert_json_snapshot!(TraceEvent {
            name: "name",
            categories: Categories::default(),
            event: Event::Complete {
                wall_duration_us: 8736,
                thread_duration_us: 8730,
                stack_frame: None,
                end_stack_frame: None,
                args: Args::new(),
            },
            wall_timestamp_us: 57,
            process_id: 57,
            thread_id: 73,
        }, @r###"
        {
          "name": "name",
          "ph": "X",
          "dur": 8736,
          "tdur": 8730,
          "ts": 57,
          "pid": 57,
          "tid": 73
        }
        "###)
    }

    #[test]
    fn test_write_complete_event_with_categories() {
        insta::assert_json_snapshot!(TraceEvent {
            name: "name",
            categories: Categories::from(["apple", "banana"]),
            event: Event::Complete {
                wall_duration_us: 8736,
                thread_duration_us: 8730,
                stack_frame: None,
                end_stack_frame: None,
                args: Args::new(),
            },
            wall_timestamp_us: 57,
            process_id: 57,
            thread_id: 73,
        }, @r###"
        {
          "name": "name",
          "cat": "apple,banana",
          "ph": "X",
          "dur": 8736,
          "tdur": 8730,
          "ts": 57,
          "pid": 57,
          "tid": 73
        }
        "###)
    }

    // TODO: Add tests for the public API of trace.
    // TODO: Add a test with as stack frame.
    // TODO: Add test for counters, etc.
}
