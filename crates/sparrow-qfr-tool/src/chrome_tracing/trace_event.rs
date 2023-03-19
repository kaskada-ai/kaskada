use serde::Serialize;

use crate::chrome_tracing::args::Args;
use crate::chrome_tracing::categories::Categories;
use crate::chrome_tracing::stack_frame::StackFrameId;

#[derive(Serialize, PartialEq, Debug)]
pub(super) struct TraceEvent {
    /// The name of the event, as displayed in Trace Viewer.
    pub name: &'static str,
    /// The event categories.
    ///
    /// This is a comma separated list of categories for the event.
    /// The categories can be used to hide events in the Trace Viewer UI.
    #[serde(rename = "cat", skip_serializing_if = "Categories::is_empty")]
    pub categories: Categories,
    /// The event type.
    ///
    /// This is a single character which changes depending
    /// on the type of event.
    #[serde(flatten)]
    pub event: Event,
    /// The tracing clock timestamp of the event.
    ///
    /// This should correspond to the elapsed time.
    ///
    /// The timestamps are provided at microsecond granularity.
    #[serde(rename = "ts")]
    pub wall_timestamp_us: u64,
    /// The process ID of the process that output this event.
    #[serde(rename = "pid")]
    pub process_id: u32,
    /// The thread ID of the process that output this event.
    #[serde(rename = "tid")]
    pub thread_id: u32,
}

#[derive(Serialize, PartialEq, Debug)]
#[serde(tag = "ph")]
pub(crate) enum Event {
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
        args: Args,
    },
    /// Indicates an instant event with no associated duration.
    #[serde(rename = "i")]
    #[allow(dead_code)]
    Instant {
        /// The scope of the instant event.
        #[serde(rename = "s", skip_serializing_if = "EventScope::is_default")]
        scope: EventScope,
        #[serde(skip_serializing_if = "Args::is_empty")]
        args: Args,
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
    Metadata { args: Args },
    /// Adds a counter event.
    ///
    /// The names are scoped to the process. The thread ID is ignored.
    ///
    /// Each argument is a value for the metric.
    /// Multiple values are stacked at the point in time they occur.
    #[serde(rename = "C")]
    Counter { args: Args },

    /// Register the creation of an object with the given ID.
    #[serde(rename = "N")]
    #[allow(dead_code)]
    NewObject { id: &'static str },
    /// Register the deletion of an object with the given ID.
    #[serde(rename = "D")]
    #[allow(dead_code)]
    DestroyObject { id: &'static str },

    /// Register a snapshot of the given object.
    #[serde(rename = "O")]
    #[allow(dead_code)]
    SnapshotObject {
        id: &'static str,
        args: SnapshotArgs,
    },

    /// Register the given object ID as the context for the current thread.
    #[serde(rename = "(")]
    #[allow(dead_code)]
    ContextEnter { id: &'static str },

    /// Leave the given context for the current thread.
    #[serde(rename = ")")]
    #[allow(dead_code)]
    ContextLeave { id: &'static str },
    // TODO: Flow events to associate information between threads?
}

/// The scope of an instant event.
///
/// Controls how tall the the event will be.
#[derive(Serialize, Eq, PartialEq, Debug)]
#[allow(dead_code)]
pub(crate) enum EventScope {
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

#[derive(Serialize, PartialEq, Debug)]
pub(crate) struct SnapshotArgs {
    pub snapshot: Args,
}

#[cfg(test)]
mod tests {

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
