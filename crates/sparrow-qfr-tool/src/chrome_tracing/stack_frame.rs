use serde::Serialize;

#[derive(Serialize, Eq, PartialEq, Debug)]
pub(crate) struct StackFrame {
    // The ID is omitted because it is included when serializing
    // the frames in a trace (as a map from ID to the remaining fields).
    #[serde(skip_serializing)]
    pub(crate) id: StackFrameId,
    #[serde(skip_serializing_if = "str::is_empty")]
    pub(crate) category: &'static str,
    pub(crate) name: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) parent: Option<StackFrameId>,
}

/// Stack Frame ID.
#[repr(transparent)]
#[derive(Serialize, Eq, PartialEq, Debug, Hash, Clone, Copy)]
#[serde(transparent)]
pub(crate) struct StackFrameId(pub u32);
